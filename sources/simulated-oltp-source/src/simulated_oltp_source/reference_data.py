"""Reading and structural validation of the geographic/demographic reference file.

The Source does not talk to the Lakehouse: it reads three flat JSON files the platform
wrote with `retail-platform export-oltp-reference`. This module is the only entry point for
that input, and it distrusts the file: an export that is truncated, from an old schema, or
empty for a warehouse must FAIL the generation, never silently produce biased customers.

ORDER MATTERS. The position of each row in `address_candidates.json` is the
`candidate_index` the customer carries, and it is what makes lineage auditable down to the
exact tramo. That is why everything here is a list, in file order — never a `set` or a
`dict` rebuilt out of order, whose iteration depends on PYTHONHASHSEED and would break
reproducibility by seed.
"""

from __future__ import annotations

import json
import os

ADDRESS_CANDIDATES_FILE = "address_candidates.json"
POPULATION_WEIGHTS_FILE = "municipality_population_weights.json"
AGE_DISTRIBUTION_FILE = "province_age_distribution.json"

REFERENCE_FILES = (
    ADDRESS_CANDIDATES_FILE,
    POPULATION_WEIGHTS_FILE,
    AGE_DISTRIBUTION_FILE,
)

CANDIDATE_FIELDS = (
    "wh",
    "province_code",
    "municipality_code",
    # The Callejero renders the municipality name differently from table 29005 in 370 of
    # 370 cases in scope ("BRUC (EL)" vs "Bruc, El"). Both names coexist in the reference
    # under distinct labels; the customer carries the 29005 one, which is the readable one
    # and the same as warehouse_service_area_seed. The comparable key is the CODE, never
    # the name.
    "municipality_name_callejero",
    "street_name",
    "is_pseudo_address",
    "postal_code",
    "numbering_type",
)
WEIGHT_FIELDS = (
    "wh",
    "province_code",
    "province_name",
    "municipality_code",
    "municipality_name",
    "population_total",
    "proportion_within_wh",
    "sex_hombres_proportion",
    "sex_mujeres_proportion",
)
AGE_FIELDS = ("province_code", "age", "proportion")

# Headers the reference MUST carry since the customer base stopped being a projection of
# the resident population. Absence means an old-schema reference — generated before
# min_customer_age existed — and it cannot be used silently: it would again produce a base
# with newborn account holders, which is exactly the defect the cutoff exists to close.
# See CONTRACT.md section 2.
MIN_AGE_HEADER = "min_customer_age"
ALLOCATION_HEADER = "customer_allocation"
ALLOCATION_ROWS = "by_warehouse"


class ReferenceError(Exception):
    """Reference missing, unreadable, incomplete or inconsistent."""


def _load_file(directory: str, name: str) -> dict:
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        raise ReferenceError(
            f"reference file missing: {path}. Generate it with "
            f"`make oltp-export-reference` before extracting."
        )
    try:
        with open(path, "rb") as handle:
            payload = json.loads(handle.read().decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ReferenceError(f"{path}: invalid JSON ({exc})") from exc
    if not isinstance(payload, dict):
        raise ReferenceError(f"{path}: root is not a JSON object")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReferenceError(f"{path}: missing 'rows' list or list is empty")
    return payload


def _require_fields(path: str, rows: list, fields: tuple, limit: int = 200) -> None:
    """Check required fields. Samples the first rows rather than the whole file.

    `address_candidates.json` has hundreds of thousands of rows: scanning all of it on every
    run would cost more than it protects. A truncated or old-schema export gives itself away
    in the first rows, and the rest is covered when each field is used.
    """
    for position, row in enumerate(rows[:limit]):
        if not isinstance(row, dict):
            raise ReferenceError(f"{path}: row {position} is not a JSON object")
        missing = [field for field in fields if field not in row]
        if missing:
            raise ReferenceError(f"{path}: row {position} is missing field(s) {missing}")


class Reference:
    """Immutable index over the three files, ready for deterministic sampling."""

    def __init__(self, directory: str, candidates: dict, weights: dict, ages: dict):
        self.directory = directory
        self.callejero_ingestion_date = candidates.get("callejero_ingestion_date")
        self.population_ingestion_date = weights.get("population_ingestion_date")
        self.population_series_ingestion_date = ages.get("population_series_ingestion_date")
        self.population_year = weights.get("population_year")
        self.population_reference_date = weights.get("population_reference_date")
        self.age_year = ages.get("year")
        self.age_reference_date = ages.get("reference_date")
        self.age_fk_periodo = ages.get("fk_periodo")

        # THE MINIMUM AGE BELONGS TO THE REFERENCE, not to this Source. Who may be a customer
        # is decided by the platform, in customer_premises_seed; here it is read, recorded in
        # the manifest and RE-CHECKED — the delivered distribution cannot contain an age
        # below it.
        self.min_customer_age = ages.get(MIN_AGE_HEADER)
        allocation = weights.get(ALLOCATION_HEADER) or {}
        self.customer_allocation = allocation
        self.allocation_rule = allocation.get("rule")
        self.penetration_pct = allocation.get("penetration_pct")
        self.penetration_source = allocation.get("penetration_source")
        self.served_population = allocation.get("served_population")
        self.served_adult_population = allocation.get("served_adult_population")
        # Lookup dict, never iterated: insertion order does not influence any sampling, and
        # the ordered list it comes from is preserved in customer_allocation.
        self._targets = {
            row["wh"]: int(row["customers"])
            for row in allocation.get(ALLOCATION_ROWS, [])
            if isinstance(row, dict) and "wh" in row and "customers" in row
        }
        self.orphan_tramos_excluded = (candidates.get("excluded_rows") or {}).get(
            "no_street_or_pseudo_match"
        )

        self._candidates = candidates["rows"]

        # (wh, province_code, municipality_code) -> [candidate_index, ...], in file order.
        # We keep the INDEX, not the row: that is what goes into the customer.
        self._by_municipality: dict[tuple, list[int]] = {}
        for index, row in enumerate(self._candidates):
            key = (row["wh"], row["province_code"], row["municipality_code"])
            self._by_municipality.setdefault(key, []).append(index)

        self._weights: dict[str, list[dict]] = {}
        for row in weights["rows"]:
            self._weights.setdefault(row["wh"], []).append(row)

        self._ages: dict[str, tuple[list[int], list[float]]] = {}
        grouped: dict[str, list[dict]] = {}
        for row in ages["rows"]:
            grouped.setdefault(row["province_code"], []).append(row)
        for province, rows in grouped.items():
            self._ages[province] = (
                [int(row["age"]) for row in rows],
                [float(row["proportion"]) for row in rows],
            )

    def warehouses(self) -> list[str]:
        return sorted(self._weights)

    def municipalities(self, wh: str) -> list[dict]:
        rows = self._weights.get(wh)
        if not rows:
            raise ReferenceError(
                f"reference has no municipality for wh={wh!r}. "
                f"Available warehouses: {self.warehouses()}"
            )
        return rows

    def candidate(self, index: int) -> dict:
        return self._candidates[index]

    def candidate_count(self) -> int:
        return len(self._candidates)

    def candidates_of(self, wh: str, province_code: str, municipality_code: str) -> list[int]:
        indexes = self._by_municipality.get((wh, province_code, municipality_code))
        if not indexes:
            raise ReferenceError(
                f"reference has no address candidate for "
                f"wh={wh!r} province={province_code!r} municipality={municipality_code!r}"
            )
        return indexes

    def customer_target(self, wh: str) -> int:
        """How many customers this warehouse should have, according to the reference.

        It is what `extract` uses when `--count` is omitted. There is NO default: a guessed
        number here would produce a base sized by nobody, which is where the identical 5,000
        for FUAs that differ 4.6x in population used to come from.
        """
        target = self._targets.get(wh)
        if target is None:
            raise ReferenceError(
                f"reference has no customer target for wh={wh!r}. Warehouses with a target: "
                f"{sorted(self._targets)}. Regenerate the reference with "
                f"`make oltp-export-reference`, or pass --count explicitly."
            )
        if target < 1:
            raise ReferenceError(f"invalid customer target for wh={wh!r}: {target}")
        return target

    def ages_of(self, province_code: str) -> tuple[list[int], list[float]]:
        entry = self._ages.get(province_code)
        if not entry:
            raise ReferenceError(
                f"reference has no age distribution for province {province_code!r}"
            )
        return entry


def _require_customer_scope(directory: str, weights: dict, ages: dict) -> None:
    """The reference declares who may be a customer, and its content must obey it.

    THREE things, none of them optional:

      1. `min_customer_age` in the age distribution header. Without it, this Source has no
         way to tell "customer base distribution" from "whole population distribution", and
         would again generate the 18.01% of minors measured on 2026-08-31 — 3,602 of
         20,000, with ages starting from zero.
      2. No age below it in the rows. The header is the promise; this is the check.
         A reference truncated halfway would pass item 1 and fail here.
      3. `customer_allocation` in the weights header. Without it, `extract` without --count
         has no target, and falling back to a default would mean going back to a hand-typed
         number.

    An old-schema reference fails with an actionable message instead of generating
    silently — the same discipline as the rest of this module.
    """
    age_path = os.path.join(directory, AGE_DISTRIBUTION_FILE)
    minimum = ages.get(MIN_AGE_HEADER)
    if not isinstance(minimum, int):
        raise ReferenceError(
            f"{age_path}: no integer '{MIN_AGE_HEADER}' in the header. This reference was "
            f"generated before the customer base started excluding minors; regenerate with "
            f"`make oltp-export-reference`."
        )
    below = sorted(
        {int(row["age"]) for row in ages["rows"] if int(row["age"]) < minimum}
    )
    if below:
        raise ReferenceError(
            f"{age_path}: age(s) {below[:10]} below the {MIN_AGE_HEADER}={minimum} that the "
            f"header itself declares."
        )

    weights_path = os.path.join(directory, POPULATION_WEIGHTS_FILE)
    allocation = weights.get(ALLOCATION_HEADER)
    if not isinstance(allocation, dict) or not allocation.get(ALLOCATION_ROWS):
        raise ReferenceError(
            f"{weights_path}: no '{ALLOCATION_HEADER}.{ALLOCATION_ROWS}' in the header. That "
            f"is where each warehouse's customer count comes from when --count is omitted."
        )


def load(directory: str) -> Reference:
    """Read and validate the three reference files in a directory."""
    if not os.path.isdir(directory):
        raise ReferenceError(
            f"reference directory not found: {directory}. Generate it with "
            f"`make oltp-export-reference`."
        )
    candidates = _load_file(directory, ADDRESS_CANDIDATES_FILE)
    weights = _load_file(directory, POPULATION_WEIGHTS_FILE)
    ages = _load_file(directory, AGE_DISTRIBUTION_FILE)

    _require_fields(
        os.path.join(directory, ADDRESS_CANDIDATES_FILE), candidates["rows"], CANDIDATE_FIELDS
    )
    _require_fields(
        os.path.join(directory, POPULATION_WEIGHTS_FILE), weights["rows"], WEIGHT_FIELDS
    )
    _require_fields(
        os.path.join(directory, AGE_DISTRIBUTION_FILE), ages["rows"], AGE_FIELDS
    )
    _require_customer_scope(directory, weights, ages)
    return Reference(directory, candidates, weights, ages)
