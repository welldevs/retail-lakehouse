"""Reading and structural validation of the five reference files.

The Source does not talk to the Lakehouse: it reads five flat JSON files the platform wrote
with `retail-platform export-orders-reference`. This module is the only entry point for that
input, and it DISTRUSTS the file — a truncated export, an old schema, or one that is empty for
a warehouse must FAIL the generation, never silently produce biased orders.

ORDER MATTERS. Everything here is a list, in file order — never a `set` or a `dict` rebuilt out
of order, whose iteration depends on PYTHONHASHSEED and would break per-seed reproducibility.
The (warehouse, date) indexes are looked up by key, never iterated.

PRICE IS A STRING, AND STAYS A STRING
-------------------------------------
`unit_price` arrives as a string and becomes `decimal.Decimal`, never `float`. That is
obligation 4.4 of the Mercadona contract: converting currency to float loses decimal precision.
`Decimal` is stdlib, so the FROZEN boundary still stands.
"""

from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation

from .demand import DemandError, DemandModel

CUSTOMERS_FILE = "customers.json"
CATALOG_FILE = "catalog.json"
CALENDAR_FILE = "calendar.json"
PREMISES_FILE = "premises.json"
DEMAND_FILE = "demand_profile.json"

REFERENCE_FILES = (CUSTOMERS_FILE, CATALOG_FILE, CALENDAR_FILE, PREMISES_FILE, DEMAND_FILE)

CUSTOMER_FIELDS = (
    "customer_id",
    "wh",
    "province_code",
    "municipality_code",
    "postal_code",
    "birth_year",
    "first_ingestion_date",
)
CATALOG_FIELDS = (
    "wh",
    "price_as_of",
    "source_product_id",
    "display_name",
    "category_id",
    "subgroup_id",
    "unit_price",
    "tax_percentage",
    # Resolved BY THE PLATFORM against the versioned mapping. It arrives here as a label, and
    # the generator does not know — nor needs to know — which rule produced it.
    "demand_group",
)
CALENDAR_FIELDS = ("wh", "order_date", "price_as_of", "price_source")

PRICE_OBSERVED = "observed"
PRICE_CARRIED_FORWARD = "carried_forward"
PRICE_SOURCES = (PRICE_OBSERVED, PRICE_CARRIED_FORWARD)


class ReferenceError(Exception):
    """Missing, unreadable, incomplete or inconsistent reference."""


def _load_file(directory: str, name: str) -> dict:
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        raise ReferenceError(
            f"reference file missing: {path}. Generate it with "
            f"`make orders-export-reference` before extracting."
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
        raise ReferenceError(f"{path}: missing 'rows' list or empty list")
    return payload


def _require_fields(path: str, rows: list, fields: tuple, limit: int = 200) -> None:
    """Check required fields. Samples the first rows, not the whole file.

    `catalog.json` has tens of thousands of rows: scanning all of them on every run would cost
    more than it protects. A truncated or old-schema export gives itself away in the first
    rows, and the rest is covered when each field is used.
    """
    for position, row in enumerate(rows[:limit]):
        if not isinstance(row, dict):
            raise ReferenceError(f"{path}: row {position} is not a JSON object")
        missing = [field for field in fields if field not in row]
        if missing:
            raise ReferenceError(f"{path}: row {position} missing field(s) {missing}")


def _decimal(value, path: str, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ReferenceError(f"{path}: {field} is not a decimal: {value!r}") from exc


class Reference:
    """Immutable index over the four files, ready for deterministic sampling."""

    def __init__(self, directory: str, customers: dict, catalog: dict, calendar: dict,
                 premises: dict, demand: dict):
        self.directory = directory
        self.customer_ingestion_dates = list(customers.get("customer_ingestion_dates") or [])
        self.roster_ingestion_date = customers.get("roster_ingestion_date")
        self.catalog_ingestion_dates = list(catalog.get("catalog_ingestion_dates") or [])
        self.window_from = calendar.get("window_from")
        self.window_to = calendar.get("window_to")
        self.premises_sha256 = premises.get("seed_sha256")
        self.premises_seed_path = premises.get("seed_path")
        self.carried_forward_rows = calendar.get("carried_forward_rows")

        if not self.customer_ingestion_dates:
            raise ReferenceError(f"{CUSTOMERS_FILE}: no customer_ingestion_dates in the header")

        # wh -> [customer, ...] in file order. The POSITION is not carried into the event
        # (unlike Phase 1's candidate_index), but order still matters: it is what index-based
        # sampling walks, and it must be stable across runs.
        self._customers_by_wh: dict[str, list[dict]] = {}
        for row in customers["rows"]:
            self._customers_by_wh.setdefault(row["wh"], []).append(row)

        # (wh, price_as_of) -> [product, ...]. We keep the whole row: the generator needs
        # price, name, category and subgroup to build the basket and to substitute within
        # the same subgroup.
        self._catalog: dict[tuple, list[dict]] = {}
        for row in catalog["rows"]:
            row = dict(row)
            row["unit_price"] = _decimal(row["unit_price"], CATALOG_FILE, "unit_price")
            if row["unit_price"] <= 0:
                raise ReferenceError(
                    f"{CATALOG_FILE}: non-positive unit_price for "
                    f"{row.get('source_product_id')!r} in {row.get('wh')!r}"
                )
            self._catalog.setdefault((row["wh"], row["price_as_of"]), []).append(row)

        # (wh, price_as_of, subgroup_id) -> [position in that pair's catalog, ...]. Substitution
        # index: swapping a product for another of the SAME subgroup keeps the swap within the
        # assortment that warehouse actually sells on that day.
        self._by_subgroup: dict[tuple, list[int]] = {}
        self._by_category: dict[tuple, list[int]] = {}
        # (wh, price_as_of, demand_group) -> [position, ...], plus the SORTED TUPLE of groups
        # present in each pair. The tuple is sorted here, once, and not in the draw loop:
        # sorting by key is what keeps the CDF from depending on PYTHONHASHSEED.
        self._by_demand_group: dict[tuple, list[int]] = {}
        groups_by_pair: dict[tuple, set] = {}
        for key, products in self._catalog.items():
            for position, row in enumerate(products):
                self._by_subgroup.setdefault(key + (row["subgroup_id"],), []).append(position)
                self._by_category.setdefault(key + (row["category_id"],), []).append(position)
                group = row.get("demand_group")
                if not group:
                    raise ReferenceError(
                        f"{CATALOG_FILE}: product {row.get('source_product_id')!r} has no "
                        f"demand_group. The reference export must stamp it."
                    )
                self._by_demand_group.setdefault(key + (group,), []).append(position)
                groups_by_pair.setdefault(key, set()).add(group)
        self._demand_groups_of: dict[tuple, tuple] = {
            key: tuple(sorted(groups)) for key, groups in groups_by_pair.items()
        }

        # CDF per (pair, COHORT), computed once. Recomputing it per line would cost a scan of
        # dozens of groups per basket line, and the result would be identical.
        #
        # ONLY THE REACHABLE COHORTS. The region is fixed within a warehouse, so a
        # (warehouse, date) pair has at most as many cohorts as there are age bands — not the
        # cartesian product. Building all 16 combinations for each pair would spend memory on
        # vectors no draw for that warehouse can reach.
        self.demand = DemandModel(demand)
        self._demand_cdf: dict[tuple, tuple] = {}
        for key, groups in self._demand_groups_of.items():
            wh = key[0]
            try:
                region = self.demand.region_of(wh)
            except DemandError as exc:
                raise ReferenceError(f"{DEMAND_FILE}: {exc}") from exc
            for band, _ceiling in self.demand.age_bands:
                cohort = f"{band}|{region}"
                if cohort not in self.demand.cohort_weights:
                    raise ReferenceError(
                        f"{DEMAND_FILE}: warehouse {wh!r} may have a customer in band "
                        f"{band!r}, but cohort {cohort!r} has no weight vector."
                    )
                try:
                    self._demand_cdf[key + (cohort,)] = self.demand.cumulative(groups, cohort)
                except DemandError as exc:
                    raise ReferenceError(
                        f"{DEMAND_FILE}: {exc} (pair {key}, cohort {cohort})"
                    ) from exc

        # (wh, order_date) -> calendar row.
        self._calendar: dict[tuple, dict] = {}
        for row in calendar["rows"]:
            if row["price_source"] not in PRICE_SOURCES:
                raise ReferenceError(
                    f"{CALENDAR_FILE}: price_source outside the vocabulary: "
                    f"{row['price_source']!r}"
                )
            self._calendar[(row["wh"], row["order_date"])] = row

        self.premises = dict(premises.get("values") or {})
        if not self.premises:
            raise ReferenceError(f"{PREMISES_FILE}: missing the 'values' block")

    # -- queries -------------------------------------------------------------------

    def warehouses(self) -> list[str]:
        return sorted(self._customers_by_wh)

    def order_dates(self, wh: str) -> list[str]:
        return sorted(date for (w, date) in self._calendar if w == wh)

    def customers_of(self, wh: str) -> list[dict]:
        rows = self._customers_by_wh.get(wh)
        if not rows:
            raise ReferenceError(
                f"reference has no customer for wh={wh!r}. "
                f"Available warehouses: {self.warehouses()}"
            )
        return rows

    def calendar_of(self, wh: str, order_date: str) -> dict:
        row = self._calendar.get((wh, order_date))
        if row is None:
            raise ReferenceError(
                f"reference has no calendar row for wh={wh!r} day={order_date!r}. "
                f"The exported window was {self.window_from} to {self.window_to}."
            )
        return row

    def catalog_of(self, wh: str, price_as_of: str) -> list[dict]:
        rows = self._catalog.get((wh, price_as_of))
        if not rows:
            raise ReferenceError(
                f"reference has no catalog for wh={wh!r} price_as_of={price_as_of!r}"
            )
        return rows

    def demand_groups_of(self, wh: str, price_as_of: str) -> tuple:
        """Groups with products in that (warehouse, day), in stable order."""
        groups = self._demand_groups_of.get((wh, price_as_of))
        if not groups:
            raise ReferenceError(
                f"reference has no demand group for wh={wh!r} price_as_of={price_as_of!r}"
            )
        return groups

    def demand_cdf_of(self, wh: str, price_as_of: str, cohort: str) -> tuple:
        return self._demand_cdf[(wh, price_as_of, cohort)]

    def positions_in_demand_group(self, wh: str, price_as_of: str, group: str) -> list[int]:
        return self._by_demand_group.get((wh, price_as_of, group), [])

    def substitutes_in_subgroup(self, wh: str, price_as_of: str, subgroup_id) -> list[int]:
        return self._by_subgroup.get((wh, price_as_of, subgroup_id), [])

    def substitutes_in_category(self, wh: str, price_as_of: str, category_id) -> list[int]:
        return self._by_category.get((wh, price_as_of, category_id), [])

    def customer_version_at(self, first_ingestion_date: str, order_date: str) -> str | None:
        """Customer generation in force on the order date, or None if they did not exist yet.

        The base is append-only (growing preserves the first N byte for byte), so a customer
        present in one generation is present in all later ones; it is enough to take the
        largest date <= order_date that is not before their first appearance.
        """
        if order_date < first_ingestion_date:
            return None
        candidates = [d for d in self.customer_ingestion_dates if d <= order_date]
        return max(candidates) if candidates else None


def _load_plain(directory: str, name: str) -> dict:
    """Read a reference JSON that does NOT have the `rows` list (premises, demand profile)."""
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        raise ReferenceError(f"reference file missing: {path}")
    try:
        with open(path, "rb") as handle:
            payload = json.loads(handle.read().decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ReferenceError(f"{path}: invalid JSON ({exc})") from exc
    if not isinstance(payload, dict):
        raise ReferenceError(f"{path}: root is not a JSON object")
    return payload


def load(directory: str) -> Reference:
    """Read and validate the five reference files of a directory."""
    if not os.path.isdir(directory):
        raise ReferenceError(
            f"reference directory not found: {directory}. Generate it with "
            f"`make orders-export-reference`."
        )
    customers = _load_file(directory, CUSTOMERS_FILE)
    catalog = _load_file(directory, CATALOG_FILE)
    calendar = _load_file(directory, CALENDAR_FILE)

    premises = _load_plain(directory, PREMISES_FILE)
    demand = _load_plain(directory, DEMAND_FILE)

    _require_fields(os.path.join(directory, CUSTOMERS_FILE), customers["rows"], CUSTOMER_FIELDS)
    _require_fields(os.path.join(directory, CATALOG_FILE), catalog["rows"], CATALOG_FIELDS)
    _require_fields(os.path.join(directory, CALENDAR_FILE), calendar["rows"], CALENDAR_FIELDS)
    return Reference(directory, customers, catalog, calendar, premises, demand)
