"""Builds the demand profile that the Orders Source consumes.

WHY THIS MODULE EXISTS, AND WHY IT LIVES IN THE PLATFORM
--------------------------------------------------------
Same reason as `orders_reference.py`: the Source is FROZEN (`dependencies = []`, checked by
AST) and does not talk to the Lakehouse. The PLATFORM reads eight versioned seeds, resolves
the mapping against the observed catalog and writes ONE flat JSON; the Source reads it with
the stdlib `json` and draws from it. Neither side imports the other's code, and no
calibration rule lives inside the generator.

WHAT THIS MODULE CALIBRATES, AND WHAT IT DELIBERATELY DOES NOT
--------------------------------------------------------------
It calibrates: the PROBABILITY OF A DEMAND GROUP appearing on a basket line.
It does not calibrate: how many orders per day, how many lines per basket, how many units
per line, nor what price the product has. MAPA measures the resident's household
consumption — it does not measure online-store orders, baskets, or purchase cadence. Those
remain `synthetic` premises in `order_premises_seed.csv`, and that boundary is the most
important point of this phase.

THE CHAIN, WITH PRICE OUT OF THE DEMAND PATH
--------------------------------------------
    demand group       P(g)  <- MAPA VOLUME target (kg/L), tilted by channel
           |
    product in group         <- UNIFORM (no source measures turnover per SKU)
           |
    quantity                 <- w(k)=1/2^(k-1), unchanged
           |
    unit price               <- OBSERVED (Mercadona's purchasable_unit_price)
           |
    order value              <- consequence, never a goal

Price appears on no arrow pointing at demand. An expensive category can have low volume
and high revenue, and that becomes a RESULT of the model instead of a defect: in MAPA,
shellfish is 0.81% of volume and 2.88% of value.

THE BRIDGE BETWEEN A KG TARGET AND DRAWING LINES
------------------------------------------------
The MAPA target is a KG share; the draw picks LINES. The bridge is the OBSERVED mean
package size, never the price:

    P(group g)  proportional to   volume_target_g / mean_kg_per_line_g

A group whose typical product weighs 1 kg needs fewer lines to deliver the same volume
than a group whose typical product weighs 100 g. Without that division, "14% of volume in
fruit" would become "14% of lines in fruit", which is something else.

WHEN THE VOLUME TARGET CANNOT BE USED
-------------------------------------
`net_content_kg_l` is null when `reference_format` is neither mass nor volume (`ud`, `dz`).
A group whose coverage falls below `volume_coverage_min` gets NO volume target: the mean kg
would be computed over a small, biased sample of the group itself. It gets a LINE share
equal to the MAPA volume share — a declared fallback, reported separately in the reality
check, and it is what keeps HUEVOS (sold in 'ud') from coming in through a per-egg weight
that no source in this repo measures.

THE COHORT LAYER (mapa_2025_v2)
-------------------------------
Everything above describes the AGGREGATE, and still holds word for word. What v2 adds is
one floor above it: P(group) becomes P(group | customer cohort).

The cohort has two dimensions, and they are the ONLY two in which an observed customer
attribute coincides with a published cut of the report:

    age_band   <- the customer's birth_year, which came from the INE 31304 age distribution
    region     <- the customer's province_code, which came from the Callejero, mapped to CCAA

Household life cycle (9 types) and socioeconomic level (5 levels) are the report's richest
cuts and were left out: the customer has no household composition nor income, and
assigning them would be inventing the attribute — the same prohibition Phase 1 applied to
density per street section.

The affinity index takes numerator and denominator from the SAME chart, so it is
dimensionless and does not depend on our age pyramid having the shape of Spain's:

    idx(g, band)  =  volume_share(g, band) / population_share(band)
    raw_weight(g, cohort)  =  w_g  x  idx_age(g, band)  x  idx_region(g, ccaa)

THE MULTIPLICATION ASSUMES INDEPENDENCE between age and region. The report publishes both
marginals and never the cross-tab; it is a premise declared in `cohort_independence`, not a
measurement.

WHY THE INDEX CANNOT BE READ AS A SHARE. MAPA's `% Poblacion` is the population that LIVES
IN HOUSEHOLDS whose main shopper is in that band — a household with a 40-year-old shopper
carries the children into `35_49`. Our customer is an individual. That is why the number
only enters as a RELATIVE index between bands, and never as an absolute share.

AGGREGATE NEUTRALITY THROUGH IPF
--------------------------------
Without correction, the aggregate mix would drift from the v1 target just because our
cohort distribution is not MAPA's — the previous phase's calibration would be undone
sideways, with nothing failing. `ipf_calibrate` adjusts one factor per group until the mean
of the per-cohort weights, weighted by the real cohort distribution AMONG ORDERS,
reproduces `w_g` exactly.

That gives this layer a clean criterion: THE AGGREGATE DOES NOT MOVE. What changes is the
conditional, and that is where the effect should be looked for.
"""

from __future__ import annotations

import csv
import hashlib
import os
from decimal import Decimal

from . import product_choice

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")

BENCHMARK_SEED = "mapa_2025_benchmark_seed.csv"
MAPPING_SEED = "demand_category_mapping_seed.csv"
PROFILE_SEED = "demand_profile_seed.csv"
SEASONALITY_SEED = "demand_seasonality_seed.csv"
COHORT_AGE_SEED = "demand_cohort_age_seed.csv"
COHORT_REGION_SEED = "demand_cohort_region_seed.csv"
REGION_SEED = "mapa_2025_region_seed.csv"
CCAA_MAP_SEED = "ine_ccaa_map_seed.csv"

SEEDS = (
    BENCHMARK_SEED,
    MAPPING_SEED,
    PROFILE_SEED,
    SEASONALITY_SEED,
    COHORT_AGE_SEED,
    COHORT_REGION_SEED,
    REGION_SEED,
    CCAA_MAP_SEED,
)

# The report's four bands, verbatim, with the UPPER BOUND of each. The lower bound of the
# first does not live here: who may order is `min_buyer_age`, a premise of the orders
# domain, and mixing the two would make the band definition depend on an eligibility rule
# that has nothing to do with consumption.
AGE_BANDS = (
    ("LT35", 34),
    ("35_49", 49),
    ("50_64", 64),
    ("GE65", None),
)

# Code of the national average in `mapa_2025_region_seed`. It is NOT a region: it is the
# denominator the others are read against, and it must be told apart so it never enters as
# a servable community.
NATIONAL_CODE = "00"

# Tolerance of the two read checksums on the report. Wide enough for labels with one
# decimal place (four bands of 31.3 add up to 100.1 with nothing wrong), narrow enough to
# catch a swapped digit, which is the real defect.
COHORT_SUM_TOLERANCE = Decimal("0.5")

# Targets that exist in the mapping but are NOT MAPA categories. They are scope labels, and
# the code must tell them apart from a benchmark key so it does not look up a share for them.
NO_FOOD = "NO_FOOD"
SIN_BENCHMARK = "SIN_BENCHMARK"
SCOPE_LABELS = (NO_FOOD, SIN_BENCHMARK)

WILDCARD = "*"

REQUIRED_PARAMS = (
    "demand_model_version",
    "food_line_share",
    "benchmark_volume_coverage_pct",
    "unbenchmarked_allocation",
    "within_group_selection",
    "volume_coverage_min",
    "channel_reference_pct",
    "channel_fresh_pct",
    "channel_rest_pct",
    "cohort_dimensions",
    "cohort_independence",
    "cohort_calibration",
    "ipf_tolerance",
    "ipf_max_iterations",
    "region_frequency_basis",
    "region_frequency_normalization",
)


class DemandProfileError(Exception):
    """Missing seed, incomplete mapping or inconsistent weight."""


# --------------------------------------------------------------------------------
# 1. Reading the seeds
# --------------------------------------------------------------------------------

def _read_seed(seeds_dir: str, name: str) -> list[dict]:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise DemandProfileError(f"missing seed: {path}")
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise DemandProfileError(f"{path}: empty seed")
    return rows


def seeds_sha256(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """sha256 of each seed, for the manifest.

    Without this, "which profile generated this day" would be answered by the version —
    which someone may forget to bump when editing a weight. The hash does not forget.
    """
    digests = {}
    for name in SEEDS:
        path = os.path.join(seeds_dir, name)
        if not os.path.exists(path):
            raise DemandProfileError(f"missing seed: {path}")
        with open(path, "rb") as handle:
            digests[name] = hashlib.sha256(handle.read()).hexdigest()
    return digests


def _decimal_or_none(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except Exception as exc:  # noqa: BLE001 - the value came from a hand-edited CSV
        raise DemandProfileError(f"non-numeric value in seed: {value!r}") from exc


def load_benchmark(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> benchmark row, with the numbers already typed."""
    benchmark = {}
    for row in _read_seed(seeds_dir, BENCHMARK_SEED):
        key = row["mapa_key"].strip()
        if key in benchmark:
            raise DemandProfileError(f"repeated mapa_key in the benchmark: {key}")
        benchmark[key] = {
            "mapa_key": key,
            "mapa_label": row["mapa_label"].strip(),
            "scope": row["scope"].strip(),
            "use_as_weight": row["use_as_weight"].strip().lower() == "true",
            "volume_share_pct": _decimal_or_none(row["volume_share_pct"]),
            "value_share_pct": _decimal_or_none(row["value_share_pct"]),
            "avg_price_eur_kg": _decimal_or_none(row["avg_price_eur_kg"]),
            "ecommerce_volume_pct": _decimal_or_none(row["ecommerce_volume_pct"]),
            "channel_basis": row["channel_basis"].strip(),
            "informe_section": row["informe_section"].strip(),
            "provenance": row["provenance"].strip(),
        }
    for key in SCOPE_LABELS:
        if key not in benchmark:
            raise DemandProfileError(
                f"the benchmark must declare the scope row {key}, even without numbers: "
                f"that is where it is written why that target exists"
            )
    missing = [
        row["mapa_key"] for row in benchmark.values()
        if row["use_as_weight"] and row["volume_share_pct"] is None
    ]
    if missing:
        raise DemandProfileError(
            f"keys marked use_as_weight without volume_share_pct: {missing}. "
            f"A weight without a target would silently become zero."
        )
    return benchmark


def load_mapping(seeds_dir: str = DEFAULT_SEEDS_DIR) -> list[dict]:
    return [
        {
            "l1": row["l1"].strip(),
            "l2": row["l2"].strip(),
            "l3": row["l3"].strip(),
            "mapa_key": row["mapa_key"].strip(),
        }
        for row in _read_seed(seeds_dir, MAPPING_SEED)
    ]


def load_params(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    params = {row["param_key"].strip(): row["value"].strip()
              for row in _read_seed(seeds_dir, PROFILE_SEED)}
    missing = [k for k in REQUIRED_PARAMS if k not in params]
    if missing:
        raise DemandProfileError(f"demand_profile_seed is missing parameter(s) {missing}")
    return params


def load_seasonality(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """month -> factor on the ORDER RATE, not on the mix.

    The distinction matters and comes from the evidence: the report publishes monthly
    spend for TOTAL food (five months, in prose) and does NOT publish a monthly profile per
    category — the monthly charts are images. A global factor on the mix would normalize
    away and do nothing; on the order rate it does exactly what the evidence supports.
    """
    factors = {}
    for row in _read_seed(seeds_dir, SEASONALITY_SEED):
        month = int(row["month"])
        if not 1 <= month <= 12:
            raise DemandProfileError(f"month outside 1..12 in the seasonal profile: {month}")
        if month in factors:
            raise DemandProfileError(f"repeated month in the seasonal profile: {month}")
        factor = _decimal_or_none(row["factor"])
        if factor is None or factor <= 0:
            raise DemandProfileError(f"seasonal factor missing or non-positive in month {month}")
        factors[month] = factor
    if len(factors) != 12:
        raise DemandProfileError(
            f"the seasonal profile needs all 12 months; got {sorted(factors)}"
        )
    return factors


def _load_cohort(seeds_dir: str, name: str, key_field: str) -> dict:
    """Reads a cohort seed and returns mapa_key -> {cohort key: (population, volume)}.

    THE CHECK LIVES HERE, and not only in the test: a number misread from one of the
    report's charts is the only realistic kind of error in this table, and it breaks the
    sum. Letting the read pass and checking three layers later would mean finding the
    defect after it had already calibrated demand.
    """
    out: dict[str, dict] = {}
    for row in _read_seed(seeds_dir, name):
        key = row["mapa_key"].strip()
        cohort = row[key_field].strip()
        population = _decimal_or_none(row["population_share_pct"])
        volume = _decimal_or_none(row["volume_share_pct"])
        if population is None or volume is None:
            raise DemandProfileError(
                f"{name}: {key}/{cohort} without a population or volume share. An empty "
                f"cell would become index zero, which is a strong and undeclared claim."
            )
        if population <= 0:
            raise DemandProfileError(
                f"{name}: {key}/{cohort} has population {population}; the index would divide by zero"
            )
        target = out.setdefault(key, {})
        if cohort in target:
            raise DemandProfileError(f"{name}: repeated row in {key}/{cohort}")
        target[cohort] = (population, volume)
    return out


def load_cohort_age(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> age band -> (population share, volume share).

    Fails if a group does not bring all FOUR bands, or if the volume shares do not add up
    to 100. The four bands are a partition of the universe: if they do not add up, either a
    row is missing or a digit was misread — and both would produce a plausible index that
    would never fail on its own.
    """
    bands = tuple(name for name, _ in AGE_BANDS)
    data = _load_cohort(seeds_dir, COHORT_AGE_SEED, "age_band")
    for key, by_band in sorted(data.items()):
        missing = [b for b in bands if b not in by_band]
        if missing:
            raise DemandProfileError(
                f"{COHORT_AGE_SEED}: {key} is missing band(s) {missing}. The four bands "
                f"are a partition; missing one is not partial coverage, it is a wrong total."
            )
        for label, index in (("volume", 1), ("population", 0)):
            weight_sum = sum(by_band[b][index] for b in bands)
            if abs(weight_sum - Decimal("100")) > COHORT_SUM_TOLERANCE:
                raise DemandProfileError(
                    f"{COHORT_AGE_SEED}: {key} sums {weight_sum} of {label} across the four "
                    f"bands, not 100. A label of the report was misread."
                )
    return data


def load_cohort_region(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> ccaa_code -> (population share, volume share).

    NO SUM CHECKSUM, and the difference from age is real: this seed only carries the served
    communities, which are 4 of 17, so there is no partition to close. What can be checked
    is that each community's POPULATION share is the same in every group — it is the
    universe, not a measurement of the category — and `assert_cohort_seeds` does that.
    """
    return _load_cohort(seeds_dir, COHORT_REGION_SEED, "ccaa_code")


def load_region_reference(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """ccaa_code -> {label, per-capita consumption, per-capita spend}. Includes the national average."""
    regions = {}
    for row in _read_seed(seeds_dir, REGION_SEED):
        code = row["ccaa_code"].strip()
        if code in regions:
            raise DemandProfileError(f"{REGION_SEED}: repeated ccaa_code {code}")
        per_capita = _decimal_or_none(row["per_capita_kg_l"])
        if per_capita is None or per_capita <= 0:
            raise DemandProfileError(
                f"{REGION_SEED}: {code} without a positive per-capita consumption"
            )
        regions[code] = {
            "ccaa_code": code,
            "ccaa_label": row["ccaa_label"].strip(),
            "per_capita_kg_l": per_capita,
            "per_capita_eur": _decimal_or_none(row["per_capita_eur"]),
            "informe_section": row["informe_section"].strip(),
        }
    if NATIONAL_CODE not in regions:
        raise DemandProfileError(
            f"{REGION_SEED} lacks the {NATIONAL_CODE} row (national average). It is not a "
            f"region, it is the denominator the others are read against."
        )
    return regions


def load_province_ccaa(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """province_code -> ccaa_code. No default target, for the same reason as the mapping."""
    province_map = {}
    for row in _read_seed(seeds_dir, CCAA_MAP_SEED):
        province = row["province_code"].strip()
        if province in province_map:
            raise DemandProfileError(f"{CCAA_MAP_SEED}: repeated province {province}")
        province_map[province] = row["ccaa_code"].strip()
    return province_map


def ccaa_of(province_ccaa: dict, province_code: str) -> str:
    """The community of a province, or an error.

    NO DEFAULT. A new province falling into a default community would silently inherit
    another region's consumption profile — and the resulting mix would be plausible, which
    is exactly the kind of defect this repo treats as the most expensive.
    """
    code = (province_code or "").strip()
    if code not in province_ccaa:
        raise DemandProfileError(
            f"province {code!r} is not in {CCAA_MAP_SEED}. Add the row — there is no "
            f"default community."
        )
    return province_ccaa[code]


def age_band_of(age: int) -> str:
    """The report band for an age IN COMPLETED YEARS on the order day."""
    for name, ceiling in AGE_BANDS:
        if ceiling is None or age <= ceiling:
            return name
    raise DemandProfileError(f"age {age!r} falls in no band")


# --------------------------------------------------------------------------------
# 2. Mapping resolution
# --------------------------------------------------------------------------------

def _specificity(rule: dict) -> int:
    """The more specific the rule, the larger the number. l3 weighs more than l2, which weighs more than l1.

    Weights 4/2/1 and not 3/2/1: with powers of two no combination of levels ties with
    another, so "more specific" is never ambiguous by sum.
    """
    return ((1 if rule["l1"] != WILDCARD else 0)
            + (2 if rule["l2"] != WILDCARD else 0)
            + (4 if rule["l3"] != WILDCARD else 0))


def _matches(rule: dict, l1: str, l2: str, l3: str) -> bool:
    return ((rule["l1"] == WILDCARD or rule["l1"] == l1)
            and (rule["l2"] == WILDCARD or rule["l2"] == l2)
            and (rule["l3"] == WILDCARD or rule["l3"] == l3))


def resolve(mapping: list[dict], l1: str, l2: str, l3: str) -> str:
    """The demand group key for a catalog triple.

    NO DEFAULT. A triple without a rule raises instead of falling into a silent bucket: a
    default here would be an undeclared demand premise, which is exactly what the seeds
    exist to prevent. Two equally specific rules also raise — picking the first would make
    the result depend on the file order.
    """
    candidates = [r for r in mapping if _matches(r, l1, l2, l3)]
    if not candidates:
        raise DemandProfileError(
            f"no mapping rule matches ({l1!r}, {l2!r}, {l3!r}). "
            f"Add the rule to {MAPPING_SEED} — there is no default target."
        )
    best = max(_specificity(r) for r in candidates)
    winners = [r for r in candidates if _specificity(r) == best]
    if len(winners) > 1:
        raise DemandProfileError(
            f"({l1!r}, {l2!r}, {l3!r}) matches {len(winners)} equally specific "
            f"rules: {[r['mapa_key'] for r in winners]}"
        )
    return winners[0]["mapa_key"]


def unused_rules(mapping: list[dict], triples: list[tuple]) -> list[dict]:
    """Rules that win no catalog triple.

    A dead rule breaks nothing today, and that is why it is dangerous: it documents a
    decision that is not in force. Whoever reads the seed will believe it.
    """
    winners = set()
    for l1, l2, l3 in triples:
        candidates = [(i, r) for i, r in enumerate(mapping) if _matches(r, l1, l2, l3)]
        if not candidates:
            continue
        best = max(_specificity(r) for _, r in candidates)
        for i, r in candidates:
            if _specificity(r) == best:
                winners.add(i)
    return [r for i, r in enumerate(mapping) if i not in winners]


# --------------------------------------------------------------------------------
# 3. Channel tilt
# --------------------------------------------------------------------------------

def channel_tilt(row: dict, params: dict) -> tuple[Decimal, str]:
    """Factor that takes the HOUSEHOLD CONSUMPTION share to the E-COMMERCE share.

    MAPA measures the resident's consumption, not the online order. Treating one as the
    other would be the error this whole phase exists to avoid. The report gives three
    channel granularities, and this function uses the best available for each group, always
    declaring which one:

      fine   e-commerce share published for this group          (18 blocks of the report)
      group  published for the parent group (meats inherit from CARNES)
      coarse only the fresh 1.1% / rest 2.8% cut over the 2.2% total

    There is no fourth option: inventing a percentage for a group with no channel row would
    be exactly what §3 of the plan forbade.
    """
    reference = Decimal(params["channel_reference_pct"])
    basis = row.get("channel_basis") or "coarse"
    ecom = row.get("ecommerce_volume_pct")
    if basis in ("fine", "group") and ecom is not None:
        return ecom / reference, basis
    fresh = Decimal(params["channel_fresh_pct"])
    rest = Decimal(params["channel_rest_pct"])
    base = fresh if row.get("scope") == "fresh" else rest
    return base / reference, "coarse"


# --------------------------------------------------------------------------------
# 3b. Propensity by cohort
# --------------------------------------------------------------------------------

ONE = Decimal("1")


def affinity_index(population_share: Decimal, volume_share: Decimal) -> Decimal:
    """How much a cohort buys of a group, relative to what its size would lead one to expect.

    Numerator and denominator come from the SAME chart of the report, so the quotient is
    dimensionless: it does not depend on our age pyramid having the shape of Spain's, and
    so it survives the denominator difference described in the module header.

    1.0 means "buys in proportion to its size". There is no artificial ceiling or floor:
    HUEVOS is 0.74 among under-35s and AGUA is 0.31 in Madrid, and both are measurements,
    not noise to be trimmed.
    """
    return volume_share / population_share


def region_frequency(
    region_reference: dict,
    served: list[str],
    weights: dict | None = None,
) -> dict:
    """ccaa_code -> frequency index, renormalized over the SERVED communities.

    The report publishes per-capita consumption by community (section 3). Catalonia
    consumes 620.82 kg or litres per person per year against Madrid's 505.86 — 22.7% more,
    measured.

    TWO DECISIONS, both declared in `demand_profile_seed`:

    1. Regional intensity becomes order FREQUENCY, not basket size. The report gives kg per
       year and does not publish household purchase frequency ('actos de compra' only
       appears in the out-of-home chapter), so splitting the intensity between frequency
       and basket would be inventing the split. Choosing one and declaring it is honest;
       choosing without declaring would not be.

    2. The renormalization is over the SERVED communities, weighted by the size of each
       one's base, and not against the national average of 577.32. The national average
       includes regions we do not serve: using it would make the total order count drop a
       few percent without that meaning anything. This way the weighted sum of the indices
       is exactly 1, the order total stays where it was, and the phase only changes the
       SPLIT between warehouses.
    """
    if not served:
        raise DemandProfileError("no served community: nothing to renormalize")
    missing = sorted(c for c in set(served) if c not in region_reference)
    if missing:
        raise DemandProfileError(
            f"{REGION_SEED} does not declare per-capita consumption for {missing}"
        )
    served_weights = {c: Decimal(1) for c in set(served)} if not weights else {
        c: Decimal(weights.get(c, 0)) for c in set(served)
    }
    mass = sum(served_weights.values(), Decimal("0"))
    if mass <= 0:
        raise DemandProfileError("the served communities add up to zero weight")
    mean = sum(
        served_weights[c] * region_reference[c]["per_capita_kg_l"] for c in sorted(served_weights)
    ) / mass
    if mean <= 0:
        raise DemandProfileError("non-positive mean per-capita consumption")
    return {c: region_reference[c]["per_capita_kg_l"] / mean for c in sorted(served_weights)}


def ipf_calibrate(
    base_weights: dict,
    index: dict,
    cohort_mass: dict,
    tolerance: Decimal,
    max_iterations: int,
) -> tuple[dict, int, Decimal]:
    """Adjusts one factor per group until the per-cohort mean reproduces the aggregate weight.

    THE PROBLEM THIS SOLVES. Without correction,

        p(g | c)  =  w_g . i(g,c) / SUM_h w_h . i(h,c)

    and the mean `SUM_c m_c . p(g|c)` does NOT return to `w_g`, because the denominator
    varies from cohort to cohort. The deviation is not noise: it shifts the previous phase's
    aggregate calibration with nothing failing, and a shifted mix still looks plausible.

    THE CORRECTION. One factor `f_g` per group, adjusted by proportional iteration:

        p(g | c)  =  w_g . f_g . i(g,c) / SUM_h w_h . f_h . i(h,c)
        a_g       =  SUM_c m_c . p(g|c)
        f_g       <- f_g . w_g / a_g

    Converges for a positive matrix and returns `f`, the number of iterations and the
    largest remaining deviation. With an index matrix all equal to 1.0 the first iteration
    already closes and `f` stays at 1 — IPF is the identity when there is nothing to
    correct, and a test proves it.

    Iteration order ALWAYS by sorted key: Decimal addition is not associative in the last
    digit, and iterating a dict would make the result depend on PYTHONHASHSEED.
    """
    groups = tuple(sorted(base_weights))
    cohorts = tuple(sorted(cohort_mass))
    if not groups:
        raise DemandProfileError("IPF with no group at all")
    if not cohorts:
        raise DemandProfileError("IPF with no cohort at all")

    factor = {g: ONE for g in groups}
    worst = None
    for iteration in range(1, max_iterations + 1):
        aggregate = {g: Decimal("0") for g in groups}
        for c in cohorts:
            raw = {g: base_weights[g] * factor[g] * index[c][g] for g in groups}
            total = sum(raw[g] for g in groups)
            if total <= 0:
                raise DemandProfileError(
                    f"cohort {c!r} sums zero weight over {len(groups)} groups"
                )
            mass = cohort_mass[c]
            for g in groups:
                aggregate[g] += mass * raw[g] / total
        worst = max(abs(aggregate[g] - base_weights[g]) for g in groups)
        if worst <= tolerance:
            return factor, iteration, worst
        for g in groups:
            if aggregate[g] <= 0:
                raise DemandProfileError(
                    f"group {g!r} ended up with zero aggregate mass during IPF"
                )
            factor[g] = factor[g] * base_weights[g] / aggregate[g]

    raise DemandProfileError(
        f"IPF did not converge in {max_iterations} iterations; largest deviation {worst}. "
        f"Accepting the matrix as is would undo the previous version's aggregate calibration."
    )


# --------------------------------------------------------------------------------
# 4. Building the profile
# --------------------------------------------------------------------------------

def _mean(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values))


def _content(value) -> Decimal | None:
    """`net_content_kg_l` as a Decimal, or None when there is no usable content.

    Accepts None, empty string and NaN. NaN is not paranoia: a pandas DataFrame returns NaN
    for null, and `Decimal('nan')` does NOT raise — it builds a NaN that survives
    arithmetic and silently poisons the mean, which is worse than the error.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "<na>"):
        return None
    try:
        number = Decimal(text)
    except Exception:  # noqa: BLE001 - value coming from a CSV or a driver
        return None
    if not number.is_finite() or number <= 0:
        return None
    return number


def cohort_key(age_band: str, ccaa_code: str) -> str:
    """The text key of a cohort. A single function, used on both sides of the boundary."""
    return f"{age_band}|{ccaa_code}"


def _build_cohorts(
    base_weights: dict,
    group_blocks: dict,
    customers_by_cohort: dict,
    warehouse_regions: dict,
    params: dict,
    seeds_dir: str,
) -> dict:
    """The cohort layer over the aggregate weights, already calibrated by IPF.

    `customers_by_cohort` counts the ELIGIBLE customers of each (band, community) —
    eligibility belongs to the orders domain (`min_buyer_age`) and arrives resolved.
    Counting here those who cannot order would bias the cohort mass and, in turn, the IPF.
    """
    age = load_cohort_age(seeds_dir)
    region = load_cohort_region(seeds_dir)
    region_ref = load_region_reference(seeds_dir)

    if not warehouse_regions:
        raise DemandProfileError("no warehouse has a resolved autonomous community")
    served = sorted(set(warehouse_regions.values()))
    if NATIONAL_CODE in served:
        raise DemandProfileError(
            f"{NATIONAL_CODE} is the national average, not a servable community"
        )

    # --- 1. frequency by community, weighted by each one's eligible base ---------------
    by_ccaa: dict[str, Decimal] = {}
    for (_band, ccaa), count in customers_by_cohort.items():
        by_ccaa[ccaa] = by_ccaa.get(ccaa, Decimal("0")) + Decimal(count)
    orphans = sorted(c for c in by_ccaa if c not in served)
    if orphans:
        raise DemandProfileError(
            f"there are customers in community(ies) no warehouse serves: {orphans}"
        )
    frequency = region_frequency(region_ref, served, weights=by_ccaa)

    # --- 2. mass of each cohort AMONG ORDERS, not among customers --------------------
    # The distinction is why frequency comes in here: a Catalonia cohort weighs more in
    # the aggregate mix than the same cohort in Madrid, because it places more orders.
    # Running IPF against the CUSTOMER distribution would balance the wrong books.
    raw_mass = {
        cohort_key(band, ccaa): Decimal(count) * frequency[ccaa]
        for (band, ccaa), count in customers_by_cohort.items()
        if count > 0
    }
    total_mass = sum(raw_mass.values(), Decimal("0"))
    if total_mass <= 0:
        raise DemandProfileError("no eligible customer in any cohort")
    mass = {c: raw_mass[c] / total_mass for c in sorted(raw_mass)}

    # --- 3. index matrix ---------------------------------------------------------------
    groups = tuple(sorted(base_weights))
    no_data = sorted(
        g for g in groups
        if g not in SCOPE_LABELS and (g not in age or g not in region)
    )
    if no_data:
        raise DemandProfileError(
            f"group(s) with a MAPA weight but no demographic cut: {no_data}. Leaving them "
            f"silently neutral would hide an extraction gap behind a plausible mix; add "
            f"the rows to {COHORT_AGE_SEED} and {COHORT_REGION_SEED}."
        )

    index: dict[str, dict] = {}
    for key in sorted(mass):
        band, ccaa = key.split("|", 1)
        entry = {}
        for g in groups:
            factor = ONE
            if g in age:
                if band not in age[g]:
                    raise DemandProfileError(f"{COHORT_AGE_SEED}: {g} is missing band {band}")
                age_row = age[g][band]
                factor *= affinity_index(age_row[0], age_row[1])
            if g in region:
                if ccaa not in region[g]:
                    raise DemandProfileError(
                        f"{COHORT_REGION_SEED}: {g} is missing community {ccaa}"
                    )
                region_row = region[g][ccaa]
                factor *= affinity_index(region_row[0], region_row[1])
            entry[g] = factor
        index[key] = entry

    # --- 4. IPF WITHIN EACH BLOCK, never crossing their boundary ----------------------
    #
    # The restriction is not an implementation detail; it was found by measuring.
    # Normalizing the whole cohort at once, NO_FOOD and SIN_BENCHMARK — which have index 1.0
    # for lack of evidence — came out with 0.60x the slice in the 65+ cohort relative to
    # under-35. That is: the model would start CLAIMING that people over 65 buy 40% less
    # drugstore per basket line. Nobody measured that. The effect was pure normalization
    # residue, and it was LARGER than most measured effects.
    #
    # Pinning each block's slice in every cohort gives `food_line_share` back the status
    # the seed gives it — a declared, uniform premise — and confines propensity to
    # redistributing WITHIN the scope where evidence exists. A neutral index then truly
    # means "no effect", instead of "effect left over from the arithmetic".
    tolerance = Decimal(params["ipf_tolerance"])
    max_iterations = int(params["ipf_max_iterations"])

    by_block: dict[str, list] = {}
    for g in groups:
        by_block.setdefault(group_blocks[g], []).append(g)

    factor: dict[str, Decimal] = {}
    total_by_block: dict[str, Decimal] = {}
    iterations = 0
    deviation = Decimal("0")
    for block in sorted(by_block):
        members = sorted(by_block[block])
        total = sum(base_weights[g] for g in members)
        total_by_block[block] = total
        if total <= 0:
            for g in members:
                factor[g] = ONE
            continue
        block_base = {g: base_weights[g] / total for g in members}
        block_index = {c: {g: index[c][g] for g in members} for c in mass}
        partial, its, err = ipf_calibrate(
            block_base, block_index, mass, tolerance, max_iterations
        )
        factor.update(partial)
        iterations = max(iterations, its)
        deviation = max(deviation, err)

    served_weights = []
    for key in sorted(mass):
        entry = {}
        for block, members in sorted(by_block.items()):
            total = total_by_block[block]
            if total <= 0:
                for g in members:
                    entry[g] = Decimal("0")
                continue
            raw = {g: base_weights[g] * factor[g] * index[key][g] for g in members}
            weight_sum = sum(raw[g] for g in members)
            for g in members:
                entry[g] = total * raw[g] / weight_sum
        served_weights.append({
            "cohort": key,
            "groups": [
                {"demand_group": g, "line_weight": str(entry[g])} for g in groups
            ],
        })

    return {
        "dimensions": [d.strip() for d in params["cohort_dimensions"].split(",")],
        "independence": params["cohort_independence"],
        "calibration": params["cohort_calibration"],
        "independence_note": (
            "The report publishes the age and community marginals and NEVER the cross-tab "
            "of the two. Multiplying both indices assumes conditional independence: it is "
            "a declared premise, not a measurement."
        ),
        "index_note": (
            "MAPA's '% Poblacion' is the population that LIVES IN HOUSEHOLDS whose main "
            "shopper is in that band, not the population of that age. Our customer is an "
            "individual. That is why the number enters as a RELATIVE index between bands "
            "and never as an absolute share — and why IPF exists."
        ),
        "age_bands": [
            {"key": name, "max_age": ceiling} for name, ceiling in AGE_BANDS
        ],
        "regions": [
            {
                "ccaa_code": c,
                "ccaa_label": region_ref[c]["ccaa_label"],
                "per_capita_kg_l": str(region_ref[c]["per_capita_kg_l"]),
                "warehouses": sorted(w for w, r in warehouse_regions.items() if r == c),
                "frequency_index": str(frequency[c]),
            }
            for c in served
        ],
        "region_frequency_basis": params["region_frequency_basis"],
        "region_frequency_normalization": params["region_frequency_normalization"],
        "region_frequency_note": (
            "The index weighs HOW MANY customers order at each warehouse, never the basket "
            "size: the report gives intensity in kg per year and does not publish "
            "household purchase frequency. Renormalized over the served communities, so "
            "the order total does not move and what changes is the split between warehouses."
        ),
        "neutral_groups": sorted(g for g in groups if g in SCOPE_LABELS),
        "neutral_note": (
            "NO_FOOD and SIN_BENCHMARK keep index 1.0 in every cohort because the report "
            "does not measure them — drugstore, cleaning and pets are outside its universe. "
            "It is not the claim that all ages buy shampoo equally; it is the absence of any "
            "measurement supporting otherwise."
        ),
        "block_shares_are_constant": True,
        "block_shares_note": (
            "Each block's slice (calibrated, food without benchmark, non-food) is the SAME "
            "in every cohort, and IPF runs within each one. Without that boundary, the "
            "neutral-index groups absorbed the normalization residue: NO_FOOD came out with "
            "0.60x the slice in 65+ against under-35, which would claim that older people buy "
            "40% less drugstore per line — a number no source in this repo measures, and "
            "larger than most effects that are measured."
        ),
        "ipf_iterations": iterations,
        "ipf_max_abs_error": str(deviation),
        "ipf_note": (
            "Acceptance criterion of this layer: the AGGREGATE mix does not move. The "
            "per-cohort weights, weighted by the real cohort distribution among orders, "
            "reproduce the aggregate calibration weights. The phase's effect is in the "
            "conditional, and that is where it should be looked for."
        ),
        "mass": [{"cohort": c, "share": str(mass[c])} for c in sorted(mass)],
        "weights": served_weights,
    }


def build(
    catalog_rows: list[dict],
    seeds_dir: str = DEFAULT_SEEDS_DIR,
    *,
    customers_by_cohort: dict,
    warehouse_regions: dict,
) -> dict:
    """Demand profile from the observed catalog and the eight seeds.

    `catalog_rows` must carry `demand_group` — ALREADY RESOLVED — and `net_content_kg_l`,
    which may be null. Resolution does NOT happen here on purpose: it lives in a single
    place (`orders_reference._stamp_demand_group`), for the same reason the Silver gate
    lives in `silver_gate.plan()`. Two resolutions diverge on the first tweak, and the
    divergence would be invisible because both would produce a plausible mix.

    `customers_by_cohort` maps (age band, ccaa) -> how many ELIGIBLE customers, and
    `warehouse_regions` maps warehouse -> ccaa. Both are REQUIRED, not optional with a
    neutral fallback: a profile without the cohort layer stamped with version v2 would be a
    silent no-op — the worst possible outcome for a phase whose success criterion is
    precisely that the aggregate does not move.
    """
    benchmark = load_benchmark(seeds_dir)
    params = load_params(seeds_dir)
    seasonality = load_seasonality(seeds_dir)

    if not catalog_rows:
        raise DemandProfileError("empty catalog: nothing to weight")

    # --- 1. group the catalog by the already-stamped group ------------------------
    by_group: dict[str, dict] = {}
    for row in catalog_rows:
        key = row.get("demand_group")
        if not key:
            raise DemandProfileError(
                "catalog row without `demand_group`. Resolving the mapping is the "
                "responsibility of whoever builds the catalog, and does not happen here."
            )
        if row.get("popularity_weight") is None or row.get("quantity_max") is None:
            raise DemandProfileError(
                "catalog row without `popularity_weight`/`quantity_max`. They are stamped by "
                "product_choice.stamp before the profile is built: the kg per line must be the "
                "one of the basket the generator actually draws (CR-017)."
            )
        bucket = by_group.setdefault(
            key, {"products": 0, "with_kg": [], "without_kg": 0, "kg_weight": Decimal("0"),
                  "kg_volume": Decimal("0")}
        )
        bucket["products"] += 1
        content = _content(row.get("net_content_kg_l"))
        if content is None:
            bucket["without_kg"] += 1
        else:
            bucket["with_kg"].append(content)
            # kg per LINE, as drawn: popularity x expected units x content.
            weight = Decimal(str(row["popularity_weight"]))
            units = Decimal(str(round(product_choice.expected_quantity(int(row["quantity_max"])), 9)))
            bucket["kg_weight"] += weight
            bucket["kg_volume"] += weight * units * content

    unknown = [k for k in by_group if k not in benchmark]
    if unknown:
        raise DemandProfileError(
            f"the mapping points to key(s) the benchmark does not declare: {unknown}"
        )

    coverage_min = Decimal(params["volume_coverage_min"])

    # --- 2. three blocks: benchmark, food without benchmark, non-food ------------
    bench_keys = sorted(k for k in by_group
                          if k not in SCOPE_LABELS and benchmark[k]["use_as_weight"])
    unbench_keys = sorted(k for k in by_group
                        if k == SIN_BENCHMARK or (k not in SCOPE_LABELS
                                                  and not benchmark[k]["use_as_weight"]))
    nofood_keys = sorted(k for k in by_group if k == NO_FOOD)

    if not bench_keys:
        raise DemandProfileError(
            "no catalog group fell into a weightable MAPA key. The mapping or the "
            "benchmark are misaligned, and without this there is no calibration at all."
        )

    # --- 3. volume target, tilted by channel -------------------------------------
    groups: dict[str, dict] = {}
    raw_bench: dict[str, Decimal] = {}
    for key in bench_keys:
        entry = benchmark[key]
        info = by_group[key]
        tilt, basis = channel_tilt(entry, params)
        volume_target = entry["volume_share_pct"] * tilt

        covered = len(info["with_kg"])
        coverage = Decimal(covered) / Decimal(info["products"])
        if coverage >= coverage_min:
            mean_kg = info["kg_volume"] / info["kg_weight"]
            weight = volume_target / mean_kg
            target_path = "volume"
        else:
            # DECLARED fallback: line share equal to volume share. It is not
            # equivalent, and the reality check reports these groups in a separate block.
            mean_kg = None
            weight = volume_target
            target_path = "lines"

        raw_bench[key] = weight
        groups[key] = {
            "demand_group": key,
            "block": "benchmark",
            "scope": entry["scope"],
            "mapa_volume_share_pct": str(entry["volume_share_pct"]),
            "mapa_value_share_pct": str(entry["value_share_pct"]) if entry["value_share_pct"] is not None else None,
            "mapa_avg_price_eur_kg": str(entry["avg_price_eur_kg"]) if entry["avg_price_eur_kg"] is not None else None,
            "channel_tilt": str(round(tilt, 6)),
            "channel_basis": basis,
            "target_path": target_path,
            "kg_coverage": str(round(coverage, 4)),
            "mean_kg_per_line": str(round(mean_kg, 6)) if mean_kg is not None else None,
            "assortment_products": info["products"],
            "informe_section": entry["informe_section"],
        }

    # --- 4. the three blocks add up to 1, and each split has a written origin ----
    #   food               <- declared premise `food_line_share`
    #     with benchmark   <- coverage of the benchmark ITSELF (86.12%), not a new premise
    #     without benchmark <- the complement, split by assortment size
    #   non-food           <- the rest, split by assortment size
    food_share = Decimal(params["food_line_share"])
    if not Decimal("0") < food_share <= Decimal("1"):
        raise DemandProfileError(f"food_line_share outside (0, 1]: {food_share}")
    bench_coverage = Decimal(params["benchmark_volume_coverage_pct"]) / Decimal("100")
    if not Decimal("0") < bench_coverage <= Decimal("1"):
        raise DemandProfileError(
            f"benchmark_volume_coverage_pct outside (0, 100]: {params['benchmark_volume_coverage_pct']}"
        )

    bench_block = food_share * bench_coverage
    unbench_block = food_share * (Decimal("1") - bench_coverage)
    nofood_block = Decimal("1") - food_share

    # A BLOCK WITH NO GROUP IN THE CATALOG. It really happens with a small catalog — a test
    # fixture, or a window in which the source only brought food. That block's weight has
    # nowhere to go, and dropping it would make the weights add up to less than 1.
    #
    # Redistribute PROPORTIONALLY to the blocks that exist, and RECORD that it happened.
    # Without the record it would be a silent correction: `demand_profile.json` would show
    # weights that do not match the declared parameters, with the difference unexplained.
    blocks_present = {
        "benchmark_block": (bench_block, bool(bench_keys)),
        "sin_benchmark_block": (unbench_block, bool(unbench_keys)),
        "no_food_block": (nofood_block, bool(nofood_keys)),
    }
    empty_blocks = [name for name, (_weight, present) in blocks_present.items() if not present]
    if empty_blocks:
        live = sum(weight for weight, present in blocks_present.values() if present)
        if live <= 0:
            raise DemandProfileError("no block has a group in the catalog")
        scale = Decimal("1") / live
        bench_block = bench_block * scale if bench_keys else Decimal("0")
        unbench_block = unbench_block * scale if unbench_keys else Decimal("0")
        nofood_block = nofood_block * scale if nofood_keys else Decimal("0")

    unbench_total = nofood_total = Decimal("0")
    if unbench_keys:
        unbench_total = sum(Decimal(by_group[k]["products"]) for k in unbench_keys)
    if nofood_keys:
        nofood_total = sum(Decimal(by_group[k]["products"]) for k in nofood_keys)

    raw_total = sum(raw_bench.values(), Decimal("0"))
    for key in bench_keys:
        groups[key]["line_weight"] = str(bench_block * raw_bench[key] / raw_total)

    for key in unbench_keys:
        info = by_group[key]
        groups[key] = {
            "demand_group": key,
            "block": "sin_benchmark",
            "scope": benchmark[key]["scope"],
            "target_path": "assortment",
            "assortment_products": info["products"],
            "line_weight": str(unbench_block * Decimal(info["products"]) / unbench_total),
            "note": "food without a volume target in the report; share proportional to assortment",
        }

    for key in nofood_keys:
        info = by_group[key]
        groups[key] = {
            "demand_group": key,
            "block": "no_food",
            "scope": "none",
            "target_path": "assortment",
            "assortment_products": info["products"],
            "line_weight": str(nofood_block * Decimal(info["products"]) / nofood_total),
            "note": "outside the MAPA universe; share proportional to assortment",
        }

    weight_sum = sum(Decimal(g["line_weight"]) for g in groups.values())
    if abs(weight_sum - Decimal("1")) > Decimal("0.000001"):
        raise DemandProfileError(f"the weights add up to {weight_sum}, not 1")

    return {
        "demand_model_version": params["demand_model_version"],
        "benchmark": "MAPA — Informe del Consumo Alimentario en España 2025",
        "benchmark_note": (
            "The PDF's title page says 'Informe del consumo alimentario en España 2024'; the "
            "whole body reports the year 2025 ('A cierre del año 2025', 'frente a los 26.823,4 "
            "millones del año 2024'). It is residue of the previous edition on the credits "
            "page. The numbers in this profile come from the BODY."
        ),
        "seeds_sha256": seeds_sha256(seeds_dir),
        "params": params,
        "blocks": {
            "food_line_share": str(food_share),
            "benchmark_block": str(bench_block),
            "sin_benchmark_block": str(unbench_block),
            "no_food_block": str(nofood_block),
        },
        # Empty in the normal case. Filled when a block had no group in the catalog and its
        # weight was redistributed — the redistribution is visible here, and not only in the
        # difference between the declared parameters and the resulting weights.
        "blocks_redistributed": empty_blocks,
        "seasonality_applies_to": "daily_order_rate",
        "seasonality": {str(m): str(f) for m, f in sorted(seasonality.items())},
        "seasonality_note": (
            "Neutral for lack of evidence, not by oversight: the report's monthly charts "
            "are images and there is no monthly profile per category. It applies to the "
            "order rate, never to the mix — a global factor on the mix would normalize away "
            "and do nothing. The current window only covers August."
        ),
        "groups": [groups[k] for k in sorted(groups)],
        "cohorts": _build_cohorts(
            {k: Decimal(g["line_weight"]) for k, g in groups.items()},
            {k: g["block"] for k, g in groups.items()},
            customers_by_cohort,
            warehouse_regions,
            params,
            seeds_dir,
        ),
    }
