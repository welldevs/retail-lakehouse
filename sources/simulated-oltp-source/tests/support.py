"""Minimal synthetic reference, with the edge cases the suite needs to prove.

The fixture does not mimic the volume of the real data (216 thousand tramos), it mimics the
SHAPE and every case the numbering rule and geographic coherence must handle:

  * `numbering_type='0'`  -> the street has no real numbering, a number must never come out;
  * range `0..0` with `numbering_type='2'` -> exists in the real data (14 tramos) and would
    give house number 0 if the code only filtered by numbering_type;
  * range without any number of the declared parity (even between 3 and 3) -> no number either;
  * pseudo-street -> tramo that does not hang off a named street;
  * two warehouses with disjoint municipalities -> proves no customer crosses the FUA;
  * one municipality with weight 0.9 and another with 0.1 -> proves the weight is respected;
  * one municipality with several equally weighted candidates -> proves the TRAMO choice is
    uniform, not weighted by population (which does not exist at that granularity).

The street/neighborhood strings below (e.g. "RUA UM") are opaque fixture values, kept as-is.
"""

from __future__ import annotations

import json
import os

WH_A = "wha"
WH_B = "whb"

# Indexes are the candidate identity (candidate_index): the order of this list matters.
CANDIDATES = [
    # 0..2: wha / province 01 / municipality 001 — three tramos, to test uniformity
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": None,
        "street_name": "RUA UM", "is_pseudo_address": False, "postal_code": "01001",
        "numbering_type": "1", "number_from": 1, "number_to": 9,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": "NUCLEO UM",
        "street_name": "RUA DOIS", "is_pseudo_address": False, "postal_code": "01002",
        "numbering_type": "2", "number_from": 2, "number_to": 10,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": None,
        "street_name": "PARQUE DA PSEUDOVIA", "is_pseudo_address": True,
        "postal_code": "01003", "numbering_type": "0", "number_from": 0, "number_to": 0,
    },
    # 3..5: wha / province 01 / municipality 002 — only cases where no number is possible
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA DEGENERADA", "is_pseudo_address": False,
        "postal_code": "01010", "numbering_type": "2", "number_from": 0, "number_to": 0,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA SEM PAR", "is_pseudo_address": False,
        "postal_code": "01011", "numbering_type": "2", "number_from": 3, "number_to": 3,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA IMPAR UNICA", "is_pseudo_address": False,
        "postal_code": "01012", "numbering_type": "1", "number_from": 3, "number_to": 3,
    },
    # 6: whb / province 02 / municipality 010 — another warehouse, another province
    {
        "wh": WH_B, "province_code": "02", "municipality_code": "010",
        "municipality_name_callejero": "GAMA", "neighborhood_name": None,
        "street_name": "RUA GAMA", "is_pseudo_address": False, "postal_code": "02010",
        "numbering_type": "1", "number_from": 11, "number_to": 21,
    },
]

WEIGHTS = [
    {
        "wh": WH_A, "province_code": "01", "province_name": "Provincia Alfa",
        "municipality_code": "001", "municipality_name": "Alfa",
        "population_total": 9000, "proportion_within_wh": 0.9,
        "sex_hombres_proportion": 0.5, "sex_mujeres_proportion": 0.5,
    },
    {
        "wh": WH_A, "province_code": "01", "province_name": "Provincia Alfa",
        "municipality_code": "002", "municipality_name": "Beta",
        "population_total": 1000, "proportion_within_wh": 0.1,
        "sex_hombres_proportion": 0.5, "sex_mujeres_proportion": 0.5,
    },
    {
        "wh": WH_B, "province_code": "02", "province_name": "Provincia Beta",
        "municipality_code": "010", "municipality_name": "Gama",
        "population_total": 500, "proportion_within_wh": 1.0,
        "sex_hombres_proportion": 1.0, "sex_mujeres_proportion": 0.0,
    },
]

AGES = (
    [{"province_code": "01", "age": age, "proportion": 1 / 3} for age in (30, 31, 32)]
    + [{"province_code": "02", "age": age, "proportion": 0.5} for age in (40, 41)]
)

# The delivered distribution is the CUSTOMER BASE one: already truncated and renormalized by
# the platform. Every fixture age is above the minimum on purpose — whoever wants to exercise
# the refusal uses AGES_WITH_MINOR below, and then the refusal is the behavior under test,
# not an accident.
MIN_CUSTOMER_AGE = 18
AGES_WITH_MINOR = AGES + [{"province_code": "01", "age": 7, "proportion": 0.0}]

# Targets DIFFERENT from each other and from the fixture's default count (50): only then can
# a test tell "read the reference target" from "used the command-line number".
CUSTOMER_TARGETS = {WH_A: 37, WH_B: 11}

ALLOCATION = {
    "rule": "per_warehouse_population",
    "population_basis": "adult_resident_population",
    "min_customer_age": MIN_CUSTOMER_AGE,
    "penetration_pct": 2.2,
    "penetration_source": "demand_profile.channel_reference_pct",
    "served_population": 10500,
    "served_adult_population": 8631.0,
    "total_customers": 48,
    "by_warehouse": [
        {"wh": WH_A, "population_total": 10000, "adult_population": 8220.0, "customers": 37},
        {"wh": WH_B, "population_total": 500, "adult_population": 411.0, "customers": 11},
    ],
}


def write_reference(
    directory: str, candidates=None, weights=None, ages=None,
    allocation=..., min_customer_age=...,
) -> str:
    """Write the three reference files. Returns the directory.

    `allocation` and `min_customer_age` use `...` as absence instead of `None`: `None` is a
    legitimate value the tests need to be able to inject to prove the Source rejects an
    old-schema reference.
    """
    os.makedirs(directory, exist_ok=True)
    payloads = {
        "address_candidates.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "callejero_ingestion_date": "2026-08-25",
            "excluded_rows": {"no_street_or_pseudo_match": 3},
            "rows": CANDIDATES if candidates is None else candidates,
        },
        "municipality_population_weights.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "population_ingestion_date": "2026-08-26",
            "population_year": 2025,
            "population_reference_date": "2024-12-31",
            "customer_allocation": ALLOCATION if allocation is ... else allocation,
            "rows": WEIGHTS if weights is None else weights,
        },
        "province_age_distribution.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "population_series_ingestion_date": "2026-08-26",
            "year": 2022,
            "reference_date": "2022-06-30",
            "fk_periodo": 27,
            "excluded_age_labels": ["Total", "85 y más años"],
            "min_customer_age": (
                MIN_CUSTOMER_AGE if min_customer_age is ... else min_customer_age
            ),
            "rows": AGES if ages is None else ages,
        },
    }
    for payload in payloads.values():
        # An ABSENT key is different from a null key: that is what an old-schema reference
        # looks like, and that is the case the Source must reject.
        for key in ("customer_allocation", "min_customer_age"):
            if key in payload and payload[key] is None:
                payload.pop(key)
    for name, payload in payloads.items():
        with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
    return directory


class Args:
    """Simple namespace, to call the handlers without going through argparse."""

    def __init__(self, **values):
        self.__dict__.update(values)


def extract_args(reference: str, out: str, wh: str = WH_A, **overrides) -> Args:
    values = {
        "reference": reference,
        "out": out,
        "wh": wh,
        "date": "2026-08-27",
        "count": 50,
        "seed": 7,
        "overwrite": False,
    }
    values.update(overrides)
    return Args(**values)


def validate_args(partition: str, reference: str, strict: bool = False) -> Args:
    return Args(partition=partition, reference=reference, strict=strict)
