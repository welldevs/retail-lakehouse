"""Minimal fixtures: a complete reference on disk, with no network and no Lakehouse.

Small on purpose — a catalog of a few dozen products and a handful of customers. What is tested
here is the RULE (price comes from the catalog, customer comes from the warehouse, the fold
closes), and a rule needs no volume. Volume is exercised by `validate` against the real
partition.
"""

from __future__ import annotations

import json
import os

WH = "mad1"
OTHER_WH = "bcn1"
ORDER_DATE = "2026-08-24"
PRICE_AS_OF = "2026-08-24"
FIRST_INGESTION = "2026-08-24"

PREMISES = {
    "daily_order_rate": "0.5",
    "min_buyer_age": "18",
    "basket_lines_min": "2",
    "basket_lines_mode": "3",
    "basket_lines_max": "5",
    "quantity_max": "3",
    "substitution_rate": "0.2",
    "removal_rate": "0.1",
    "payment_failure_rate": "0.05",
    "cancellation_rate": "0.05",
    "delivery_failure_rate": "0.05",
    "return_rate": "0.05",
    "order_hour_min": "8",
    "order_hour_max": "20",
    "slot_hours": "2",
    "slot_lead_hours_min": "2",
    "slot_lead_hours_max": "10",
    "minutes_to_payment_min": "1",
    "minutes_to_payment_max": "5",
    "minutes_to_cancel_min": "3",
    "minutes_to_cancel_max": "30",
    "minutes_to_picking_min": "10",
    "minutes_to_picking_max": "60",
    "minutes_per_line_picked": "2",
    "minutes_to_dispatch_min": "5",
    "minutes_to_dispatch_max": "20",
    "minutes_to_delivered_min": "10",
    "minutes_to_delivered_max": "60",
    "minutes_to_return_min": "60",
    "minutes_to_return_max": "600",
    "sla_minutes_picking": "90",
}


# Three demand groups in the fixture, with UNEQUAL weights on purpose: with equal weights the
# weighted draw would be indistinguishable from the uniform one, and a test that passes on both
# implementations tests neither.
DEMAND_GROUPS = ("GROUP_A", "GROUP_B", "GROUP_C")
DEMAND_WEIGHTS = {"GROUP_A": "0.6", "GROUP_B": "0.3", "GROUP_C": "0.1"}

# The report's four bands, and one community per warehouse — the fixture mirrors the shape of
# the real profile, including the collinearity between warehouse and community.
AGE_BANDS = (("LT35", 34), ("35_49", 49), ("50_64", 64), ("GE65", None))
WAREHOUSE_CCAA = {WH: "13", OTHER_WH: "09"}

# Reference year for the fixture ages. Fixed, not `date.today()`: a fixture that ages with the
# clock would make the same test change its result next year.
FIXTURE_YEAR = int(ORDER_DATE[:4])


def catalog_rows(wh: str = WH, price_as_of: str = PRICE_AS_OF, total: int = 30) -> list[dict]:
    """Synthetic catalog with populated subgroups, so that substitution has a candidate."""
    rows = []
    for index in range(total):
        rows.append(
            {
                "wh": wh,
                "price_as_of": price_as_of,
                "source_product_id": f"p{index:04d}",
                "display_name": f"Product {index}",
                "category_id": 10 + index % 3,
                "subgroup_id": 100 + index % 5,
                "demand_group": DEMAND_GROUPS[index % len(DEMAND_GROUPS)],
                "unit_price": f"{1 + index % 7}.{(index * 7) % 100:02d}",
                "tax_percentage": "21.000",
            }
        )
    return rows


def cohort_payload(weights, cohort_weights=None, frequency=None) -> dict:
    """The fixture's `cohorts` section.

    By default EVERY cohort gets the aggregate vector: that is the flat index matrix, and with
    it the per-cohort draw must reproduce the aggregate draw exactly. A test that passes on
    both implementations tests neither, so the tests that need differentiation pass
    `cohort_weights` explicitly.
    """
    keys = [
        f"{band}|{ccaa}"
        for ccaa in sorted(set(WAREHOUSE_CCAA.values()))
        for band, _ceiling in AGE_BANDS
    ]
    vectors = cohort_weights or {c: weights for c in keys}
    indices = frequency or {c: "1.0" for c in sorted(set(WAREHOUSE_CCAA.values()))}
    return {
        "dimensions": ["age", "region"],
        "independence": "multiplicative",
        "calibration": "ipf",
        "age_bands": [{"key": name, "max_age": ceiling} for name, ceiling in AGE_BANDS],
        "regions": [
            {
                "ccaa_code": ccaa,
                "ccaa_label": f"CCAA {ccaa}",
                "warehouses": sorted(w for w, c in WAREHOUSE_CCAA.items() if c == ccaa),
                "frequency_index": indices[ccaa],
            }
            for ccaa in sorted(set(WAREHOUSE_CCAA.values()))
        ],
        "mass": [{"cohort": c, "share": str(1 / len(keys))} for c in sorted(vectors)],
        "weights": [
            {
                "cohort": chave,
                "groups": [
                    {"demand_group": g, "line_weight": str(vectors[chave][g])}
                    for g in sorted(vectors[chave])
                ],
            }
            for chave in sorted(vectors)
        ],
    }


def demand_payload(
    weights=None,
    seasonality=None,
    version: str = "fixture_v1",
    cohort_weights=None,
    frequency=None,
) -> dict:
    """Minimal demand profile, in the same format the platform writes."""
    weights = dict(DEMAND_WEIGHTS if weights is None else weights)
    return {
        "demand_model_version": version,
        "benchmark": "fixture",
        "seeds_sha256": {},
        "blocks": {},
        "seasonality_applies_to": "daily_order_rate",
        "seasonality": {str(m): "1.0" for m in range(1, 13)}
        if seasonality is None
        else {str(m): str(v) for m, v in seasonality.items()},
        "groups": [
            {"demand_group": key, "line_weight": weights[key], "block": "benchmark"}
            for key in sorted(weights)
        ],
        "cohorts": cohort_payload(weights, cohort_weights, frequency),
    }


# One age per band, cycled: the fixture covers all four bands even with 8 customers, and none
# of them ends up empty by accident of sample size.
FIXTURE_AGES = (25, 42, 57, 71)


def customer_rows(
    wh: str = WH, total: int = 8, first: str = FIRST_INGESTION, ages=None
) -> list[dict]:
    ages = tuple(ages or FIXTURE_AGES)
    return [
        {
            "customer_id": f"cust_{wh}_{index:06d}",
            "wh": wh,
            "province_code": "28",
            "municipality_code": "079",
            "postal_code": f"280{index:02d}",
            "birth_year": FIXTURE_YEAR - ages[index % len(ages)],
            "first_ingestion_date": first,
        }
        for index in range(total)
    ]


def write_reference(
    directory: str,
    customers=None,
    catalog=None,
    calendar=None,
    premises=None,
    customer_ingestion_dates=None,
    demand=None,
) -> str:
    """Write the five reference files. Returns the directory."""
    os.makedirs(directory, exist_ok=True)
    dates = customer_ingestion_dates or [FIRST_INGESTION]
    payloads = {
        "customers.json": {
            "customer_ingestion_dates": dates,
            "roster_ingestion_date": dates[-1],
            "warehouses": [WH],
            "rows": customer_rows() if customers is None else customers,
        },
        "catalog.json": {
            "catalog_ingestion_dates": [PRICE_AS_OF],
            "rows": catalog_rows() if catalog is None else catalog,
        },
        "calendar.json": {
            "window_from": ORDER_DATE,
            "window_to": ORDER_DATE,
            "warehouses": [WH],
            "carried_forward_rows": 0,
            "rows": calendar
            if calendar is not None
            else [
                {
                    "wh": WH,
                    "order_date": ORDER_DATE,
                    "price_as_of": PRICE_AS_OF,
                    "price_source": "observed",
                }
            ],
        },
        "premises.json": {
            "seed_path": "platform/dbt/seeds/order_premises_seed.csv",
            "seed_sha256": "0" * 64,
            "label": "synthetic",
            "values": dict(premises or PREMISES),
            "rows": [],
        },
        "demand_profile.json": demand_payload() if demand is None else demand,
    }
    for name, payload in payloads.items():
        with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
    return directory


class Args:
    """Simple namespace standing in for the argparse result."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
