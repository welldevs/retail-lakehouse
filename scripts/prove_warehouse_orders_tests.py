#!/usr/bin/env python3
"""Proves that the Orders warehouse tests know how to FAIL.

    make warehouse-prove-tests

WHY THIS SCRIPT EXISTS. "A check that never failed is not a check" is the repo's rule,
and Milestone 7 just showed why: the first STAGE load put EVERY timestamp in the year
56,648,666, and the 166 dbt nodes built GREEN. Row counts matched, no type changed, the
grain stayed unique, and a funnel made of `count_if(marco is not null)` stays exact when the
instant is shifted 56 million years. The defect was found by a human reading 80,000,060
minutes of picking time in a mart.

A green test only means something after it has been seen red. This script injects, into
the REAL warehouse data, the specific defect each test claims to catch; demands that the
test fail; undoes it; and demands that it pass again. All five are exercised on the same
data the operation uses — not on a fixture that already agrees with the expectation.

SAFE BY CONSTRUCTION: everything this script edits lives in GOLD and MART, which are
ENTIRELY rebuildable by `dbt build` from STAGE. Nothing here touches STAGE,
RAW or the Lakehouse. Even so, each injection is undone by its own pair of commands and
the script ends by rebuilding what it touched — and a sentinel checks, at the end, that the
counts went back to what they were.
"""

from __future__ import annotations

import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "platform", "src"))

from retail_platform.snowflake_load import connect, DEFAULT_DATABASE  # noqa: E402

DBT = os.path.join("platform", ".venv", "bin", "dbt")
PROJECT = ["--project-dir", "platform/dbt", "--profiles-dir", "platform/dbt",
           "--target", "snowflake"]

DB = os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_DATABASE)
GOLD = f"{DB}.GOLD"
MART = f"{DB}.MART"

# Each case is (test, the defect it claims to catch, injection, repair). The injection is always
# the SMALLEST possible change — one row, one column — because a test that only fails in the
# face of gross damage protects against nothing.
CASES = [
    {
        "test": "assert_every_order_has_a_customer_version",
        "catches": "SCD2 resolved to a version other than the one in effect on the order date",
        "inject": [
            f"update {GOLD}.FACT_ORDER set customer_version_from = dateadd(day, -30, customer_version_from) "
            f"where order_id = 'ord_mad1_20260827_000010'",
        ],
        "repair": [
            f"update {GOLD}.FACT_ORDER set customer_version_from = customer_ingestion_date "
            f"where order_id = 'ord_mad1_20260827_000010'",
        ],
    },
    {
        "test": "assert_fact_order_amount_equals_sum_of_items",
        "catches": "fanout in one of the two joins with DIM_PRODUCT, or net_amount filled "
                   "for an order that was never picked",
        "inject": [
            f"update {GOLD}.FACT_ORDER set net_amount = net_amount + 0.01 "
            f"where order_id = 'ord_bcn1_20260825_000005' and net_amount is not null",
        ],
        "repair": [
            f"update {GOLD}.FACT_ORDER set net_amount = net_amount - 0.01 "
            f"where order_id = 'ord_bcn1_20260825_000005' and net_amount is not null",
        ],
    },
    {
        "test": "assert_order_item_price_matches_price_snapshot",
        "catches": "a paid price that is not the price published at that warehouse on that day — "
                   "the cross-check between the synthetic source and the real API",
        "inject": [
            f"update {GOLD}.FACT_ORDER_ITEM set unit_price_paid = unit_price_paid + 0.10 "
            f"where order_id = 'ord_svq1_20260826_000003' and line_no = 1 "
            f"and fulfilled_source_product_id is not null",
        ],
        "repair": [
            f"update {GOLD}.FACT_ORDER_ITEM set unit_price_paid = unit_price_paid - 0.10 "
            f"where order_id = 'ord_svq1_20260826_000003' and line_no = 1 "
            f"and fulfilled_source_product_id is not null",
        ],
    },
    {
        "test": "assert_order_funnel_totals_match_fact_order",
        "catches": "a day-warehouse vanishing from the mart, and deliveries counted by STATUS "
                   "instead of by MILESTONE — the 61 returned orders that were also delivered",
        "inject": [
            # BOTH halves of what the test promises: one row disappears, another starts counting
            # deliveries by status. A grand total would still match in both cases.
            f"delete from {MART}.MART_ORDER_FUNNEL "
            f"where order_date = '2026-08-24' and wh = 'mad1'",
            f"update {MART}.MART_ORDER_FUNNEL set orders_delivered = orders_delivered - orders_returned "
            f"where order_date = '2026-08-25' and wh = 'bcn1'",
        ],
        "repair": [],   # only a rebuild brings the deleted row back
        "rebuild": "mart_order_funnel",
    },
    {
        "test": "assert_order_milestones_are_plausible_against_the_order_date",
        "catches": "a UNIT error in the timestamp when crossing the boundary — the real defect "
                   "that put the 6,400 orders in the year 56,648,666",
        "inject": [
            # Reproduces the microseconds-read-as-milliseconds shift on a single order.
            # Milestones stay in the right order relative to each other: that is why comparing
            # milestones with one another would not catch it, and only anchoring on order_date does.
            f"update {GOLD}.FACT_ORDER set "
            f"  placed_at = dateadd(year, 1000, placed_at), "
            f"  picked_at = dateadd(year, 1000, picked_at) "
            f"where order_id = 'ord_vlc1_20260824_000007'",
        ],
        "repair": [
            f"update {GOLD}.FACT_ORDER set "
            f"  placed_at = dateadd(year, -1000, placed_at), "
            f"  picked_at = dateadd(year, -1000, picked_at) "
            f"where order_id = 'ord_vlc1_20260824_000007'",
        ],
    },
]

SENTINEL = [
    (f"{GOLD}.FACT_ORDER", 6400),
    (f"{GOLD}.FACT_ORDER_ITEM", 120693),
    (f"{MART}.MART_ORDER_FUNNEL", 16),
]


def execute(cursor, statements):
    for sql in statements:
        cursor.execute(sql)


def run_test(name: str) -> int:
    return subprocess.run(
        [DBT, "test", *PROJECT, "--select", name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode


def rebuild(target: str) -> None:
    subprocess.run([DBT, "build", *PROJECT, "--select", target],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


def main() -> int:
    conn = connect()
    cursor = conn.cursor()

    print("sentinel before ..", end=" ")
    before = {}
    for table, expected in SENTINEL:
        cursor.execute(f"select count(*) from {table}")
        before[table] = cursor.fetchone()[0]
        if before[table] != expected:
            print(f"\nREFUSED: {table} has {before[table]} rows, expected {expected}. "
                  f"The base is already outside the known state — run `make warehouse-refresh` "
                  f"first. Injecting a defect on a dirty base produces a false positive.")
            return 1
    print("ok")
    print()

    failures = []
    for case in CASES:
        name = case["test"]
        print(f"{name}")
        print(f"  catches ...... {case['catches']}")

        if run_test(name) != 0:
            print("  REFUSED: the test already fails BEFORE the injection. A red that was "
                  "already red proves nothing.")
            failures.append(name)
            continue

        execute(cursor, case["inject"])
        code = run_test(name)
        print(f"  with defect    exit={code} "
              f"{'FAILED (expected)' if code else 'PASSED — the test is blind'}")
        if code == 0:
            failures.append(name)

        execute(cursor, case["repair"])
        if case.get("rebuild"):
            rebuild(case["rebuild"])
        clean = run_test(name)
        print(f"  restored ..... exit={clean} "
              f"{'passes again' if clean == 0 else 'STILL FAILS — the repair did not close'}")
        if clean != 0:
            failures.append(name)
        print()

    # The script touched real data. Ending without re-checking the WHOLE SET would leave the
    # warehouse in a state only the five checks above saw — and each one looked at one thing only.
    print("whole warehouse suite ..", end=" ", flush=True)
    full = subprocess.run([DBT, "test", *PROJECT],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("green" if full.returncode == 0 else "FAILED")
    if full.returncode != 0:
        failures.append("full suite after the repairs")
    print()

    print("sentinel after ...", end=" ")
    for table, _ in SENTINEL:
        cursor.execute(f"select count(*) from {table}")
        after = cursor.fetchone()[0]
        if after != before[table]:
            print(f"\nREFUSED: {table} went from {before[table]} to {after}.")
            failures.append(f"sentinel:{table}")
    conn.close()
    if not failures:
        print("ok")

    print()
    if failures:
        print(f"FAILED — {len(set(failures))} test(s) did not prove they can fail:")
        for name in sorted(set(failures)):
            print(f"  - {name}")
        return 1
    print(f"PASSED: the {len(CASES)} Orders warehouse tests fail in the face of the "
          f"defect each one claims to catch, and pass again after the repair.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
