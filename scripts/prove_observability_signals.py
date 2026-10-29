#!/usr/bin/env python3
"""Proves, against the real system, whether the six observability signals already exist.

WHY THIS EXISTS BEFORE INSTRUMENTING ANYTHING.

`BACKLOG.md`'s Observability row is explicit: before building
`application -> OpenTelemetry -> Collector -> backend`, one must PROVE that
the six required signals do not already exist through another path — latency, error,
throughput, failures, processing, pipeline state. Assuming absence by default is the same
disease as any other unmeasured claim in this project.

This script instruments nothing. It READS the real system (local partitions in `data/`, the
platform SQL, the DAG files) and returns a YES/PARTIAL/NO verdict per signal, with the
detail that supports the verdict. It is safe by construction: read-only, no writes.

    make observability-prove-signals
    make observability-prove-signals --warehouse   (optional: checks the real Snowflake)

Re-running after a change (e.g. CR-005) is the proof mechanism itself: a signal that
was NO and returns YES demonstrates the closure, with no invented number.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

DATA_SOURCES = {
    "mercadona_catalog_api": "data/mercadona",
    "ine_population_api": "data/ine",
    "ine_callejero": "data/callejero",
    "simulated_oltp": "data/oltp",
    "simulated_orders": "data/orders",
}
RUNLOG_THRESHOLD_SECONDS = 10.0

CLI_PY = os.path.join(REPO, "platform", "src", "retail_platform", "cli.py")
SNOWFLAKE_EXPORT_PY = os.path.join(REPO, "platform", "src", "retail_platform", "warehouse_cut.py")
FACT_INGESTION_RUN_SQL = os.path.join(
    REPO, "platform", "dbt", "models", "warehouse", "gold", "fact_ingestion_run.sql"
)
SILVER_MODELS_DIR = os.path.join(REPO, "platform", "dbt", "models", "silver")
DAGS_DIR = os.path.join(REPO, "orchestration", "airflow", "dags")

results: list[tuple[str, str, str]] = []  # (signal, verdict YES/PARTIAL/NO, detail)


def record(signal: str, verdict: str, detail: str) -> None:
    results.append((signal, verdict, detail))
    print(f"  [{verdict:8}] {signal}")
    for line in detail.split("\n"):
        print(f"             {line}")


def read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


# ---- Q1: PROCESSING — progress log proportional to the measured duration ---------------

def q1_processing() -> None:
    print()
    print("Q1. PROCESSING — does each Source write progress proportional to its measured duration?")
    missing: list[str] = []
    found_any = False
    for name, rel in DATA_SOURCES.items():
        root = os.path.join(REPO, rel)
        manifests = sorted(glob.glob(os.path.join(root, "**", "_manifest.json"), recursive=True))
        if not manifests:
            print(f"    {name}: no local partition under {rel}/ — not measured on this machine")
            continue
        found_any = True
        durations = []
        without_log = []
        for path in manifests:
            try:
                manifest = json.loads(read(path))
            except (OSError, json.JSONDecodeError):
                continue
            duration = manifest.get("duration_seconds")
            if duration is None:
                continue
            durations.append(duration)
            has_run_log = os.path.exists(os.path.join(os.path.dirname(path), "_run.log"))
            if duration > RUNLOG_THRESHOLD_SECONDS and not has_run_log:
                without_log.append((path, duration))
        if not durations:
            continue
        mark = "yes" if not without_log else "NO"
        print(f"    {name}: {len(manifests)} partition(s), duration {min(durations):.1f}-"
              f"{max(durations):.1f}s, _run.log when > {RUNLOG_THRESHOLD_SECONDS:.0f}s: {mark}")
        if without_log:
            missing.append(name)
            for path, duration in without_log[:2]:
                rel_path = os.path.relpath(path, REPO)
                print(f"        missing _run.log: {rel_path} ({duration:.1f}s)")

    if not found_any:
        record("processing", "NOT MEASURED",
               "no local partition in data/ — run make daily / oltp-refresh-all / "
               "orders-refresh-all / ine-refresh / callejero-refresh before measuring")
        return
    if missing:
        record("processing", "NO",
               "source(s) with duration above the threshold and no progress log: "
               + ", ".join(missing))
        return
    record("processing", "YES",
           "every Source with a measured duration above the threshold has _run.log; the others "
           "run fast enough for the mechanism not to be justified (see detail above)")


# ---- Q2: THROUGHPUT / FAILURES — do they already reach Gold? ---------------------------

def q2_throughput_failures() -> None:
    print()
    print("Q2. THROUGHPUT / FAILURES — do declared_rows/failure_count/anomaly_count/complete "
          "reach Gold?")
    sql = read(FACT_INGESTION_RUN_SQL)
    fields = ["declared_rows", "failure_count", "anomaly_count", "complete"]
    missing = [c for c in fields if c not in sql]
    if missing:
        record("throughput/failures (Gold)", "NO",
               f"fact_ingestion_run.sql does not select: {', '.join(missing)}")
        return
    record("throughput/failures (Gold)", "YES",
           f"fact_ingestion_run.sql selects {', '.join(fields)}")


# ---- Q3: LATENCY — do duration_seconds/started_at_utc/finished_at_utc reach Gold? ------

def q3_latency() -> None:
    print()
    print("Q3. LATENCY — do duration_seconds/started_at_utc/finished_at_utc reach Gold?")
    sql_gold = read(FACT_INGESTION_RUN_SQL)
    sql_export = read(SNOWFLAKE_EXPORT_PY)
    fields = ["duration_seconds", "started_at_utc", "finished_at_utc"]

    not_in_gold = [c for c in fields if c not in sql_gold]
    not_in_stage = [c for c in fields if c not in sql_export]

    # Where the data ALREADY exists today, one layer below (Silver) — to make clear that the
    # gap is one of PROJECTION, not of missing data.
    silver_models = {
        "raw_manifest.sql": os.path.join(SILVER_MODELS_DIR, "mercadona", "raw_manifest.sql"),
        "silver_oltp_manifest.sql": os.path.join(
            SILVER_MODELS_DIR, "simulated_oltp", "silver_oltp_manifest.sql"),
        "silver_orders_manifest.sql": os.path.join(
            SILVER_MODELS_DIR, "simulated_orders", "silver_orders_manifest.sql"),
    }
    in_silver = []
    for name, path in silver_models.items():
        if os.path.exists(path) and all(c in read(path) for c in fields):
            in_silver.append(name)

    if not not_in_gold:
        record("latency (Gold)", "YES",
               f"fact_ingestion_run.sql already selects {', '.join(fields)}")
        return

    detail = (f"missing in fact_ingestion_run.sql: {', '.join(not_in_gold)}; "
              f"missing in the STG_INGESTION_RUN projection (warehouse_cut.py): "
              f"{', '.join(not_in_stage) or '(none — already unioned, only Gold is missing)'}")
    if len(in_silver) == len(silver_models):
        detail += (f"\nALL THREE FIELDS ALREADY EXIST in Silver ({', '.join(in_silver)}) — "
                   "the gap is SQL projection, not missing data.")
    record("latency (Gold)", "NO", detail)


# ---- Q4: FAILURES — FACT_INGESTION_RUN coverage per source -----------------------------

def q4_coverage() -> None:
    print()
    print("Q4. COVERAGE — how many of the 5 Sources make it into FACT_INGESTION_RUN?")
    sql_export = read(SNOWFLAKE_EXPORT_PY)
    expected_models = {
        "mercadona_catalog_api": "raw_manifest",
        "simulated_oltp": "silver_oltp_manifest",
        "simulated_orders": "silver_orders_manifest",
    }
    covered = [name for name, model in expected_models.items() if f"from {model}" in sql_export]
    missing = ["ine_population_api", "ine_callejero"]
    # PARTIAL, not YES: 3/5 is the real state, even though the absence of the other two is an
    # accepted decision (CR-005c) and not a defect. Labeling it YES would hide the fraction.
    record("coverage (Gold)",
           "YES" if len(covered) == 5 else "PARTIAL",
           f"covered ({len(covered)}/5, with wh axis): {', '.join(covered)}\n"
           f"left out (no wh axis, no manifest model): {', '.join(missing)}")


# ---- Q5: ERROR — traceback preserved in cli.py's unmapped exceptions -------------------

def q5_error() -> None:
    print()
    print("Q5. ERROR — do cli.py's unmapped exceptions preserve the traceback?")
    code = read(CLI_PY)
    sites = [m.start() for m in re.finditer(r"except Exception as exc:", code)]
    has_logging_import = bool(re.search(r"^import logging\s*$", code, re.MULTILINE))
    with_traceback = 0
    for pos in sites:
        # A small window after "except Exception as exc:" up to the next control
        # line — that is where `logging.exception(...)` would have to appear for this site.
        snippet = code[pos:pos + 300]
        if "logging.exception(" in snippet:
            with_traceback += 1

    if not sites:
        record("error (cli.py)", "NOT MEASURED", "no `except Exception` found")
        return
    if with_traceback == len(sites) and has_logging_import:
        record("error (cli.py)", "YES",
               f"{with_traceback}/{len(sites)} unmapped-exception sites call "
               "logging.exception(...) — full traceback preserved")
        return
    record("error (cli.py)", "NO",
           f"{len(sites)} `except Exception as exc:` sites (approximate lines via "
           f"text offset), {with_traceback} with logging.exception(...) — the rest "
           "print only str(exc), with no traceback")


# ---- Q6: PIPELINE STATE — does an Airflow task failure leave a structured record? ------

def q6_pipeline_state() -> None:
    print()
    print("Q6. PIPELINE STATE — does an Airflow task failure leave a record beyond the "
          "scheduler?")
    if not os.path.isdir(DAGS_DIR):
        record("pipeline state (Airflow)", "NOT MEASURED", f"{DAGS_DIR} not found")
        return
    dags = sorted(glob.glob(os.path.join(DAGS_DIR, "*.py")))
    with_callback = []
    without_callback = []
    for path in dags:
        name = os.path.basename(path)
        if "on_failure_callback" in read(path):
            with_callback.append(name)
        else:
            without_callback.append(name)

    if not dags:
        record("pipeline state (Airflow)", "NOT MEASURED", "no DAG found")
        return
    if not without_callback:
        record("pipeline state (Airflow)", "YES",
               f"{len(with_callback)}/{len(dags)} DAGs with on_failure_callback")
        return
    record("pipeline state (Airflow)", "NO",
           f"0/{len(dags)} DAGs with on_failure_callback/sla= — a task failure stays only "
           "in the scheduler's internal state")


# ---- --warehouse: checks the real Snowflake, optional ----------------------------------

def q_warehouse_optional(connection_name: str, database: str) -> None:
    print()
    print("EXTRA (--warehouse). Does the REAL Snowflake FACT_INGESTION_RUN already have the 3 "
          "latency columns?")
    try:
        from retail_platform.snowflake_load import connect
    except ImportError as exc:
        record("latency (live Snowflake)", "NOT VERIFIED",
               f"could not import retail_platform: {exc}")
        return
    try:
        connection = connect(connection_name)
    except Exception as exc:  # missing config, expired account, etc. — not this script's focus
        record("latency (live Snowflake)", "NOT VERIFIED",
               f"no connection ({type(exc).__name__}: {str(exc)[:200]}) — run with a "
               "configured Snowflake account to check live")
        return
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"select column_name from {database}.information_schema.columns "
            "where table_schema = 'GOLD' and table_name = 'FACT_INGESTION_RUN'"
        )
        columns = {row[0].lower() for row in cursor.fetchall()}
        fields = {"duration_seconds", "started_at_utc", "finished_at_utc"}
        missing = fields - columns
        record("latency (live Snowflake)", "NO" if missing else "YES",
               f"columns missing at the real target: {', '.join(sorted(missing)) or '(none)'}")
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--warehouse", action="store_true",
                        help="also checks the real Snowflake FACT_INGESTION_RUN")
    parser.add_argument("--connection", default=os.environ.get("SNOWFLAKE_CONNECTION", "spark_retail"))
    parser.add_argument("--database", default=os.environ.get("SNOWFLAKE_DATABASE", "RETAIL"))
    args = parser.parse_args()

    print("proof of the six required observability signals (BACKLOG.md, Observability)")
    print("=" * 88)

    q1_processing()
    q2_throughput_failures()
    q3_latency()
    q4_coverage()
    q5_error()
    q6_pipeline_state()
    if args.warehouse:
        q_warehouse_optional(args.connection, args.database)

    print()
    print("=" * 88)
    print("summary:")
    for signal, verdict, _ in results:
        print(f"  [{verdict:8}] {signal}")

    absent = [s for s, v, _ in results if v == "NO"]
    if absent:
        print()
        print(f"{len(absent)} signal(s) genuinely absent: {', '.join(absent)}")
        print("that is what authorizes closing exactly those — nothing more.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
