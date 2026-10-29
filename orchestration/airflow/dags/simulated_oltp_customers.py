"""On-demand DAG: synthetic customer base anchored in real geography.

Fourth source, and the ONLY DERIVED one: the other three are upstream external data, this
one consumes the Silver they produced. That changes the shape of the DAG in two places that
have no parallel in the others:

  1. `export_reference` runs ONCE and feeds all four warehouses. The Source is FROZEN
     (dependencies = []) and cannot open a connection to the Lakehouse, so the platform
     first materializes what it needs into three flat JSON files. Putting it inside each
     warehouse's branch would repeat the same heavy query (216 thousand address
     candidates) four times to produce the same file.
  2. There is NO NETWORK in any step — no pool, no throttle, no guard_date. `extract` reads
     local JSON and writes local JSON; the only external I/O is `land` to object storage.
     The whole extraction for the four warehouses takes ~6 s.

schedule=None for the same structural reason as the Callejero: there is no "new data" to
fetch. The customer base changes when SOMEONE decides to change it — more customers,
another seed, or because the upstream Callejero/population were re-ingested. Manual
trigger, with the parameters below.

REGENERATING THE BASE IS ADDITIVE. The generator consumes a single random.Random(seed) in a
fixed order and nothing before the loop depends on `count`, so the first N customers of a
larger generation are byte-for-byte the same as before — verified end to end (200 -> 5,000
preserved the 200). That is why `overwrite` exists as a parameter instead of being
forbidden: raising `customers_per_wh` does not invalidate the customers that already exist.
Conditions: same seed, same reference, same ingestion_date, and the SAME REGISTRATION
SCOPE — since Phase 6, the minimum age, the penetration rate and the allocation rule are on
that list, because all three swap the people behind the same ids just as much as the seed
does. The manifest stores all of them in `config`/`reference`, and `history` accumulates
the previous runs, so a regeneration leaves a trail.

THE SIZE OF THE BASE, HOWEVER, NO LONGER COMES FROM HERE by default. `customers_per_wh` is
null, and in that case `extract` uses the target that `export-oltp-reference` derived from
each warehouse's adult population. Filling in the parameter is an explicit override, and
the manifest records `count_source: "cli"`.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
DATA_ROOT = f"{REPO}/data/oltp"
REFERENCE_ROOT = f"{REPO}/data/oltp-reference"

OLTP_SOURCE_SRC = f"{REPO}/sources/simulated-oltp-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")

# The same four warehouses as Mercadona, and not by coincidence: each one's service area
# (AUF) comes from warehouse_service_area, derived around each `wh`'s home municipality.
WAREHOUSES = os.environ.get("RETAIL_OLTP_WAREHOUSES", "mad1,bcn1,svq1,vlc1").split(",")

# Exit codes from the source's CONTRACT.md section 7.
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
    """Run and FORWARD the output, line by line, to the task log."""
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    print("$", " ".join(argv))
    process = subprocess.Popen(
        argv, cwd=REPO, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
    )
    with process.stdout:
        for line in process.stdout:
            print(line, end="")
    return process.wait()


def _run_source(argv: list[str]) -> int:
    """Invoke the Source on Airflow's interpreter. Safe because dependencies = []."""
    return _run([SOURCE_PYTHON, "-m", "simulated_oltp_source", *argv],
                {"PYTHONPATH": OLTP_SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoke the platform in its separate venv, with code from the mounted repo."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def _log_task_failure(context: dict) -> None:
    """CR-005: the only structured record a task failure leaves, besides the scheduler's
    internal state. Uses the logger the task already writes to
    (`logging.getLogger("airflow.task")`) — no new file, no new service.
    """
    ti = context.get("task_instance")
    exc = context.get("exception")
    logging.getLogger("airflow.task").error(
        "task failed dag_id=%s task_id=%s run_id=%s try_number=%s exception_type=%s "
        "exception=%s",
        context.get("dag").dag_id if context.get("dag") else "?",
        ti.task_id if ti else "?",
        context.get("run_id"),
        ti.try_number if ti else "?",
        type(exc).__name__ if exc is not None else "?",
        exc,
    )


def partition_path(ingestion_date: str, warehouse: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}/wh={warehouse}"


def reference_path(ingestion_date: str) -> str:
    return f"{REFERENCE_ROOT}/ingestion_date={ingestion_date}"


def export_reference(ds: str, **_) -> None:
    """The bridge between the Lakehouse and the FROZEN Source. Runs once, covers all four wh.

    `export-oltp-reference` already refuses with an actionable message if the Callejero or
    the population have not landed — there is no gate to duplicate here.
    """
    code = _run_platform(["export-oltp-reference", "--out", REFERENCE_ROOT, "--date", ds])
    if code != EXIT_OK:
        raise RuntimeError(f"export-oltp-reference failed (exit {code})")


def already_landed(warehouse: str, ds: str, params: dict, **_) -> bool:
    """Short-circuit that looks at the DESTINATION, same reasoning as the other three sources.

    With `overwrite`, the gate switches OFF. Without this the parameter would be inert:
    whoever triggers a regeneration asks for exactly the case the gate considers "nothing
    to do" — the partition has already landed — and the whole branch would be skipped
    before extract.
    """
    if params.get("overwrite"):
        print("overwrite requested: gate off, the branch runs even though already landed.")
        return True

    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code == EXIT_OK:
        print("partition already landed and verified at the destination: nothing to do.")
        return False
    print(f"verify-landing returned {code}: continuing the pipeline.")
    return True


def extract(warehouse: str, ds: str, params: dict, **_) -> None:
    partition = partition_path(ds, warehouse)
    overwrite = bool(params.get("overwrite"))

    if os.path.exists(os.path.join(partition, "_SUCCESS")) and not overwrite:
        print("partition already complete and immutable on disk: extract skipped (not a failure).")
        print("Trigger with overwrite=true to regenerate the base.")
        return

    reference = reference_path(ds)
    if not os.path.isdir(reference):
        raise RuntimeError(
            f"missing reference: {reference}. The export_reference task should have "
            f"produced it — check its log."
        )

    argv = [
        "extract",
        "--reference", reference,
        "--out", DATA_ROOT,
        "--wh", warehouse,
        "--date", ds,
        "--seed", str(params["seed"]),
    ]
    # --count ONLY when someone asks for it explicitly. When omitted, the Source uses the
    # target `export-oltp-reference` derived from that warehouse's adult population — see
    # the parameter's docstring. Passing a number here by default is what made all four
    # warehouses come out the same size.
    if params.get("customers_per_wh"):
        argv.extend(["--count", str(params["customers_per_wh"])])
    if overwrite:
        argv.append("--overwrite")

    code = _run_source(argv)
    if code == EXIT_FATAL:
        raise RuntimeError(f"extract failed fatally (exit {code}); retrying will not fix it")
    if code == EXIT_PARTIAL:
        raise RuntimeError(f"partial extract (exit {code}): there are entries in failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract returned exit {code}")


def validate(warehouse: str, ds: str, **_) -> None:
    """--reference is NOT optional here, unlike the other three sources.

    The central guarantee — every customer lives at a real address in its warehouse's
    service area — can only be re-checked by rereading the same reference that generated
    the partition.
    """
    code = _run_source([
        "validate", partition_path(ds, warehouse),
        "--reference", reference_path(ds), "--strict",
    ])
    if code != EXIT_OK:
        raise RuntimeError(f"validate rejected the partition (exit {code})")


def land(warehouse: str, ds: str, **_) -> None:
    code = _run_platform(["land", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"land failed (exit {code})")


def verify_landing(warehouse: str, ds: str, **_) -> None:
    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"verify-landing rejected the destination (exit {code})")


def silver(**_) -> None:
    """Silver `dbt build`, with the gate applied by the platform.

    THE GATE DOES NOT LIVE HERE, and it used to — that was the defect. Each DAG carried its
    own partial copy of the decision (excluding its OWN source, and nothing else), the
    Makefile carried the full version, and all six diverged. This DAG had no gate at all and
    failed every day from the moment `silver_live_order_state` was born, because the Iceberg
    projection can only be read when the catalog responds — and the catalog has nothing to
    do with this DAG's source.

    Now there is one verb: `retail_platform silver-build` observes what has landed and
    whether the catalog responds, and builds `--exclude`/`--vars` itself. See silver_gate.py.
    """
    code = _run_platform([
        "silver-build",
        "--project-dir", f"{REPO}/platform/dbt",
        "--profiles-dir", f"{REPO}/platform/dbt",
    ])
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build failed (exit {code})")


with DAG(
    dag_id="simulated_oltp_customers",
    description="Synthetic customer base with real Callejero addresses (RAW + Silver)",
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
    max_active_runs=1,
    params={
        # NULL, not a number. The size of the base is derived from each warehouse's adult
        # population by the penetration rate declared in customer_premises_seed, and comes
        # in the reference header. Filling it in here is an explicit OVERRIDE, recorded as
        # `count_source: cli` in the manifest — not the normal path.
        "customers_per_wh": None,
        "seed": 20260827,
        # Deliberate regeneration. Raising customers_per_wh with the SAME seed and the SAME
        # date is additive: existing customers are preserved byte for byte. Changing the
        # seed swaps the people behind the same ids — and so does changing the minimum age,
        # the rate or the allocation rule. The manifest records all of that in `history`.
        "overwrite": False,
    },
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _log_task_failure,
    },
    tags=["source:simulated", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    build_reference = PythonOperator(
        task_id="export_reference",
        python_callable=export_reference,
        retries=1,
    )

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # Same rule as the Mercadona DAG, and for the same defect measured there: with
        # all_success, a single short-circuited warehouse would drag silver into `skipped`
        # and the freshly landed data of the other three would never be transformed.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for warehouse in WAREHOUSES:
        gate = ShortCircuitOperator(
            task_id=f"skip_if_landed_{warehouse}",
            python_callable=already_landed,
            op_kwargs={"warehouse": warehouse},
            # With more than one warehouse the default True is WRONG: the short-circuit
            # would skip ALL of the downstream, ignoring each task's trigger_rule, including
            # that of `silver`, which is shared. See mercadona_catalog_daily.
            ignore_downstream_trigger_rules=False,
        )

        do_extract = PythonOperator(
            task_id=f"extract_{warehouse}",
            python_callable=extract,
            op_kwargs={"warehouse": warehouse},
            retries=0,
        )

        do_validate = PythonOperator(
            task_id=f"validate_{warehouse}",
            python_callable=validate,
            op_kwargs={"warehouse": warehouse},
            retries=0,
        )

        do_land = PythonOperator(
            task_id=f"land_{warehouse}",
            python_callable=land,
            op_kwargs={"warehouse": warehouse},
            # The network IS retryable, and `land` is idempotent: an object with the
            # expected checksum is skipped, not re-uploaded.
            retries=2,
        )

        do_verify = PythonOperator(
            task_id=f"verify_landing_{warehouse}",
            python_callable=verify_landing,
            op_kwargs={"warehouse": warehouse},
            retries=1,
        )

        build_reference >> gate >> do_extract >> do_validate >> do_land >> do_verify
        do_verify >> build_silver
