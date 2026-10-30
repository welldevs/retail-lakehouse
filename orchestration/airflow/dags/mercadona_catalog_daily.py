"""Daily DAG: extract -> validate -> land -> verify -> silver.

Five decisions in this DAG come from MEASURED behavior of the upstream API and of the
Source, not from convention. Each one is commented where it is enforced:

  1. exit 2 from `extract` does NOT mean the day failed (complete, immutable partition);
  2. catchup=False and guard_date: backfill is IMPOSSIBLE for this source;
  3. 1-slot pool: the throttle lives inside the process, not across processes;
  4. the Source runs with PYTHONPATH and zero dependencies, without clashing with Airflow;
  5. data/ is scratch; object storage becomes the source of truth.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone

from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

try:  # Airflow 3.x
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Repository root: mounted in the container, or the checkout itself on the host.
REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
DATA_ROOT = f"{REPO}/data/mercadona"

# No code is imported by this DAG: everything is invoked as a subprocess, with PYTHONPATH
# pointing at the mounted repository. Editing a module therefore needs no image rebuild,
# and the DAG carries no dependency of the Source or of the platform.
SOURCE_SRC = f"{REPO}/sources/mercadona-catalog-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

# TWO RUNTIMES. The Source has dependencies = [] and runs on Airflow's own interpreter —
# there is no third-party package to clash with. The platform needs boto3/duckdb/dbt,
# which collide with Airflow's pins (jinja2, click, pydantic), so it lives in a separate
# venv. The defaults below point at the HOST venv so that `airflow dags test` works from
# the checkout; in the container, compose overrides them with /opt/platform-venv.
SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")

# Four cities, chosen by ASSORTMENT DIVERGENCE measured against the API, not by market
# size. Warehouses in the same city (mad1/mad2/mad3) have IDENTICAL product sets and differ
# only in price, so a second Madrid would cost 152 requests to add a single axis. Across
# cities, both axes vary. `alc1` was dropped: it duplicates `vlc1` (34.4% divergence, the
# lowest in the matrix — same autonomous community), while `svq1` comes in at 52.3%.
# See ARCHITECTURE.md.
#
# CAREFUL WHEN EDITING: an invalid `wh` does NOT fail. The API returns 200 and falls back
# to `vlc1`, so a typo here produces a partition labelled `wh=<typo>` holding Valencia's
# data — wrong yet internally consistent, and no downstream test catches it.
WAREHOUSES = ["mad1", "bcn1", "vlc1", "svq1"]

# ONE slot. The Source's throttle is PER PROCESS
# (http_client.py: 1/delay req/s measured from the start of the previous request), so two
# concurrent `extract` runs double the real rate against the host. Measured: 152 requests
# in 9 s produced ~20% 403s and intermittent blocking for minutes; sequential at 1.5 s gave
# 0 failures. With several warehouses, this pool is the ONLY thing keeping the rate safe.
API_POOL = "mercadona_api"

# Exit codes from the Source's CONTRACT.md section 7.
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
    """Run and FORWARD the output, line by line, to the task log.

    Letting the subprocess inherit the file descriptors looks like it works and does not:
    the output goes to the container's stdout, not to the task log, and a non-zero exit
    code reaches Airflow with no explanation next to it. Line-by-line streaming (instead of
    capturing everything at the end) matters because extract takes ~227 s: without it the
    task stays silent for four minutes.
    """
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
    return _run([SOURCE_PYTHON, "-m", "mercadona_catalog_source", *argv],
                {"PYTHONPATH": SOURCE_SRC})


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


def collection_date(logical_date: datetime, data_interval_end: datetime) -> str:
    """The day this run collects — the later of the run's two dates, as YYYY-MM-DD.

    NOT `ds`. A SCHEDULED cron run carries the START of its interval as logical date: the
    06:00 run that executes on 10-30 is dated 10-29. Using `ds` made `guard_date` refuse
    every scheduled run since the DAG existed (48 of 48 failed; every collection that
    landed was a manual trigger, whose logical date is the trigger moment). The interval
    END is the day the run actually executes. A manual run's interval ends at the last
    06:00 before the trigger, so for it the logical date is the later one. Taking the max
    covers both, and a manual trigger dated in the past still lands in the past — which
    `guard_date` then refuses.
    """
    return max(logical_date, data_interval_end).strftime("%Y-%m-%d")


def guard_date(logical_date: datetime, data_interval_end: datetime, **_) -> None:
    """Fail if the DAG run is not for today (UTC).

    THERE IS NO BACKFILL for this source: the API only serves TODAY's price. A run dated in
    the past would write today's prices under that date's key — silently wrong data that no
    downstream test would catch, because the partition would be internally consistent. The
    8-day gap between 2026-08-16 and 2026-08-24 is unrecoverable for this reason.

    THREE LAYERS, verified:
      1. Airflow itself refuses an execution_date in the future;
      2. catchup=False + start_date prevent scheduling runs before the start;
      3. this guard covers a MANUAL trigger for any date != today.

    While start_date == today, layer 3 is redundant. It becomes the ONLY protection the
    next day, when dates between start_date and today become schedulable by manual
    trigger — which is exactly when the mistake becomes possible.
    """
    day = collection_date(logical_date, data_interval_end)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if day != today:
        raise ValueError(
            f"run dated {day}, today is {today}. The API only serves today's price: "
            f"extracting for a past date would produce wrong data. Backfill does not exist "
            f"for this source."
        )
    print(f"date checks out: {day}")


def already_landed(logical_date: datetime, data_interval_end: datetime, warehouse: str,
                   **_) -> bool:
    """Short-circuit that looks at the DESTINATION, not the local disk.

    Reusing `verify-landing` as the gate, instead of checking a marker, has three effects
    an `os.path.exists` would not give:

      - exit 0 -> the partition is at the destination AND its checksums match: nothing to do;
      - exit 1 -> landed but divergent: we carry on, and `land` fixes it (an object with a
        wrong checksum is re-uploaded), i.e. the DAG self-heals;
      - exit 2 -> it does not even exist locally: we carry on to extract.

    Checking the LOCAL `_SUCCESS` here would be a mistake: it would skip `land` for a
    partition extracted by hand and never landed.
    """
    ds = collection_date(logical_date, data_interval_end)
    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code == EXIT_OK:
        print("partition already landed and verified at the destination: nothing to do.")
        return False
    print(f"verify-landing returned {code}: continuing the pipeline.")
    return True


def extract(logical_date: datetime, data_interval_end: datetime, warehouse: str,
            **_) -> None:
    """Run the Source's extract.

    The Source has ZERO runtime dependencies, so it runs on the worker's own interpreter via
    PYTHONPATH, with no venv and no clash with Airflow's pinned dependencies. That is the
    practical payoff of keeping dependencies = [].
    """
    ds = collection_date(logical_date, data_interval_end)
    # A complete partition on disk is IMMUTABLE: re-running returns exit 2. That is not a
    # failure of the day — it is guarantee 5 of the contract working. We detect it BEFORE
    # calling, so success is not turned into an error. A PARTIAL partition does not enter
    # here: resuming is exactly what we want to happen (guarantee 7).
    if os.path.exists(os.path.join(partition_path(ds, warehouse), "_SUCCESS")):
        print("partition already complete and immutable on disk: extract skipped (not a failure).")
        return

    code = _run_source(["extract", "--out", DATA_ROOT, "--wh", warehouse, "--date", ds])
    if code == EXIT_FATAL:
        # Unusable partition or invalid usage. Retrying does not help: the input does not change.
        raise RuntimeError(f"extract failed fatally (exit {code}); retrying will not fix it")
    if code == EXIT_PARTIAL:
        # Categories in failures[]. The partition exists but the contract rejects it at
        # validation (guarantee 16), so we stop here with the cause spelled out.
        raise RuntimeError(f"partial extract (exit {code}): there are entries in failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract returned exit {code}")


def validate(logical_date: datetime, data_interval_end: datetime, warehouse: str,
             **_) -> None:
    """Gate. Exit 1 rejects without retry: rereading the same bytes gives the same result."""
    ds = collection_date(logical_date, data_interval_end)
    code = _run_source(["validate", partition_path(ds, warehouse), "--strict"])
    if code != EXIT_OK:
        raise RuntimeError(f"validate rejected the partition (exit {code})")


def land(logical_date: datetime, data_interval_end: datetime, warehouse: str,
         **_) -> None:
    ds = collection_date(logical_date, data_interval_end)
    code = _run_platform(["land", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"land failed (exit {code})")


def verify_landing(logical_date: datetime, data_interval_end: datetime, warehouse: str,
                   **_) -> None:
    ds = collection_date(logical_date, data_interval_end)
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
    dag_id="mercadona_catalog_daily",
    description="RAW in object storage and Silver from the Mercadona catalog",
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 24),
    # BACKFILL IS IMPOSSIBLE: the API only serves today's price. Turning catchup on would
    # fill the lake with partitions dated in the past holding today's prices.
    catchup=False,
    # One run at a time. Two concurrent runs would fight over the same partition on disk
    # and double the request rate against the API.
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _log_task_failure,
    },
    tags=["source:mercadona", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    for warehouse in WAREHOUSES:
        check_date = PythonOperator(
            task_id=f"guard_date_{warehouse}",
            python_callable=guard_date,
        )

        gate = ShortCircuitOperator(
            task_id=f"skip_if_landed_{warehouse}",
            python_callable=already_landed,
            op_kwargs={"warehouse": warehouse},
            # The DEFAULT is True, and with more than one warehouse that is WRONG: the
            # short-circuit skips ALL of the downstream, ignoring each task's trigger_rule —
            # including that of `silver`, which is shared. Observed result: bcn1 landed
            # successfully and `silver` was skipped anyway, because mad1 short-circuited.
            #
            # With False, the gate skips only ITS OWN branch, and the normal skip cascade
            # handles the rest. `silver` goes back to deciding by its own trigger_rule.
            ignore_downstream_trigger_rules=False,
        )

        do_extract = PythonOperator(
            task_id=f"extract_{warehouse}",
            python_callable=extract,
            op_kwargs={"warehouse": warehouse},
            pool=API_POOL,
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

        check_date >> gate >> do_extract >> do_validate >> do_land >> do_verify

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # The DEFAULT rule (all_success) is WRONG here, and the bug only shows up with more
        # than one warehouse: one warehouse's short-circuit marks its verify_landing as
        # `skipped`, and with all_success that drags `silver` along — the freshly landed data
        # of the OTHER warehouse would never be transformed.
        #
        # none_failed_min_one_success gives the right behavior in all three cases:
        #   all short-circuited     -> nothing new, silver `skipped` (correct, and cheap);
        #   at least one landed     -> silver RUNS;
        #   any failed              -> silver does not run on top of suspect RAW.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # NO custom trigger_rule here: the default (all_success) is the right one, unlike
    # build_silver above.
    #   build_silver `skipped` (every wh short-circuited, nothing new today) -> this task
    #     is `skipped` too, by cascade. Correct: nothing changed, no reason to reload the
    #     warehouse.
    #   build_silver passed (at least one wh landed) -> triggers.
    #   build_silver failed -> does not trigger. We do not push suspect Silver to the warehouse.
    trigger_warehouse_load = TriggerDagRunOperator(
        task_id="trigger_warehouse_load",
        trigger_dag_id="warehouse_load",
        # Fire-and-forget: a warehouse failure (network, database down) stays isolated in
        # warehouse_load and must not fail the next day's catalog.
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Silver reads from object storage, so it depends on the verify of ALL warehouses.
    for task in dag.tasks:
        if task.task_id.startswith("verify_landing_"):
            task >> build_silver

    build_silver >> trigger_warehouse_load
