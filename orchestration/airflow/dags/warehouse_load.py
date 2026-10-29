"""Analytical warehouse DAG: Silver cut -> local Postgres -> GOLD -> MART.

This is THE FIRST DAG in this repo that crosses the boundary between two engines. The other
five stay entirely on the lakehouse side (external source -> RAW -> Silver, all DuckDB);
this one takes the Silver any of them produced and carries it to another database, with
another credential, on another network. That is exactly where retries and exit codes
matter, which is why this edge belongs to Airflow and not to a loose script.

ENGINE: local Postgres, not Snowflake. This DAG pointed at Snowflake until 2026-09, when the
trial used by this project expired — the `load_stage` task started failing authentication
("Your free trial has ended... virtual warehouses have been suspended"). The destination
changed (see DECISIONS.md, CR-007); the three decisions below, and the export/load split,
stay the same, because the models/warehouse/ tree is engine-neutral.

THREE DECISIONS, each with its reason where it is enforced:

  1. ITS OWN DAG, not a TaskGroup at the end of mercadona_catalog_daily. The cut reads the
     Silver of ALL sources — catalog, population, Callejero and synthetic customers — so
     hanging it off one of them would make the warehouse refresh depend on whichever source
     ran last. To chain it, use TriggerDagRunOperator; to run it alone, trigger this one.

  2. IT TAKES NO `wh`. Unlike the source DAGs, there is no warehouse axis here: the cut
     covers all four in a single query, and the dimensional model is global.

  3. `export` AND `load` ARE SEPARATE TASKS, for the same reason `land` and
     `verify-landing` are. The cut fails on data (a wrong query, an empty Silver); the load
     fails on network or credentials. Merging them would make "the cut is wrong" arrive as
     a connection failure, and the retry would insist on something retrying cannot fix —
     hence `export` has retries=0 and `load` has 2.

The `bootstrap` (schemas, roles, grants) is NOT here on purpose: it runs once per
container, not every day. See `make warehouse-bootstrap`.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
STAGE_DIR = f"{REPO}/data/warehouse-cut"

PLATFORM_SRC = f"{REPO}/platform/src"
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")
DBT = os.environ.get("RETAIL_DBT", f"{REPO}/platform/.venv/bin/dbt")

# All three have defaults: unlike the Snowflake account, `warehouse-postgres` is the
# project's own convention (compose starts the container, no real secret) — see
# docker-compose.yml, which overrides HOST/PORT with the address INSIDE the compose network
# (the :5434 in .env only applies to whoever runs on the host).
WAREHOUSE_PG_HOST = os.environ.get("WAREHOUSE_PG_HOST", "localhost")
WAREHOUSE_PG_PORT = os.environ.get("WAREHOUSE_PG_PORT", "5434")
WAREHOUSE_PG_DB = os.environ.get("WAREHOUSE_PG_DB", "retail")
# EXPLICIT in the DAG, even though it is the loader's default: this is where one reads
# which role the automation uses. The load overwrites, and must not be able to create
# schemas or read GOLD/MART — same reason as the RETAIL_LOADER this field replaced.
WAREHOUSE_PG_LOAD_ROLE = os.environ.get("RETAIL_WAREHOUSE_PG_LOAD_ROLE", "retail_loader")

# The sources whose Silver the cut reads. If none has landed, there is nothing to load and
# the DAG short-circuits instead of failing — same `has-data` criterion as the other three.
SOURCES = ["mercadona_catalog_api", "ine_callejero", "ine_population_api", "simulated_oltp", "simulated_orders"]

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
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


def _run_platform(argv: list[str]) -> int:
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv], {"PYTHONPATH": PLATFORM_SRC})


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


def silver_has_data(**_) -> bool:
    """With no source landed, there is no Silver to cut.

    Short-circuit, not failure: a freshly cloned repository before the first ingestion is
    not broken — it just has nothing to load yet.
    """
    present = [s for s in SOURCES if _run_platform(["has-data", s]) == EXIT_OK]
    if not present:
        print("no source has landed: nothing to cut.")
        return False
    print(f"sources with data: {', '.join(present)}")
    return True


def export_recorte(**_) -> None:
    """Read the Silver and write local parquet. Does NOT talk to the warehouse.

    retries=0 on the task: a failure here is a DATA failure (wrong query, empty Silver,
    unmapped type). Repeating the same query on the same data gives the same error.
    """
    code = _run_platform(["export-cut", "--out", STAGE_DIR])
    if code != EXIT_OK:
        raise RuntimeError(f"export-cut failed (exit {code}); retrying will not fix it")


def load_stage(**_) -> None:
    """Drop+create the STAGE tables + COPY FROM STDIN + row-count re-check, table by table."""
    code = _run_platform([
        "load-postgres", "--stage-dir", STAGE_DIR,
        "--host", WAREHOUSE_PG_HOST, "--port", WAREHOUSE_PG_PORT, "--dbname", WAREHOUSE_PG_DB,
        "--role", WAREHOUSE_PG_LOAD_ROLE,
    ])
    if code == EXIT_FATAL:
        raise RuntimeError(f"load-postgres failed fatally (exit {code})")
    if code != EXIT_OK:
        raise RuntimeError(f"load-postgres failed the row-count re-check (exit {code})")


def build_gold(**_) -> None:
    """`--target postgres` switches off the whole Silver tree via the dbt_project guard."""
    code = _run([DBT, "build", "--project-dir", f"{REPO}/platform/dbt",
                 "--profiles-dir", f"{REPO}/platform/dbt", "--target", "postgres"],
                {"PYTHONPATH": PLATFORM_SRC})
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build --target postgres failed (exit {code})")


with DAG(
    dag_id="warehouse_load",
    description="Silver cut into local Postgres and rebuild of GOLD and MART",
    # No cron: the warehouse should be refreshed AFTER an ingestion, not at a fixed time
    # that might land in the middle of one. Chain it with TriggerDagRunOperator from the
    # source DAG, or trigger it by hand (`make warehouse-trigger`).
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _log_task_failure,
    },
    tags=["layer:warehouse", "engine:postgres"],
    doc_md=__doc__,
) as dag:

    gate = ShortCircuitOperator(
        task_id="skip_if_no_silver",
        python_callable=silver_has_data,
        ignore_downstream_trigger_rules=False,
    )

    do_export = PythonOperator(
        # task_id and callable keep their historical name (`recorte` = cut); renaming the
        # task_id would orphan the task's run history in Airflow.
        task_id="export_recorte",
        python_callable=export_recorte,
        # A data failure is not retryable: the same query on the same Silver fails the same way.
        retries=0,
    )

    do_load = PythonOperator(
        task_id="load_stage",
        python_callable=load_stage,
        # The network IS retryable, and the load is idempotent: STAGE tables are a full
        # refresh (drop+create), so repeating does not duplicate rows.
        retries=2,
    )

    do_build = PythonOperator(
        task_id="build_gold_and_mart",
        python_callable=build_gold,
        retries=1,
    )

    gate >> do_export >> do_load >> do_build
