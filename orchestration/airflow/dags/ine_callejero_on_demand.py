"""On-demand DAG: RAW in object storage and Silver from the INE Callejero.

Third source, next to mercadona_catalog_daily and ine_population_on_demand. No cron, like
population — but for a different and even stronger structural reason:

  1. schedule=None: the Callejero HAS NO API. It is published twice a year for manual
     download from the INE website. A run only makes sense after someone has downloaded
     the new files and put them in CALLEJERO_IN inside the mounted repository — there is
     nothing for this DAG to "check" on its own, unlike population (which at least has an
     API to query on demand). Manual trigger (`make callejero-trigger` or the UI), always
     after a human download.
  2. no dedicated pool: there is no network request at all here — `extract` only copies
     local files (the source's CONTRACT.md, section 1). There is nothing to throttle.
  3. no guard_date: same reason as population — the immutable partition (partition.py)
     already protects against accidental rewrites, and "today's data labelled as
     yesterday" is impossible when the source is a local directory, not an API with a clock.
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
DATA_ROOT = f"{REPO}/data/callejero"

CALLEJERO_SOURCE_SRC = f"{REPO}/sources/ine-callejero-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")

# Where the files manually downloaded from the INE website must be, INSIDE the repository
# mounted in the container, before triggering this DAG.
CALLEJERO_IN = os.environ.get("RETAIL_CALLEJERO_IN", f"{REPO}/temp")
PROVINCES = os.environ.get("RETAIL_CALLEJERO_PROVINCES", "08,28,41,46")

# Exit codes from the source's CONTRACT.md section 7 (same values as the other two
# sources, which are independent — sharing them here creates no coupling).
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
    return _run([SOURCE_PYTHON, "-m", "ine_callejero_source", *argv],
                {"PYTHONPATH": CALLEJERO_SOURCE_SRC})


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


def partition_path(ingestion_date: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}"


def already_landed(ds: str, **_) -> bool:
    """Short-circuit that looks at the DESTINATION, not the local disk — same reasoning as
    the other two sources: exit 0 = nothing to do; exit 1 = landed but divergent, `land`
    self-heals; exit 2 = does not exist, carry on to extract."""
    code = _run_platform(["verify-landing", partition_path(ds)])
    if code == EXIT_OK:
        print("partition already landed and verified at the destination: nothing to do.")
        return False
    print(f"verify-landing returned {code}: continuing the pipeline.")
    return True


def extract(ds: str, **_) -> None:
    if os.path.exists(os.path.join(partition_path(ds), "_SUCCESS")):
        print("partition already complete and immutable on disk: extract skipped (not a failure).")
        return
    if not os.path.isdir(CALLEJERO_IN):
        raise RuntimeError(
            f"CALLEJERO_IN does not exist: {CALLEJERO_IN}. Download the Callejero files "
            f"from the INE website and put them in that directory (inside the mounted "
            f"repository) before triggering this DAG."
        )

    code = _run_source(
        ["extract", "--in", CALLEJERO_IN, "--out", DATA_ROOT, "--provinces", PROVINCES, "--date", ds]
    )
    if code == EXIT_FATAL:
        raise RuntimeError(f"extract failed fatally (exit {code}); retrying will not fix it")
    if code == EXIT_PARTIAL:
        raise RuntimeError(f"partial extract (exit {code}): there are entries in failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract returned exit {code}")


def validate(ds: str, **_) -> None:
    code = _run_source(["validate", partition_path(ds), "--strict"])
    if code != EXIT_OK:
        raise RuntimeError(f"validate rejected the partition (exit {code})")


def land(ds: str, **_) -> None:
    code = _run_platform(["land", partition_path(ds)])
    if code != EXIT_OK:
        raise RuntimeError(f"land failed (exit {code})")


def verify_landing(ds: str, **_) -> None:
    code = _run_platform(["verify-landing", partition_path(ds)])
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
    dag_id="ine_callejero_on_demand",
    description="RAW in object storage and Silver from the INE Callejero (official geography)",
    schedule=None,
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _log_task_failure,
    },
    tags=["source:ine", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    gate = ShortCircuitOperator(
        task_id="skip_if_landed",
        python_callable=already_landed,
        ignore_downstream_trigger_rules=False,
    )

    do_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        retries=0,
    )

    do_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        retries=0,
    )

    do_land = PythonOperator(
        task_id="land",
        python_callable=land,
        retries=2,
    )

    do_verify = PythonOperator(
        task_id="verify_landing",
        python_callable=verify_landing,
        retries=1,
    )

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # Same trigger_rule as the other two: if the gate short-circuited (nothing new),
        # silver still runs (cheap, idempotent) instead of being dragged into `skipped`.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    gate >> do_extract >> do_validate >> do_land >> do_verify >> build_silver
