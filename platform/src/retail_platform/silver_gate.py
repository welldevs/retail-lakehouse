"""The Silver `dbt build` gate: what to EXCLUDE and which VAR to pass, decided in one place.

WHY THIS MODULE EXISTS, and the concrete defect that created it.

The Silver dbt project has 21 models, and not all of them can always be built. Two
reasons, both legitimate:

  1. A SOURCE THAT HAS NOT LANDED ANYTHING YET. `read_json` over an empty prefix does not
     return zero rows — it FAILS. A freshly cloned repository, or a new source, would
     bring the whole build down because of a tree that shouldn't even exist yet.
  2. THE ICEBERG PROJECTION. `silver_live_order_state` reads by `metadata_location`, and
     that path comes from the CATALOG, never from a storage scan — DuckDB refuses to guess,
     and the refusal is right. Without the stream plane up, there is no catalog to ask.

Until 2026-08-31 this decision lived in SIX places: the Makefile `silver` target, with all
five gates; and each of the five DAGs, with a single gate — its own source's. None of them
had the Iceberg one.

THE RESULT WAS EXACTLY WHAT ONE EXPECTS FROM DUPLICATED LOGIC: the
`mercadona_catalog_daily` DAG had NO gate at all (Mercadona always has data, so nobody
missed it) and started failing every day as soon as `silver_live_order_state` was born, in
Milestone 6 — 4 warehouses extracted, validated and landed successfully, and `silver`
falling over at the end with "no version-hint could be found". The data was right; the
gate just lived in the wrong file.

Now there is one place. `plan()` is PURE — it receives what was observed and returns the
arguments — so the whole decision is testable without network, MinIO or catalog.
`resolve()` does the observing. `build()` runs dbt. Makefile and DAGs call the same verb.

WHAT THIS MODULE DOES NOT DO: it does not decide what an error is. If a source HAS data and
its model fails, the build fails — as it should. Excluding is for what doesn't exist yet,
never for what is broken.
"""

from __future__ import annotations

import os
import subprocess

# Each source with the prefix that identifies it in RAW and the models that die without it.
# The order is the order in which the sources arrived, and the names are the ones dbt knows.
SOURCE_MODELS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("mercadona_catalog_api", (
        "silver_product_price", "silver_category", "silver_price_change", "raw_manifest",
    )),
    ("ine_population_api", (
        "silver_ine_population_series", "silver_ine_population_by_municipality",
    )),
    ("ine_callejero", (
        "silver_callejero_sections", "silver_callejero_population_units",
        "silver_callejero_streets", "silver_callejero_pseudo_addresses",
        "silver_callejero_tramos",
    )),
    ("simulated_oltp", (
        "silver_customer", "silver_oltp_manifest",
    )),
    ("simulated_orders", (
        "silver_order_event", "silver_order", "silver_order_line", "silver_orders_manifest",
        # The seal test reads `silver_orders_manifest`; with no order partition landed it
        # would try to `ref()` something that was not built — same reason as the Iceberg nodes.
        "assert_raw_matches_the_frozen_capture",
    )),
)

# THE ICEBERG TABLES, and the nodes that die without each one.
#
# Model and test go in and out TOGETHER: excluding only the model would leave the test
# trying to `ref()` something that was not built.
#
# WHY THIS BECAME A LIST IN PHASE 7. It used to be a pair of constants, because there was
# only the live projection. When the stock ledger arrived, the options were a second pair
# of constants with the same logic written again — which is how the gate had ended up
# scattered across six files — or a list. The practical gain is that a third Iceberg
# table, if it ever exists, is one line here and nothing else.
#
# Each entry is (key, dbt var, nodes). The KEY is what `resolve()` uses to ask for the
# metadata; the VAR is how dbt receives the path.
ICEBERG_TABLES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("live_order_state", "live_order_state_metadata", (
        "silver_live_order_state", "assert_live_projection_matches_batch_fold",
    )),
    # THE STOCK LEDGER IS WRITTEN BY SPARK, which is optional by decision. A repository
    # where nobody ran `make stock-ledger` builds the whole Silver without it — and that is
    # the property that keeps Spark out of the project's default path.
    ("stock_ledger", "stock_ledger_metadata", (
        "silver_stock_ledger",
        "assert_stock_ledger_conserves_the_balance",
        "assert_stock_replenishment_respects_the_lead_time",
        "assert_stock_window_can_exercise_replenishment",
    )),
)

# Compatibility for callers that only need the live projection.
ICEBERG_NODES = ICEBERG_TABLES[0][2]
ICEBERG_VAR = ICEBERG_TABLES[0][1]


def plan(landed: dict, iceberg_metadata) -> list[str]:
    """`dbt build` arguments, given what was observed. PURE: it looks at nothing.

    `landed` maps source prefix -> whether there is a landed object. A prefix missing from
    the dictionary counts as NOT landed, which is the safe side: excluding a model that
    could have been built costs one run; trying to build one that can't costs the build.

    `iceberg_metadata` maps an ICEBERG_TABLES key -> metadata path, or None. A plain string
    is still accepted and applies to the live projection, which was the only case until
    Phase 7 — the DAGs and the Makefile call `build()`, never `plan()`, so the old form only
    survives because of the tests that exercise it.
    """
    if iceberg_metadata is None or isinstance(iceberg_metadata, str):
        iceberg_metadata = {ICEBERG_TABLES[0][0]: iceberg_metadata}

    exclude: list[str] = []
    for prefix, models in SOURCE_MODELS:
        if not landed.get(prefix):
            exclude.extend(models)

    variables: dict[str, str] = {}
    for key, var, nodes in ICEBERG_TABLES:
        path = iceberg_metadata.get(key)
        if path:
            variables[var] = path
        else:
            exclude.extend(nodes)

    arguments: list[str] = []
    if exclude:
        arguments += ["--exclude", *exclude]
    if variables:
        # `--vars` as inline YAML. The path is an s3:// URI with slashes and colons, so it
        # goes in quotes: without them YAML reads `s3:` as a key and the var arrives empty.
        body = ", ".join(f'{var}: "{path}"' for var, path in sorted(variables.items()))
        arguments += ["--vars", f"{{{body}}}"]
    return arguments


def has_landed(prefix: str, config=None) -> bool:
    """Is there at least one object under <raw_bucket>/<prefix>/?"""
    from .config import from_env

    config = config or from_env()
    response = config.client().list_objects_v2(
        Bucket=config.raw_bucket, Prefix=prefix.rstrip("/") + "/", MaxKeys=1
    )
    return response.get("KeyCount", 0) > 0


def iceberg_metadata() -> dict:
    """Path of each table's current metadata, or None where the catalog doesn't answer.

    Absence is NOT an error here, for two different reasons: the stream plane comes up on
    demand (`make stream-up`), and Spark is optional by decision. A `make silver` with
    neither of them must build all the other models in peace.
    """
    result: dict[str, str | None] = {}
    try:
        from .orders_projection import metadata_location

        result["live_order_state"] = metadata_location() or None
    except Exception:
        result["live_order_state"] = None
    try:
        from .stock_bridge import metadata_location as stock_metadata_location

        result["stock_ledger"] = stock_metadata_location() or None
    except Exception:
        result["stock_ledger"] = None
    return result


def resolve(config=None) -> tuple[list[str], dict]:
    """Observes the world and returns (dbt arguments, what was observed)."""
    from .config import from_env

    config = config or from_env()
    landed = {}
    for prefix, _ in SOURCE_MODELS:
        try:
            landed[prefix] = has_landed(prefix, config)
        except Exception:
            # Object storage being down is a real problem — but the one who reports it is
            # dbt, with the engine's message, not a gate that swallowed the exception.
            landed[prefix] = False
    metadata = iceberg_metadata()
    return plan(landed, metadata), {"landed": landed, "iceberg_metadata": metadata}


def build(project_dir: str, profiles_dir: str, *, dbt: str = None,
          extra: list[str] = None, config=None) -> tuple[int, list[str], dict]:
    """Runs `dbt build` with the gate applied. Returns (code, arguments, observed)."""
    arguments, observed = resolve(config)
    executable = dbt or os.environ.get("RETAIL_DBT", "dbt")
    command = [
        executable, "build",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        *arguments, *(extra or []),
    ]
    return subprocess.call(command), arguments, observed
