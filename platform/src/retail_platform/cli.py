"""Command-line interface of the platform.

The list of subcommands lives in `python -m retail_platform --help`, generated from the
parser below — a hand-written copy here drifted and missed nine of them.

Exit codes, in the same spirit as the Source's CONTRACT.md section 7, so that the
orchestrator decides by code and not by parsing logs:

    0  success
    1  verification failed
    2  invalid usage / unusable partition
    3  unhandled exception
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

from . import __version__
from .config import ConfigError, from_env
from .land import LandingError, land
from .manifest import ManifestError
from .customers_reference import DEFAULT_SEEDS_DIR
from .verify import verify

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_FATAL = 2
EXIT_UNHANDLED = 3


def _cmd_land(args) -> int:
    config = from_env()
    result = land(config, args.partition)
    print(f"partition ........ {result.partition}")
    print(f"target ........... s3://{result.bucket}/{result.prefix}")
    print(f"objects .......... {result.total} ({result.uploaded} uploaded, "
          f"{result.skipped} skipped)")
    print(f"bytes uploaded ... {result.bytes_uploaded}")
    print("OK: partition landed, _SUCCESS written last.")
    return EXIT_OK


def _cmd_verify(args) -> int:
    config = from_env()
    errors, summary = verify(config, args.partition)
    print(f"partition ........ {summary['partition']}")
    print(f"target ........... s3://{summary['bucket']}/{summary['prefix']}")
    print(f"run_id ........... {summary['run_id']}")
    print(f"objects .......... {summary['checked']} read and rechecked")
    print(f"orphans .......... {summary['orphans']}")
    print(f"bytes ............ {summary['bytes']}")
    print()
    if errors:
        print(f"FAILED ({len(errors)} problem(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: checksums, sizes and inventory rechecked against the object storage.")
    return EXIT_OK


def _cmd_prune_local(args) -> int:
    """Remove the LOCAL copy of a partition, but only after proving it is intact in the
    object storage.

    The local `data/` is extraction scratch: after `land` + `verify-landing`, the object
    storage is the truth and the local copy is redundant. It is not small — one INE
    partition takes between 264 and 384 MB, and nothing was ever deleted.

    The order here is the guarantee, and there are TWO checks before any removal:

      1. the local copy matches its own manifest (sha256 and size recomputed);
      2. the target matches that same manifest — the same check as verify-landing.

    The first one is not redundant. `verify-landing` compares the OBJECT with the MANIFEST,
    and would pass with a local file corrupted after it was landed — the removal would look
    safe without anything having checked the local bytes. Without (1), the promise
    "I only delete what I proved intact" would be bigger than what the code does.

    There is no --force mode: a partition that is not intact on both sides is not
    redundant, and the only way to lose data would be to delete exactly that one.
    """
    import shutil

    from .manifest import ManifestError
    from .manifest import read as read_manifest

    config = from_env()
    partition = os.path.normpath(args.partition)

    # Guard against a wrong argument turning into an rm -rf on something that is not a partition.
    manifest_file = os.path.join(partition, "_manifest.json")
    if not os.path.isfile(manifest_file):
        print(f"ERROR: {partition} does not look like a partition (no _manifest.json). "
              f"Nothing was deleted.")
        return EXIT_FATAL

    # (1) is the local copy still what it declares to be?
    try:
        declared = read_manifest(partition)
    except ManifestError as exc:
        print(f"ERROR: unusable local manifest ({exc}). Nothing was deleted.")
        return EXIT_FATAL

    local_problems: list[str] = []
    for entry in declared.files:
        if not os.path.exists(entry.local_path):
            local_problems.append(f"local file missing: {entry.path}")
            continue
        sha, size = entry.digest_on_disk()
        if sha != entry.sha256:
            local_problems.append(f"local sha256 differs from the manifest: {entry.path}")
        elif size != entry.bytes:
            local_problems.append(f"local size differs from the manifest: {entry.path}")
    if local_problems:
        print(f"partition ........ {partition}")
        print()
        print(f"REFUSED ({len(local_problems)} problem(s)) — the local copy was NOT deleted:")
        for item in local_problems:
            print(f"  - {item}")
        print("The local copy does not match its own manifest: look before removing.")
        return EXIT_FAILED

    # (2) does the target match the same manifest?
    errors, summary = verify(config, partition)
    if errors:
        print(f"partition ........ {summary['partition']}")
        print(f"target ........... s3://{summary['bucket']}/{summary['prefix']}")
        print()
        print(f"REFUSED ({len(errors)} problem(s)) — the local copy was NOT deleted:")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED

    bytes_local = sum(
        os.path.getsize(os.path.join(base, name))
        for base, _, names in os.walk(partition)
        for name in names
    )
    shutil.rmtree(partition)
    print(f"partition ........ {summary['partition']}")
    print(f"target ........... s3://{summary['bucket']}/{summary['prefix']}")
    print(f"local files ...... {len(declared.files)} rechecked against the manifest")
    print(f"objects .......... {summary['checked']} rechecked at the target")
    print(f"freed ............ {bytes_local} bytes")
    print("OK: local copy removed; the object storage still holds the intact partition.")
    return EXIT_OK


DEFAULT_QUERY = """
select ingestion_date, warehouse, count(*) as row_count,
       count(distinct source_product_id) as products
from silver_product_price group by 1, 2 order by 1
"""


def _cmd_query(args) -> int:
    from .query import connect, connect_lakehouse

    config = from_env()
    # Default: in-memory views over the parquet, without touching the .duckdb. See connect_lakehouse.
    connection = (connect(config, database=args.database) if args.database
                  else connect_lakehouse(config))
    sql = args.sql or DEFAULT_QUERY
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    rows = result.fetchall()
    widths = [
        max(len(columns[i]), *(len(str(r[i])) for r in rows)) if rows else len(columns[i])
        for i in range(len(columns))
    ]
    print("  ".join(name.ljust(widths[i]) for i, name in enumerate(columns)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(value).ljust(widths[i]) for i, value in enumerate(row)))
    print(f"\n{len(rows)} row(s)")
    return EXIT_OK


def _cmd_silver_build(args) -> int:
    """Silver `dbt build` with the gate applied — the ONLY place that decides what to exclude.

    Before 2026-08-31 this decision lived in six files: the Makefile's `silver` target,
    with the five gates, and each of the five DAGs, with its own source's gate and no
    other. The Mercadona DAG had no gate at all — Mercadona always has data, so nobody
    missed it — and started failing every day as soon as `silver_live_order_state` was
    born in Milestone 6. See silver_gate.py.
    """
    from .silver_gate import build

    code, arguments, observed = build(
        args.project_dir, args.profiles_dir, dbt=args.dbt, extra=args.dbt_args or None
    )
    if not args.quiet:
        for prefix, present in observed["landed"].items():
            print(f"{prefix:.<28} {'landed' if present else 'ABSENT (excluded)'}")
        for table, metadata in (observed["iceberg_metadata"] or {}).items():
            print(f"{'iceberg ' + table:.<28} {metadata or 'CATALOG UNAVAILABLE (excluded)'}")
        if arguments:
            print(f"{'arguments':.<28} {' '.join(arguments)}")
    return EXIT_OK if code == 0 else EXIT_FAILED


def _cmd_has_data(args) -> int:
    """Is there at least one landed object under <raw_bucket>/<prefix>/?

    Exists so the Makefile can decide, BEFORE calling `dbt build`, whether to exclude from
    the build the Silver model of a source that has not landed anything yet — without it,
    a `dbt build` of the whole project would fail just because of a newly added source,
    even when the others have data as usual. Generic: any source can be queried by its
    prefix, not just one in particular.
    """
    config = from_env()
    prefix = args.prefix.rstrip("/") + "/"
    response = config.client().list_objects_v2(
        Bucket=config.raw_bucket, Prefix=prefix, MaxKeys=1
    )
    return EXIT_OK if response.get("KeyCount", 0) > 0 else EXIT_FAILED


def _cmd_export_oltp_reference(args) -> int:
    """Materialize Silver as three flat JSON files for the simulated OLTP Source.

    The Source is FROZEN (dependencies = []) and cannot open a connection to the Lakehouse;
    this subcommand is the only bridge, and it is a physical contract, not an import. See
    customers_reference.py.
    """
    from .customers_reference import ReferenceExportError, export

    config = from_env()
    for prefix in ("ine_callejero", "ine_population_api"):
        response = config.client().list_objects_v2(
            Bucket=config.raw_bucket, Prefix=f"{prefix}/", MaxKeys=1
        )
        if not response.get("KeyCount", 0):
            print(
                f"ERROR: nothing landed under {prefix}/. Run `make callejero-refresh` and "
                f"`make ine-refresh` before exporting the reference."
            )
            return EXIT_FATAL

    out_dir = os.path.join(args.out, f"ingestion_date={args.date}")
    try:
        summary = export(config, out_dir, seeds_dir=args.seeds_dir)
    except ReferenceExportError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    print(f"target ........... {summary['out_dir']}")
    print(f"warehouses ....... {', '.join(summary['warehouses'])}")
    print(f"addresses ........ {summary['address_candidates']} candidates "
          f"({summary['orphan_tramos_excluded']} orphan tramo(s) excluded)")
    print(f"municipalities ... {summary['municipalities']}")
    allocation = summary["customer_allocation"]
    print(f"ages ............. {summary['age_rows']} rows "
          f"(year={summary['age_year']} fk_periodo={summary['age_fk_periodo']}, "
          f"age >= {allocation['min_customer_age']} renormalized)")
    print(f"population ....... year={summary['population_year']}")
    print(f"allocation ....... {allocation['rule']} over "
          f"{allocation['population_basis']}, rate {allocation['penetration_pct']}% "
          f"({allocation['penetration_source']})")
    print(f"  population ..... {allocation['served_population']:,} served, "
          f"{allocation['served_adult_population']:,.0f} adult")
    for row in allocation["by_warehouse"]:
        print(f"  {row['wh']} ........... {row['customers']:,} customers "
              f"({row['adult_population']:,.0f} adults)")
    print(f"  total .......... {allocation['total_customers']:,} customers "
          f"(a consequence of population, not a split quota)")
    for name, size in sorted(summary["bytes"].items()):
        print(f"  {name} ... {size} bytes")
    print("OK: reference exported.")
    return EXIT_OK


def _cmd_export_orders_reference(args) -> int:
    """Materialize Silver as four flat JSON files for the simulated Orders Source.

    Same bridge as `export-oltp-reference`, one level further: the Source is FROZEN and
    cannot open a connection to the Lakehouse, so customer, catalog, price calendar and the
    premises table cross as a physical contract. See orders_reference.py.
    """
    from .orders_reference import OrdersReferenceError, export

    config = from_env()
    for prefix in ("mercadona_catalog_api", "simulated_oltp"):
        response = config.client().list_objects_v2(
            Bucket=config.raw_bucket, Prefix=f"{prefix}/", MaxKeys=1
        )
        if not response.get("KeyCount", 0):
            print(
                f"ERROR: nothing landed under {prefix}/. An order needs a real customer and "
                f"a real catalog: run `make daily` and `make oltp-refresh-all` first."
            )
            return EXIT_FATAL

    out_dir = os.path.join(args.out, f"ingestion_date={args.date}")
    try:
        summary = export(
            config, out_dir, args.window_from, args.window_to, seeds_dir=args.seeds_dir
        )
    except OrdersReferenceError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    print(f"target ........... {summary['out_dir']}")
    print(f"window ........... {summary['window_from']} to {summary['window_to']} "
          f"({summary['days']} day(s))")
    print(f"warehouses ....... {', '.join(summary['warehouses'])}")
    print(f"customers ........ {summary['customers']} "
          f"(generations: {', '.join(summary['customer_ingestion_dates'])})")
    print(f"catalog .......... {summary['catalog_rows']} rows in "
          f"{len(summary['catalog_ingestion_dates'])} snapshot(s): "
          f"{', '.join(summary['catalog_ingestion_dates'])}")
    print(f"carry-forward .... {summary['carried_forward_rows']} (warehouse, day) pair(s) "
          f"without a price observed on that day")
    print(f"premises ......... sha256={summary['premises_sha256'][:16]}... (all synthetic)")
    print(
        f"demand ........... {summary['demand_model_version']} — "
        f"{summary['demand_groups']} groups, {summary['demand_cohorts']} cohorts, "
        f"IPF in {summary['demand_ipf_iterations']} iteration(s)"
    )
    print(
        f"eligible ......... {summary['eligible_customers']} customer(s); "
        f"{summary['below_min_buyer_age']} below the minimum buying age"
    )
    for name, size in sorted(summary["bytes"].items()):
        print(f"  {name} ... {size} bytes")
    print("OK: reference exported.")
    return EXIT_OK


def _cmd_orders_oltp_ddl(args) -> int:
    """Print the OLTP DDL without connecting to anything.

    Same reason as `snowflake-ddl`: reviewing the schema — the CHECKs, the partial index,
    the status vocabulary — should not require a running database. It is what the platform
    promises, in text, before promising it over the network.
    """
    from .orders_oltp import DDL

    print(DDL.strip())
    return EXIT_OK


def _cmd_orders_oltp_init(args) -> int:
    from .orders_oltp import OrdersOltpError, connect, create_schema, drop_schema

    try:
        with connect(args.dsn) as connection:
            if args.reset:
                # `prune-local` has no `--force` because deleting a landed partition is the
                # operator's decision. Here the criterion is different and the asymmetry is
                # deliberate: the OLTP is a REPLICA of the log, rebuildable in minutes by
                # `orders-apply`. What it keeps of its own — `published_at` — is redone by
                # draining the outbox again. Nothing here is the only copy of anything.
                print("--reset: dropping orders, order_line and outbox...")
                drop_schema(connection)
            create_schema(connection)
        print(f"dsn .............. {args.dsn}")
        print("OK: OLTP schema ready (orders, order_line, outbox).")
        return EXIT_OK
    except OrdersOltpError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL


def _cmd_orders_apply(args) -> int:
    """Replicate the event log into the OLTP: state and outbox in the SAME transaction, per event.

    This is where the Kafka trigger becomes satisfied. Not because an event went
    somewhere, but because it is now BORN inside the transaction that changes the order —
    which is the only way for the topic and the database not to diverge silently.
    """
    from .orders_oltp import OrdersOltpError, apply_partition, connect

    try:
        with connect(args.dsn) as connection:
            result = apply_partition(connection, args.partition, verify=not args.no_verify)
    except (OrdersOltpError, ManifestError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    print(f"partition ........ {result.partition}")
    print(f"events in log .... {result.events}")
    print(f"applied .......... {result.applied}")
    print(f"already applied .. {result.skipped} (event_id already in outbox — idempotent replay)")
    print(f"orders ........... {result.orders}")
    for kind, count in sorted(result.by_type.items()):
        print(f"  {kind} ... {count}")
    print("OK: state and outbox written in the same transaction, one event per transaction.")
    return EXIT_OK


def _cmd_orders_outbox(args) -> int:
    from .orders_oltp import OrdersOltpError, connect, outbox_summary, verify_outbox

    try:
        with connect(args.dsn) as connection:
            summary = outbox_summary(connection)
            proof = verify_outbox(connection, args.verify) if args.verify else None
    except (OrdersOltpError, ManifestError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    print(f"outbox rows ...... {summary['outbox_rows']}")
    print(f"unpublished ...... {summary['unpublished']}")
    print(f"orders ........... {summary['orders_in_outbox']}")
    for kind, count in sorted(summary["by_event_type"].items()):
        print(f"  {kind} ... {count}")
    print("order state in the OLTP:")
    for status, count in sorted(summary["orders_by_status"].items()):
        print(f"  {status} ... {count}")

    if proof is None:
        return EXIT_OK

    print()
    print(f"partition ........ {proof['partition']}")
    print(f"declared log ..... {proof['declared_rows']} rows, {proof['declared_bytes']} bytes")
    print(f"                   sha256 {proof['declared_sha256']}")
    print(f"outbox log ....... {proof['outbox_rows']} rows, {proof['outbox_bytes']} bytes")
    print(f"                   sha256 {proof['outbox_sha256']}")
    if not proof["matches"]:
        print(
            "FAILED: the log rebuilt from the outbox does NOT reproduce the manifest's. "
            "The outbox lost, duplicated or altered an event."
        )
        return EXIT_FAILED
    print("OK: the outbox rebuilds the partition log byte for byte.")
    return EXIT_OK


def _cmd_orders_projection_init(args) -> int:
    from .orders_stream import (
        OrdersStreamError, create_projection_schema, ensure_database, projection_connect,
    )

    try:
        created = ensure_database(args.projection_dsn)
        with projection_connect(args.projection_dsn) as connection:
            create_projection_schema(connection)
    except OrdersStreamError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL
    print(f"dsn .............. {args.projection_dsn}")
    print(f"database ......... {'created' if created else 'already existed'}")
    print("OK: live_order_state ready. It is a READ MODEL: a database separate from the "
          "OLTP on purpose.")
    return EXIT_OK


def _cmd_orders_publish(args) -> int:
    """Drain the outbox into the topic. AT-LEAST-ONCE, and that is written down, not hidden.

    Publishing before marking `published_at` means that dying midway REPUBLISHES. The
    reverse order would LOSE. Losing is irreversible; duplicating is absorbable by the
    consumer — and that is why the consumer has to be idempotent by obligation, not by
    elegance.
    """
    from .orders_oltp import OrdersOltpError, connect
    from .orders_stream import OrdersStreamError, build_producer, publish

    try:
        producer = build_producer(args.bootstrap)
        with connect(args.dsn) as connection:
            total_published = 0
            rounds = 0
            while True:
                result = publish(connection, producer, args.topic,
                                 batch_size=args.batch_size)
                total_published += result.published
                rounds += 1
                if not args.follow:
                    break
                if result.published:
                    print(f"  batch {rounds}: {result.published} published")
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\ninterrupted. {total_published} event(s) published and marked.")
        return EXIT_OK
    except (OrdersOltpError, OrdersStreamError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    print(f"broker ........... {args.bootstrap}")
    print(f"topic ............ {args.topic}")
    print(f"published ........ {total_published}")
    print("OK: outbox drained. Semantics: at-least-once from the outbox to the broker.")
    return EXIT_OK


def _cmd_orders_project(args) -> int:
    """Consume the topic and maintain `live_order_state`. Idempotent by (order_id, sequence_no).

    Deduplicating by sequence against the state that already exists — and not by a set of
    event_ids that grows without bound — is only possible because per-key ordering is
    guaranteed. The same comparison detects a GAP, which is loss, and refuses instead of
    applying.
    """
    import contextlib

    from .orders_stream import (
        OrdersStreamError, PostgresProjection, build_consumer, project,
        projection_connect, read_sla_minutes,
    )

    # THE SEAM DECLARED IN MILESTONE 5, exercised. Swapping the read model's storage does not
    # change a line of `project()`: both sinks expose `load/save/commit/rollback/count/digest`
    # and nothing else. If this branch needed one more method, the seam would have leaked.
    def open_sink():
        if args.sink == "iceberg":
            from .orders_projection import IcebergProjection, catalog, ensure_table

            cat = catalog()
            return contextlib.nullcontext(
                IcebergProjection(ensure_table(cat), cat=cat)
            )
        connection = projection_connect(args.projection_dsn)
        return contextlib.closing(_PostgresSink(connection))

    class _PostgresSink(PostgresProjection):
        def close(self):
            self._connection.close()

    try:
        sla = args.sla_minutes or read_sla_minutes(args.seeds_dir)
        consumer = build_consumer(args.bootstrap, args.group, topic=args.topic,
                                  from_beginning=args.from_beginning)
        with open_sink() as projection:
            before = projection.digest()
            try:
                result = project(consumer, projection, sla_minutes=sla,
                                 batch_size=args.batch_size,
                                 idle_timeout=args.idle_timeout,
                                 strict=not args.allow_gaps)
            finally:
                consumer.close()
            after = projection.digest()
            rows = projection.count()
    except OrdersStreamError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED
    except Exception as exc:
        from .orders_projection import OrdersProjectionError

        if not isinstance(exc, OrdersProjectionError):
            raise
        print(f"ERROR: {exc}")
        logging.exception("orders-project failed")
        return EXIT_FAILED

    print(f"broker ........... {args.bootstrap}")
    print(f"group ............ {args.group}")
    print(f"sink ............. {args.sink}")
    print(f"picking SLA ...... {sla} min (from order_premises_seed.csv)")
    print(f"applied .......... {result.applied}")
    print(f"duplicates ....... {result.duplicates} (discarded by sequence_no)")
    print(f"gaps ............. {len(result.gaps)}")
    print(f"orders in projection {rows}")
    print(f"SLA breached ..... {result.sla_breaches}")
    print(f"digest before .... {before[:32]}")
    print(f"digest after ..... {after[:32]}")
    if before == after and result.applied == 0:
        print("OK: nothing new — the projection was already up to date (idempotent replay).")
    else:
        print("OK: offset committed AFTER the write — at-least-once delivery, "
              "exactly-once effect on the projection.")
    return EXIT_OK


def _cmd_orders_lag(args) -> int:
    from .orders_stream import OrdersStreamError, group_lag

    try:
        lag = group_lag(args.bootstrap, args.topic, args.group)
    except OrdersStreamError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED
    print(f"topic ............ {args.topic}")
    print(f"group ............ {args.group}")
    total = 0
    for partition in sorted(lag):
        entry = lag[partition]
        total += entry["lag"]
        print(f"  partition {partition} ... committed={entry['committed']} "
              f"high={entry['high']} lag={entry['lag']}")
    print(f"total lag ........ {total}")
    return EXIT_OK


def _cmd_orders_replay(args) -> int:
    """Rewind the group to the beginning. Does NOT delete the projection — that is the point.

    Reprocessing everything ON TOP of the existing state is what proves idempotent
    consumption. Deleting first would prove that the fold is deterministic, which is a
    different thing and already proven.
    """
    from .orders_stream import OrdersStreamError, reset_group

    try:
        partitions = reset_group(args.bootstrap, args.topic, args.group)
    except OrdersStreamError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED
    print(f"group {args.group!r} rewound on {partitions} partition(s).")
    print("The projection was NOT deleted: reprocessing on top is what proves idempotency.")
    print("Run `make orders-project` to reprocess.")
    return EXIT_OK


def _cmd_iceberg_init(args) -> int:
    from .orders_projection import (
        OrdersProjectionError, catalog, ensure_catalog_database, ensure_table,
    )

    try:
        created = ensure_catalog_database()
        cat = catalog(warehouse=args.warehouse)
        table = ensure_table(cat)
    except OrdersProjectionError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL
    print(f"catalog database . {'created' if created else 'already existed'}")
    print(f"warehouse ........ {args.warehouse}")
    print(f"table ............ {table.name()}")
    print(f"current metadata . {table.metadata_location}")
    print("OK: live_order_state ready in Iceberg, with a SQL catalog in the OLTP's Postgres.")
    return EXIT_OK


def _cmd_iceberg_metadata(args) -> int:
    """Where the CURRENT metadata is, according to the catalog.

    `make silver` calls this to pass the path as a var to dbt. DuckDB refuses to discover
    it on its own — and the refusal is right: scanning storage for the newest metadata may
    find one that has not been committed yet.
    """
    from .orders_projection import OrdersProjectionError, metadata_location

    try:
        path = metadata_location()
    except OrdersProjectionError as exc:
        if not args.quiet:
            print(f"ERROR: {exc}")
        return EXIT_FAILED
    print(path)
    return EXIT_OK


def _cmd_stock_consumption(args) -> int:
    """Read the OBSERVED consumption from Silver and write it to `operations.stock_consumption`.

    It is the INPUT of the Spark stock job. It crosses as an Iceberg table, and not as
    parquet read directly, for a measured reason: Spark already talks to MinIO through one
    path — Iceberg's S3FileIO — and adding `hadoop-aws` plus the AWS SDK v1 bundle to read
    parquet would be a second integration with the object storage, ~200 MB of jars, and its
    own set of failure modes. As a side effect, the interop that justified Iceberg is now
    exercised by the PRODUCTION path and not only by an experiment.
    """
    from .stock_bridge import StockLedgerError, export_consumption

    try:
        summary = export_consumption()
    except StockLedgerError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED
    # The keys are the summary dict's (stock_bridge.py); the labels are what gets printed.
    for key, label in (("linhas", "rows"), ("series", "series"), ("unidades", "units"),
                       ("de", "from"), ("ate", "to")):
        print(f"{label:.<18} {summary[key]}")
    print(f"{'metadata':.<18} {summary['metadata_location']}")
    return EXIT_OK


def _cmd_freeze(args) -> int:
    """Seal the capture: write docs/FREEZE.md and the seed the test reads."""
    from .freeze import (FreezeError, capture_id, collect, escrever_pagina,
                         escrever_seed)

    try:
        records = collect()
    except FreezeError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED
    page = escrever_pagina(records, args.page)
    seed = escrever_seed(records, args.seed)
    print(f"capture_id ....... {capture_id(records)}")
    print(f"partitions ....... {len(records)}")
    print(f"records .......... {sum(r['records'] for r in records)}")
    print(f"page ............. {os.path.relpath(page)}")
    print(f"seed ............. {os.path.relpath(seed)}")
    return EXIT_OK


def _cmd_freeze_check(args) -> int:
    """Re-read RAW and compare it with the seal. Exits 1 on ANY difference.

    This is the verb that turns the discipline "don't touch RAW after closing" into a
    check. Without it, the promise holds only as long as nobody forgets.
    """
    from .freeze import FreezeError, capture_id, collect, comparar, ler_seed

    try:
        sealed = ler_seed(args.seed)
        current = collect()
    except FreezeError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    problems = comparar(sealed, current)
    print(f"sealed ........... {capture_id(sealed)}  ({len(sealed)} partitions)")
    print(f"current .......... {capture_id(current)}  ({len(current)} partitions)")
    if problems:
        print()
        print(f"REFUSED ({len(problems)} difference(s)) — the sealed capture CHANGED:")
        for problem in problems[:20]:
            print(f"  - {problem}")
        if len(problems) > 20:
            print(f"  ... and {len(problems) - 20} more")
        return EXIT_FAILED
    print()
    print("OK: the capture is intact, byte for byte, against the seal.")
    return EXIT_OK


def _cmd_spark_evidence(args) -> int:
    """Run the stock job on BOTH engines and write the comparison, favorable or not."""
    from .spark_evidence import collect, write

    data = collect(rodar_spark=not args.no_spark)
    target = write(data, args.out)
    print(f"written to ....... {os.path.relpath(target)}")
    # The keys are the collected dict's (spark_evidence.py); the labels are what gets printed.
    for key, label in (("catalogo", "catalog"), ("nao_gatilho", "non_trigger"),
                       ("python", "python"), ("spark", "spark")):
        section = data.get(key, {})
        state = f"ERROR: {section['erro'][:80]}" if "erro" in section else "observed"
        print(f"{label:.<18} {state}")
    return EXIT_OK


def _cmd_orders_rebuild_projection(args) -> int:
    """The BATCH layer of the pair: rebuild live_order_state from RAW.

    It is the second writer of the same table — which is literally the Iceberg trigger. It
    shares the fold with the consumer, and so agreement between the two is NOT evidence of
    correctness: it is evidence that they don't trample each other. Correctness comes from
    `orders-reconcile`.
    """
    from .orders_projection import (
        OrdersProjectionError, catalog, partitions_of, rebuild,
    )

    partitions = partitions_of(args.root, through=args.through)
    if not partitions:
        print(f"ERROR: no complete partition under {args.root}.")
        return EXIT_FATAL
    try:
        if getattr(args, "reset", False):
            print("RESET: the table will be dropped and recreated before the rebuild.")
        result = rebuild(
            catalog(), partitions, seeds_dir=args.seeds_dir,
            reset=getattr(args, "reset", False),
        )
    except (OrdersProjectionError, ManifestError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    print(f"partitions read .. {result.partitions}")
    print(f"events ........... {result.events}")
    print(f"orders ........... {result.orders}")
    print(f"commits .......... {result.commits}")
    print(f"conflicts ........ {result.conflicts} (resolved by re-read and retry)")
    print(f"rows discarded ... {result.dropped_as_stale} (streaming already had equal or "
          f"newer state)")
    print("OK: batch rebuild written to the SAME table the streaming writes.")
    return EXIT_OK


def _cmd_orders_reconcile(args) -> int:
    """Compare the three folds. If they diverge, exit 1.

    `silver_order` is the only one of the three that shares no code with any other — a
    window function in SQL over the whole log. It is against it that this counts as a check.
    """
    from .orders_projection import OrdersProjectionError, reconcile

    try:
        report = reconcile(dsn=args.dsn)
    except OrdersProjectionError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FAILED

    for name, quantity in sorted(report["orders"].items()):
        print(f"orders in {name:.<11} {quantity}")
    print(f"compared ......... {report['compared']}")
    for name, missing in sorted(report["missing"].items()):
        print(f"MISSING in {name}: {', '.join(missing)}")
    if report["divergences"]:
        print(f"divergences ...... {report['divergence_count']}")
        for row in report["divergences"]:
            values = " ".join(
                f"{k}={row[k]!r}" for k in ("iceberg", "silver", "oltp")
            )
            print(f"  {row['order_id']} {row['coluna']}: {values}")
    if not report["ok"]:
        print("FAILED: the folds diverged. Stream and batch did NOT converge.")
        return EXIT_FAILED
    print("OK: Iceberg, Silver and OLTP agree on every compared order.")
    return EXIT_OK


def _cmd_export_snowflake(args) -> int:
    """Cut Silver into local parquet. Does NOT talk to Snowflake.

    Separate from `load-snowflake` for the same reason `land` and `verify-landing` are
    separate: producing the artifact and moving it fail for different reasons, and joining
    both in a single verb would make "the cut is wrong" indistinguishable from "the network
    went down".
    """
    from .warehouse_cut import WarehouseCutError, export

    try:
        summary = export(from_env(), args.out)
    except WarehouseCutError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    total = sum(d["rows"] for d in summary.values())
    print(f"target ........... {args.out}")
    for name in sorted(summary):
        d = summary[name]
        print(f"  {name:<30} {d['rows']:>9,} rows  {d['bytes'] / 1024:>8.1f} KB  {d['grain']}")
    print(f"  {'TOTAL':<30} {total:>9,} rows")
    print("OK: Silver cut written as parquet.")
    return EXIT_OK


def _cmd_snowflake_ddl(args) -> int:
    """Print the STAGE DDL. Connects nowhere — it is for review."""
    from .query import connect_lakehouse
    from .warehouse_cut import dump_ddl

    connection = connect_lakehouse(from_env())
    try:
        print(dump_ddl(connection, args.database))
    finally:
        connection.close()
    return EXIT_OK


def _cmd_snowflake_bootstrap(args) -> int:
    """Database, schemas, roles and grants. Runs once, requires ACCOUNTADMIN."""
    from .snowflake_load import ROLES, SnowflakeLoadError, bootstrap, connect

    try:
        connection = connect(args.connection)
    except SnowflakeLoadError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    cursor = connection.cursor()
    try:
        done = bootstrap(cursor, args.database, args.grant_to_user, args.warehouse)
    except Exception as exc:
        print(f"ERROR: the target refused: {exc}")
        logging.exception("snowflake-bootstrap failed")
        return EXIT_FAILED
    finally:
        cursor.close()
        connection.close()

    print(f"target ........... {args.database} (connection '{args.connection}')")
    print(f"statements ....... {len(done)} applied")
    for name, description in ROLES.items():
        print(f"  {name:<20} {description}")

    # An applied grant is not a working grant. The check runs with `use secondary roles
    # none`, without which it would pass by mistake on an account where the user also has
    # ACCOUNTADMIN — see check_isolation.
    from .snowflake_load import check_isolation

    problems = check_isolation(
        lambda role: connect(args.connection, role=role), args.database
    )
    if problems:
        print("\nISOLATION DOES NOT HOLD:")
        for p in problems:
            print(f"  - {p}")
        return EXIT_FAILED
    print("isolation ........ checked role by role (with secondary roles disabled)")
    print("OK: schemas, roles and grants applied and verified.")
    return EXIT_OK


def _cmd_load_snowflake(args) -> int:
    """Create the STAGE tables, upload the parquet by PUT and load with COPY INTO."""
    from .query import connect_lakehouse
    from .warehouse_cut import SPECS
    from .snowflake_load import SnowflakeLoadError, connect, load, verify

    expected = {}
    for spec in SPECS:
        path = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
        if not os.path.exists(path):
            print(f"ERROR: missing parquet: {path}. Run `make warehouse-export` first.")
            return EXIT_FATAL

    duck = connect_lakehouse(from_env())
    try:
        # The source count comes from the parquet ITSELF, not from re-running the query:
        # rechecking against the SQL would prove the SQL is deterministic, not that the
        # bytes uploaded are the ones that were generated.
        for spec in SPECS:
            path = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
            expected[spec["name"]] = {
                "rows": duck.execute(
                    f"select count(*) from read_parquet('{path}')"
                ).fetchone()[0]
            }

        try:
            connection = connect(args.connection, role=args.role)
        except SnowflakeLoadError as exc:
            print(f"ERROR: {exc}")
            return EXIT_FATAL

        try:
            summary = load(connection, duck, args.stage_dir, args.database)
            problems = verify(connection, args.stage_dir, expected, args.database)
        except SnowflakeLoadError as exc:
            print(f"ERROR: {exc}")
            return EXIT_FATAL
        except Exception as exc:
            print(f"ERROR: the target refused: {exc}")
            logging.exception("snowflake-load failed")
            return EXIT_FAILED
        finally:
            connection.close()
    finally:
        duck.close()

    print(f"target ........... {args.database}.STAGE (connection '{args.connection}')")
    print(f"role ............. {args.role}")
    for name in sorted(summary):
        print(f"  {name:<30} {summary[name]['rows']:>9,} rows")
    if problems:
        print("\nMISMATCH between the generated parquet and what reached the target:")
        for p in problems:
            print(f"  - {p}")
        return EXIT_FAILED
    print(f"  {'TOTAL':<30} {sum(d['rows'] for d in summary.values()):>9,} rows")
    print("OK: loaded and rechecked count by count.")
    return EXIT_OK


def _cmd_postgres_bootstrap(args) -> int:
    """Schemas, roles and grants (incl. `default privileges`) on the local Postgres.

    Second warehouse target, next to `snowflake-bootstrap` — same separation between
    creating infrastructure (here) and loading data (`load-postgres`). See postgres_load.py.
    """
    from .postgres_load import ROLES, PostgresLoadError, bootstrap, connect

    try:
        connection = connect(host=args.host, port=args.port, dbname=args.dbname)
    except PostgresLoadError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    cursor = connection.cursor()
    try:
        done = bootstrap(cursor, args.grant_to_user)
        connection.commit()
    except Exception as exc:
        connection.rollback()
        print(f"ERROR: the target refused: {exc}")
        logging.exception("postgres-bootstrap failed")
        return EXIT_FAILED
    finally:
        cursor.close()
        connection.close()

    print(f"target ........... {args.host}:{args.port}/{args.dbname}")
    print(f"statements ....... {len(done)} applied")
    for name, description in ROLES.items():
        print(f"  {name:<20} {description}")
    print("OK: schemas, roles and grants applied.")
    return EXIT_OK


def _cmd_load_postgres(args) -> int:
    """Recreate the STAGE tables (drop+create) and load via COPY FROM STDIN.

    Reuses the same cut as `export-cut` — the parquet has nothing Snowflake-specific, only
    the directory name is a historical leftover (see DECISIONS.md).
    """
    from .postgres_load import PostgresLoadError, connect, load, verify
    from .query import connect_lakehouse
    from .warehouse_cut import SPECS

    for spec in SPECS:
        path = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
        if not os.path.exists(path):
            print(f"ERROR: missing parquet: {path}. Run `make warehouse-export` first.")
            return EXIT_FATAL

    duck = connect_lakehouse(from_env())
    try:
        expected = {}
        for spec in SPECS:
            path = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
            expected[spec["name"]] = {
                "rows": duck.execute(
                    f"select count(*) from read_parquet('{path}')"
                ).fetchone()[0]
            }

        try:
            connection = connect(
                host=args.host, port=args.port, dbname=args.dbname, role=args.role
            )
        except PostgresLoadError as exc:
            print(f"ERROR: {exc}")
            return EXIT_FATAL

        try:
            summary = load(connection, duck, args.stage_dir)
            problems = verify(connection, args.stage_dir, expected)
        except PostgresLoadError as exc:
            print(f"ERROR: {exc}")
            return EXIT_FATAL
        except Exception as exc:
            print(f"ERROR: the target refused: {exc}")
            logging.exception("load-postgres failed")
            return EXIT_FAILED
        finally:
            connection.close()
    finally:
        duck.close()

    print(f"target ........... {args.host}:{args.port}/{args.dbname}.STAGE")
    print(f"role ............. {args.role}")
    for name in sorted(summary):
        print(f"  {name:<30} {summary[name]['rows']:>9,} rows")
    if problems:
        print("\nMISMATCH between the generated parquet and what reached the target:")
        for p in problems:
            print(f"  - {p}")
        return EXIT_FAILED
    print(f"  {'TOTAL':<30} {sum(d['rows'] for d in summary.values()):>9,} rows")
    print("OK: loaded and rechecked count by count.")
    return EXIT_OK


def _cmd_demand_reality_check(args) -> int:
    """Measure the current mix and compare it with the frozen BEFORE and with MAPA.

    With `--snapshot NAME` it only FREEZES the measurement and exits. It is a separate step
    on purpose: after regenerating the window the previous state no longer exists anywhere,
    and a reality check without a BEFORE can only say "this is how it is today" — which is
    half the question.
    """
    from . import config as config_module
    from .demand_check import (
        DEFAULT_SNAPSHOT_DIR,
        DemandCheckError,
        calibration_error,
        load_snapshot,
        measure,
        render,
        save_snapshot,
        write,
    )
    from .query import connect_lakehouse

    connection = connect_lakehouse(config_module.from_env())
    try:
        current = measure(connection, args.seeds_dir)
    except DemandCheckError as exc:
        print(f"ERROR: {exc}")
        return 1
    finally:
        connection.close()

    if args.snapshot:
        target = os.path.join(DEFAULT_SNAPSHOT_DIR, f"{args.snapshot}.json")
        save_snapshot(current, target)
        groups = len(current["groups"])
        print(f"snapshot '{args.snapshot}' frozen at {target} ({groups} groups)")
        return 0

    before = None
    if args.before:
        path = args.before
        if not os.path.sep in path and not path.endswith(".json"):
            path = os.path.join(DEFAULT_SNAPSHOT_DIR, f"{path}.json")
        try:
            before = load_snapshot(path)
        except DemandCheckError as exc:
            print(f"WARNING: {exc}")

    write(render(current, before, args.seeds_dir, antes_nome=args.before), args.out)
    error = calibration_error(current, args.seeds_dir)
    if args.out != "-":
        print(f"reality check written to {args.out}")
        if before is None:
            print("WARNING: without a BEFORE, the page only compares the current state with MAPA.")
        if error["medio"] is not None:
            print(
                f"error vs target ..... mean {error['medio']:.3f} pt, "
                f"worst {error['pior']:.3f} pt ({error['pior_grupo']}), "
                f"over {error['grupos']} group(s)"
            )

    # THE THRESHOLD IS NOT A QUALITY SCORE. It is wide on purpose and exists to catch ONE
    # case: calibration no longer being applied without anything blowing up. Before this
    # phase the worst deviation was 11.44 points; inertia announces itself at that order of
    # magnitude.
    if error["pior"] is not None and float(error["pior"]) > args.max_error:
        print(
            f"FAILED: the worst deviation ({error['pior']:.3f} pt in {error['pior_grupo']}) "
            f"exceeds the declared threshold of {args.max_error} pt. Either the profile is no "
            f"longer applied, or the benchmark changed without the window being regenerated."
        )
        return 1
    return 0


def _cmd_stream_evidence(args) -> int:
    """Observe OLTP, broker and projection and write the dated evidence.

    Does NOT fail when one of the three is down, and that is deliberate: partial evidence
    is useful, and the missing section shows up as a DECLARED ABSENCE instead of an
    invented number. Code 1 is reserved for the case where NONE of the three answered —
    then there is no evidence at all, just a file with four warnings.
    """
    from . import config as config_module
    from .stream_evidence import collect, render, write

    data = collect(
        config_module.from_env(),
        bootstrap=args.bootstrap,
        topic=args.topic,
        dsn=args.dsn,
    )

    # The keys are the collected dict's (stream_evidence.py); the labels are what gets printed.
    planes = (("oltp", "oltp"), ("broker", "broker"), ("projecao", "projection"),
              ("reconciliacao", "reconciliation"))
    observed = [key for key, _ in planes if "erro" not in data[key]]

    if args.out == "-":
        print(render(data), end="")
    else:
        target = write(data, args.out)
        for key, label in planes:
            state = "ok" if "erro" not in data[key] else "absent"
            print(f"{label:.<17} {state}")
        print(f"written to ....... {os.path.relpath(target)}")

    if not observed:
        print("ERROR: none of the three planes answered. Bring them up with `make stream-up`.")
        return EXIT_FAILED
    return EXIT_OK


def _cmd_snowflake_evidence(args) -> int:
    """Observe the target and write the report. Validates nothing — dbt does the validating."""
    from .snowflake_evidence import collect, render
    from .snowflake_load import SnowflakeLoadError, connect

    try:
        connection = connect(args.connection)
    except SnowflakeLoadError as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    try:
        data = collect(
            connection, lambda role: connect(args.connection, role=role), args.database
        )
    except Exception as exc:
        print(f"ERROR: the target refused: {exc}")
        logging.exception("snowflake-evidence failed")
        return EXIT_FAILED
    finally:
        connection.close()

    text = render(data)
    if args.out == "-":
        print(text, end="")
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text)
        ident = data["identidade"]
        print(f"account .......... {ident['conta']} (role {ident['papel']})")
        print(f"objects .......... {len(data['objetos'])} in STAGE/GOLD/MART")
        print(f"isolation ........ {'holds' if not data['isolamento'] else 'DOES NOT HOLD'}")
        print(f"written to ....... {args.out}")

    # Evidence that records broken isolation is not evidence, it is an alert.
    return EXIT_FAILED if data["isolamento"] else EXIT_OK


def _cmd_duckdb_secret(args) -> int:
    from .query import SECRET_NAME, create_persistent_secret

    storage = create_persistent_secret(from_env())
    print(f"secret '{SECRET_NAME}' written ({storage}).")
    print("Now any DuckDB client opens the file without configuring anything:")
    print("    duckdb platform/dbt/retail.duckdb")
    print("    select count(*) from silver_product_price;")
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="retail-platform",
        description="Platform: lands and verifies Source snapshots in the object storage.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    land_parser = subparsers.add_parser("land", help="upload a partition to the object storage")
    land_parser.add_argument("partition", help="path of the partition on disk")
    land_parser.set_defaults(handler=_cmd_land)

    verify_parser = subparsers.add_parser(
        "verify-landing", help="re-read from the object storage and recheck the partition"
    )
    verify_parser.add_argument("partition", help="path of the partition on disk")
    verify_parser.set_defaults(handler=_cmd_verify)

    query_parser = subparsers.add_parser(
        "query", help="query Silver (connection already configured for the object storage)"
    )
    query_parser.add_argument(
        "sql", nargs="?", default=None, help="SQL to run (default: summary per partition)"
    )
    query_parser.add_argument(
        "--database", default=None,
        help="query a .duckdb file instead of the in-memory views over the parquet",
    )
    query_parser.set_defaults(handler=_cmd_query)

    secret_parser = subparsers.add_parser(
        "duckdb-secret",
        help="write a DuckDB secret so that any client can open the .duckdb",
    )
    secret_parser.set_defaults(handler=_cmd_duckdb_secret)

    prune_parser = subparsers.add_parser(
        "prune-local",
        help="delete the local copy of a partition, only if it is intact at the target",
    )
    prune_parser.add_argument("partition", help="path of the partition on disk")
    prune_parser.set_defaults(handler=_cmd_prune_local)

    oltp_parser = subparsers.add_parser(
        "export-oltp-reference",
        help="materialize Silver as flat JSON for the simulated OLTP Source",
    )
    oltp_parser.add_argument(
        "--out", default="data/oltp-reference", help="root directory of the reference"
    )
    oltp_parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="reference date YYYY-MM-DD (default: today in UTC)",
    )
    oltp_parser.add_argument(
        "--seeds-dir",
        default=DEFAULT_SEEDS_DIR,
        help="dbt seeds directory (not reachable through the Lakehouse views)",
    )
    oltp_parser.set_defaults(handler=_cmd_export_oltp_reference)

    orders_parser = subparsers.add_parser(
        "export-orders-reference",
        help="materialize Silver as flat JSON for the simulated Orders Source",
    )
    orders_parser.add_argument(
        "--out", default="data/orders-reference", help="root directory of the reference"
    )
    orders_parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="reference date YYYY-MM-DD (default: today in UTC)",
    )
    # The window is mandatory and has no default: who knows from when to when the orders
    # exist is the operator, and guessing here would produce a fact base silently different
    # on every run. Same spirit as the mandatory `--reference` in validate.
    orders_parser.add_argument(
        "--from",
        dest="window_from",
        required=True,
        help="first order day of the window, YYYY-MM-DD",
    )
    orders_parser.add_argument(
        "--to",
        dest="window_to",
        required=True,
        help="last order day of the window, YYYY-MM-DD",
    )
    orders_parser.add_argument(
        "--seeds-dir",
        default=DEFAULT_SEEDS_DIR,
        help="dbt seeds directory (not reachable through the Lakehouse views)",
    )
    orders_parser.set_defaults(handler=_cmd_export_orders_reference)

    # ---- orders OLTP and outbox -----------------------------------------------
    # Four separate verbs by the usual criterion: printing the DDL needs no database,
    # creating the schema needs no log, applying doesn't need to know how to publish, and
    # checking the outbox must not share code with whoever wrote it.
    from .orders_oltp import DEFAULT_DSN as OLTP_DEFAULT_DSN

    oltp_ddl = subparsers.add_parser(
        "orders-oltp-ddl", help="print the orders OLTP DDL without connecting to anything"
    )
    oltp_ddl.set_defaults(handler=_cmd_orders_oltp_ddl)

    oltp_init = subparsers.add_parser(
        "orders-oltp-init", help="create orders, order_line and outbox in the OLTP"
    )
    oltp_init.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="OLTP DSN")
    oltp_init.add_argument(
        "--reset",
        action="store_true",
        help="drop the three tables before creating them (the OLTP is a replica of the log)",
    )
    oltp_init.set_defaults(handler=_cmd_orders_oltp_init)

    oltp_apply = subparsers.add_parser(
        "orders-apply",
        help="replicate the partition log into the OLTP: state and outbox in the same transaction",
    )
    oltp_apply.add_argument("partition", help="path of the orders partition")
    oltp_apply.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="OLTP DSN")
    oltp_apply.add_argument(
        "--no-verify",
        action="store_true",
        help="skip checking the log's sha256 against the manifest (don't use)",
    )
    oltp_apply.set_defaults(handler=_cmd_orders_apply)

    oltp_outbox = subparsers.add_parser(
        "orders-outbox", help="outbox and OLTP state; with --verify, rebuilds the log"
    )
    oltp_outbox.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="OLTP DSN")
    oltp_outbox.add_argument(
        "--verify",
        metavar="PARTITION",
        help="rebuild the log from the outbox and compare its sha256 with the manifest",
    )
    oltp_outbox.set_defaults(handler=_cmd_orders_outbox)

    # ---- transport, replay and consumption --------------------------------------
    from .orders_stream import (
        DEFAULT_BOOTSTRAP as KAFKA_BOOTSTRAP,
        DEFAULT_GROUP as KAFKA_GROUP,
        DEFAULT_PROJECTION_DSN as PROJECTION_DSN,
        DEFAULT_SEEDS_DIR as STREAM_SEEDS_DIR,
        DEFAULT_TOPIC as KAFKA_TOPIC,
    )

    proj_init = subparsers.add_parser(
        "orders-projection-init", help="create the read model's database and table"
    )
    proj_init.add_argument("--projection-dsn", default=PROJECTION_DSN)
    proj_init.set_defaults(handler=_cmd_orders_projection_init)

    pub = subparsers.add_parser(
        "orders-publish", help="drain the outbox into the topic (at-least-once, by design)"
    )
    pub.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="OLTP DSN")
    pub.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    pub.add_argument("--topic", default=KAFKA_TOPIC)
    pub.add_argument("--batch-size", type=int, default=1000)
    pub.add_argument("--follow", action="store_true", help="keep draining in a loop")
    pub.add_argument("--interval", type=float, default=2.0,
                     help="pause between batches with --follow")
    pub.set_defaults(handler=_cmd_orders_publish)

    prj = subparsers.add_parser(
        "orders-project", help="consume the topic and maintain live_order_state (idempotent)"
    )
    prj.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    prj.add_argument("--topic", default=KAFKA_TOPIC)
    prj.add_argument("--group", default=KAFKA_GROUP)
    prj.add_argument("--projection-dsn", default=PROJECTION_DSN)
    prj.add_argument("--batch-size", type=int, default=500)
    prj.add_argument("--idle-timeout", type=float, default=6.0,
                     help="seconds without a message before considering the topic drained")
    prj.add_argument("--from-beginning", action="store_true",
                     help="a new group starts at offset 0 instead of the end")
    prj.add_argument("--sla-minutes", type=int,
                     help="picking threshold (default: order_premises_seed.csv)")
    prj.add_argument("--seeds-dir", default=STREAM_SEEDS_DIR)
    # A gap is LOSS. `--allow-gaps` exists only to diagnose a topic already truncated by
    # retention; using it in a pipeline would trade "I lost an event" for "I didn't notice".
    prj.add_argument("--allow-gaps", action="store_true",
                     help="record gaps instead of failing (diagnosis only)")
    # The read model's semantics don't change with storage: it is the same small interface
    # on both sides. `iceberg` is what gives the same table its second writer.
    prj.add_argument("--sink", choices=("postgres", "iceberg"), default="postgres",
                     help="where the read model lives (default: postgres)")
    prj.set_defaults(handler=_cmd_orders_project)

    lag = subparsers.add_parser("orders-lag", help="consumer group lag, per partition")
    lag.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    lag.add_argument("--topic", default=KAFKA_TOPIC)
    lag.add_argument("--group", default=KAFKA_GROUP)
    lag.set_defaults(handler=_cmd_orders_lag)

    replay = subparsers.add_parser(
        "orders-replay", help="rewind the group to the beginning (does not delete the projection)"
    )
    replay.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    replay.add_argument("--topic", default=KAFKA_TOPIC)
    replay.add_argument("--group", default=KAFKA_GROUP)
    replay.set_defaults(handler=_cmd_orders_replay)

    # ---- live projection in Iceberg ---------------------------------------------
    from .orders_projection import WAREHOUSE as ICEBERG_WAREHOUSE

    ice_init = subparsers.add_parser(
        "iceberg-init", help="create the SQL catalog and the live_order_state table"
    )
    ice_init.add_argument("--warehouse", default=ICEBERG_WAREHOUSE)
    ice_init.set_defaults(handler=_cmd_iceberg_init)

    ice_meta = subparsers.add_parser(
        "iceberg-metadata", help="print the current metadata_location (dbt needs it)"
    )
    ice_meta.add_argument("--quiet", action="store_true",
                          help="don't print the error; only the exit code matters")
    ice_meta.set_defaults(handler=_cmd_iceberg_metadata)

    frz = subparsers.add_parser(
        "freeze", help="seal the capture: docs/FREEZE.md + the seed the test reads")
    frz.add_argument("--page", default="docs/FREEZE.md")
    frz.add_argument("--seed", default="platform/dbt/seeds/frozen_capture_seed.csv")
    frz.set_defaults(handler=_cmd_freeze)

    frz_ck = subparsers.add_parser(
        "freeze-check", help="re-read RAW and compare with the seal; exits 1 on any difference")
    frz_ck.add_argument("--seed", default="platform/dbt/seeds/frozen_capture_seed.csv")
    frz_ck.set_defaults(handler=_cmd_freeze_check)

    spk_ev = subparsers.add_parser(
        "spark-evidence",
        help="run the ledger on both engines and write docs/spark-evidence/README.md",
    )
    spk_ev.add_argument("--out", default="docs/spark-evidence/README.md")
    spk_ev.add_argument("--no-spark", action="store_true",
                        help="Python side only; the page declares the other one absent")
    spk_ev.set_defaults(handler=_cmd_spark_evidence)

    stock_cons = subparsers.add_parser(
        "stock-consumption",
        help="export the observed consumption from Silver to operations.stock_consumption",
    )
    stock_cons.set_defaults(handler=_cmd_stock_consumption)

    ice_rebuild = subparsers.add_parser(
        "orders-rebuild-projection",
        help="rebuild live_order_state from RAW — the table's SECOND writer",
    )
    ice_rebuild.add_argument("--root", default="data/orders")
    ice_rebuild.add_argument(
        "--through", metavar="YYYY-MM-DD",
        help="only the partitions up to this date — the lambda pair's cut")
    ice_rebuild.add_argument("--seeds-dir", default=STREAM_SEEDS_DIR)
    ice_rebuild.add_argument(
        "--reset", action="store_true",
        help="DESTRUCTIVE: drop and recreate the table before rebuilding. Needed when the "
             "Source was regenerated with another seed, reference, premises or "
             "demand_model_version — the monotonic merge assumes an order_id is always the "
             "same order, and then it is not.",
    )
    ice_rebuild.set_defaults(handler=_cmd_orders_rebuild_projection)

    ice_rec = subparsers.add_parser(
        "orders-reconcile", help="compare Iceberg x Silver x OLTP; exits 1 on any divergence"
    )
    ice_rec.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="OLTP DSN")
    ice_rec.set_defaults(handler=_cmd_orders_reconcile)

    # ---- Snowflake ------------------------------------------------------------
    # Three verbs, not one: the cut runs without credentials, the DDL runs without a
    # connection, and only the load needs both. Joining everything would make "the cut is
    # wrong" be reported as a network failure.
    from .warehouse_cut import DEFAULT_OUT as CUT_DEFAULT_OUT
    from .snowflake_load import DEFAULT_LOAD_ROLE as SF_DEFAULT_LOAD_ROLE
    from .snowflake_load import DEFAULT_WAREHOUSE as SF_DEFAULT_WAREHOUSE

    sf_export = subparsers.add_parser(
        "export-cut",
        help="cut Silver into local parquet (does not talk to Snowflake)",
    )
    sf_export.add_argument("--out", default=CUT_DEFAULT_OUT, help="directory of the cut")
    sf_export.set_defaults(handler=_cmd_export_snowflake)

    sf_ddl = subparsers.add_parser(
        "snowflake-ddl", help="print the STAGE DDL, derived from the cut itself"
    )
    sf_ddl.add_argument("--database", default="RETAIL")
    sf_ddl.set_defaults(handler=_cmd_snowflake_ddl)

    sf_boot = subparsers.add_parser(
        "snowflake-bootstrap",
        help="create database, schemas, roles and grants (once, requires ACCOUNTADMIN)",
    )
    sf_boot.add_argument("--database", default="RETAIL")
    sf_boot.add_argument("--connection", default="spark_retail")
    sf_boot.add_argument(
        "--grant-to-user", default=None,
        help="grant the three roles to this user and DISABLE their secondary roles",
    )
    sf_boot.add_argument(
        "--warehouse", default=SF_DEFAULT_WAREHOUSE,
        help=f"warehouse whose usage is granted to the three roles (default {SF_DEFAULT_WAREHOUSE})",
    )
    sf_boot.set_defaults(handler=_cmd_snowflake_bootstrap)

    sf_load = subparsers.add_parser(
        "load-snowflake", help="create the STAGE tables, upload by PUT and load with COPY INTO"
    )
    sf_load.add_argument("--stage-dir", default=CUT_DEFAULT_OUT)
    sf_load.add_argument("--database", default="RETAIL")
    sf_load.add_argument(
        "--connection", default="spark_retail",
        help="connection name in ~/.snowflake/config.toml (no secret lives in the repo)",
    )
    # The LOAD role overrides the connection's. The connection points to the administrative
    # role because it is the one that runs the bootstrap; loading parquet doesn't need that
    # and should not be able to create a database.
    sf_load.add_argument(
        "--role", default=SF_DEFAULT_LOAD_ROLE,
        help=f"role that runs the load (default {SF_DEFAULT_LOAD_ROLE})",
    )
    sf_load.set_defaults(handler=_cmd_load_snowflake)

    sf_ev = subparsers.add_parser(
        "snowflake-evidence",
        help="observe the target and write the dated evidence (ownership, volume, isolation)",
    )
    sf_ev.add_argument("--database", default="RETAIL")
    sf_ev.add_argument("--connection", default="spark_retail")
    sf_ev.add_argument(
        "--out", default="docs/warehouse-evidence/README.md",
        help="markdown target; '-' writes to standard output",
    )
    sf_ev.set_defaults(handler=_cmd_snowflake_evidence)

    # ---- Postgres (second warehouse target, fallback for the Snowflake trial) --------
    # The same two verbs that need a connection in the Snowflake pair (bootstrap and load);
    # the cut (`export-cut`) is reused as is — see postgres_load.py.
    from .postgres_load import (
        DEFAULT_DBNAME as PG_DEFAULT_DBNAME,
        DEFAULT_HOST as PG_DEFAULT_HOST,
        DEFAULT_LOAD_ROLE as PG_DEFAULT_LOAD_ROLE,
        DEFAULT_PORT as PG_DEFAULT_PORT,
    )

    pg_boot = subparsers.add_parser(
        "postgres-bootstrap",
        help="create schemas, roles and grants on the local Postgres (idempotent)",
    )
    pg_boot.add_argument("--host", default=PG_DEFAULT_HOST)
    pg_boot.add_argument("--port", type=int, default=PG_DEFAULT_PORT)
    pg_boot.add_argument("--dbname", default=PG_DEFAULT_DBNAME)
    pg_boot.add_argument(
        "--grant-to-user", default=None,
        help="grant the three roles to this login user/role",
    )
    pg_boot.set_defaults(handler=_cmd_postgres_bootstrap)

    pg_load = subparsers.add_parser(
        "load-postgres",
        help="recreate the stage tables (drop+create) and load via COPY FROM STDIN",
    )
    pg_load.add_argument("--stage-dir", default=CUT_DEFAULT_OUT)
    pg_load.add_argument("--host", default=PG_DEFAULT_HOST)
    pg_load.add_argument("--port", type=int, default=PG_DEFAULT_PORT)
    pg_load.add_argument("--dbname", default=PG_DEFAULT_DBNAME)
    pg_load.add_argument(
        "--role", default=PG_DEFAULT_LOAD_ROLE,
        help=f"role activated by `set role` after connecting (default {PG_DEFAULT_LOAD_ROLE})",
    )
    pg_load.set_defaults(handler=_cmd_load_postgres)

    stream_ev = subparsers.add_parser(
        "stream-evidence",
        help="observe OLTP, broker and projection and write the stream's dated evidence",
    )
    stream_ev.add_argument("--bootstrap", default=None, help="broker; default KAFKA_BOOTSTRAP")
    stream_ev.add_argument("--topic", default=None, help="topic; default KAFKA_ORDERS_TOPIC")
    stream_ev.add_argument("--dsn", default=None, help="OLTP DSN; default ORDERS_OLTP_DSN")
    stream_ev.add_argument(
        "--out", default="docs/stream-evidence/README.md",
        help="markdown target; '-' writes to standard output",
    )
    stream_ev.set_defaults(handler=_cmd_stream_evidence)

    demand_check = subparsers.add_parser(
        "demand-reality-check",
        help="compare the observed mix with the frozen BEFORE and with the MAPA benchmark",
    )
    demand_check.add_argument(
        "--snapshot", default=None,
        help="instead of rendering, freeze the current measurement under this name",
    )
    demand_check.add_argument(
        # THE BEFORE IS ALWAYS THE IMMEDIATELY PREVIOUS STATE, not the oldest one that exists.
        # `before_mapa_2025_v1` (uniform mix, pre-calibration) stays on disk and is cited in
        # the text, but using it as the default would make the page add up the effects of two
        # phases in a single column — and the revenue drop from the price fix would be read
        # as if it came from the cohort layer.
        "--before", default="before_mapa_2025_v2",
        help="name or path of the BEFORE snapshot (default: the state before this version)",
    )
    demand_check.add_argument("--seeds-dir", default=os.path.join("platform", "dbt", "seeds"))
    demand_check.add_argument(
        "--max-error", type=float, default=3.0,
        help="largest tolerated deviation, in percentage points, against the tilted target. "
             "WIDE on purpose: it exists to catch inert calibration, not to be chased.",
    )
    demand_check.add_argument(
        "--out", default=os.path.join("docs", "demand-evidence", "README.md"),
        help="markdown target; '-' writes to standard output",
    )
    demand_check.set_defaults(handler=_cmd_demand_reality_check)

    silver_build = subparsers.add_parser(
        "silver-build",
        help="Silver dbt build with the gate (exclusions + Iceberg var) applied",
    )
    silver_build.add_argument("--project-dir", default="platform/dbt")
    silver_build.add_argument("--profiles-dir", default="platform/dbt")
    silver_build.add_argument("--dbt", default=None, help="executable; default RETAIL_DBT")
    silver_build.add_argument("--quiet", action="store_true")
    silver_build.add_argument(
        "dbt_args", nargs="*",
        help="extra arguments passed on to dbt, after the gate",
    )
    silver_build.set_defaults(handler=_cmd_silver_build)

    has_data_parser = subparsers.add_parser(
        "has-data",
        help="exit code 0 if some landed object exists under the prefix, 1 if not",
    )
    has_data_parser.add_argument("prefix", help="source prefix, e.g.: ine_population_api")
    has_data_parser.set_defaults(handler=_cmd_has_data)

    return parser


def main(argv: list[str] | None = None) -> int:
    # Only the traceback: the short ERROR line still goes through print(), which is the
    # format the operator reads. Without this, an exception that no specific handler maps
    # comes out as a single line with str(exc) and nothing else — whoever needs to diagnose
    # has to reproduce locally to know where, even with exit code 3 already saying "unhandled".
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except (ManifestError, LandingError, ConfigError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL
    except KeyboardInterrupt:
        print("\ninterrupted by the user", flush=True)
        return EXIT_UNHANDLED
    except Exception as exc:
        print(f"UNHANDLED ERROR: {type(exc).__name__}: {exc}")
        logging.exception("unhandled exception in %s", getattr(args, "command", "?"))
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(main())
