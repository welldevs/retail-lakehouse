"""Orchestrates the generation of one order-event partition.

"extract" here does NOT touch the network — neither this Source nor any other part of this
package opens a socket. The verb is kept for Makefile/DAG symmetry with the other four sources.
What it does is: read the four reference files the platform pre-computed from Silver, generate
the event log of that (warehouse, day) and land the partition with manifest and _SUCCESS.

Write order, always in this order: data -> manifest -> _SUCCESS. A process killed midway never
leaves a partition that announces itself complete without being so.

THE MANIFEST DECLARES THE FOLD, AND THAT IS NOT REDUNDANCY
----------------------------------------------------------
`totals` carries `order_rows`, `net_amount_picked`, `substituted_lines` and the like — numbers
that only exist after folding the log. There is no `orders.json` alongside: the state is not
materialized here, and the manifest is the only declaration of what the log means. The dbt test
reconciles Silver against it, closing manifest -> RAW -> parquet.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from . import SOURCE_NAME, __version__
from .canonical import write_jsonl
from .events import EventError
from .orders_generator import GenerationError, generate
from .partition import (
    EVENTS_FILE,
    PartitionError,
    assert_writable,
    build_history,
    events_path,
    mark_success,
    partition_path,
    write_manifest,
)
from .premises import PremiseError, Premises
from .reference_data import ReferenceError, load
from .schema import fingerprint, missing_fields, totals_of

EXIT_OK = 0
EXIT_FATAL = 2


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(args) -> int:
    started = _now()
    order_date = args.date or started.strftime("%Y-%m-%d")

    try:
        partition = partition_path(args.out, order_date, args.wh)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
        reference = load(args.reference)
        premises = Premises(reference.premises)
        events = generate(reference, premises, args.wh, order_date, args.seed)
    except (PartitionError, ReferenceError, PremiseError, GenerationError, EventError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    absent = missing_fields(events)
    if absent:
        # Should never happen — the generator builds from the same vocabulary this checks —
        # but writing a partition with a missing field would be worse than refusing.
        print(f"ERROR: generated events missing a declared field: {absent[:20]}")
        return EXIT_FATAL

    sha256, size, records = write_jsonl(events_path(partition), events)
    finished = _now()

    totals = totals_of(events)
    totals["bytes"] = size
    calendar = reference.calendar_of(args.wh, order_date)
    relative = os.path.relpath(events_path(partition), args.out)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{args.wh}_{order_date.replace('-', '')}"

    manifest = {
        "run_id": run_id,
        "started_at_utc": _stamp(started),
        "finished_at_utc": _stamp(finished),
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "complete": True,
        "source": {
            "name": SOURCE_NAME,
            "wh": args.wh,
            "generator_version": __version__,
        },
        "partition": {"ingestion_date": order_date, "warehouse": args.wh},
        # The THREE inputs that change the orders under the same order_id. Without all three
        # recorded, an --overwrite would swap the orders without leaving a trace.
        "config": {
            "seed": args.seed,
            "orders": totals["order_rows"],
            "premises_sha256": reference.premises_sha256,
            # FOURTH NON-ADDITIVE CONDITION. Changing the demand model changes the orders
            # behind the same ids, exactly like changing seed, reference or premises.
            # It lives in config, not only in reference, so that re-reading an old partition
            # answers which model it was born with without depending on the export still existing.
            "demand_model_version": reference.demand.version,
            "reference": os.path.normpath(args.reference),
        },
        # Input provenance: which snapshots back these orders.
        "reference": {
            "customer_ingestion_dates": reference.customer_ingestion_dates,
            "roster_ingestion_date": reference.roster_ingestion_date,
            "catalog_ingestion_dates": reference.catalog_ingestion_dates,
            "price_as_of": calendar["price_as_of"],
            "price_source": calendar["price_source"],
            "window_from": reference.window_from,
            "window_to": reference.window_to,
            "premises_seed_path": reference.premises_seed_path,
            "premises_sha256": reference.premises_sha256,
            "demand_model_version": reference.demand.version,
            "demand_benchmark": reference.demand.benchmark,
            "demand_seeds_sha256": reference.demand.seeds_sha256,
        },
        "totals": totals,
        "schema_fingerprint": fingerprint(),
        "files": [
            {
                "path": relative,
                "stage": "order_events",
                "sha256": sha256,
                "bytes": size,
                "records": records,
            }
        ],
        "failures": [],
        "anomalies": [],
        "history": build_history(previous),
    }

    write_manifest(partition, manifest)
    mark_success(partition, True, run_id)

    states = ", ".join(f"{e}={n}" for e, n in totals["orders_by_state"].items())
    print(f"partition ........ {partition}")
    print(f"run_id ........... {run_id}")
    print(f"reference ........ {args.reference}")
    print(f"  customers ...... generation in force between {', '.join(reference.customer_ingestion_dates)}")
    print(f"  price .......... price_as_of={calendar['price_as_of']} "
          f"({calendar['price_source']})")
    print(f"  premises ....... sha256={reference.premises_sha256[:16]}... (all synthetic)")
    print(f"  demand ......... {reference.demand.version} ({len(reference.demand.groups)} groups)")
    print(f"seed ............. {args.seed} (day sub-seed derived via sha256)")
    print(f"orders ........... {totals['order_rows']} from {totals['customers_used']} customer(s)")
    print(f"events ........... {totals['event_rows']} across {len(totals['event_rows_by_type'])} types")
    print(f"lines ............ {totals['line_rows']} "
          f"({totals['substituted_lines']} substituted, {totals['removed_lines']} removed)")
    print(f"value ............ placed {totals['gross_amount_placed']} / "
          f"picked {totals['net_amount_picked']}")
    print(f"states ........... {states}")
    print(f"file ............. {EVENTS_FILE} ({size} bytes, {records} lines)")
    print("OK: partition complete, _SUCCESS written last.")
    return EXIT_OK
