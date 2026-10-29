"""Orchestrates the generation of one partition of synthetic customers.

"extract" here does NOT touch the network — neither this Source nor any other part of this
package opens a socket. The verb is kept for Makefile/DAG symmetry with the other three
sources, and what it does is: read the reference file the platform precomputed from
Silver, generate that warehouse's customers and land the partition with manifest and
_SUCCESS. Same precedent as `ine-callejero-source`'s `extract --in <dir>`, one level up.

Write order, always in this order: data -> manifest -> _SUCCESS. A process killed midway
never leaves a partition that announces itself complete without being so.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from . import SOURCE_NAME, __version__
from .canonical import write_json
from .customers_generator import GenerationError, generate
from .partition import (
    CUSTOMERS_FILE,
    PartitionError,
    assert_writable,
    build_history,
    customers_path,
    mark_success,
    partition_path,
    write_manifest,
)
from .reference_data import ReferenceError, load
from .schema import count_customers, fingerprint

EXIT_OK = 0
EXIT_FATAL = 2


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(args) -> int:
    started = _now()
    ingestion_date = args.date or started.strftime("%Y-%m-%d")

    try:
        partition = partition_path(args.out, ingestion_date, args.wh)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
        reference = load(args.reference)
        # WHERE THE BASE SIZE COMES FROM. Without --count, from the target the reference
        # derived from that warehouse's adult population; with --count, from the operator.
        # Both cases go into the manifest WITH their PROVENANCE: without it, a base generated
        # with a manual override would be indistinguishable from a population-derived one
        # six months later.
        count_source = "cli" if args.count is not None else "reference"
        count = args.count if args.count is not None else reference.customer_target(args.wh)
        customers = generate(
            reference, args.wh, count, args.seed, ingestion_date
        )
    except (PartitionError, ReferenceError, GenerationError) as exc:
        print(f"ERROR: {exc}")
        return EXIT_FATAL

    sha256, size = write_json(customers_path(partition), customers)
    finished = _now()

    # (province, municipality) pair, not just the code: a municipality code is unique WITHIN
    # the province. Today each warehouse falls in a single province, but the official Madrid
    # FUA has 38 municipalities in Avila/Guadalajara/Toledo that were left out for lack of
    # Callejero — if they ever come in, counting only the code would silently undercount.
    municipalities_used = len(
        {(c["province_code"], c["municipality_code"]) for c in customers}
    )
    house_number_null = sum(1 for c in customers if c["house_number"] is None)
    pseudo_rows = sum(
        1 for c in customers if reference.candidate(c["candidate_index"])["is_pseudo_address"]
    )
    relative = os.path.relpath(customers_path(partition), args.out)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{args.wh}"

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
        "partition": {"ingestion_date": ingestion_date, "warehouse": args.wh},
        # The seed lives in the manifest and not in each record: it is constant for the whole
        # partition. Without it recorded here, the generation would be irreproducible.
        "config": {
            "count": count,
            "count_source": count_source,
            "seed": args.seed,
            "reference": os.path.normpath(args.reference),
        },
        # Input provenance: which Silver snapshots back these customers.
        "reference": {
            "callejero_ingestion_date": reference.callejero_ingestion_date,
            "population_ingestion_date": reference.population_ingestion_date,
            "population_series_ingestion_date": reference.population_series_ingestion_date,
            "population_year": reference.population_year,
            "population_reference_date": reference.population_reference_date,
            "age_year": reference.age_year,
            "age_reference_date": reference.age_reference_date,
            "age_fk_periodo": reference.age_fk_periodo,
            "address_candidates": reference.candidate_count(),
            "orphan_tramos_excluded": reference.orphan_tramos_excluded,
            # THE CUSTOMER BASE IS NOT THE POPULATION, and the manifest records by how much.
            # Without these four lines, a partition generated with another minimum age or
            # another rate would be indistinguishable from this one — and the two swap the
            # people behind the same ids.
            "min_customer_age": reference.min_customer_age,
            "customer_target": reference.customer_target(args.wh)
            if reference.customer_allocation else None,
            "allocation_rule": reference.allocation_rule,
            "penetration_pct": reference.penetration_pct,
            "penetration_source": reference.penetration_source,
        },
        "totals": {
            "customer_rows": len(customers),
            "municipalities_used": municipalities_used,
            "house_number_null": house_number_null,
            "pseudo_address_rows": pseudo_rows,
            "bytes": size,
        },
        "schema_fingerprint": fingerprint([customers]),
        "files": [
            {
                "path": relative,
                "stage": "customers",
                "sha256": sha256,
                "bytes": size,
                "records": count_customers(customers),
            }
        ],
        "failures": [],
        "anomalies": [],
        "history": build_history(previous),
    }

    write_manifest(partition, manifest)
    mark_success(partition, True, run_id)

    print(f"partition ........ {partition}")
    print(f"run_id ........... {run_id}")
    print(f"reference ........ {args.reference}")
    print(f"  callejero ...... ingestion_date={reference.callejero_ingestion_date}")
    print(f"  population ..... ingestion_date={reference.population_ingestion_date} "
          f"year={reference.population_year}")
    print(f"  age ............ year={reference.age_year} "
          f"fk_periodo={reference.age_fk_periodo} (provincial proxy)")
    print(f"  customer base .. age >= {reference.min_customer_age}, "
          f"rate {reference.penetration_pct}% ({reference.allocation_rule})")
    print(f"seed ............. {args.seed}")
    print(f"customers ........ {len(customers)} in {municipalities_used} municipality(ies) "
          f"[count from {count_source}]")
    print(f"no number ........ {house_number_null} (no numbering in the Callejero)")
    print(f"pseudo-street .... {pseudo_rows}")
    print(f"file ............. {CUSTOMERS_FILE} ({size} bytes)")
    print("OK: partition complete, _SUCCESS written last.")
    return EXIT_OK
