"""Independent validation of a snapshot partition.

Re-reads every file declared in the manifest, recomputing checksum, counts and fingerprint
instead of accepting the recorded values, and scans the directory to detect files the
manifest does not declare.

ON TOP OF THAT, AND THIS IS WHAT SETS THIS SOURCE APART: it re-reads the same reference
file `extract` used and re-checks, customer by customer, that the assigned address really
exists and belongs to the Functional Urban Area of the right warehouse. None of the other
three sources re-validates against an external input, but here the central guarantee —
"100% of postal codes belong to the right warehouse" — can only be re-checked this way. A
unit test proves the rule on fixtures; this proves the result on the data that was actually
landed. Hence `--reference` being mandatory.

Two kinds of problem:
  - INTEGRITY and GEOGRAPHIC COHERENCE -> always fatal (code 1).
  - QUALITY/DISTRIBUTION -> measured and reported; fatal only with --strict.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json
from .customers_generator import NUMBERING_EVEN, NUMBERING_NONE, NUMBERING_ODD
from .partition import MANIFEST_NAME, SUCCESS_NAME, manifest_path, success_path
from .reference_data import ReferenceError, load
from .schema import count_customers, customers_of, fingerprint, missing_fields

EXIT_OK = 0
EXIT_FAILED = 1

# Customer fields that MUST be identical to the source row in address_candidates.json.
# They are the assigned address: if any diverges, the customer does not live where they
# claim to live.
#
# `municipality_name` is NOT included: the two INE products render the same municipality
# differently in 370 of 370 cases in scope ("BRUC (EL)" in the Callejero vs "Bruc, El" in
# table 29005). The customer carries the 29005 name, which is the readable one and the same
# as warehouse_service_area_seed; the Callejero keeps its own in municipality_name_callejero.
# Comparing names from different vocabularies would fail a correct partition — what is
# compared is the municipality CODE, just below, which is the real key.
GEO_FIELDS = (
    "wh",
    "province_code",
    "municipality_code",
    "street_name",
    "postal_code",
    "numbering_type",
)


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def _check_house_number(customer: dict, candidate: dict) -> str | None:
    """The numbering rule, re-checked against the tramo's real range."""
    number = customer.get("house_number")
    numbering = candidate["numbering_type"]

    if numbering == NUMBERING_NONE:
        if number is not None:
            return (
                f"{customer['customer_id']}: house_number={number} on a tramo without "
                f"numbering (numbering_type='0') — fabricated address"
            )
        return None
    if number is None:
        return None  # degenerate range or no number of the right parity: legitimate
    if not isinstance(number, int) or number < 1:
        return f"{customer['customer_id']}: invalid house_number ({number!r})"

    parity = 1 if numbering == NUMBERING_ODD else 0
    if numbering in (NUMBERING_ODD, NUMBERING_EVEN) and number % 2 != parity:
        return (
            f"{customer['customer_id']}: house_number={number} contradicts "
            f"numbering_type={numbering!r}"
        )
    low = candidate.get("number_from")
    high = candidate.get("number_to")
    if isinstance(low, int) and isinstance(high, int):
        if not (min(low, high) <= number <= max(low, high)):
            return (
                f"{customer['customer_id']}: house_number={number} outside the tramo's "
                f"real range [{low}, {high}]"
            )
    return None


def _check_minimum_age(customers: list, reference, ingestion_date: str) -> list:
    """No account holder below the minimum age the reference declares.

    FATAL, not a distribution warning. What this catches has already happened: on
    2026-08-31, 18.01% of the base was under 18 — 3,602 of 20,000, with ages starting from
    zero, including newborn account holders. While age did nothing that was harmless; from
    the moment it drives demand, 18% of the base lands in the benchmark's youngest band
    while being children, and nothing breaks.

    Age is reconstructed the way the generator built it — `year(ingestion_date) -
    birth_year` — and not from the clock. Comparing with today's date would make this
    validation change its answer on its own as time passes, and an immutable partition would
    start failing without anything in it having changed.

    The minimum age comes from the REFERENCE, never from a constant in this file: who may be
    a customer is decided by the platform, in customer_premises_seed.
    """
    minimum = reference.min_customer_age
    if not isinstance(minimum, int):
        return [
            "reference has no min_customer_age: nothing to check this partition's "
            "customer ages against"
        ]
    year = int(str(ingestion_date)[:4])
    minors: list[str] = []
    no_age: list[str] = []
    for customer in customers:
        birth_year = customer.get("birth_year")
        if not isinstance(birth_year, int):
            no_age.append(f"{customer.get('customer_id')}: birth_year={birth_year!r}")
            continue
        age = year - birth_year
        if age < minimum:
            minors.append(f"{customer.get('customer_id')}: {age} years old")

    problems = []
    if minors:
        problems.append(
            f"{len(minors)} customer(s) below min_customer_age={minimum} on "
            f"{ingestion_date}: {minors[:10]}"
        )
    # SEPARATE from the minors, not added to them: a null birth_year is not a low age, it is
    # an age that does not exist. Lumping both into one count would make the report claim
    # there are N children when there may be none, and the operator would go looking for the
    # wrong thing. Fatal all the same — an unverifiable age cannot pass.
    if no_age:
        problems.append(
            f"{len(no_age)} customer(s) without an integer birth_year, so age is not "
            f"verifiable: {no_age[:10]}"
        )
    return problems


def _check_reference(customers: list, reference, warehouse: str) -> tuple[list, list, int]:
    """Geospatial coherence of each customer against the reference. The heart of validation.

    Returns (errors, warnings, approved_customers). The third value is counted, not derived
    from `len(customers) - len(errors)`: errors that are not per customer (such as a
    duplicated customer_id) would throw the count off.
    """
    errors: list[str] = []
    warnings: list[str] = []
    approved = 0

    # Municipalities this warehouse's Functional Urban Area actually covers. A set only for
    # membership tests — never iterated, so it does not influence any sampling.
    service_area = {
        (m["province_code"], m["municipality_code"]) for m in reference.municipalities(warehouse)
    }
    total_candidates = reference.candidate_count()

    seen_ids: set = set()
    duplicated: set = set()
    municipalities_used: set = set()
    with_number = 0

    for customer in customers:
        customer_id = customer.get("customer_id")
        if customer_id in seen_ids:
            duplicated.add(customer_id)
        seen_ids.add(customer_id)

        if customer.get("wh") != warehouse:
            errors.append(
                f"{customer_id}: wh={customer.get('wh')!r} is not the partition's ({warehouse!r})"
            )
            continue

        index = customer.get("candidate_index")
        if not isinstance(index, int) or not (0 <= index < total_candidates):
            errors.append(
                f"{customer_id}: candidate_index={index!r} outside the reference "
                f"(0..{total_candidates - 1})"
            )
            continue
        candidate = reference.candidate(index)

        divergent = [
            field
            for field in GEO_FIELDS
            if customer.get(field) != candidate.get(field)
        ]
        if divergent:
            errors.append(
                f"{customer_id}: field(s) {divergent} diverge from row "
                f"{index} of address_candidates.json"
            )
            continue

        key = (customer["province_code"], customer["municipality_code"])
        if key not in service_area:
            errors.append(
                f"{customer_id}: municipality {key[0]}/{key[1]} outside the Functional "
                f"Urban Area of {warehouse}"
            )
            continue
        municipalities_used.add(key)

        problem = _check_house_number(customer, candidate)
        if problem:
            errors.append(problem)
            continue
        if customer.get("house_number") is not None:
            with_number += 1
        approved += 1

    if duplicated:
        errors.append(f"duplicated customer_id in partition: {sorted(duplicated)[:10]}")

    available = len(service_area)
    if len(municipalities_used) == 1 and available > 1 and len(customers) > 10:
        warnings.append(
            f"all {len(customers)} customers fell in a single municipality, with "
            f"{available} available in the FUA — the population weight may be degenerate"
        )
    if customers and with_number == 0:
        warnings.append(
            "no customer received a house number: legitimate if all drawn tramos "
            "are numbering_type='0', suspicious otherwise"
        )
    return errors, warnings, approved


def run(args) -> int:
    partition = args.partition
    errors: list[str] = []
    warnings: list[str] = []

    path = manifest_path(partition)
    if not os.path.exists(path):
        print(f"ERROR: manifest not found at {path}")
        return EXIT_FAILED
    try:
        manifest, _ = read_json(path)
    except CorruptFileError as exc:
        print(f"ERROR: manifest unreadable: {exc}")
        return EXIT_FAILED
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        print(f"ERROR: manifest has no 'files' list: {path}")
        return EXIT_FAILED

    # The snapshot root is two levels up: ingestion_date= and wh=.
    root = os.path.abspath(os.path.join(partition, "..", ".."))
    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        errors.append(
            f"manifest_version {version!r} differs from the supported one ({MANIFEST_VERSION})"
        )

    warehouse = (manifest.get("partition") or {}).get("warehouse")
    config = manifest.get("config") or {}

    print(f"partition ........ {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"warehouse ........ {warehouse}")
    print(f"seed ............. {config.get('seed')}")
    print(f"complete ......... {manifest.get('complete')}")

    # ---- Integrity of the declared files -----------------------------------
    checked = 0
    declared: set = set()
    payloads: list = []
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or "path" not in entry:
            errors.append(f"malformed manifest entry: {entry!r}")
            continue
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            errors.append(f"path outside the snapshot root: {entry['path']}")
            continue
        declared.add(os.path.abspath(target))
        if not os.path.exists(target):
            errors.append(f"missing file: {entry['path']}")
            continue
        blob = read_bytes(target)
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum mismatch: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"size mismatch: {entry['path']}")
        try:
            payload, _ = read_json(target)
        except CorruptFileError as exc:
            errors.append(f"unreadable file: {exc}")
            continue
        payloads.append(payload)
        records = count_customers(payload)
        if records != entry.get("records"):
            errors.append(
                f"count mismatch in {entry['path']}: "
                f"manifest={entry.get('records')} file={records}"
            )
    print(f"files ............ {checked}/{len(manifest['files'])} read and checked")

    if manifest.get("failures"):
        errors.append(f"{len(manifest['failures'])} failure(s) recorded in the manifest")

    # ---- Files on disk not declared in the manifest ------------------------
    orphans: list[str] = []
    known_root_files = {MANIFEST_NAME, SUCCESS_NAME}
    partition_abs = os.path.abspath(partition)
    for base, _, names in os.walk(partition_abs):
        for name in sorted(names):
            candidate_path = os.path.abspath(os.path.join(base, name))
            if base == partition_abs and name in known_root_files:
                continue
            if candidate_path not in declared:
                orphans.append(os.path.relpath(candidate_path, partition_abs))
    print(f"orphans .......... {len(orphans)}")
    if orphans:
        errors.append(f"file(s) on disk outside the manifest: {orphans}")

    # ---- _SUCCESS marker consistent with 'complete' ------------------------
    has_success = os.path.exists(success_path(partition))
    if bool(manifest.get("complete")) != has_success:
        errors.append(
            f"_SUCCESS {'present' if has_success else 'absent'} contradicts "
            f"complete={manifest.get('complete')}"
        )

    customers = [row for payload in payloads for row in customers_of(payload)]

    # ---- Schema and required fields ----------------------------------------
    absent = missing_fields(customers)
    if absent:
        errors.append(f"required field(s) missing from some record: {absent}")
    observed_fingerprint = fingerprint(payloads)
    declared_fingerprint = manifest.get("schema_fingerprint") or {}
    if observed_fingerprint.get("sha256") != declared_fingerprint.get("sha256"):
        errors.append(
            f"schema_fingerprint mismatch: manifest="
            f"{declared_fingerprint.get('sha256')} observed={observed_fingerprint.get('sha256')}"
        )
    # The house_number key must exist in every record, even when null: that is what keeps
    # the partition fingerprint from depending on the seed.
    if customers and "house_number" not in set(observed_fingerprint.get("customer_keys") or []):
        errors.append("no record declares the house_number key")

    # ---- Totals re-checked against the re-read -----------------------------
    totals = manifest.get("totals") or {}
    observed_totals = {
        "customer_rows": len(customers),
        "municipalities_used": len(
            {(c.get("province_code"), c.get("municipality_code")) for c in customers}
        ),
        "house_number_null": sum(1 for c in customers if c.get("house_number") is None),
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} mismatch: manifest={totals.get(key)} observed={observed}"
            )
    if totals.get("customer_rows") != config.get("count"):
        errors.append(
            f"config.count={config.get('count')} does not match "
            f"totals.customer_rows={totals.get('customer_rows')}"
        )

    # ---- Geospatial coherence against the reference ------------------------
    try:
        reference = load(args.reference)
    except ReferenceError as exc:
        print()
        print(f"FAILED: reference unusable: {exc}")
        return EXIT_FAILED

    declared_reference = (manifest.get("reference") or {}).get("callejero_ingestion_date")
    if declared_reference != reference.callejero_ingestion_date:
        errors.append(
            f"reference from another Callejero snapshot: manifest={declared_reference} "
            f"file={reference.callejero_ingestion_date}"
        )

    ingestion_date = (manifest.get("partition") or {}).get("ingestion_date")
    errors.extend(_check_minimum_age(customers, reference, ingestion_date))
    print(f"customer base .... minimum age {reference.min_customer_age} "
          f"(reference target: {reference.customer_allocation.get('rule')})")

    geo_errors, geo_warnings, approved = _check_reference(customers, reference, warehouse)
    errors.extend(geo_errors[:20])
    if len(geo_errors) > 20:
        errors.append(f"... and {len(geo_errors) - 20} more geographic coherence problem(s)")
    warnings.extend(geo_warnings)
    print(f"coherence ........ {approved}/{len(customers)} customers with a "
          f"real address in the FUA of {warehouse}")

    if args.strict and warnings:
        errors.append(f"{len(warnings)} distribution warning(s) (--strict)")

    print()
    for item in warnings:
        print(f"WARNING: {item}")
    if errors:
        print(f"FAILED ({len(errors)} problem(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integrity, schema, totals and geospatial coherence validated.")
    return EXIT_OK
