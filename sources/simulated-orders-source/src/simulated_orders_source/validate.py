"""Independent validation of an event-log partition.

Re-reads the file declared in the manifest, recomputing checksum, count, fingerprint and ALL
totals instead of accepting the recorded values, and walks the directory to detect files the
manifest does not declare.

ON TOP OF THAT, and this is where the value is: it re-reads the same reference `extract` used
and re-checks, event by event, the four guarantees no checksum reaches —

  1. the order's customer belongs to the partition's warehouse (measured rule: 0 postal codes
     cross warehouses);
  2. every ordered product exists in THAT warehouse's catalog ON THAT date;
  3. the price paid is the observed price of that (warehouse, date, product), decimal place by
     decimal place — compared as Decimal, never as float;
  4. each order's event sequence is contiguous and the state machine accepts it.

A validator that only checks checksums would prove integrity, not coherence. Hence --reference
is required, as in the customers Source.

Two kinds of problem:
  - INTEGRITY and COHERENCE -> always fatal (code 1).
  - QUALITY/DISTRIBUTION    -> measured and reported; fatal only with --strict.
"""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json, read_jsonl
from .events import (
    ENVELOPE_FIELDS,
    EVENT_VERSION,
    EventError,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PICKED,
    ORDER_PLACED,
    TERMINAL_STATES,
    apply_transition,
    event_id,
)
from .partition import MANIFEST_NAME, SUCCESS_NAME, manifest_path, success_path
from .demand import DemandError
from .reference_data import ReferenceError, load
from .schema import events_of, fingerprint, group_by_order, missing_fields, totals_of

EXIT_OK = 0
EXIT_FAILED = 1

MAX_REPORTED = 20


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def _decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _check_envelope(events, warehouse, order_date) -> list[str]:
    """Envelope: deterministic identity, warehouse, day and vocabulary."""
    problems: list[str] = []
    seen: set = set()
    for event in events:
        missing = [f for f in ENVELOPE_FIELDS if f not in event]
        if missing:
            problems.append(f"event missing envelope field {missing}: {event.get('event_id')}")
            continue
        if event["wh"] != warehouse:
            problems.append(
                f"event {event['event_id']} with wh={event['wh']!r}, but the partition is "
                f"{warehouse!r}"
            )
        if event["event_version"] != EVENT_VERSION:
            problems.append(
                f"event {event['event_id']} with event_version={event['event_version']!r}, "
                f"supported={EVENT_VERSION}"
            )
        # event_id is derivable: recomputing it is the only way to prove it was neither
        # invented nor shuffled between orders.
        expected = event_id(event["order_id"], event["sequence_no"])
        if event["event_id"] != expected:
            problems.append(
                f"event_id {event['event_id']} does not derive from "
                f"({event['order_id']}, {event['sequence_no']}); expected {expected}"
            )
        if event["event_id"] in seen:
            problems.append(f"duplicate event_id: {event['event_id']}")
        seen.add(event["event_id"])
        if not event["occurred_at"].startswith(order_date) and event["event_type"] == ORDER_PLACED:
            problems.append(
                f"order {event['order_id']} placed at {event['occurred_at']}, outside the "
                f"partition day ({order_date})"
            )
    return problems


def _check_orders(grouped, reference, warehouse, order_date) -> tuple[list[str], list[str], int]:
    """Order-by-order coherence against the reference. Returns (errors, warnings, passed)."""
    problems: list[str] = []
    warnings: list[str] = []
    passed = 0

    try:
        calendar = reference.calendar_of(warehouse, order_date)
    except ReferenceError as exc:
        return [str(exc)], [], 0

    price_as_of = calendar["price_as_of"]
    catalog = {row["source_product_id"]: row for row in reference.catalog_of(warehouse, price_as_of)}
    customers = {row["customer_id"]: row for row in reference.customers_of(warehouse)}
    minimum = int(float(reference.premises["min_buyer_age"]))

    for order_id in sorted(grouped):
        order_events = grouped[order_id]
        failed = False

        # ---- contiguous sequence ------------------------------------------------
        expected = list(range(1, len(order_events) + 1))
        observed = [e["sequence_no"] for e in order_events]
        if observed != expected:
            problems.append(
                f"{order_id}: sequence_no not contiguous — observed {observed[:8]}..., "
                f"expected 1..{len(order_events)}"
            )
            failed = True

        # ---- state machine ------------------------------------------------------
        state = None
        try:
            for event in order_events:
                state = apply_transition(state, event["event_type"])
        except EventError as exc:
            problems.append(f"{order_id}: {exc}")
            failed = True

        first = order_events[0]
        if first["event_type"] != ORDER_PLACED:
            problems.append(
                f"{order_id}: first event is {first['event_type']!r}, not "
                f"{ORDER_PLACED!r}"
            )
            failed = True
            continue

        placed = first["payload"]

        # ---- the customer belongs to that warehouse, and already existed --------
        customer = customers.get(placed.get("customer_id"))
        if customer is None:
            problems.append(
                f"{order_id}: customer {placed.get('customer_id')!r} does not belong to "
                f"{warehouse!r} in the reference"
            )
            failed = True
        else:
            version = reference.customer_version_at(customer["first_ingestion_date"], order_date)
            if version is None:
                problems.append(
                    f"{order_id}: customer {customer['customer_id']} only exists from "
                    f"{customer['first_ingestion_date']}, after {order_date}"
                )
                failed = True
            elif placed.get("customer_ingestion_date") != version:
                problems.append(
                    f"{order_id}: customer_ingestion_date={placed.get('customer_ingestion_date')!r} "
                    f"is not the version in force on {order_date} ({version})"
                )
                failed = True
            for field in ("province_code", "municipality_code", "postal_code"):
                if placed.get(field) != customer[field]:
                    problems.append(
                        f"{order_id}: {field}={placed.get(field)!r} differs from the customer "
                        f"record ({customer[field]!r})"
                    )
                    failed = True

            # ---- the stamped age band is the one the customer's age produces ----
            # RECOMPUTED, not merely checked against itself: the stamp decides which weight
            # vector drew the basket, and a wrong stamp would produce a mix consistent with
            # another cohort — plausible, no nulls, no broken total.
            age = int(order_date[:4]) - int(customer["birth_year"])
            if age < minimum:
                problems.append(
                    f"{order_id}: customer {customer['customer_id']} was {age} year(s) old on "
                    f"{order_date}, below min_buyer_age={minimum}"
                )
                failed = True
            try:
                expected_band = reference.demand.band_of(age)
            except DemandError as exc:
                problems.append(f"{order_id}: {exc}")
                failed = True
            else:
                if placed.get("buyer_age_band") != expected_band:
                    problems.append(
                        f"{order_id}: buyer_age_band={placed.get('buyer_age_band')!r}, but "
                        f"the customer's age {age} falls in {expected_band!r}"
                    )
                    failed = True

        # ---- the declared price basis is the calendar's --------------------------
        if placed.get("price_as_of") != price_as_of:
            problems.append(
                f"{order_id}: price_as_of={placed.get('price_as_of')!r}, but the calendar says "
                f"{price_as_of!r} for {warehouse}/{order_date}"
            )
            failed = True
        if placed.get("price_source") != calendar["price_source"]:
            problems.append(
                f"{order_id}: price_source={placed.get('price_source')!r}, but the calendar "
                f"says {calendar['price_source']!r}"
            )
            failed = True

        # ---- product and price of each line -------------------------------------
        lines = {}
        gross = Decimal("0")
        for line in placed.get("lines") or []:
            lines[line["line_no"]] = dict(line)
            product = catalog.get(line["source_product_id"])
            if product is None:
                problems.append(
                    f"{order_id} line {line['line_no']}: product "
                    f"{line['source_product_id']!r} does not exist in the {warehouse} catalog "
                    f"on {price_as_of}. Ordering what the warehouse does not sell would be "
                    f"inventing assortment."
                )
                failed = True
                continue
            price = _decimal(line.get("unit_price"))
            if price is None or price != product["unit_price"]:
                problems.append(
                    f"{order_id} line {line['line_no']}: unit_price="
                    f"{line.get('unit_price')!r}, but the observed price in "
                    f"{warehouse}/{price_as_of} for {line['source_product_id']} is "
                    f"{product['unit_price']}"
                )
                failed = True
                continue
            if not isinstance(line.get("quantity"), int) or line["quantity"] < 1:
                problems.append(
                    f"{order_id} line {line['line_no']}: invalid quantity "
                    f"({line.get('quantity')!r})"
                )
                failed = True
                continue
            gross += price * line["quantity"]

        declared = _decimal(placed.get("gross_amount"))
        if declared is None or declared != gross:
            problems.append(
                f"{order_id}: gross_amount={placed.get('gross_amount')!r} is not the sum of "
                f"the placed lines ({gross})"
            )
            failed = True
        if placed.get("line_count") != len(lines):
            problems.append(
                f"{order_id}: line_count={placed.get('line_count')!r} differs from the "
                f"{len(lines)} line(s) present"
            )
            failed = True

        # ---- the fold: substitution and removal change the basket ----------------
        effective = {no: dict(line) for no, line in lines.items()}
        for event in order_events[1:]:
            payload = event["payload"]
            if event["event_type"] == ORDER_LINE_SUBSTITUTED:
                no = payload.get("line_no")
                if no not in effective:
                    problems.append(f"{order_id}: substitution of nonexistent line {no!r}")
                    failed = True
                    continue
                substitute = catalog.get(payload.get("substitute_source_product_id"))
                if substitute is None:
                    problems.append(
                        f"{order_id} line {no}: substitute "
                        f"{payload.get('substitute_source_product_id')!r} does not exist in "
                        f"the {warehouse} catalog on {price_as_of}"
                    )
                    failed = True
                    continue
                price = _decimal(payload.get("substitute_unit_price"))
                if price is None or price != substitute["unit_price"]:
                    problems.append(
                        f"{order_id} line {no}: substitute_unit_price="
                        f"{payload.get('substitute_unit_price')!r}, observed "
                        f"{substitute['unit_price']}"
                    )
                    failed = True
                    continue
                effective[no]["unit_price"] = str(price)
            elif event["event_type"] == ORDER_LINE_REMOVED:
                no = payload.get("line_no")
                if no not in effective:
                    problems.append(f"{order_id}: removal of nonexistent line {no!r}")
                    failed = True
                    continue
                if payload.get("reason") != "unavailable":
                    problems.append(
                        f"{order_id} line {no}: reason={payload.get('reason')!r} outside the "
                        f"vocabulary"
                    )
                    failed = True
                effective.pop(no)
            elif event["event_type"] == ORDER_PICKED:
                net = sum(
                    (_decimal(l["unit_price"]) * l["quantity"] for l in effective.values()),
                    Decimal("0"),
                )
                declared_picked = _decimal(payload.get("picked_amount"))
                if declared_picked is None or declared_picked != net:
                    problems.append(
                        f"{order_id}: picked_amount={payload.get('picked_amount')!r} does not "
                        f"match the fulfilled lines ({net})"
                    )
                    failed = True
                if payload.get("picked_line_count") != len(effective):
                    problems.append(
                        f"{order_id}: picked_line_count={payload.get('picked_line_count')!r} "
                        f"differs from the {len(effective)} fulfilled line(s)"
                    )
                    failed = True

        # ---- time does not go backwards within an order --------------------------
        instants = [e["occurred_at"] for e in order_events]
        if instants != sorted(instants):
            problems.append(f"{order_id}: occurred_at goes backwards within the order")
            failed = True

        if not failed:
            passed += 1

    # ---- distribution warnings ------------------------------------------------
    total = len(grouped)
    if total:
        changed = sum(
            1
            for order_events in grouped.values()
            if any(
                e["event_type"] in (ORDER_LINE_SUBSTITUTED, ORDER_LINE_REMOVED)
                for e in order_events
            )
        )
        if changed == 0:
            # The most important warning of this Source. If no basket changed, the final value
            # is derivable from the first event, the fold is trivial and the event model
            # becomes decoration — which is exactly what ARCHITECTURE.md refuses.
            warnings.append(
                "no order had a substituted or removed line: the fold became trivial and the "
                "log no longer justifies the event model"
            )
        terminal = sum(
            1 for order_events in grouped.values()
            if any(e["event_type"] in TERMINAL_STATES for e in order_events)
        )
        if terminal == total and total > 1:
            warnings.append("all orders ended on the same event")

    return problems, warnings, passed


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

    partition_block = manifest.get("partition") or {}
    warehouse = partition_block.get("warehouse")
    order_date = partition_block.get("ingestion_date")
    config = manifest.get("config") or {}

    print(f"partition ........ {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"warehouse ........ {warehouse}")
    print(f"order day ........ {order_date}")
    print(f"seed ............. {config.get('seed')}")
    print(f"premises ......... {str(config.get('premises_sha256'))[:16]}...")
    print(f"demand ........... {config.get('demand_model_version')}")
    print(f"complete ......... {manifest.get('complete')}")

    # ---- Integrity of the declared files --------------------------------------
    checked = 0
    declared: set = set()
    events: list = []
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
            rows, _ = read_jsonl(target)
        except CorruptFileError as exc:
            errors.append(f"unreadable file: {exc}")
            continue
        events.extend(events_of(rows))
        if len(rows) != entry.get("records"):
            errors.append(
                f"count mismatch in {entry['path']}: "
                f"manifest={entry.get('records')} file={len(rows)}"
            )
    print(f"files ............ {checked}/{len(manifest['files'])} read and checked")

    if manifest.get("failures"):
        errors.append(f"{len(manifest['failures'])} failure(s) recorded in the manifest")

    # ---- Files on disk not declared in the manifest ---------------------------
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

    # ---- _SUCCESS marker consistent with 'complete' ---------------------------
    has_success = os.path.exists(success_path(partition))
    if bool(manifest.get("complete")) != has_success:
        errors.append(
            f"_SUCCESS {'present' if has_success else 'absent'} contradicts "
            f"complete={manifest.get('complete')}"
        )

    # ---- Schema and required fields -------------------------------------------
    absent = missing_fields(events)
    if absent:
        errors.append(f"declared field(s) missing from some event: {absent[:MAX_REPORTED]}")
    declared_fingerprint = manifest.get("schema_fingerprint") or {}
    if fingerprint().get("sha256") != declared_fingerprint.get("sha256"):
        errors.append(
            f"schema_fingerprint mismatch: manifest={declared_fingerprint.get('sha256')} "
            f"this code's vocabulary={fingerprint().get('sha256')}"
        )

    # ---- Total ordering of the log ---------------------------------------------
    order_keys = [(e.get("occurred_at"), e.get("order_id"), e.get("sequence_no")) for e in events]
    if order_keys != sorted(order_keys):
        errors.append(
            "log out of order: lines must be sorted by "
            "(occurred_at, order_id, sequence_no)"
        )

    errors.extend(_check_envelope(events, warehouse, order_date)[:MAX_REPORTED])

    # ---- Totals re-checked against the re-read --------------------------------
    observed_totals = totals_of(events)
    observed_totals["bytes"] = sum(
        e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)
    )
    totals = manifest.get("totals") or {}
    for key, observed in sorted(observed_totals.items()):
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} mismatch: manifest={totals.get(key)} observed={observed}"
            )
    if totals.get("order_rows") != config.get("orders"):
        errors.append(
            f"config.orders={config.get('orders')} does not match "
            f"totals.order_rows={totals.get('order_rows')}"
        )

    # ---- Coherence against the reference --------------------------------------
    try:
        reference = load(args.reference)
    except ReferenceError as exc:
        print()
        print(f"FAILED: unusable reference: {exc}")
        return EXIT_FAILED

    declared_demand = (manifest.get("config") or {}).get("demand_model_version")
    if declared_demand != reference.demand.version:
        errors.append(
            f"reference with a different demand model: manifest={declared_demand} "
            f"file={reference.demand.version}. The mix behind these orders is not what "
            f"this directory's profile describes."
        )

    declared_premises = (manifest.get("reference") or {}).get("premises_sha256")
    if declared_premises != reference.premises_sha256:
        errors.append(
            f"reference with a different premises table: manifest={declared_premises} "
            f"file={reference.premises_sha256}. The rates behind these orders are not "
            f"the ones in this directory."
        )

    grouped = group_by_order(events)
    order_errors, order_warnings, passed = _check_orders(
        grouped, reference, warehouse, order_date
    )
    errors.extend(order_errors[:MAX_REPORTED])
    if len(order_errors) > MAX_REPORTED:
        errors.append(f"... and {len(order_errors) - MAX_REPORTED} more coherence problem(s)")
    warnings.extend(order_warnings)
    print(f"coherence ........ {passed}/{len(grouped)} orders with real customer, product and "
          f"price from {warehouse}")
    print(f"fold ............. placed {observed_totals['gross_amount_placed']} / "
          f"picked {observed_totals['net_amount_picked']} "
          f"({observed_totals['substituted_lines']} substitution(s), "
          f"{observed_totals['removed_lines']} removal(s))")

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
    print("OK: integrity, schema, totals, state machine and coherence validated.")
    return EXIT_OK
