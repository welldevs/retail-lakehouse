"""Event shape, schema fingerprint and re-checkable totals.

THE FINGERPRINT IS OF THE DECLARED CONTRACT, NOT OF WHATEVER HAPPENED TO COME OUT
---------------------------------------------------------------------------------
In the other four sources the fingerprint is the UNION of the observed keys. Here that would
be a defect, and Phase 1 had already recorded why in another form ("an optional key would make
the partition fingerprint depend on the seed"): not every event type appears every day — a day
with no returns would have no `order_returned` in the observed set, and the fingerprint of two
correct partitions would diverge by chance.

So the fingerprint covers the WHOLE vocabulary, statically: the envelope fields plus the
declared payload fields of each of the 12 types. It changes when the CODE changes, which is
exactly what it exists to detect. What actually came out on that day lives in
`totals.event_rows_by_type`, where variation is legitimate.

`missing_fields` goes the other way: it checks the observed against the declared. Together the
two catch drift in both directions.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from .events import (
    ENVELOPE_FIELDS,
    EVENT_TYPES,
    ORDER_CANCELLED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_DISPATCHED,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_PICKED,
    ORDER_PICKING_STARTED,
    ORDER_PLACED,
    ORDER_RETURNED,
    TERMINAL_STATES,
    fold,
)

# Declared payload fields of each type. Two types have an empty payload on purpose: the
# instant and the transition ARE the information, and an invented field there would be noise.
PAYLOAD_FIELDS = {
    ORDER_PLACED: (
        "customer_id",
        "customer_ingestion_date",
        "province_code",
        "municipality_code",
        "postal_code",
        "price_as_of",
        "price_source",
        "delivery_slot_start",
        "delivery_slot_end",
        "line_count",
        "gross_amount",
        "lines",
    ),
    ORDER_PAYMENT_AUTHORIZED: ("payment_method", "authorized_amount"),
    ORDER_PAYMENT_FAILED: ("decline_reason",),
    ORDER_CANCELLED: ("cancelled_by", "reason"),
    ORDER_PICKING_STARTED: (),
    ORDER_LINE_SUBSTITUTED: (
        "line_no",
        "source_product_id",
        "substitute_source_product_id",
        "substitute_unit_price",
        "quantity",
    ),
    ORDER_LINE_REMOVED: ("line_no", "source_product_id", "quantity", "reason"),
    ORDER_PICKED: ("picked_line_count", "picked_amount"),
    ORDER_DISPATCHED: (),
    ORDER_DELIVERED: ("delivered_within_slot", "delivered_line_count", "delivered_amount"),
    ORDER_DELIVERY_FAILED: ("reason",),
    ORDER_RETURNED: ("returned_line_count", "returned_line_no", "returned_amount"),
}

LINE_FIELDS = (
    "line_no",
    "source_product_id",
    "category_id",
    "subgroup_id",
    "quantity",
    "unit_price",
)


def events_of(payload: object) -> list:
    """Events of a log. Empty list for any unexpected shape."""
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def fingerprint() -> dict:
    """Fingerprint of the declared vocabulary. Depends on neither seed nor day."""
    declared = {
        "envelope_keys": sorted(ENVELOPE_FIELDS),
        "line_keys": sorted(LINE_FIELDS),
        "payload_keys": {kind: sorted(PAYLOAD_FIELDS[kind]) for kind in sorted(EVENT_TYPES)},
    }
    blob = json.dumps(declared, ensure_ascii=False, sort_keys=True).encode("utf-8")
    declared["sha256"] = hashlib.sha256(blob).hexdigest()
    return declared


def missing_fields(events) -> list[str]:
    """Declared fields missing from some event, aggregated (not one error per line)."""
    absent: set = set()
    for event in events:
        absent.update(f"envelope.{f}" for f in ENVELOPE_FIELDS if f not in event)
        kind = event.get("event_type")
        payload = event.get("payload")
        if kind not in PAYLOAD_FIELDS or not isinstance(payload, dict):
            continue
        absent.update(f"{kind}.{f}" for f in PAYLOAD_FIELDS[kind] if f not in payload)
        if kind == ORDER_PLACED:
            for line in payload.get("lines") or []:
                if isinstance(line, dict):
                    absent.update(f"line.{f}" for f in LINE_FIELDS if f not in line)
    return sorted(absent)


def group_by_order(events) -> dict:
    """Events per order, each list in sequence_no order.

    The output `dict` is looked up and sorted by the caller, never iterated to produce
    content — insertion order here comes from the log, which is already deterministic.
    """
    grouped: dict[str, list] = {}
    for event in events:
        grouped.setdefault(event["order_id"], []).append(event)
    for order_id in grouped:
        grouped[order_id].sort(key=lambda e: e["sequence_no"])
    return grouped


def totals_of(events) -> dict:
    """Log totals, derived from the FOLD and not from a state written alongside.

    This is what the manifest declares and what the dbt test re-checks against Silver. The
    monetary values exist in two versions on purpose: `gross_amount` is what was PLACED and
    `net_amount` is what was PICKED. That the two differ is the proof that the fold is
    non-trivial — if they were always equal, the log would be a date stamp.
    """
    grouped = group_by_order(events)
    by_type: dict[str, int] = {kind: 0 for kind in EVENT_TYPES}
    for event in events:
        kind = event.get("event_type")
        if kind in by_type:
            by_type[kind] += 1

    by_state: dict[str, int] = {}
    gross = Decimal("0")
    net = Decimal("0")
    line_rows = 0
    customers: set = set()
    carried_forward_orders = 0

    for order_id in sorted(grouped):
        order_events = grouped[order_id]
        # strict=False: counting must not blow up on a tampered log. See events.fold.
        state = fold(order_events, strict=False)
        by_state[state] = by_state.get(state, 0) + 1

        placed = order_events[0].get("payload") or {}
        gross += Decimal(str(placed.get("gross_amount", "0")))
        line_rows += len(placed.get("lines") or [])
        if placed.get("customer_id") is not None:
            customers.add(placed["customer_id"])
        if placed.get("price_source") not in (None, "observed"):
            carried_forward_orders += 1

        for event in order_events:
            if event.get("event_type") == ORDER_PICKED:
                net += Decimal(str((event.get("payload") or {}).get("picked_amount", "0")))

    return {
        "event_rows": len(events),
        "order_rows": len(grouped),
        "line_rows": line_rows,
        "customers_used": len(customers),
        "substituted_lines": by_type[ORDER_LINE_SUBSTITUTED],
        "removed_lines": by_type[ORDER_LINE_REMOVED],
        "carried_forward_orders": carried_forward_orders,
        "gross_amount_placed": str(gross),
        "net_amount_picked": str(net),
        "event_rows_by_type": by_type,
        "orders_by_state": dict(sorted(by_state.items())),
        "orders_terminal": sum(by_state.get(s, 0) for s in TERMINAL_STATES),
    }
