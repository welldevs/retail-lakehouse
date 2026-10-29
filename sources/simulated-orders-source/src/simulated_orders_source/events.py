"""Event vocabulary, state machine and envelope.

This is the CONTRACT in code. The generator emits from here and validate re-checks from here
— a single place defines what a valid event is, so the two cannot diverge.

THE ENVELOPE IS DETERMINISTIC AND CLOCK-FREE
--------------------------------------------
`event_id` is `sha256(order_id|sequence_no)`, never `uuid4()`: there is no entropy and no clock
in this Source. `occurred_at` derives from the partition date and the offsets declared in the
premises, never from `datetime.now()`. It is the same as guarantee 5 of the Phase 1 contract
("birth_year derives from ingestion_date, never from the clock"), applied to event time.

`recorded_at` does NOT exist here, on purpose. Delay and reordering are properties of the
TRANSPORT, and whoever consumes measures them — on the other side of the broker, with their own
clock. Stamping them here would invent a delay nobody observed, and would also destroy the
byte-for-byte reproducibility of the log.

VOCABULARY: PRESERVE, DO NOT PROMISE
------------------------------------
`order_line_removed` carries `reason = "unavailable"`, not "out_of_stock". There is no stock
fact in any source of this platform; naming it as if there were would promise data nobody
measured. Same discipline that made Phase 1 preserve INE's `numbering_type` instead of
inventing an `address_precision`.
"""

from __future__ import annotations

import hashlib

# ---------------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------------

PLACED = "PLACED"
CONFIRMED = "CONFIRMED"
PICKING = "PICKING"
PICKED = "PICKED"
IN_TRANSIT = "IN_TRANSIT"
DELIVERED = "DELIVERED"
PAYMENT_FAILED = "PAYMENT_FAILED"
CANCELLED = "CANCELLED"
DELIVERY_FAILED = "DELIVERY_FAILED"
RETURNED = "RETURNED"

STATES = (
    PLACED,
    CONFIRMED,
    PICKING,
    PICKED,
    IN_TRANSIT,
    DELIVERED,
    PAYMENT_FAILED,
    CANCELLED,
    DELIVERY_FAILED,
    RETURNED,
)

# Sentinel for a TAMPERED or incomplete log. It is not a domain state: no order is born or
# ends here. It exists so that COUNTING events of a broken log is possible without raising —
# the state machine check is what reports the invalid transition, and a counter that blows up
# would turn "invalid partition" (code 1) into "unhandled exception" (code 3), erasing the
# difference between bad data and a validator bug.
INVALID = "INVALID"

# States no event can leave. `DELIVERED` is NOT here: a return can still happen after it,
# which is exactly why it does not close the order.
TERMINAL_STATES = (PAYMENT_FAILED, CANCELLED, DELIVERY_FAILED, RETURNED)

# ---------------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------------

ORDER_PLACED = "order_placed"
ORDER_PAYMENT_AUTHORIZED = "order_payment_authorized"
ORDER_PAYMENT_FAILED = "order_payment_failed"
ORDER_CANCELLED = "order_cancelled"
ORDER_PICKING_STARTED = "order_picking_started"
ORDER_LINE_SUBSTITUTED = "order_line_substituted"
ORDER_LINE_REMOVED = "order_line_removed"
ORDER_PICKED = "order_picked"
ORDER_DISPATCHED = "order_dispatched"
ORDER_DELIVERED = "order_delivered"
ORDER_DELIVERY_FAILED = "order_delivery_failed"
ORDER_RETURNED = "order_returned"

EVENT_TYPES = (
    ORDER_PLACED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_CANCELLED,
    ORDER_PICKING_STARTED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_LINE_REMOVED,
    ORDER_PICKED,
    ORDER_DISPATCHED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_RETURNED,
)

# Order state AFTER each event. The two line events do not change the order state — they
# change the BASKET, which is what makes the fold non-trivial.
STATE_AFTER = {
    ORDER_PLACED: PLACED,
    ORDER_PAYMENT_AUTHORIZED: CONFIRMED,
    ORDER_PAYMENT_FAILED: PAYMENT_FAILED,
    ORDER_CANCELLED: CANCELLED,
    ORDER_PICKING_STARTED: PICKING,
    ORDER_LINE_SUBSTITUTED: PICKING,
    ORDER_LINE_REMOVED: PICKING,
    ORDER_PICKED: PICKED,
    ORDER_DISPATCHED: IN_TRANSIT,
    ORDER_DELIVERED: DELIVERED,
    ORDER_DELIVERY_FAILED: DELIVERY_FAILED,
    ORDER_RETURNED: RETURNED,
}

# State required BEFORE each event. `None` for the event that opens the aggregate.
STATE_BEFORE = {
    ORDER_PLACED: (None,),
    ORDER_PAYMENT_AUTHORIZED: (PLACED,),
    ORDER_PAYMENT_FAILED: (PLACED,),
    ORDER_CANCELLED: (PLACED, CONFIRMED),
    ORDER_PICKING_STARTED: (CONFIRMED,),
    ORDER_LINE_SUBSTITUTED: (PICKING,),
    ORDER_LINE_REMOVED: (PICKING,),
    ORDER_PICKED: (PICKING,),
    ORDER_DISPATCHED: (PICKED,),
    ORDER_DELIVERED: (IN_TRANSIT,),
    ORDER_DELIVERY_FAILED: (IN_TRANSIT,),
    ORDER_RETURNED: (DELIVERED,),
}

# Envelope fields, present in EVERY event and always with these names.
ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "order_id",
    "sequence_no",
    "wh",
    "producer",
    "payload",
)

EVENT_VERSION = 1

# Closed payload vocabularies.
CANCELLED_BY = ("customer", "system")
CANCEL_REASONS = ("changed_mind", "slot_no_longer_suitable", "duplicate_order")
DECLINE_REASONS = ("insufficient_funds", "card_expired", "issuer_declined")
REMOVAL_REASON = "unavailable"
DELIVERY_FAILURE_REASONS = ("customer_absent", "address_unreachable")
PAYMENT_METHODS = ("card", "wallet")

LINE_FULFILLED = "fulfilled"
LINE_SUBSTITUTED = "substituted"
LINE_REMOVED = "removed"
LINE_STATUSES = (LINE_FULFILLED, LINE_SUBSTITUTED, LINE_REMOVED)


class EventError(Exception):
    """Event outside the vocabulary, or a transition the state machine does not allow."""


def event_id(order_id: str, sequence_no: int) -> str:
    """Deterministic event identity.

    Derived from the aggregate and the position, not from a clock or entropy: re-running the
    same (reference, premises, seed, day, warehouse) must produce the SAME event_id, otherwise
    the downstream consumer cannot deduplicate across replays.
    """
    blob = f"{order_id}|{sequence_no}".encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:32]


def envelope(
    order_id: str,
    wh: str,
    sequence_no: int,
    event_type: str,
    occurred_at: str,
    payload: dict,
    producer: str,
) -> dict:
    if event_type not in STATE_AFTER:
        raise EventError(f"event type outside the vocabulary: {event_type!r}")
    return {
        "event_id": event_id(order_id, sequence_no),
        "event_type": event_type,
        "event_version": EVENT_VERSION,
        "occurred_at": occurred_at,
        "order_id": order_id,
        "sequence_no": sequence_no,
        "wh": wh,
        "producer": producer,
        "payload": payload,
    }


def apply_transition(state, event_type: str) -> str:
    """Apply an event to a state. Raises EventError if the transition does not exist.

    It is the same function the generator uses to build and validate uses to re-check, on
    purpose: a generator that emitted an invalid transition would be caught by its own
    vocabulary, not only by a dbt test three layers further down.
    """
    allowed = STATE_BEFORE.get(event_type)
    if allowed is None:
        raise EventError(f"event type outside the vocabulary: {event_type!r}")
    if state in TERMINAL_STATES:
        raise EventError(
            f"event {event_type!r} after terminal state {state!r}: a closed "
            f"aggregate receives no more events"
        )
    if state not in allowed:
        raise EventError(
            f"invalid transition: {event_type!r} requires a state in {allowed}, but the order "
            f"is in {state!r}"
        )
    return STATE_AFTER[event_type]


def fold(events, strict: bool = True) -> str:
    """Resulting state of a sequence of events of ONE order, in order.

    `strict=True` (default) raises EventError on the first invalid transition: that is what the
    generator uses, because emitting an impossible transition is a generator bug.

    `strict=False` returns the INVALID sentinel and stops: that is what a COUNTER uses, because
    counting a tampered log must produce a diverging number, not an exception.
    """
    state = None
    for event in events:
        try:
            state = apply_transition(state, event["event_type"])
        except EventError:
            if strict:
                raise
            return INVALID
    return state
