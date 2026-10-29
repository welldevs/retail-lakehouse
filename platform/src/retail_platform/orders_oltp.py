"""The orders OLTP and its transactional outbox.

WHY THIS EXISTS. The trigger written for Kafka in ARCHITECTURE.md is "a genuinely
event-driven source: POS, webhook, CDC FROM AN OLTP". Republishing the log in batch onto a
topic does not satisfy that trigger — it would add a broker and zero information. What
satisfies it is the event being BORN inside the transaction that changes the order's state.
That is what this module does, and the property it proves — atomicity — is the only reason
the next milestone has the right to exist.

THE PROPERTY, STATED PRECISELY: for every event, either (the state change AND the outbox
row) are visible, or neither is. Never one without the other. Without that, the OLTP state
and the topic diverge with nothing failing, and the divergence only shows up weeks later
in a plausible number.

WHAT THE DOUBLE PROVES AND WHAT ONLY POSTGRES PROVES. The offline tests (fake_pg.py) prove
the BOUNDARY: that both writes go out between the same BEGIN and the same COMMIT, with no
intermediate commit — which is the real defect one commits writing this (one extra
`commit()` in the middle turns an outbox into a dual-write). That the boundary MEANS
something is a property of the engine, and can only be proven against a real Postgres:
`make orders-prove-atomicity` injects a trigger that makes the outbox insert blow up and
checks that the state change did not survive either. Both proofs are needed and neither
replaces the other.

THREE INDEPENDENT GUARDS, WHICH MUST AGREE:

  1. `outbox.event_id` UNIQUE          — re-applying the same event duplicates nothing
  2. `orders.last_sequence_no`         — the OLTP REJECTS an out-of-order event
  3. `orders.status` in `from_states`  — the OLTP REJECTS an invalid transition

Guard 2 is what gives meaning to the Kafka partition key in Milestone 5: if the OLTP
accepted out-of-order events, preserving order by `order_id` in the broker would be
decoration. It is because it rejects them that `key = order_id` becomes a requirement and
not a preference.

THE STATE MACHINE IS REDECLARED HERE, NOT IMPORTED. The Source is FROZEN and this platform
never imports its code — the contract is physical. Declaring the transitions again, from
CONTRACT.md section 5, is the same principle as `verify.py` re-reading the object instead
of trusting what it just wrote: verification instead of trust. If the two declarations
diverge, `orders-apply` fails loudly on the first affected transition.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from . import manifest as manifest_reader

SOURCE_NAME = "simulated_orders"
EVENTS_FILE = "order_events.jsonl"

DEFAULT_DSN = os.environ.get(
    "OLTP_DSN", "postgresql://oltp:oltp@localhost:5433/oltp"
)


class OrdersOltpError(Exception):
    """The OLTP rejected the log: out of order, invalid transition or diverging value."""


# --------------------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------------------
#
# `outbox.event_json` stores THE CANONICAL LOG LINE, verbatim — not a re-serialization.
# That is what goes to the broker in Milestone 5, and that is why the log rebuilt from the
# outbox must match the partition manifest's sha256 (`orders-outbox --verify`).
#
# The envelope columns exist to ROUTE (key, order, filter), not to store the truth again.
# So that the two cannot diverge, four CHECKs tie each column to the JSON itself: a row
# cannot be routed under a key that disagrees with the payload it carries. The engine
# guarantees it, not convention.
#
# `payload` is NOT a column: `event_json::jsonb -> 'payload'` answers any query without
# storing a second copy. Two representations of the same truth diverge.
#
# `created_at default now()` is the ONLY clock in this chain, and it is legitimate: the
# Source log has no `recorded_at` on purpose ("no clock"), because delay and reordering are
# stamped by whoever RECEIVES. This is the moment of receiving.

DDL = """
create table if not exists orders (
    order_id             text        primary key,
    wh                   text        not null,
    order_date           date        not null,
    customer_id          text        not null,
    status               text        not null,
    last_sequence_no     integer     not null check (last_sequence_no > 0),
    placed_at            timestamptz not null,
    updated_at           timestamptz not null,
    delivery_slot_start  timestamptz not null,
    delivery_slot_end    timestamptz not null,
    line_count           integer     not null check (line_count > 0),
    picked_line_count    integer,
    gross_amount         numeric(12,2) not null,
    -- NULL UNTIL DETERMINED, not `gross` nor zero. `net_amount` answers "how much was
    -- actually picked", and an order cancelled before picking never answered that.
    -- Initializing it with the placed amount would make the column answer ANOTHER question
    -- ("how much is the order still worth") under the SAME name `silver_order.net_amount`
    -- uses for the first — and `orders-reconcile`, in Milestone 6, would compare two
    -- different questions. Measured: 298 of the 6,400 orders (196 cancelled, 102 with
    -- payment declined) die before picking. They were exactly the 298 divergences between
    -- the two folds.
    net_amount           numeric(12,2),
    returned_amount      numeric(12,2) not null default 0,
    delivered_within_slot boolean,
    constraint orders_status_vocabulary check (status in (
        'PLACED', 'CONFIRMED', 'PICKING', 'PICKED', 'IN_TRANSIT', 'DELIVERED',
        'PAYMENT_FAILED', 'CANCELLED', 'DELIVERY_FAILED', 'RETURNED'))
);

create table if not exists order_line (
    order_id                    text    not null references orders(order_id),
    line_no                     integer not null check (line_no > 0),
    source_product_id           text    not null,
    quantity                    integer not null check (quantity > 0),
    unit_price                  numeric(12,2) not null,
    status                      text    not null,
    fulfilled_source_product_id text,
    fulfilled_unit_price        numeric(12,2),
    line_amount                 numeric(12,2) not null,
    primary key (order_id, line_no),
    -- The SAME vocabulary as `silver_order_line.line_status`, not a similar one. If the two
    -- sides named the same fact differently, `orders-reconcile` (Milestone 6) would need a
    -- translation table — and a translation table between two folds is exactly where
    -- "compares two wrong things and passes" lives.
    constraint order_line_status_vocabulary check (status in (
        'placed', 'fulfilled', 'substituted', 'removed', 'not_picked')),
    -- A removed line has neither a fulfilled product nor an amount. A substituted line has
    -- both. Without this, a defective applier would leave a stale `line_amount` on a removed
    -- line and the order's `net_amount` would close against a wrong sum with nothing failing.
    constraint order_line_removed_has_no_amount check (
        status <> 'removed' or (fulfilled_source_product_id is null and line_amount = 0)),
    constraint order_line_substituted_has_a_substitute check (
        status <> 'substituted' or fulfilled_source_product_id is not null),
    constraint order_line_fulfilled_has_a_product check (
        status <> 'fulfilled' or fulfilled_source_product_id is not null),
    -- A line of an order that died before picking delivered nothing: no fulfilled product
    -- and no amount. What the customer ORDERED stays in source_product_id and unit_price.
    constraint order_line_not_picked_has_no_amount check (
        status <> 'not_picked'
        or (fulfilled_source_product_id is null and line_amount = 0))
);

create table if not exists outbox (
    outbox_id     bigserial   primary key,
    event_id      text        not null unique,
    order_id      text        not null,
    event_type    text        not null,
    event_version integer     not null,
    sequence_no   integer     not null,
    occurred_at   timestamptz not null,
    wh            text        not null,
    producer      text        not null,
    event_json    text        not null,
    created_at    timestamptz not null default now(),
    published_at  timestamptz,
    constraint outbox_routing_matches_event check (
        event_id    = (event_json::jsonb ->> 'event_id')
        and order_id    = (event_json::jsonb ->> 'order_id')
        and event_type  = (event_json::jsonb ->> 'event_type')
        and wh          = (event_json::jsonb ->> 'wh')
        and sequence_no = (event_json::jsonb ->> 'sequence_no')::integer)
);

-- PARTIAL index: the publisher's queue is only what has not been published yet. Once the
-- outbox accumulates millions of published rows, a full index would make the publisher scan
-- history on every round. This index shrinks as the queue drains, which is the opposite.
create index if not exists outbox_unpublished
    on outbox (outbox_id) where published_at is null;

create index if not exists outbox_order_sequence on outbox (order_id, sequence_no);
"""

DROP = """
drop table if exists outbox;
drop table if exists order_line;
drop table if exists orders;
"""


# --------------------------------------------------------------------------------------
# The write plan: pure, testable without any database
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Statement:
    """One write, with the number of rows it MUST affect.

    `expect_rows` is the guard: an update that does not find the order in the expected state
    affects zero rows and Postgres does not complain. Without checking the rowcount, applying
    an out-of-order event would be silent — the worst possible failure mode in an event chain.
    """

    sql: str
    params: tuple
    expect_rows: int | None
    detail: str


@dataclass(frozen=True)
class Check:
    """A scalar check inside the same transaction. If it diverges, the whole transaction falls."""

    sql: str
    params: tuple
    expected: object
    detail: str


# Valid transitions, redeclared from the Source's CONTRACT.md section 5. `from_states` is the
# set of states from which the event may be applied.
STATE_BEFORE = {
    "order_payment_authorized": ("PLACED",),
    "order_payment_failed": ("PLACED",),
    "order_cancelled": ("PLACED", "CONFIRMED"),
    "order_picking_started": ("CONFIRMED",),
    "order_line_substituted": ("PICKING",),
    "order_line_removed": ("PICKING",),
    "order_picked": ("PICKING",),
    "order_dispatched": ("PICKED",),
    "order_delivered": ("IN_TRANSIT",),
    "order_delivery_failed": ("IN_TRANSIT",),
    "order_returned": ("DELIVERED",),
}

STATE_AFTER = {
    "order_payment_authorized": "CONFIRMED",
    "order_payment_failed": "PAYMENT_FAILED",
    "order_cancelled": "CANCELLED",
    "order_picking_started": "PICKING",
    "order_line_substituted": "PICKING",
    "order_line_removed": "PICKING",
    "order_picked": "PICKED",
    "order_dispatched": "IN_TRANSIT",
    "order_delivered": "DELIVERED",
    "order_delivery_failed": "DELIVERY_FAILED",
    "order_returned": "RETURNED",
}

_INSERT_ORDER = """
insert into orders (
    order_id, wh, order_date, customer_id, status, last_sequence_no,
    placed_at, updated_at, delivery_slot_start, delivery_slot_end,
    line_count, gross_amount)
values (%s, %s, %s, %s, 'PLACED', %s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_LINE = """
insert into order_line (
    order_id, line_no, source_product_id, quantity, unit_price, status, line_amount)
values (%s, %s, %s, %s, %s, 'placed', %s)
"""

# The order guard AND the state guard in the SAME where clause. If either does not hold, the
# update affects zero rows and `expect_rows=1` brings the transaction down. Doing both checks
# in a SELECT beforehand would be a race condition: between reading and writing, another
# session may have advanced the order.
_ADVANCE = """
update orders
   set status = %s,
       last_sequence_no = %s,
       updated_at = %s{extra}
 where order_id = %s
   and last_sequence_no = %s
   and status = any(%s)
"""

_RECOMPUTE_NET = """
update orders
   set net_amount = (select coalesce(sum(line_amount), 0)
                       from order_line where order_id = %s)
 where order_id = %s
"""


def _parse_ts(value: str) -> datetime:
    """ISO-8601 with a Z suffix. Converted in Python so as not to depend on the server parser."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _advance(event, status, *, extra="", extra_params=()):
    order_id = event["order_id"]
    seq = event["sequence_no"]
    allowed = list(STATE_BEFORE[event["event_type"]])
    params = (
        (status, seq, _parse_ts(event["occurred_at"]))
        + tuple(extra_params)
        + (order_id, seq - 1, allowed)
    )
    return Statement(
        _ADVANCE.format(extra=extra),
        params,
        expect_rows=1,
        # Kept in Portuguese on purpose: scripts/prove_oltp_atomicity.py checks for the
        # substring "fora de ordem" in this message. Translate both together.
        detail=(
            f"{order_id} seq={seq} {event['event_type']}: pedido inexistente, evento fora "
            f"de ordem (esperado last_sequence_no={seq - 1}) ou estado fora de {allowed}"
        ),
    )


def plan(event) -> list:
    """The state writes of ONE event. Pure function: reads no database, has no clock.

    Does not include the outbox row — the transaction is assembled by `_apply_event`, and the
    order there is deliberate (outbox first, see the comment at that point).
    """
    kind = event["event_type"]
    order_id = event["order_id"]
    payload = event["payload"]
    occurred_at = _parse_ts(event["occurred_at"])

    if kind == "order_placed":
        gross = Decimal(payload["gross_amount"])
        ops = [
            Statement(
                _INSERT_ORDER,
                (
                    order_id,
                    event["wh"],
                    occurred_at.date(),
                    payload["customer_id"],
                    event["sequence_no"],
                    occurred_at,
                    occurred_at,
                    _parse_ts(payload["delivery_slot_start"]),
                    _parse_ts(payload["delivery_slot_end"]),
                    payload["line_count"],
                    gross,
                ),
                expect_rows=1,
                detail=f"{order_id}: order already exists in the OLTP",
            )
        ]
        for line in payload["lines"]:
            unit = Decimal(line["unit_price"])
            ops.append(
                Statement(
                    _INSERT_LINE,
                    (
                        order_id,
                        line["line_no"],
                        line["source_product_id"],
                        line["quantity"],
                        unit,
                        unit * line["quantity"],
                    ),
                    expect_rows=1,
                    detail=f"{order_id} line {line['line_no']}: already exists in the OLTP",
                )
            )
        return ops

    if kind in ("order_line_substituted", "order_line_removed"):
        line_no = payload["line_no"]
        # The order guard COMES FIRST. Touching the line first and then discovering the event
        # was out of order would work (the whole transaction falls), but would leave the
        # error message pointing at the symptom instead of the cause.
        ops = [_advance(event, STATE_AFTER[kind])]
        if kind == "order_line_substituted":
            amount = Decimal(payload["substitute_unit_price"]) * payload["quantity"]
            ops.append(
                Statement(
                    """
                    update order_line
                       set status = 'substituted',
                           fulfilled_source_product_id = %s,
                           fulfilled_unit_price = %s,
                           line_amount = %s
                     where order_id = %s and line_no = %s and status = 'placed'
                    """,
                    (
                        payload["substitute_source_product_id"],
                        Decimal(payload["substitute_unit_price"]),
                        amount,
                        order_id,
                        line_no,
                    ),
                    expect_rows=1,
                    detail=f"{order_id} line {line_no}: missing or already changed",
                )
            )
        else:
            ops.append(
                Statement(
                    """
                    update order_line
                       set status = 'removed',
                           fulfilled_source_product_id = null,
                           fulfilled_unit_price = null,
                           line_amount = 0
                     where order_id = %s and line_no = %s and status = 'placed'
                    """,
                    (order_id, line_no),
                    expect_rows=1,
                    detail=f"{order_id} line {line_no}: missing or already changed",
                )
            )
        ops.append(
            Statement(
                _RECOMPUTE_NET,
                (order_id, order_id),
                expect_rows=1,
                detail=f"{order_id}: could not recompute net_amount",
            )
        )
        return ops

    if kind == "order_picked":
        # The `picked_amount` declared in the event is checked AGAINST THE OLTP'S OWN STATE,
        # inside the transaction. It is not distrust of the Source: it is the only way to
        # guarantee the OLTP fold and the Silver fold start from the same place. If they ever
        # diverged, milestone 6 (`orders-reconcile`) would compare two wrong things and PASS.
        return [
            _advance(
                event,
                "PICKED",
                extra=", picked_line_count = %s",
                extra_params=(payload["picked_line_count"],),
            ),
            # THE UNTOUCHED LINE ONLY BECOMES `fulfilled` HERE. There is no per-line event
            # saying "this one was picked as ordered" — `order_picked` declares that at the
            # order grain, and the OLTP already checks `picked_line_count` against its own
            # state just below. Distributing a declared fact is not inventing; labelling
            # `fulfilled` at placement, as this model used to do, was.
            Statement(
                """
                update order_line
                   set status = 'fulfilled',
                       fulfilled_source_product_id = source_product_id,
                       fulfilled_unit_price = unit_price
                 where order_id = %s and status = 'placed'
                """,
                (order_id,),
                # How many lines remain untouched varies per order; the guard is the Check
                # just below, which requires that NO line remains undefined.
                expect_rows=None,
                detail=f"{order_id}: could not fulfil the untouched lines",
            ),
            Check(
                "select count(*) from order_line where order_id = %s and status = 'placed'",
                (order_id,),
                0,
                detail=f"{order_id}: an undefined line is left after picking",
            ),
            Check(
                "select coalesce(sum(line_amount), 0) from order_line where order_id = %s",
                (order_id,),
                Decimal(payload["picked_amount"]),
                detail=f"{order_id}: declared picked_amount does not match the OLTP lines",
            ),
            Check(
                "select count(*) from order_line where order_id = %s and status <> 'removed'",
                (order_id,),
                payload["picked_line_count"],
                detail=f"{order_id}: declared picked_line_count does not match the lines",
            ),
            Statement(
                _RECOMPUTE_NET,
                (order_id, order_id),
                expect_rows=1,
                detail=f"{order_id}: could not recompute net_amount",
            ),
        ]

    if kind == "order_delivered":
        return [
            _advance(
                event,
                "DELIVERED",
                extra=", delivered_within_slot = %s",
                extra_params=(payload["delivered_within_slot"],),
            )
        ]

    if kind == "order_returned":
        return [
            _advance(
                event,
                "RETURNED",
                extra=", returned_amount = %s",
                extra_params=(Decimal(payload["returned_amount"]),),
            )
        ]

    if kind in ("order_cancelled", "order_payment_failed"):
        # The order died before picking. The lines were neither fulfilled nor removed: nobody
        # got to touch them. Leaving them `placed` would make the OLTP keep forever a
        # "pending" state for a terminal order.
        return [
            _advance(event, STATE_AFTER[kind]),
            Statement(
                """
                update order_line
                   set status = 'not_picked',
                       fulfilled_source_product_id = null,
                       fulfilled_unit_price = null,
                       line_amount = 0
                 where order_id = %s and status = 'placed'
                """,
                (order_id,),
                expect_rows=None,
                detail=f"{order_id}: could not close the order lines",
            ),
        ]

    if kind in STATE_AFTER:
        return [_advance(event, STATE_AFTER[kind])]

    raise OrdersOltpError(
        f"{event['order_id']} seq={event['sequence_no']}: event_type {kind!r} outside the "
        f"vocabulary this OLTP knows"
    )


_INSERT_OUTBOX = """
insert into outbox (
    event_id, order_id, event_type, event_version, sequence_no,
    occurred_at, wh, producer, event_json)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict (event_id) do nothing
"""


def outbox_statement(event, event_json: str) -> Statement:
    """The outbox row. `event_json` is THE LOG LINE, verbatim — not re-serialized."""
    return Statement(
        _INSERT_OUTBOX,
        (
            event["event_id"],
            event["order_id"],
            event["event_type"],
            event["event_version"],
            event["sequence_no"],
            _parse_ts(event["occurred_at"]),
            event["wh"],
            event["producer"],
            event_json,
        ),
        # None on purpose: 0 rows here is NOT an error, it is the "already applied" signal.
        expect_rows=None,
        detail=f"{event['event_id']}: insert no outbox",
    )


# --------------------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------------------

@dataclass
class ApplyResult:
    partition: str
    events: int = 0
    applied: int = 0
    skipped: int = 0
    orders: int = 0
    by_type: dict = field(default_factory=dict)


def _execute(cur, ops) -> None:
    for op in ops:
        cur.execute(op.sql, op.params)
        if isinstance(op, Check):
            row = cur.fetchone()
            got = row[0] if row else None
            if got != op.expected:
                raise OrdersOltpError(
                    f"{op.detail}: OLTP={got!r} event={op.expected!r}"
                )
        elif op.expect_rows is not None and cur.rowcount != op.expect_rows:
            raise OrdersOltpError(
                f"{op.detail} (affected {cur.rowcount} rows, expected {op.expect_rows})"
            )


def apply_event(connection, event, event_json: str) -> bool:
    """Applies ONE event. Returns True if applied, False if it was already applied.

    THE TRANSACTION IS THIS WHOLE BODY, and the `commit` at the end is the only one there is.
    One extra `commit()` between the outbox and the state change would turn this function
    into a dual-write — and that is exactly the defect `test_orders_oltp` records and fails.

    OUTBOX FIRST, and not by taste: the `on conflict do nothing` on the unique `event_id` is
    what decides whether the event was already applied. Asking the state
    (`last_sequence_no`) would give the SAME answer — the two guards agree by construction —
    but as a REJECTION, not a "skip". On a replay of the same day, skipping is the correct
    behavior.
    """
    try:
        with connection.cursor() as cur:
            outbox = outbox_statement(event, event_json)
            cur.execute(outbox.sql, outbox.params)
            if cur.rowcount == 0:
                # Already applied. Nothing to undo, but the rollback closes the transaction
                # explicitly instead of leaving an open transaction travelling to the next
                # event — which is how an `idle in transaction` is born.
                connection.rollback()
                return False
            _execute(cur, plan(event))
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise


def read_log(partition_path: str) -> list[tuple[dict, str]]:
    """(event, canonical line) in file order, which already is the apply order."""
    path = os.path.join(partition_path, EVENTS_FILE)
    if not os.path.exists(path):
        raise OrdersOltpError(f"event log not found: {path}")
    out = []
    with open(path, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")
            if not line:
                continue
            try:
                out.append((json.loads(line), line))
            except ValueError as exc:
                raise OrdersOltpError(f"{path}:{number}: unreadable line: {exc}") from exc
    return out


def verify_log(partition_path: str):
    """Checks the log's sha256 against the manifest BEFORE replicating anything.

    Replicating a tampered log into the OLTP would contaminate the outbox, and the outbox is
    what goes to the broker. The point of checking here, and not later, is that later is
    too late.
    """
    partition = manifest_reader.read(partition_path)
    if partition.source_name != SOURCE_NAME:
        raise OrdersOltpError(
            f"partition of {partition.source_name!r}: this OLTP only applies {SOURCE_NAME!r}"
        )
    entries = [entry for entry in partition.files if entry.path.endswith(EVENTS_FILE)]
    if len(entries) != 1:
        raise OrdersOltpError(
            f"manifest declares {len(entries)} log files; expected exactly 1"
        )
    entry = entries[0]
    digest, size = entry.digest_on_disk()
    if digest != entry.sha256 or size != entry.bytes:
        raise OrdersOltpError(
            f"log diverges from the manifest: sha256 on disk {digest} != {entry.sha256}"
        )
    return partition


def apply_partition(connection, partition_path: str, *, verify: bool = True) -> ApplyResult:
    """Replicates a partition's log into the OLTP, one event per transaction.

    ONE TRANSACTION PER EVENT, not per order or per partition. It is the only cut that
    matches what a real OLTP does — each state change is a closed deal — and the only one in
    which the property proven here means what it promises. One transaction per partition
    would prove "the whole day is atomic", which no store guarantees.
    """
    if verify:
        verify_log(partition_path)
    result = ApplyResult(partition=partition_path)
    orders = set()
    for event, line in read_log(partition_path):
        result.events += 1
        if apply_event(connection, event, line):
            result.applied += 1
            result.by_type[event["event_type"]] = result.by_type.get(event["event_type"], 0) + 1
        else:
            result.skipped += 1
        orders.add(event["order_id"])
    result.orders = len(orders)
    return result


# --------------------------------------------------------------------------------------
# Outbox check
# --------------------------------------------------------------------------------------

def rebuild_log(connection, ingestion_date: str, wh: str) -> bytes:
    """Rebuilds the log from the outbox, in the Source's canonical order.

    The order is `(occurred_at, order_id, sequence_no)` because that is what the Source
    declares in the CONTRACT — not `outbox_id`, which is INSERTION order. The two coincide
    today; depending on the coincidence would make this check pass by luck.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            select o.event_json
              from outbox o
              join orders r on r.order_id = o.order_id
             where r.order_date = %s and o.wh = %s
             order by o.occurred_at, o.order_id, o.sequence_no
            """,
            (ingestion_date, wh),
        )
        rows = cur.fetchall()
    return "".join(f"{row[0]}\n" for row in rows).encode("utf-8")


def verify_outbox(connection, partition_path: str) -> dict:
    """Proves the outbox lost nothing, duplicated nothing and altered nothing.

    It is the strongest proof this milestone has to give, and it costs nothing: if the log
    rebuilt from the outbox rows reproduces the manifest's sha256, then the whole chain —
    read, apply in a transaction, write the outbox, re-read, re-order — is faithful byte for
    byte. Counting rows would not prove that; comparing sums would not either.
    """
    partition = verify_log(partition_path)
    entry = [e for e in partition.files if e.path.endswith(EVENTS_FILE)][0]
    blob = rebuild_log(connection, partition.ingestion_date, partition.axis_value)
    digest = hashlib.sha256(blob).hexdigest()
    return {
        "partition": partition_path,
        "ingestion_date": partition.ingestion_date,
        "wh": partition.axis_value,
        "declared_sha256": entry.sha256,
        "outbox_sha256": digest,
        "declared_bytes": entry.bytes,
        "outbox_bytes": len(blob),
        "declared_rows": entry.records,
        "outbox_rows": blob.count(b"\n"),
        "matches": digest == entry.sha256 and len(blob) == entry.bytes,
    }


def outbox_summary(connection) -> dict:
    with connection.cursor() as cur:
        cur.execute(
            """
            select count(*), count(*) filter (where published_at is null),
                   count(distinct order_id)
              from outbox
            """
        )
        total, unpublished, orders = cur.fetchone()
        cur.execute(
            "select event_type, count(*) from outbox group by 1 order by 1"
        )
        by_type = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("select status, count(*) from orders group by 1 order by 1")
        by_status = {row[0]: row[1] for row in cur.fetchall()}
    return {
        "outbox_rows": total,
        "unpublished": unpublished,
        "orders_in_outbox": orders,
        "by_event_type": by_type,
        "orders_by_status": by_status,
    }


# --------------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------------

def connect(dsn: str = None):
    """Connection to the OLTP. Imports psycopg on demand, like boto3 and the Snowflake connector.

    The late import is what keeps `plan()` and `outbox_statement()` — the functions that carry
    all the rules — testable in an environment without psycopg and without a running database.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise OrdersOltpError(
            "psycopg is not installed in this venv. Run `make venv`."
        ) from exc
    try:
        return psycopg.connect(dsn or DEFAULT_DSN)
    except psycopg.Error as exc:
        raise OrdersOltpError(
            f"could not connect to the OLTP ({dsn or DEFAULT_DSN}): {exc}\n"
            "The stream plane does not come up with `make up`. Run `make stream-up`."
        ) from exc


def create_schema(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(DDL)
    connection.commit()


def drop_schema(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(DROP)
    connection.commit()
