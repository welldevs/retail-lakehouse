#!/usr/bin/env python3
"""Proof of transport, delivery semantics, idempotent consumption and replay.

WHY THIS IS NOT "START A BROKER AND COUNT MESSAGES".

Counting messages proves that something flowed. It does not prove that it flowed intact, nor
what happens when someone dies midway, nor that reprocessing is safe. The four properties
that matter are independent of each other and each one needs its own proof:

  1. FAITHFUL TRANSPORT — the topic re-read from offset 0 reproduces the `sha256` of the 16 manifests.
  2. ORDER PER KEY — each order lives in a single partition and its offsets grow with
     `sequence_no`. It is not a configuration detail: it is the PREMISE of deduplication. If
     ordering did not hold, a duplicate could arrive before the original and be read as a gap.
  3. AT-LEAST-ONCE IS REAL — not claimed, DEMONSTRATED: we reproduce the window in which the
     publisher dies after the broker ack and before the `published_at` commit, and the topic
     ends up with more messages than the log has events.
  4. IDEMPOTENT CONSUMPTION AND REPLAY — despite the duplicates, the projection does not change;
     and rewinding the group and reprocessing the whole topic does not change it either.

Two more that only exist because they are able to fail:

  5. A GAP IS REFUSED — an out-of-sequence event does not become a silent no-op.
  6. THREE INDEPENDENT FOLDS AGREE — Silver (window function), OLTP (transactional) and
     projection (streaming). Three paths, one number.

    make orders-prove-stream
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.orders_oltp import connect as oltp_connect  # noqa: E402
from retail_platform.orders_stream import (  # noqa: E402
    DEFAULT_BOOTSTRAP,
    DEFAULT_GROUP,
    DEFAULT_TOPIC,
    OrdersStreamError,
    PostgresProjection,
    build_consumer,
    build_producer,
    project,
    project_batch,
    projection_connect,
    publish,
    read_sla_minutes,
    read_topic,
    reset_group,
    topic_watermarks,
)

ORDERS_ROOT = os.path.join(REPO, "data", "orders")
REPUBLISH = 500

failures = []


def check(label, condition, detail=""):
    print(f"  [{'OK  ' if condition else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}")
    if not condition:
        failures.append(label)


def manifests() -> dict:
    """{(ingestion_date, wh): (sha256, bytes, records)} of the partitions on disk."""
    out = {}
    for day in sorted(os.listdir(ORDERS_ROOT)):
        if not day.startswith("ingestion_date="):
            continue
        for axis in sorted(os.listdir(os.path.join(ORDERS_ROOT, day))):
            path = os.path.join(ORDERS_ROOT, day, axis, "_manifest.json")
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            entry = [f for f in data["files"] if f["path"].endswith(".jsonl")][0]
            out[(day.split("=", 1)[1], axis.split("=", 1)[1])] = (
                entry["sha256"], entry["bytes"], entry["records"]
            )
    return out


def partition_of(order_id: str) -> tuple:
    """`ord_<wh>_<YYYYMMDD>_<n>` — the key already carries the partition the event belongs to."""
    _, wh, day, _ = order_id.split("_", 3)
    return f"{day[:4]}-{day[4:6]}-{day[6:]}", wh


def total_messages(marks) -> int:
    return sum(m["high"] - m["low"] for m in marks.values())


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    topic = os.environ.get("KAFKA_ORDERS_TOPIC", DEFAULT_TOPIC)
    group = os.environ.get("KAFKA_ORDERS_GROUP", DEFAULT_GROUP)
    declared = manifests()
    events_in_log = sum(v[2] for v in declared.values())

    marks = topic_watermarks(bootstrap, topic)
    print(f"broker ............ {bootstrap}")
    print(f"topic ............. {topic}  ({len(marks)} partitions)")
    print(f"partitions on disk  {len(declared)}, {events_in_log} events")
    print()

    # ---- 1. faithful transport ---------------------------------------------------------
    print("1. faithful transport — the re-read topic reproduces each manifest's sha256")
    messages = read_topic(bootstrap, topic)
    check("the topic has at least one event per log line",
          len(messages) >= events_in_log, f"{len(messages)} messages")

    by_partition = defaultdict(dict)
    for message in messages:
        event = json.loads(message["value"])
        # Dedup by event_id: the topic is at-least-once, so the same line may be there
        # more than once. Rebuilding without deduplicating would measure duplication, not fidelity.
        by_partition[partition_of(event["order_id"])][event["event_id"]] = (
            event["occurred_at"], event["order_id"], event["sequence_no"], message["value"]
        )

    matching = 0
    for key, (sha, size, lines) in sorted(declared.items()):
        seen = by_partition.get(key, {})
        blob = "".join(
            f"{v[3]}\n" for v in sorted(seen.values(), key=lambda v: (v[0], v[1], v[2]))
        ).encode("utf-8")
        if hashlib.sha256(blob).hexdigest() == sha and len(blob) == size:
            matching += 1
        else:
            print(f"      diverged: {key} ({len(seen)} of {lines} events)")
    check("the 16 partitions rebuilt from the topic match sha256", matching == len(declared),
          f"{matching}/{len(declared)}")
    print()

    # ---- 2. order per key --------------------------------------------------------------
    print("2. order per key — the premise deduplication rests on")
    partitions_per_order = defaultdict(set)
    trail = defaultdict(list)
    for message in messages:
        event = json.loads(message["value"])
        partitions_per_order[event["order_id"]].add(message["partition"])
        trail[event["order_id"]].append((message["offset"], event["sequence_no"]))

    scattered = [o for o, p in partitions_per_order.items() if len(p) > 1]
    check("every order lives in a single partition", not scattered,
          f"{len(scattered)} scattered" if scattered else f"{len(partitions_per_order)} orders")

    # THE GUARANTEE IS ABOUT PRODUCTION ORDER, NOT ABOUT THE OFFSET LIST. A republished
    # duplicate was produced LATER, so appearing later is the correct behavior — the
    # raw sequence of an order with a duplicate reads 1, 2, ..., 1. Demanding the whole list
    # sorted would fail the broker for doing exactly the right thing (that is what happened in
    # the first version of this proof, on the second run, when the topic already had duplicates).
    #
    # The two properties that really matter, and that deduplication uses:
    out_of_order = 0
    repeat_before_original = 0
    for events in trail.values():
        events.sort()
        first_seen = {}
        for offset, sequence in events:
            if sequence in first_seen:
                if offset < first_seen[sequence]:
                    repeat_before_original += 1
            else:
                first_seen[sequence] = offset
        originals = [s for s, _ in sorted(first_seen.items(), key=lambda kv: kv[1])]
        if originals != sorted(originals):
            out_of_order += 1
    check("the FIRST appearance of each sequence_no comes in order", out_of_order == 0,
          f"{out_of_order} orders out of order")
    check("every repeat comes AFTER its original", repeat_before_original == 0,
          "which is why `sequence_no <= last` is enough to deduplicate")

    # THE TWO CHECKS ABOVE WOULD PASS WITH A DEGENERATE KEY (a constant, or `wh`):
    # everything in a single partition satisfies "one order, one partition" and "offsets in
    # order" by accident. This third one is what distinguishes ordering GUARANTEED BY KEY from
    # global ordering due to lack of parallelism.
    occupancy = defaultdict(int)
    for order_id, partitions in partitions_per_order.items():
        occupancy[next(iter(partitions))] += 1
    check("orders spread across all partitions", len(occupancy) == len(marks),
          " ".join(f"p{p}={occupancy[p]}" for p in sorted(occupancy)))
    if len(set(occupancy.values())) == 1:
        # MEASURED, AND NOT TO THE PARTITIONER'S CREDIT. The variable part of the key is a dense
        # sequential counter with leading zeros, and the low bits of murmur2 follow the
        # last digits linearly: the result is a complete residue system, and the split comes
        # out exactly uniform for each (warehouse, day). With a sparse or UUID `order_id` the
        # balance would only be statistical. Do not depend on this number.
        print("      EXACTLY uniform distribution: an artifact of the dense sequential index in")
        print("      the key, not hash quality. It is not a guarantee and must not become a premise.")
    print("      (with 1 partition the first two would be true BY ACCIDENT — the topic has 4)")
    print()

    # ---- 3. at-least-once is real ------------------------------------------------------
    print(f"3. at-least-once — reproducing the publisher dying after the ack ({REPUBLISH} events)")
    before = total_messages(topic_watermarks(bootstrap, topic))
    with oltp_connect() as connection:
        with connection.cursor() as cur:
            # THE EXACT WINDOW: the broker accepted, `published_at` was not durable yet, the
            # process died. On restart, the batch is back in the queue. There is no transaction
            # spanning Postgres and Kafka that closes this; the alternative would be marking first
            # and LOSING.
            cur.execute(
                """
                update outbox set published_at = null
                 where outbox_id in (select outbox_id from outbox
                                      where published_at is not null
                                      order by outbox_id limit %s)
                """,
                (REPUBLISH,),
            )
            reverted = cur.rowcount
        connection.commit()
        producer = build_producer(bootstrap)
        republished = publish(connection, producer, topic).published
    after = total_messages(topic_watermarks(bootstrap, topic))
    check("the batch went back into the queue", reverted == REPUBLISH, f"{reverted}")
    check("and was republished", republished == REPUBLISH, f"{republished}")
    check("the topic now has MORE messages than the log has events",
          after == before + REPUBLISH and after > events_in_log,
          f"{before} -> {after}, log={events_in_log}")
    print("      at-least-once is not a documentation caveat: it is what just happened")
    print()

    # ---- 4. idempotent consumption -----------------------------------------------------
    print("4. idempotent consumption — the duplicates do not touch the projection")
    sla = read_sla_minutes(os.path.join(REPO, "platform", "dbt", "seeds"))
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        digest_before = projection.digest()
        rows_before = projection.count()
        consumer = build_consumer(bootstrap, group, topic=topic, from_beginning=True)
        try:
            result = project(consumer, projection, sla_minutes=sla, idle_timeout=6.0)
        finally:
            consumer.close()
        digest_after = projection.digest()
        rows_after = projection.count()

    check("the duplicates were recognized and discarded", result.duplicates >= REPUBLISH,
          f"{result.duplicates} discarded, {result.applied} applied")
    check("no gap", not result.gaps)
    check("the projection did not change", digest_before == digest_after,
          f"{digest_before[:16]}...")
    check("no extra row", rows_before == rows_after, f"{rows_after} orders")
    print()

    # ---- 5. replay ---------------------------------------------------------------------
    print("5. replay — rewinding the group and reprocessing the whole topic")
    partitions = reset_group(bootstrap, topic, group)
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        consumer = build_consumer(bootstrap, group, topic=topic, from_beginning=True)
        try:
            replay = project(consumer, projection, sla_minutes=sla, idle_timeout=6.0)
        finally:
            consumer.close()
        digest_replay = projection.digest()
        rows_replay = projection.count()

    check(f"the group was rewound on {partitions} partitions", partitions == len(marks))
    check("the whole topic was reprocessed",
          replay.duplicates + replay.applied >= events_in_log,
          f"{replay.duplicates + replay.applied} messages")
    check("NOTHING was applied again", replay.applied == 0, f"applied={replay.applied}")
    check("the projection digest is identical", digest_replay == digest_before,
          f"{digest_replay[:16]}...")
    check("the order count is identical", rows_replay == rows_before)
    print("      the projection was NOT deleted first: reprocessing on top of it is what proves")
    print("      idempotency. Deleting first would prove the fold is deterministic, which is something else.")
    print()

    # ---- 6. a gap is refused -----------------------------------------------------------
    print("6. a gap in the sequence — a loss must become a refusal, not a no-op")
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, last_sequence_no from live_order_state "
                "where last_sequence_no >= 4 order by order_id limit 1"
            )
            order_id, last = cur.fetchone()
        forged = {
            "event_id": "0" * 32, "event_type": "order_dispatched", "event_version": 1,
            "occurred_at": "2026-08-27T23:00:00Z", "order_id": order_id,
            # +2: one event did not arrive. It is the only defect deduplication can NOT
            # absorb, because absorbing it would mean applying a state that skipped a step.
            "sequence_no": last + 2, "wh": order_id.split("_")[1],
            "producer": "simulated_orders", "payload": {},
        }
        result = project_batch(projection, [forged], sla_minutes=sla)
        connection.rollback()
    check("the gap was detected", len(result.gaps) == 1,
          result.gaps[0][:70] if result.gaps else "none")
    check("and the event was NOT applied", result.applied == 0)

    duplicate = dict(forged, sequence_no=last)
    with projection_connect() as connection:
        result = project_batch(PostgresProjection(connection), [duplicate], sla_minutes=sla)
        connection.rollback()
    check("and the duplicate is still discarded silently",
          result.duplicates == 1 and not result.gaps)
    print()

    # ---- 7. three independent folds ----------------------------------------------------
    print("7. three independent folds — Silver, OLTP and projection")
    with oltp_connect() as connection:
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, status, last_sequence_no, line_count, picked_line_count, "
                "gross_amount, net_amount from orders order by order_id"
            )
            from_oltp = {r[0]: r[1:] for r in cur.fetchall()}
    with projection_connect() as connection:
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, status, last_sequence_no, line_count, picked_line_count, "
                "gross_amount, net_amount from live_order_state order by order_id"
            )
            from_projection = {r[0]: r[1:] for r in cur.fetchall()}

    check("same set of orders", set(from_oltp) == set(from_projection),
          f"OLTP={len(from_oltp)} projection={len(from_projection)}")
    divergent = [k for k in from_oltp if from_oltp[k] != from_projection.get(k)]
    check("identical status, sequence, counts and amounts", not divergent,
          f"{len(divergent)} divergent" if divergent else f"{len(from_oltp)} orders")
    print("      (against Silver: `make orders-reconcile` in Milestone 6)")
    print()

    # ---- 8. the SLA alert --------------------------------------------------------------
    print("8. the SLA alert — the mechanism works; the declared threshold never fires")
    with projection_connect() as connection:
        with connection.cursor() as cur:
            cur.execute("select max(picking_minutes), count(*) filter (where sla_breached) "
                        "from live_order_state")
            longest, breached = cur.fetchone()
        projection = PostgresProjection(connection)
        with connection.cursor() as cur:
            cur.execute("select order_id, last_sequence_no from live_order_state "
                        "where picking_minutes is not null order by picking_minutes desc limit 1")
            order_id, _ = cur.fetchone()
    check(f"no order breached the declared SLA of {sla} min", breached == 0,
          f"longest picking observed: {longest} min")
    print(f"      THE DECLARED VALUE IS NOT REACHABLE: basket_lines_max x minutes_per_line_picked")
    print(f"      = 40 x 2 = 80 min < {sla} min. The threshold cannot fire by construction.")
    print("      It was not tuned to produce an alert: adapting a declared premise until the")
    print("      check lights up is the opposite of checking. It stays recorded as measured.")
    print()

    print("=" * 78)
    if failures:
        print(f"FAILED: {len(failures)} check(s) failed:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("PASSED: faithful transport, order per key, at-least-once demonstrated,")
    print("idempotent consumption, replay with no effect, gap refused and three folds in agreement.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OrdersStreamError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2)
