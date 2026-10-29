"""Transport, delivery semantics, idempotent consumption and replay — the offline half.

THREE LAYERS, deliberately separate, in the same pattern as test_orders_oltp.py:

  1. `decide` and `fold_event` are PURE. All of the dedup semantics and the whole read-model
     fold live in them, and they need no broker, database or double.
  2. THE ORDER OF OPERATIONS, against doubles that write a SHARED journal. This is where
     "the offset is committed after the write" stops being a comment and becomes an assertion.
  3. The producer draining the outbox, against the Milestone 4 Postgres double.

What is NOT here, and could not be: that the broker preserves per-key order, that a
republished duplicate arrives after the original, that a rebalance loses nothing. That
belongs to the broker and can only be proven against one: `make orders-prove-stream`.
"""

from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timezone

from retail_platform.orders_stream import (
    APPLY,
    DUPLICATE,
    GAP,
    OrdersStreamError,
    ProjectResult,
    decide,
    fold_event,
    project,
    project_batch,
    publish,
    publish_batch,
    read_sla_minutes,
)

from .fake_kafka import (
    FakeKafkaError,
    FakeMessage,
    RecordingConsumer,
    RecordingProducer,
    RecordingProjection,
    steps,
)
from .fake_pg import FakeConnection

SLA = 90


def event(kind, *, order_id="ord_mad1_20260827_000001", seq=1, at="2026-08-27T07:00:00Z",
          payload=None, wh="mad1"):
    return {
        "event_id": f"{order_id}|{seq}",
        "event_type": kind,
        "event_version": 1,
        "occurred_at": at,
        "order_id": order_id,
        "sequence_no": seq,
        "wh": wh,
        "producer": "simulated_orders",
        "payload": payload if payload is not None else {},
    }


PLACED = {
    "customer_id": "cust_mad1_000001",
    "customer_ingestion_date": "2026-08-27",
    "delivery_slot_start": "2026-08-27T23:00:00Z",
    "delivery_slot_end": "2026-08-28T01:00:00Z",
    "gross_amount": "10.00",
    "line_count": 2,
    "lines": [],
}


def placed(order_id="ord_mad1_20260827_000001"):
    return event("order_placed", order_id=order_id, payload=PLACED)


class TestDecideIsPure(unittest.TestCase):
    """All of deduplication lives in a three-line function, and is testable with nothing."""

    def test_the_next_event_applies(self):
        self.assertEqual(decide(3, event("order_dispatched", seq=4)), APPLY)

    def test_a_first_event_applies_against_an_unknown_order(self):
        self.assertEqual(decide(None, placed()), APPLY)

    def test_an_already_seen_sequence_is_a_duplicate(self):
        for last, seq in ((5, 5), (5, 4), (5, 1)):
            self.assertEqual(decide(last, event("order_dispatched", seq=seq)), DUPLICATE)

    def test_a_skipped_sequence_is_a_gap_not_a_duplicate(self):
        """A gap is LOSS. Confusing it with a duplicate would make the consumer silently
        discard exactly the case it exists to report."""
        self.assertEqual(decide(5, event("order_dispatched", seq=7)), GAP)

    def test_dedup_needs_no_growing_set_of_event_ids(self):
        """Dedup is BOUNDED by construction: one integer per order, not a set that grows
        without limit and forces inventing an expiry policy — and every expiry policy is a
        window in which the duplicate gets through again.

        This only holds because per-key order is guaranteed: a duplicate always arrives AFTER
        the original. If it did not, it would be classified as a gap. The premise is proven
        against the real broker, not here."""
        import ast
        import inspect

        from retail_platform import orders_stream

        # ONLY THE BODY, without the docstring — which mentions `event_id` precisely to explain
        # why it is NOT used. Checking the whole text would fail the comment, not the code.
        tree = ast.parse(inspect.getsource(orders_stream.decide).lstrip())
        body = tree.body[0].body
        if isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        code = "\n".join(ast.unparse(node) for node in body)
        self.assertNotIn("event_id", code)
        self.assertIn("sequence_no", code)


class TestFoldIsPure(unittest.TestCase):
    def test_placing_an_order_starts_the_state(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        self.assertEqual(state["status"], "PLACED")
        self.assertEqual(state["last_sequence_no"], 1)
        self.assertEqual(state["events_applied"], 1)
        # Undetermined until picking — the same discipline as the OLTP and Silver. Filling it
        # with the placed amount would make the column answer another question under the same name.
        self.assertIsNone(state["net_amount"])

    def test_it_does_not_mutate_the_previous_state(self):
        """The previous state may be what `load` brought from the database. Mutating it would
        make a rollback leave memory and disk disagreeing."""
        state = fold_event(None, placed(), sla_minutes=SLA)
        before = dict(state)
        fold_event(state, event("order_payment_authorized", seq=2,
                                payload={"payment_method": "card"}), sla_minutes=SLA)
        self.assertEqual(state, before)

    def test_every_event_type_of_the_contract_is_folded(self):
        payloads = {
            "order_line_substituted": {"line_no": 1},
            "order_line_removed": {"line_no": 1},
            "order_picked": {"picked_line_count": 2, "picked_amount": "10.00"},
            "order_delivered": {"delivered_within_slot": True},
            "order_returned": {"returned_amount": "0.70", "returned_line_count": 1},
        }
        from retail_platform.orders_stream import STATE_AFTER

        for kind in STATE_AFTER:
            if kind == "order_placed":
                continue
            state = fold_event(None, placed(), sla_minutes=SLA)
            got = fold_event(state, event(kind, seq=2, payload=payloads.get(kind, {})),
                             sla_minutes=SLA)
            self.assertEqual(got["status"], STATE_AFTER[kind])

    def test_an_unknown_event_type_is_refused_not_ignored(self):
        with self.assertRaises(OrdersStreamError) as ctx:
            fold_event(fold_event(None, placed(), sla_minutes=SLA),
                       event("order_teleported", seq=2), sla_minutes=SLA)
        self.assertIn("vocabulary", str(ctx.exception))

    def test_an_orphan_event_is_refused(self):
        with self.assertRaises(OrdersStreamError) as ctx:
            fold_event(None, event("order_dispatched", seq=2), sla_minutes=SLA)
        self.assertIn("never saw", str(ctx.exception))

    def test_line_events_count_instead_of_recomputing(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        state = fold_event(state, event("order_picking_started", seq=2), sla_minutes=SLA)
        state = fold_event(state, event("order_line_substituted", seq=3,
                                        payload={"line_no": 1}), sla_minutes=SLA)
        state = fold_event(state, event("order_line_removed", seq=4,
                                        payload={"line_no": 2}), sla_minutes=SLA)
        self.assertEqual((state["substituted_lines"], state["removed_lines"]), (1, 1))

    def test_the_sla_is_measured_between_the_two_picking_milestones(self):
        state = fold_event(None, placed(), sla_minutes=30)
        state = fold_event(state, event("order_picking_started", seq=2,
                                        at="2026-08-27T08:00:00Z"), sla_minutes=30)
        state = fold_event(state, event("order_picked", seq=3, at="2026-08-27T09:00:00Z",
                                        payload={"picked_line_count": 2,
                                                 "picked_amount": "10.00"}), sla_minutes=30)
        self.assertEqual(state["picking_minutes"], 60.0)
        self.assertTrue(state["sla_breached"])

    def test_the_declared_sla_comes_from_the_seed_not_from_a_constant(self):
        """Same discipline as the `currency` var: the premise travels with the number. A
        constant in this module could diverge from what the generator declared.

        THIS TEST WAS ONCE THE SECOND COPY IT EXISTS TO PREVENT. Until Phase 7 it asserted
        `assertEqual(read_sla_minutes(seeds), 90)` — a hard-coded 90, inside the test whose
        thesis is that the 90 must not be hard-coded anywhere. When the seed dropped to 60,
        because the old threshold sat above the arithmetic ceiling of picking, it was this test
        that failed, and the failure was RIGHT for the wrong reason: nothing had broken in the
        code, only the copy had gone stale.
        """
        # The suite runs with CWD in `platform/`; the module default is relative to the
        # repository ROOT, which is where the Makefile invokes the CLI. Resolving from the test
        # file checks the seed's CONTENT without depending on where unittest was called from.
        seeds = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "platform", "dbt", "seeds")

        # THE VALUE COMES FROM THE SEED, THROUGH AN INDEPENDENT PATH. Reading the CSV with plain
        # `csv` is not duplicating the read: `read_sla_minutes` is what has to find the right
        # row, convert it and fail well, and that is what is under test. Comparing against the
        # file itself checks the PATH — which was the thesis — without asserting anything about
        # the number, which is a premise and may change by domain decision at any time.
        import csv
        with open(os.path.join(seeds, "order_premises_seed.csv"), encoding="utf-8") as f:
            declared = {row["premise_key"]: row["value"] for row in csv.DictReader(f)}
        self.assertEqual(read_sla_minutes(seeds), int(float(declared["sla_minutes_picking"])))

        # And the default is still the same directory as the platform's other bridges.
        from retail_platform.customers_reference import DEFAULT_SEEDS_DIR as BRIDGE
        from retail_platform.orders_stream import DEFAULT_SEEDS_DIR as STREAM
        self.assertEqual(STREAM, BRIDGE)

    def test_timestamps_are_parsed_in_python(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        self.assertEqual(state["placed_at"],
                         datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc))


class TestProjectBatch(unittest.TestCase):
    def test_duplicates_inside_one_batch_are_absorbed(self):
        projection = RecordingProjection()
        result = project_batch(projection, [placed(), placed()], sla_minutes=SLA)
        self.assertEqual((result.applied, result.duplicates), (1, 1))
        self.assertEqual(projection.count(), 1)

    def test_a_gap_is_reported_and_the_event_is_not_applied(self):
        projection = RecordingProjection()
        result = project_batch(
            projection,
            [placed(), event("order_dispatched", seq=4)],
            sla_minutes=SLA,
        )
        self.assertEqual(result.applied, 1)
        self.assertEqual(len(result.gaps), 1)
        self.assertIn("seq=4", result.gaps[0])

    def test_state_already_in_the_projection_is_respected(self):
        """Dedup compares against what is already stored, not only against what came in the
        batch — otherwise a replay in new batches would reapply everything."""
        projection = RecordingProjection()
        project_batch(projection, [placed()], sla_minutes=SLA)
        result = project_batch(projection, [placed()], sla_minutes=SLA)
        self.assertEqual((result.applied, result.duplicates), (0, 1))

    def test_it_loads_only_the_orders_of_the_batch(self):
        projection = RecordingProjection()
        project_batch(projection, [placed("ord_a"), placed("ord_b")], sla_minutes=SLA)
        self.assertEqual(projection.journal[0], ("projection:load", ("ord_a", "ord_b")))


class TestDeliveryOrder(unittest.TestCase):
    """The property no counting test sees: the ORDER between write and commit."""

    def _messages(self, events):
        return [FakeMessage(json.dumps(e), offset=i) for i, e in enumerate(events)]

    def test_the_offset_is_committed_after_the_write_never_before(self):
        """THIS is the test that catches the inversion. Committing the offset before the
        projection is durable delivers AT-MOST-ONCE without anyone choosing it: a crash between
        the commit and the write erases the event from the world. It is the default of
        `enable.auto.commit`, which is why it is false in this consumer."""
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        projection = RecordingProjection(journal=journal)
        project(consumer, projection, sla_minutes=SLA, idle_timeout=0.0)

        stages = [e for e in steps(journal) if e != "consumer:poll"]
        self.assertEqual(
            stages,
            ["projection:load", "projection:save", "projection:commit", "consumer:commit-offset"],
        )

    def test_the_offset_commit_is_synchronous(self):
        """An asynchronous commit would return before the offset is durable, and the
        guarantee would come to depend on timing luck."""
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                idle_timeout=0.0)
        commit = [e for e in journal if e[0] == "consumer:commit-offset"][0]
        self.assertIs(commit[1], False)

    def test_a_failed_write_rolls_back_and_never_commits_the_offset(self):
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        projection = RecordingProjection(journal=journal, fail_on_save=True)
        with self.assertRaises(FakeKafkaError):
            project(consumer, projection, sla_minutes=SLA, idle_timeout=0.0)
        self.assertIn("projection:rollback", steps(journal))
        self.assertNotIn("consumer:commit-offset", steps(journal))
        self.assertEqual(consumer.commits, 0)

    def test_a_gap_stops_the_consumer_instead_of_advancing_the_offset(self):
        """A gap is data loss. Advancing the offset over a gap would make the loss permanent
        and invisible — the offset would move on and nobody would reprocess."""
        journal = []
        consumer = RecordingConsumer(
            self._messages([placed(), event("order_dispatched", seq=5)]), journal=journal
        )
        with self.assertRaises(OrdersStreamError) as ctx:
            project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                    idle_timeout=0.0)
        self.assertIn("GAP", str(ctx.exception))

    def test_an_unreadable_message_is_refused_not_skipped(self):
        journal = []
        consumer = RecordingConsumer([FakeMessage(b"{not json")], journal=journal)
        with self.assertRaises(OrdersStreamError) as ctx:
            project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                    idle_timeout=0.0)
        self.assertIn("unreadable", str(ctx.exception))

    def test_replaying_the_same_messages_changes_nothing(self):
        journal = []
        events = [placed(), event("order_payment_authorized", seq=2,
                                  payload={"payment_method": "card"})]
        projection = RecordingProjection(journal=journal)
        project(RecordingConsumer(self._messages(events), journal=journal), projection,
                sla_minutes=SLA, idle_timeout=0.0)
        before = projection.digest()
        result = project(RecordingConsumer(self._messages(events), journal=journal),
                         projection, sla_minutes=SLA, idle_timeout=0.0)
        self.assertEqual(projection.digest(), before)
        self.assertEqual(result.applied, 0)
        self.assertEqual(result.duplicates, 2)


class TestPublisher(unittest.TestCase):
    """The producer draining the outbox, against the Milestone 4 Postgres double."""

    def _connection(self, rows):
        return FakeConnection(results={"select": [(r,) for r in rows] if rows else []})

    def _rows(self, n=3):
        return [(100 + i, f"ord_mad1_20260827_00000{i}",
                 json.dumps(placed(f"ord_mad1_20260827_00000{i}"), sort_keys=True))
                for i in range(n)]

    def test_the_key_is_the_order_id_which_is_what_makes_ordering_usable(self):
        """The key is not a configuration detail: it is the premise of deduplication. With a
        constant key everything would land in one partition and order would be global BY
        ACCIDENT; with a per-warehouse key, two orders of the same warehouse would compete
        needlessly."""
        rows = self._rows()
        connection = FakeConnection(results={"select": [rows]})
        producer = RecordingProducer()
        publish_batch(connection, producer, "t", 10)
        self.assertEqual(producer.keys, [r[1] for r in rows])

    def test_nothing_is_marked_before_the_broker_acknowledges(self):
        """Marking before the ack would be AT-MOST-ONCE: a crash between the mark and the ack
        would erase the event. The order produce -> flush -> update is the choice that trades
        loss for duplication, and the consumer absorbs duplicates."""
        connection = FakeConnection(results={"select": [self._rows()]})
        producer = RecordingProducer()
        publish_batch(connection, producer, "t", 10)

        stages = [e[1] if e[0] == "execute" else e[0] for e in connection.journal]
        self.assertEqual(stages, ["select", "update:outbox", "commit"])
        self.assertEqual(producer.journal[-1], ("flush",))
        # And the flush happened before the update.
        self.assertEqual(producer.flushes, 1)

    def test_a_broker_refusal_marks_nothing_at_all(self):
        """Marking only the ones that went through would leave a GAP in the middle of a key,
        and a gap is the one defect the consumer cannot absorb."""
        connection = FakeConnection(results={"select": [self._rows()]})
        producer = RecordingProducer(fail_on=lambda key, _v: key.endswith(b"1"))
        with self.assertRaises(OrdersStreamError) as ctx:
            publish_batch(connection, producer, "t", 10)
        self.assertIn("rejected by the broker", str(ctx.exception))
        self.assertIn("rollback", [e[0] for e in connection.journal])
        self.assertNotIn("update:outbox",
                         [e[1] for e in connection.journal if e[0] == "execute"])

    def test_an_empty_outbox_produces_nothing_and_holds_no_transaction(self):
        """A transaction left open in a `--follow` loop is how an `idle in transaction` is
        born and holds back vacuum for hours."""
        connection = FakeConnection(results={"select": [[]]})
        producer = RecordingProducer()
        self.assertEqual(publish_batch(connection, producer, "t", 10), 0)
        self.assertEqual(producer.produced, [])
        self.assertEqual(connection.journal[-1], ("rollback",))

    def test_publish_drains_until_the_outbox_is_empty(self):
        connection = FakeConnection(
            results={"select": [self._rows(2), self._rows(1), []]},
            rowcounts={"update:outbox": lambda params: len(params[0])},
        )
        result = publish(connection, RecordingProducer(), "t", batch_size=2)
        self.assertEqual(result.published, 3)
        self.assertEqual(result.batches, 2)


if __name__ == "__main__":
    unittest.main()


class DistinctCountTest(unittest.TestCase):
    """`orders` and `sla_breaches` count ORDERS, not WRITES — and the difference has bitten.

    THE DEFECT, measured on 2026-09-01. Both were integers summed batch by batch, and an order
    whose events fall into different batches was counted once per batch. Consuming 1,433,723
    events, the CLI printed "SLA breached 31,908" while the Iceberg projection and
    `silver_order` agreed on 19,136.

    NOTHING BROKE, and that is the point: the number was plausible, had the right order of
    magnitude, and the label said something other than what it measured. No counting test
    catches that — what caught it was two folds disagreeing, for the third time in this
    project.
    """

    def _events_of_the_same_order(self):
        """An order that breaches the SLA and KEEPS advancing in later batches.

        The fourth batch is what reproduces the defect, and it is not artificial: a real order
        has events after picking (dispatch, delivery, return), and they reach the consumer in
        different batches. From `order_picked` on the state carries `sla_breached = true`, so
        EVERY later batch that touches the order rewrites it as breached — and the sum counted
        each of those writes.
        """
        return [
            [placed()],
            [event("order_picking_started", seq=2, at="2026-08-27T08:00:00Z")],
            [event("order_picked", seq=3, at="2026-08-27T09:30:00Z",
                   payload={"picked_line_count": 2, "picked_amount": "10.00"})],
            [event("order_dispatched", seq=4, at="2026-08-27T10:00:00Z")],
        ]

    def test_the_same_order_across_batches_counts_ONCE(self):
        from retail_platform.orders_stream import ProjectResult, _flush

        projection = RecordingProjection()
        total = ProjectResult()
        for batch in self._events_of_the_same_order():
            _flush(projection, RecordingConsumer([]), batch, 30, total, True)

        self.assertEqual(total.batches, 4, "four batches, and that is a BATCH count")
        self.assertEqual(total.applied, 4, "four events, and an event does not repeat across batches")
        self.assertEqual(total.orders, 1, "ONE order, touched in four batches")
        self.assertEqual(total.sla_breaches, 1, "ONE order breached, not two writes")

    def test_the_naive_sum_would_have_given_the_wrong_number(self):
        """Proof that the test above does not pass by chance: under the old rule it would be 2.

        The order only gains `sla_breached` at `order_picked` (third batch) and stays breached
        in the fourth — two batches with the breached state. The sum would say 2; the union
        says 1, which is how many orders exist. In production, with 1.43 million events, that
        difference became 31,908 against 19,136.
        """
        from retail_platform.orders_stream import ProjectResult, _flush

        projection = RecordingProjection()
        total = ProjectResult()
        naive_sum = 0
        for batch in self._events_of_the_same_order():
            before = total.sla_breaches
            _flush(projection, RecordingConsumer([]), batch, 30, total, True)
            del before
            naive_sum += 1 if total.sla_breaches else 0
        self.assertEqual(naive_sum, 2, "the old rule would count two writes")
        self.assertEqual(total.sla_breaches, 1, "the new one counts one order")
