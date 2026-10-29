"""The orders OLTP: write plan, transaction boundary and outbox check.

Three test layers, deliberately separate:

  1. `plan()` is a PURE function — an event goes in, a list of writes comes out. Tested with
     no database, no double, nothing. That is where all the rules live, and that is why it
     does not read the database.
  2. The transaction boundary, against `fake_pg.FakeConnection`, which records the ORDER of
     calls. Proves there is no commit between the outbox and the state change.
  3. The outbox check (`verify_outbox`), which is the byte-for-byte fidelity proof.

What is NOT here, and could not be: that `rollback` undoes. That is
`make orders-prove-atomicity`, against a real Postgres. See fake_pg.py.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from retail_platform.manifest import ManifestError
from retail_platform.orders_oltp import (
    STATE_AFTER,
    STATE_BEFORE,
    Check,
    OrdersOltpError,
    Statement,
    apply_event,
    apply_partition,
    outbox_statement,
    plan,
    read_log,
    verify_outbox,
)

from .fake_pg import FakeConnection, FakePgError

CANONICAL = dict(ensure_ascii=False, sort_keys=True)


def event(kind, *, order_id="ord_mad1_20260827_000001", seq=1, at="2026-08-27T07:00:00Z",
          payload=None, wh="mad1"):
    return {
        "event_id": hashlib.sha256(f"{order_id}|{seq}".encode()).hexdigest()[:32],
        "event_type": kind,
        "event_version": 1,
        "occurred_at": at,
        "order_id": order_id,
        "sequence_no": seq,
        "wh": wh,
        "producer": "simulated_orders",
        "payload": payload if payload is not None else {},
    }


PLACED_PAYLOAD = {
    "customer_id": "cust_mad1_000001",
    "customer_ingestion_date": "2026-08-27",
    "delivery_slot_start": "2026-08-27T23:00:00Z",
    "delivery_slot_end": "2026-08-28T01:00:00Z",
    "gross_amount": "10.00",
    "line_count": 2,
    "lines": [
        {"line_no": 1, "source_product_id": "1", "quantity": 2, "unit_price": "2.00",
         "category_id": 10, "subgroup_id": 100},
        {"line_no": 2, "source_product_id": "2", "quantity": 3, "unit_price": "2.00",
         "category_id": 11, "subgroup_id": 101},
    ],
}


def canonical_line(payload) -> str:
    return json.dumps(payload, **CANONICAL)


class TestPlanIsPure(unittest.TestCase):
    """All the rules live here, and they need no database to be exercised."""

    def test_placed_inserts_the_order_and_every_line(self):
        ops = plan(event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(len(ops), 3)  # 1 order + 2 lines
        self.assertTrue(all(isinstance(op, Statement) for op in ops))
        self.assertTrue(all(op.expect_rows == 1 for op in ops))
        # line_amount = quantity * unit price, in Decimal — never float.
        self.assertEqual(ops[1].params[-1], Decimal("4.00"))
        self.assertEqual(ops[2].params[-1], Decimal("6.00"))
        self.assertIsInstance(ops[1].params[-1], Decimal)

    def test_placed_leaves_net_amount_undetermined(self):
        """`net_amount` answers "how much was picked" — and a freshly placed order has not
        answered that yet. Initializing it with the placed amount would make the SAME column
        say "what the order is worth" in the OLTP and "how much was picked" in Silver, and the
        Milestone 6 reconciliation would compare two different questions thinking it compares
        two answers.

        Measured when this was wrong: 298 of 6,400 orders diverged between the two folds —
        exactly the 196 cancelled and the 102 with a declined payment, which die before
        picking."""
        ops = plan(event("order_placed", payload=PLACED_PAYLOAD))
        columns = " ".join(ops[0].sql.split())
        self.assertNotIn("net_amount", columns)
        self.assertIn("gross_amount", columns)

    def test_every_event_type_of_the_contract_has_a_plan(self):
        """Closed vocabulary. A new type in the Source without handling here fails loudly, and
        does not apply the event as if it were a no-op."""
        for kind in STATE_AFTER:
            payloads = {
                "order_line_substituted": {"line_no": 1, "quantity": 2,
                                           "source_product_id": "1",
                                           "substitute_source_product_id": "9",
                                           "substitute_unit_price": "3.00"},
                "order_line_removed": {"line_no": 1, "reason": "unavailable"},
                "order_picked": {"picked_line_count": 2, "picked_amount": "10.00"},
                "order_delivered": {"delivered_within_slot": True},
                "order_returned": {"returned_line_count": 1, "returned_amount": "0.70",
                                   "returned_line_no": 1},
            }
            ops = plan(event(kind, seq=2, payload=payloads.get(kind, {})))
            self.assertTrue(ops, f"{kind} has no write plan")

    def test_unknown_event_type_is_refused_not_ignored(self):
        with self.assertRaises(OrdersOltpError) as ctx:
            plan(event("order_teleported", seq=2))
        self.assertIn("vocabulary", str(ctx.exception))

    def test_the_advance_carries_both_guards_in_one_where(self):
        """Order AND state in the SAME clause. Checking first in a SELECT would be a race."""
        ops = plan(event("order_payment_authorized", seq=2))
        sql = " ".join(ops[0].sql.split())
        self.assertIn("last_sequence_no = %s", sql)
        self.assertIn("status = any(%s)", sql)
        # seq - 1 is the expected previous state; the list comes from the declared STATE_BEFORE.
        self.assertEqual(ops[0].params[-2], 1)
        self.assertEqual(ops[0].params[-1], list(STATE_BEFORE["order_payment_authorized"]))

    def test_picked_checks_the_declared_amount_against_the_oltp_state(self):
        """If the amount declared in the event and the OLTP state diverge, the transaction falls.

        Without this, the OLTP fold and the Silver fold could start from different bases — and
        `orders-reconcile`, in Milestone 6, would compare two wrong things and PASS."""
        ops = plan(event("order_picked", seq=2,
                         payload={"picked_line_count": 2, "picked_amount": "10.00"}))
        checks = [op for op in ops if isinstance(op, Check)]
        self.assertEqual([c.expected for c in checks], [0, Decimal("10.00"), 2])

    def test_picking_fulfils_the_lines_nobody_touched(self):
        """An untouched line only becomes `fulfilled` at picking — not at placement.

        `order_picked` is the only event that declares picking. Labelling `fulfilled` before it
        asserts a fact the log does not have: it was the defect Silver carried in 5,508 lines
        of 298 orders that were cancelled or had a declined payment."""
        ops = plan(event("order_picked", seq=2,
                         payload={"picked_line_count": 2, "picked_amount": "10.00"}))
        flip = " ".join(ops[1].sql.split())
        self.assertIn("set status = 'fulfilled'", flip)
        self.assertIn("status = 'placed'", flip)
        # And no line may remain undefined after that.
        self.assertEqual(ops[2].expected, 0)
        self.assertIn("undefined line is left", ops[2].detail)

    def test_dying_before_picking_closes_the_lines_as_not_picked(self):
        """Cancellation and payment decline close the lines as `not_picked`, with zero amount.
        Leaving them `placed` would keep a pending state on a terminal order."""
        for kind in ("order_cancelled", "order_payment_failed"):
            ops = plan(event(kind, seq=2, payload={"cancelled_by": "customer",
                                                   "reason": "x", "decline_reason": "y"}))
            close = " ".join(ops[1].sql.split())
            self.assertIn("set status = 'not_picked'", close)
            self.assertIn("line_amount = 0", close)
            self.assertIn("fulfilled_source_product_id = null", close)

    def test_the_line_vocabulary_is_the_same_on_both_sides(self):
        """The SAME vocabulary as silver_order_line.line_status, not a similar one: a
        translation table between two folds is where `orders-reconcile` would compare two
        wrong things and pass."""
        from retail_platform.orders_oltp import DDL
        for value in ("placed", "fulfilled", "substituted", "removed", "not_picked"):
            self.assertIn(f"'{value}'", DDL)

    def test_removed_line_keeps_no_amount_and_no_product(self):
        ops = plan(event("order_line_removed", seq=2,
                         payload={"line_no": 1, "reason": "unavailable"}))
        sql = " ".join(ops[1].sql.split())
        self.assertIn("line_amount = 0", sql)
        self.assertIn("fulfilled_source_product_id = null", sql)
        # And it only touches a line that is still 'placed': reapplying does not rewrite history.
        self.assertIn("status = 'placed'", sql)

    def test_line_events_recompute_net_amount_from_the_lines(self):
        """`net_amount` is derived from the OLTP's own lines, not accumulated by hand.

        Accumulating (`net = net - x`) turns the error of ONE application into a permanent
        error; recomputing from the sum makes the state self-correcting and makes the
        reconciliation with Silver a comparison between two independent folds, which is its
        point."""
        for kind, payload in (
            ("order_line_substituted", {"line_no": 1, "quantity": 2, "source_product_id": "1",
                                        "substitute_source_product_id": "9",
                                        "substitute_unit_price": "3.00"}),
            ("order_line_removed", {"line_no": 1, "reason": "unavailable"}),
        ):
            ops = plan(event(kind, seq=2, payload=payload))
            self.assertIn("sum(line_amount)", " ".join(ops[-1].sql.split()))

    def test_timestamps_are_parsed_in_python_not_left_to_the_server(self):
        ops = plan(event("order_payment_authorized", seq=2, at="2026-08-27T07:30:00Z"))
        self.assertEqual(ops[0].params[2],
                         datetime(2026, 8, 27, 7, 30, tzinfo=timezone.utc))


class TestOutboxStatement(unittest.TestCase):
    def test_the_outbox_carries_the_log_line_verbatim(self):
        """What goes to the broker is THE LOG LINE, not a reserialization of it.

        Reserializing would go through jsonb, which reorders keys by its own criteria — and the
        `event_id` and the manifest sha256 would no longer be verifiable end to end."""
        payload = event("order_payment_authorized", seq=2,
                        payload={"payment_method": "card", "authorized_amount": "10.00"})
        line = canonical_line(payload)
        statement = outbox_statement(payload, line)
        self.assertEqual(statement.params[-1], line)
        self.assertIn("on conflict (event_id) do nothing", " ".join(statement.sql.split()))

    def test_zero_rows_in_the_outbox_is_not_an_error(self):
        """`expect_rows=None` on purpose: 0 rows here is the 'already applied' signal."""
        statement = outbox_statement(event("order_placed", payload=PLACED_PAYLOAD), "{}")
        self.assertIsNone(statement.expect_rows)


class TestTransactionBoundary(unittest.TestCase):
    """The property that justifies Kafka: state and event, either both or neither."""

    def _apply(self, connection, evt):
        return apply_event(connection, evt, canonical_line(evt))

    def test_outbox_and_state_share_one_transaction_with_a_single_commit_at_the_end(self):
        connection = FakeConnection()
        self.assertTrue(self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD)))
        self.assertEqual(
            connection.tags,
            ["insert:outbox", "insert:orders", "insert:order_line", "insert:order_line",
             "commit"],
        )
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)

    def test_no_commit_happens_between_the_outbox_and_the_state(self):
        """THIS is the test that catches the real defect. One extra `commit()` after the outbox
        insert would turn the pattern into a dual-write without changing anything in
        appearance: the outbox would still have the right row, the state would still be right,
        and a crash between the two commits would publish an event that never happened."""
        connection = FakeConnection()
        self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        commit_at = connection.tags.index("commit")
        self.assertEqual(commit_at, len(connection.tags) - 1)
        self.assertNotIn("commit", connection.tags[:commit_at])

    def test_a_failure_in_the_middle_ends_in_rollback_and_never_in_commit(self):
        connection = FakeConnection(fail_on=lambda label, _: label == "insert:order_line")
        with self.assertRaises(FakePgError):
            self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.journal[-1], ("rollback",))

    def test_a_failure_on_the_outbox_itself_rolls_back(self):
        """The case `make orders-prove-atomicity` reproduces against the real Postgres."""
        connection = FakeConnection(fail_on=lambda label, _: label == "insert:outbox")
        with self.assertRaises(FakePgError):
            self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        # And, decisively: no state write was even issued.
        self.assertNotIn("insert:orders", connection.tags)

    def test_an_out_of_order_event_is_refused_by_rowcount_not_ignored(self):
        """An update that does not find the order in the expected state affects zero rows and
        Postgres does NOT complain. Without checking rowcount, applying out of order would be
        silent — the worst possible failure mode in an event chain."""
        connection = FakeConnection(rowcounts={"update:orders": 0})
        with self.assertRaises(OrdersOltpError) as ctx:
            self._apply(connection, event("order_payment_authorized", seq=2))
        # Portuguese on purpose: scripts/prove_oltp_atomicity.py matches this substring.
        self.assertIn("fora de ordem", str(ctx.exception))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_a_declared_amount_that_disagrees_with_the_oltp_kills_the_transaction(self):
        # Result queue: no undefined line, then a sum that does NOT match.
        connection = FakeConnection(
            results={"select": [[(0,)], [(Decimal("9.00"),)], [(2,)]]}
        )
        with self.assertRaises(OrdersOltpError) as ctx:
            self._apply(connection, event("order_picked", seq=2,
                                          payload={"picked_line_count": 2,
                                                   "picked_amount": "10.00"}))
        self.assertIn("does not match", str(ctx.exception))
        self.assertEqual(connection.commits, 0)

    def test_reapplying_an_event_writes_nothing_and_reports_it(self):
        """Idempotency through `event_id unique`, not guesswork: the outbox is the record that
        the event WAS ALREADY applied, and it commits together with the state — so the two
        cannot disagree about what already happened."""
        evt = event("order_placed", payload=PLACED_PAYLOAD)
        connection = FakeConnection(applied_event_ids=[evt["event_id"]])
        self.assertFalse(self._apply(connection, evt))
        self.assertEqual(connection.tags, ["insert:outbox", "rollback"])
        self.assertEqual(connection.commits, 0)

    def test_the_two_guards_agree_the_state_guard_would_also_have_refused(self):
        """The two guards are independent and agree by construction: reapplying an already
        applied event passes neither the outbox (conflict) nor the state (the sequence already
        advanced). The outbox is what decides, because `skip` is the right behavior on a replay
        — but if it failed, the order guard would still refuse."""
        connection = FakeConnection(rowcounts={"update:orders": 0})
        with self.assertRaises(OrdersOltpError):
            self._apply(connection, event("order_payment_authorized", seq=2))


class TestApplyPartition(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def _partition(self, events, *, ingestion_date="2026-08-27", wh="mad1", tamper=False):
        path = os.path.join(self.root, f"ingestion_date={ingestion_date}", f"wh={wh}")
        os.makedirs(path, exist_ok=True)
        blob = "".join(canonical_line(e) + "\n" for e in events).encode("utf-8")
        with open(os.path.join(path, "order_events.jsonl"), "wb") as handle:
            handle.write(blob if not tamper else blob.replace(b"mad1", b"bcn1", 1))
        manifest = {
            "manifest_version": 1,
            "run_id": "20260828T000000Z_mad1_20260827",
            "complete": True,
            "source": {"name": "simulated_orders", "lang": "es", "wh": wh},
            "partition": {"ingestion_date": ingestion_date, "warehouse": wh},
            "totals": {"event_rows": len(events)},
            "schema_fingerprint": {},
            "files": [{
                "path": f"ingestion_date={ingestion_date}/wh={wh}/order_events.jsonl",
                "sha256": hashlib.sha256(blob).hexdigest(),
                "bytes": len(blob),
                "records": len(events),
                "stage": "order_events",
            }],
        }
        with open(os.path.join(path, "_manifest.json"), "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        open(os.path.join(path, "_SUCCESS"), "w").close()
        return path

    def test_a_tampered_log_is_refused_before_a_single_write(self):
        """Checking after applying is already too late: the outbox is what goes to the broker."""
        path = self._partition([event("order_placed", payload=PLACED_PAYLOAD)], tamper=True)
        connection = FakeConnection()
        with self.assertRaises(OrdersOltpError) as ctx:
            apply_partition(connection, path)
        self.assertIn("diverges from the manifest", str(ctx.exception))
        self.assertEqual(connection.journal, [])

    def test_a_partition_of_another_source_is_refused(self):
        path = self._partition([event("order_placed", payload=PLACED_PAYLOAD)])
        manifest_file = os.path.join(path, "_manifest.json")
        with open(manifest_file, encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["source"]["name"] = "simulated_oltp"
        with open(manifest_file, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        with self.assertRaises((OrdersOltpError, ManifestError)):
            apply_partition(FakeConnection(), path)

    def test_one_transaction_per_event_not_one_per_partition(self):
        """One transaction per partition would prove 'the whole day is atomic', which no store
        guarantees. The unit has to be the same as a real OLTP's: one state change."""
        events = [
            event("order_placed", payload=PLACED_PAYLOAD),
            event("order_payment_authorized", seq=2, at="2026-08-27T07:10:00Z",
                  payload={"payment_method": "card", "authorized_amount": "10.00"}),
            event("order_cancelled", seq=3, at="2026-08-27T07:20:00Z",
                  payload={"cancelled_by": "customer", "reason": "changed_mind"}),
        ]
        connection = FakeConnection()
        result = apply_partition(connection, self._partition(events))
        self.assertEqual(result.events, 3)
        self.assertEqual(result.applied, 3)
        self.assertEqual(result.skipped, 0)
        self.assertEqual(result.orders, 1)
        self.assertEqual(connection.commits, 3)

    def test_replaying_the_same_partition_applies_nothing(self):
        events = [event("order_placed", payload=PLACED_PAYLOAD)]
        path = self._partition(events)
        connection = FakeConnection(applied_event_ids=[e["event_id"] for e in events])
        result = apply_partition(connection, path)
        self.assertEqual(result.applied, 0)
        self.assertEqual(result.skipped, 1)
        self.assertEqual(connection.commits, 0)

    def test_read_log_preserves_the_line_byte_for_byte(self):
        events = [event("order_placed", payload=PLACED_PAYLOAD)]
        path = self._partition(events)
        rows = read_log(path)
        self.assertEqual(rows[0][1], canonical_line(events[0]))


class TestVerifyOutbox(unittest.TestCase):
    """The fidelity proof: the log rebuilt from the outbox reproduces the manifest sha256.

    Counting rows would not prove that. Summing amounts would not either. Reproducing the
    bytes proves that the whole chain — read, apply in a transaction, write, re-read, reorder
    — lost nothing, duplicated nothing and altered nothing.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.events = [
            event("order_placed", payload=PLACED_PAYLOAD),
            event("order_payment_authorized", seq=2, at="2026-08-27T07:10:00Z",
                  payload={"payment_method": "card", "authorized_amount": "10.00"}),
        ]
        self.lines = [canonical_line(e) for e in self.events]
        blob = "".join(line + "\n" for line in self.lines).encode("utf-8")
        self.path = os.path.join(self._tmp.name, "ingestion_date=2026-08-27", "wh=mad1")
        os.makedirs(self.path)
        with open(os.path.join(self.path, "order_events.jsonl"), "wb") as handle:
            handle.write(blob)
        manifest = {
            "manifest_version": 1, "run_id": "r", "complete": True,
            "source": {"name": "simulated_orders", "lang": "es", "wh": "mad1"},
            "partition": {"ingestion_date": "2026-08-27", "warehouse": "mad1"},
            "totals": {}, "schema_fingerprint": {},
            "files": [{"path": "ingestion_date=2026-08-27/wh=mad1/order_events.jsonl",
                       "sha256": hashlib.sha256(blob).hexdigest(), "bytes": len(blob),
                       "records": 2, "stage": "order_events"}],
        }
        with open(os.path.join(self.path, "_manifest.json"), "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        open(os.path.join(self.path, "_SUCCESS"), "w").close()

    def test_a_faithful_outbox_reproduces_the_manifest_digest(self):
        connection = FakeConnection(results={"select": [(line,) for line in self.lines]})
        proof = verify_outbox(connection, self.path)
        self.assertTrue(proof["matches"])
        self.assertEqual(proof["outbox_sha256"], proof["declared_sha256"])
        self.assertEqual(proof["outbox_rows"], 2)

    def test_a_missing_event_makes_the_digest_diverge(self):
        connection = FakeConnection(results={"select": [(self.lines[0],)]})
        proof = verify_outbox(connection, self.path)
        self.assertFalse(proof["matches"])

    def test_a_duplicated_event_makes_the_digest_diverge(self):
        rows = [(line,) for line in self.lines] + [(self.lines[0],)]
        proof = verify_outbox(FakeConnection(results={"select": rows}), self.path)
        self.assertFalse(proof["matches"])

    def test_an_altered_event_makes_the_digest_diverge(self):
        rows = [(self.lines[0].replace('"quantity": 2', '"quantity": 3'),),
                (self.lines[1],)]
        proof = verify_outbox(FakeConnection(results={"select": rows}), self.path)
        self.assertFalse(proof["matches"])

    def test_the_rebuild_orders_by_the_canonical_key_not_by_insertion(self):
        """`outbox_id` is INSERTION order. Today the two coincide; relying on the coincidence
        would make this check pass by luck the day a partial replay reinserted an old event
        after a recent one."""
        connection = FakeConnection(results={"select": [(line,) for line in self.lines]})
        verify_outbox(connection, self.path)
        sql = " ".join(connection.journal[0][1:2])  # tag
        self.assertEqual(sql, "select")
        # The real clause: checked in the module itself; here we ensure there is a declared
        # canonical order by, and not one by outbox_id.
        from retail_platform.orders_oltp import rebuild_log
        import inspect
        source = inspect.getsource(rebuild_log)
        self.assertIn("order by o.occurred_at, o.order_id, o.sequence_no", source)
        self.assertNotIn("order by o.outbox_id", source)


if __name__ == "__main__":
    unittest.main()
