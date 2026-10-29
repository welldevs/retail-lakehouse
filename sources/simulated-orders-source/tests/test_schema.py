"""Stable fingerprint and re-checkable totals."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_orders_source import events as ev
from simulated_orders_source.orders_generator import generate
from simulated_orders_source.premises import Premises
from simulated_orders_source.reference_data import load
from simulated_orders_source.schema import (
    PAYLOAD_FIELDS,
    fingerprint,
    group_by_order,
    missing_fields,
    totals_of,
)

from .support import ORDER_DATE, PREMISES, WH, write_reference


class FingerprintTest(unittest.TestCase):
    def test_covers_the_whole_vocabulary(self):
        # What sets this Source apart: the fingerprint is of the DECLARED CONTRACT, not the
        # union of observed keys. A day without returns would have no order_returned in the
        # observed set, and two correct partitions would diverge by chance.
        declared = fingerprint()
        self.assertEqual(sorted(declared["payload_keys"]), sorted(ev.EVENT_TYPES))

    def test_is_stable_across_calls(self):
        self.assertEqual(fingerprint()["sha256"], fingerprint()["sha256"])

    def test_every_type_has_declared_payload(self):
        self.assertEqual(set(PAYLOAD_FIELDS), set(ev.EVENT_TYPES))

    def test_two_types_have_empty_payload_on_purpose(self):
        self.assertEqual(PAYLOAD_FIELDS[ev.ORDER_PICKING_STARTED], ())
        self.assertEqual(PAYLOAD_FIELDS[ev.ORDER_DISPATCHED], ())


class MissingFieldsTest(unittest.TestCase):
    def test_reports_missing_envelope_field(self):
        self.assertIn("envelope.wh", missing_fields([{"event_type": ev.ORDER_DISPATCHED}]))

    def test_reports_missing_payload_field(self):
        event = {f: "x" for f in ev.ENVELOPE_FIELDS}
        event["event_type"] = ev.ORDER_PICKED
        event["payload"] = {"picked_line_count": 1}
        self.assertIn("order_picked.picked_amount", missing_fields([event]))

    def test_reports_missing_line_field(self):
        event = {f: "x" for f in ev.ENVELOPE_FIELDS}
        event["event_type"] = ev.ORDER_PLACED
        event["payload"] = {f: "x" for f in PAYLOAD_FIELDS[ev.ORDER_PLACED]}
        event["payload"]["lines"] = [{"line_no": 1}]
        self.assertIn("line.unit_price", missing_fields([event]))


class TotalsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = load(write_reference(os.path.join(self.tmp.name, "ref")))
        self.events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.totals = totals_of(self.events)

    def test_counts_match_the_log(self):
        self.assertEqual(self.totals["event_rows"], len(self.events))
        self.assertEqual(self.totals["order_rows"], len(group_by_order(self.events)))
        self.assertEqual(sum(self.totals["event_rows_by_type"].values()), len(self.events))
        self.assertEqual(sum(self.totals["orders_by_state"].values()), self.totals["order_rows"])

    def test_every_vocabulary_type_appears_in_the_count_even_with_zero(self):
        # If a type vanished from the count for not having occurred, comparing two days would
        # require knowing which keys are missing from which — and the manifest would come to
        # depend on the draw.
        self.assertEqual(sorted(self.totals["event_rows_by_type"]), sorted(ev.EVENT_TYPES))

    def test_broken_log_counts_instead_of_blowing_up(self):
        broken = [e for e in self.events if e["event_type"] != ev.ORDER_PAYMENT_AUTHORIZED]
        totals = totals_of(broken)  # does not raise
        self.assertIn(ev.INVALID, totals["orders_by_state"])

    def test_empty_log(self):
        totals = totals_of([])
        self.assertEqual(totals["event_rows"], 0)
        self.assertEqual(totals["order_rows"], 0)


if __name__ == "__main__":
    unittest.main()
