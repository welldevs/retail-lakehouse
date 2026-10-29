"""The state machine and the envelope: the contract in code."""

from __future__ import annotations

import unittest

from simulated_orders_source import events as ev


class VocabularyTest(unittest.TestCase):
    def test_every_type_has_state_before_and_after(self):
        self.assertEqual(set(ev.EVENT_TYPES), set(ev.STATE_AFTER))
        self.assertEqual(set(ev.EVENT_TYPES), set(ev.STATE_BEFORE))

    def test_every_resulting_state_is_declared(self):
        for kind, state in ev.STATE_AFTER.items():
            self.assertIn(state, ev.STATES, f"{kind} results in an undeclared state")

    def test_only_order_placed_opens_the_aggregate(self):
        openers = [t for t, before in ev.STATE_BEFORE.items() if None in before]
        self.assertEqual(openers, [ev.ORDER_PLACED])

    def test_delivered_is_not_terminal_because_a_return_may_still_come(self):
        self.assertNotIn(ev.DELIVERED, ev.TERMINAL_STATES)
        self.assertIn(ev.DELIVERED, ev.STATE_BEFORE[ev.ORDER_RETURNED])

    def test_invalid_is_not_a_domain_state(self):
        # The sentinel must not be reachable by any transition: if it were, a corrupted order
        # would become an analyzable state instead of an error.
        self.assertNotIn(ev.INVALID, ev.STATES)
        self.assertNotIn(ev.INVALID, ev.STATE_AFTER.values())


class EventIdTest(unittest.TestCase):
    def test_is_deterministic(self):
        self.assertEqual(ev.event_id("ord_x_1_000001", 3), ev.event_id("ord_x_1_000001", 3))

    def test_changes_with_order_and_position(self):
        base = ev.event_id("ord_x_1_000001", 3)
        self.assertNotEqual(base, ev.event_id("ord_x_1_000002", 3))
        self.assertNotEqual(base, ev.event_id("ord_x_1_000001", 4))

    def test_uses_neither_clock_nor_entropy(self):
        # Two different processes must agree, otherwise the downstream consumer cannot
        # deduplicate across replays.
        import subprocess
        import sys

        code = (
            "import sys; sys.path.insert(0, 'src');"
            "from simulated_orders_source.events import event_id;"
            "print(event_id('ord_mad1_20260824_000007', 5))"
        )
        outputs = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=__file__.rsplit("/tests/", 1)[0],
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 7, 4242)
        }
        self.assertEqual(len(outputs), 1, f"event_id varied across processes: {outputs}")
        self.assertEqual(outputs.pop(), ev.event_id("ord_mad1_20260824_000007", 5))


class TransitionTest(unittest.TestCase):
    def test_happy_path(self):
        state = None
        for kind in (
            ev.ORDER_PLACED,
            ev.ORDER_PAYMENT_AUTHORIZED,
            ev.ORDER_PICKING_STARTED,
            ev.ORDER_PICKED,
            ev.ORDER_DISPATCHED,
            ev.ORDER_DELIVERED,
        ):
            state = ev.apply_transition(state, kind)
        self.assertEqual(state, ev.DELIVERED)

    def test_event_after_terminal_is_refused(self):
        state = ev.apply_transition(None, ev.ORDER_PLACED)
        state = ev.apply_transition(state, ev.ORDER_PAYMENT_FAILED)
        with self.assertRaises(ev.EventError) as caught:
            ev.apply_transition(state, ev.ORDER_PICKING_STARTED)
        self.assertIn("terminal", str(caught.exception))

    def test_out_of_order_transition_is_refused(self):
        state = ev.apply_transition(None, ev.ORDER_PLACED)
        with self.assertRaises(ev.EventError):
            ev.apply_transition(state, ev.ORDER_DISPATCHED)

    def test_unknown_type_is_refused(self):
        with self.assertRaises(ev.EventError):
            ev.apply_transition(None, "order_teleported")

    def test_line_events_do_not_touch_order_state(self):
        state = ev.PICKING
        for kind in (ev.ORDER_LINE_SUBSTITUTED, ev.ORDER_LINE_REMOVED):
            self.assertEqual(ev.apply_transition(state, kind), ev.PICKING)


class FoldTest(unittest.TestCase):
    def _log(self, *kinds):
        return [{"event_type": kind} for kind in kinds]

    def test_strict_raises_on_broken_log(self):
        with self.assertRaises(ev.EventError):
            ev.fold(self._log(ev.ORDER_PLACED, ev.ORDER_DISPATCHED))

    def test_non_strict_returns_sentinel_instead_of_blowing_up(self):
        # Counting a tampered log must produce a diverging number, never an exception:
        # otherwise "invalid partition" (code 1) would become "unhandled exception" (code 3).
        self.assertEqual(
            ev.fold(self._log(ev.ORDER_PLACED, ev.ORDER_DISPATCHED), strict=False), ev.INVALID
        )

    def test_empty_log_has_no_state(self):
        self.assertIsNone(ev.fold([]))


class EnvelopeTest(unittest.TestCase):
    def test_carries_exactly_the_declared_fields(self):
        envelope = ev.envelope(
            order_id="ord_mad1_20260824_000001",
            wh="mad1",
            sequence_no=1,
            event_type=ev.ORDER_PLACED,
            occurred_at="2026-08-24T09:00:00Z",
            payload={},
            producer="simulated_orders",
        )
        self.assertEqual(sorted(envelope), sorted(ev.ENVELOPE_FIELDS))

    def test_refuses_type_outside_vocabulary(self):
        with self.assertRaises(ev.EventError):
            ev.envelope("o", "mad1", 1, "order_teleported", "2026-08-24T09:00:00Z", {}, "p")


if __name__ == "__main__":
    unittest.main()
