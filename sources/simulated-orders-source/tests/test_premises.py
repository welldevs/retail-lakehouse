"""Premises: no default, and an inverted range fails."""

from __future__ import annotations

import unittest

from simulated_orders_source.premises import PremiseError, Premises

from .support import PREMISES


class PremisesTest(unittest.TestCase):
    def test_valid_table_passes(self):
        p = Premises(PREMISES)
        self.assertEqual(p.integer("basket_lines_mode"), 3)
        self.assertAlmostEqual(p.number("substitution_rate_other"), 0.2)

    def test_missing_key_raises_instead_of_returning_default(self):
        # The whole point of the module: a default hidden in the generator would be an
        # undeclared premise, and the table exists precisely to prevent that.
        p = Premises(PREMISES)
        with self.assertRaises(PremiseError) as caught:
            p.number("rate_nobody_declared")
        self.assertIn("default", str(caught.exception))

    def test_inverted_range_fails_on_construction(self):
        bad = dict(PREMISES, minutes_dispatch_before_slot_min="240",
                   minutes_dispatch_before_slot_max="10")
        with self.assertRaises(PremiseError) as caught:
            Premises(bad)
        self.assertIn("inverted", str(caught.exception))

    def test_proportion_outside_zero_one_fails(self):
        for value in ("1.5", "-0.1"):
            with self.assertRaises(PremiseError):
                Premises(dict(PREMISES, substitution_rate_other=value))

    def test_mode_outside_range_fails(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, basket_lines_mode="99"))

    def test_non_numeric_value_fails(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, quantity_max="three"))

    def test_integer_requires_integer_value(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES)).integer("substitution_rate")

    def test_share_vectors_must_sum_to_one(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, cadence_weight_7d="0.9"))
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, slot_lead_day_weight_1="0.2"))

    def test_slot_must_fit_the_delivery_window(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, delivery_open_hour="21", delivery_close_hour="21"))

    def test_on_time_plus_early_cannot_exceed_one(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, delivery_on_time_rate="0.95", delivery_early_rate="0.1"))


if __name__ == "__main__":
    unittest.main()
