"""Product choice within a demand group, and units per line (CR-017).

Offline and fixture-only: the weights are pure arithmetic over catalog rows.
"""

from __future__ import annotations

import os
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform import product_choice as pc  # noqa: E402

PREMISES = {
    "quantity_max": "4", "quantity_max_multi": "6", "quantity_multi_price_eur": "1.5",
    "quantity_max_mid": "2", "quantity_mid_price_eur": "8",
    "quantity_max_high": "1", "quantity_high_price_eur": "25",
    "own_brand_weight": "2.0",
}


def row(name, price, kg="1.0", group="CARNE", wh="mad1", day="2026-08-24"):
    return {"display_name": name, "unit_price": str(price), "net_content_kg_l": kg,
            "demand_group": group, "wh": wh, "price_as_of": day}


def benchmark(eur_kg, group="CARNE"):
    return {group: {"use_as_weight": True, "avg_price_eur_kg": Decimal(str(eur_kg)),
                    "volume_share_pct": Decimal("10")}}


class OwnBrandTest(unittest.TestCase):
    def test_the_four_mercadona_brands_are_recognised(self):
        for name in ("Leche entera Hacendado", "Gel Deliplus", "Papel Bosque Verde 12 rollos",
                     "Pienso Compy"):
            self.assertTrue(pc.is_own_brand(name), name)

    def test_a_brand_inside_another_word_is_not(self):
        self.assertFalse(pc.is_own_brand("Jamón de bellota ibérico 100% Covap"))
        self.assertFalse(pc.is_own_brand("Hacendados"))


class QuantityTest(unittest.TestCase):
    def test_tiers_follow_the_price(self):
        self.assertEqual(pc.quantity_max_of(0.65, PREMISES), 6)
        self.assertEqual(pc.quantity_max_of(3.00, PREMISES), 4)
        self.assertEqual(pc.quantity_max_of(12.00, PREMISES), 2)
        self.assertEqual(pc.quantity_max_of(504.00, PREMISES), 1)

    def test_expected_quantity_is_the_generator_draw(self):
        self.assertEqual(pc.expected_quantity(1), 1.0)
        # w = 1, 1/2 -> (1*1 + 2*0.5) / 1.5
        self.assertAlmostEqual(pc.expected_quantity(2), 2 / 1.5, places=12)


class WeightTest(unittest.TestCase):
    def test_a_whole_ham_is_far_rarer_than_the_sliced_pack(self):
        rows = [row("Jamón loncheado", 3.0), row("Chorizo", 2.5), row("Pavo", 3.5),
                row("Jamón de bellota ibérico", 504.0)]
        pc.stamp(rows, PREMISES, benchmark(9.0))
        weights = {r["display_name"]: float(r["popularity_weight"]) for r in rows}
        self.assertLess(weights["Jamón de bellota ibérico"], weights["Jamón loncheado"] / 10)

    def test_an_own_brand_weighs_its_declared_multiplier_more_at_the_same_price(self):
        rows = [row("Queso Hacendado", 4.0), row("Queso García Baquero", 4.0)]
        pc.stamp(rows, PREMISES, benchmark(4.0))
        weights = [float(r["popularity_weight"]) for r in rows]
        self.assertAlmostEqual(weights[0] / weights[1], 2.0, places=6)

    def test_the_median_is_taken_per_warehouse_day_and_group(self):
        """A pricier group elsewhere must not shift this group's weights."""
        rows = [row("A", 2.0), row("B", 4.0), row("C", 40.0, group="OTRO"),
                row("D", 80.0, group="OTRO")]
        pc.stamp(rows, PREMISES, benchmark(3.0))
        by = {r["display_name"]: float(r["popularity_weight"]) for r in rows}
        self.assertAlmostEqual(by["A"] / by["B"], by["C"] / by["D"], places=6)


class FitTest(unittest.TestCase):
    def test_the_fitted_elasticity_moves_eur_per_kg_toward_mapa(self):
        """Uniform choice pays the catalog's mean EUR/kg; MAPA says households pay less."""
        rows = [row("Barato", 2.0), row("Medio", 6.0), row("Caro", 30.0)]
        header = pc.stamp(rows, PREMISES, benchmark(3.0))
        fit = header["fit_by_group"][0]
        uniform, fitted = float(fit["uniform_eur_kg"]), float(fit["fitted_eur_kg"])
        self.assertGreater(header["elasticity"], 0)
        self.assertLess(abs(fitted - 3.0), abs(uniform - 3.0))

    def test_the_weight_is_a_fixed_decimal_string(self):
        rows = [row("Barato", 2.0), row("Caro", 30.0)]
        pc.stamp(rows, PREMISES, benchmark(3.0))
        for r in rows:
            self.assertRegex(r["popularity_weight"], r"^\d+\.\d{6}$")

    def test_no_benchmark_group_with_content_fails(self):
        rows = [row("Sin peso", 2.0, kg="")]
        with self.assertRaises(pc.ProductChoiceError):
            pc.stamp(rows, PREMISES, benchmark(3.0))


if __name__ == "__main__":
    unittest.main()
