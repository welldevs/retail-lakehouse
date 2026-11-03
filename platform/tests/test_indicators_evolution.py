"""The price-evolution panel's indicators, checked without Postgres and without network.

Sibling of `test_indicators_prices.py`: declared grain/kind/caveats, parameters always bound,
MART only, the generated page byte for byte. Two properties are specific to an index and are
pinned here because getting them wrong produces a PLAUSIBLE number, not an error:

  - every link has both ends inside the window (`INSIDE_WINDOW`), or the first day of a period
    carries the movement of the days before it;
  - the price is `purchasable_*` everywhere, never the raw `unit_price` (a x99 selector
    ceiling for some weight-sold products would be a 9,800% link).

What the SQL computes — that the chained index equals an independent pandas computation
from the same rows — needs the warehouse; it was measured for CR-019 and is in DECISIONS.md.
"""

from __future__ import annotations

import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "platform", "src"))

from retail_platform.indicators import contract as C  # noqa: E402
from retail_platform.indicators import evolution as EV  # noqa: E402

PAGE = os.path.join(ROOT, "docs", "indicators", "evolution.md")
MART_DIR = os.path.join(ROOT, "platform", "dbt", "models", "warehouse", "mart")
INDEX_KEYS = ("indice_de_precos", "indice_por_departamento")
RAW_PRICE = re.compile(r"(?<!purchasable_)(?<!previous_purchasable_)\bunit_price\b")


class StructureTest(unittest.TestCase):
    def test_every_indicator_declares_the_review_fields(self):
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.title.strip())
                self.assertTrue(indicator.question.strip().endswith("?"))
                self.assertTrue(indicator.grain.strip())
                self.assertTrue(indicator.kind.startswith("observed"),
                                "the evolution panel reads observed prices only")
                self.assertTrue(indicator.sql.strip())

    def test_keys_are_unique_and_do_not_collide_with_price_watch(self):
        from retail_platform.indicators import prices as PI
        keys = [i.key for i in EV.INDICATORS]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertFalse(set(keys) & {i.key for i in PI.INDICATORS})

    def test_every_indicator_declares_caveats_long_enough_to_explain_a_failure(self):
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.caveats)
                for caveat in indicator.caveats:
                    self.assertGreater(len(caveat.strip()), 60, caveat)

    def test_reads_only_the_price_evolution_mart_and_it_exists(self):
        on_disk = {os.path.splitext(f)[0].upper() for f in os.listdir(MART_DIR)
                   if f.endswith(".sql")}
        used = {m for i in EV.INDICATORS for m in i.marts}
        self.assertEqual(used, {"MART_PRICE_EVOLUTION"})
        self.assertLessEqual(used, on_disk)


class IndexMethodTest(unittest.TestCase):
    def test_every_index_link_has_both_ends_inside_the_window(self):
        for key in INDEX_KEYS:
            sql = next(i.sql for i in EV.INDICATORS if i.key == key)
            with self.subTest(key):
                self.assertIn(EV.INSIDE_WINDOW, sql)
                self.assertIn(EV.LINK, sql)
                # every aggregate of the link is filtered by the window rule
                self.assertEqual(sql.count(f"avg({EV.LINK})"),
                                 sql.count(f"avg({EV.LINK}) filter (where {EV.INSIDE_WINDOW})"))

    def test_warehouses_combine_as_a_mean_of_their_lines_not_a_pooled_link(self):
        """Pooling every warehouse's products into one link ended above all four warehouse
        lines (CR-019). The combination divides the summed log-links by the warehouse count."""
        for key in INDEX_KEYS:
            sql = next(i.sql for i in EV.INDICATORS if i.key == key)
            with self.subTest(key):
                self.assertIn("sum(log_link) / (select count(distinct wh) from links)", sql)

    def test_no_query_reads_the_raw_price(self):
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertIsNone(RAW_PRICE.search(indicator.sql),
                                  f"{indicator.key} reads the raw unit_price")

    def test_the_raw_price_check_can_fail(self):
        self.assertIsNotNone(RAW_PRICE.search("select unit_price from t"))
        self.assertIsNone(RAW_PRICE.search("select purchasable_unit_price, "
                                           "previous_purchasable_unit_price from t"))


class BoundParameterTest(unittest.TestCase):
    def test_dates_and_warehouses_are_bound(self):
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertIn("%(start)s", indicator.sql)
                self.assertIn("%(end)s", indicator.sql)
                self.assertIn("%(warehouses)s", indicator.sql)

    def test_no_query_is_left_with_a_format_placeholder(self):
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn("{", indicator.sql)
                self.assertNotIn("}", indicator.sql)

    def test_reads_only_the_mart_schema_and_never_writes(self):
        forbidden = ("insert ", "update ", "delete ", "merge ", "create ", "drop ",
                     "alter ", "truncate ", "grant ", "copy into")
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn('"GOLD"', indicator.sql)
                self.assertNotIn('"STAGE"', indicator.sql)
                self.assertIn(f"{EV.MART}.", indicator.sql)
                for verb in forbidden:
                    self.assertNotIn(verb, indicator.sql.lower())


class ContractInSyncTest(unittest.TestCase):
    def test_contract_on_disk_is_BYTE_FOR_BYTE_what_the_generator_produces(self):
        self.assertTrue(os.path.exists(PAGE),
                        "docs/indicators/evolution.md does not exist — run `make indicators-contract`")
        with open(PAGE, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), C.render_evolution(),
                             "docs/indicators/evolution.md is out of sync with evolution.py. "
                             "Run `make indicators-contract`.")

    def test_contract_carries_every_indicator_and_caveat(self):
        generated = C.render_evolution()
        self.assertIn(C.source_sha256(EV), generated)
        for indicator in EV.INDICATORS:
            with self.subTest(indicator.key):
                self.assertIn(indicator.sql.strip(), generated)
                for caveat in indicator.caveats:
                    self.assertIn(caveat, generated)


if __name__ == "__main__":
    unittest.main()
