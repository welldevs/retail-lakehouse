"""The price panel's indicators, checked without Postgres and without network.

SIBLING OF `test_indicators_operations.py`, NOT A REDUCED COPY. The same properties matter
here — declared grain/kind/caveat, parameters always bound, the panel reads only MART — but
the BIDIRECTIONAL check "every mart on disk is used" does not apply: this panel is
DELIBERATELY exclusive (2 of the warehouse's 9 marts), and testing that property would mean
proving the opposite of what this file exists to prove. Instead,
`test_panel_uses_exactly_the_two_declared_marts` pins the scope positively.

Runs inside `make test`/`platform-test` (offline, by repository invariant) because it lives
in `platform/tests/`, in the same venv that already serves the operations panel.
"""

from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "platform", "src"))

from retail_platform.indicators import contract as C  # noqa: E402
from retail_platform.indicators import prices as PI  # noqa: E402

PAGE = os.path.join(ROOT, "docs", "indicators", "prices.md")

MART_DIR = os.path.join(ROOT, "platform", "dbt", "models", "warehouse", "mart")
EXPECTED_MARTS = {"MART_PRICE_EVOLUTION", "MART_ASSORTMENT_DAILY"}


class StructureTest(unittest.TestCase):
    def test_every_indicator_declares_the_review_fields(self):
        for indicator in PI.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.title.strip())
                self.assertTrue(indicator.question.strip().endswith("?"),
                                "the question has to be a question")
                self.assertTrue(indicator.grain.strip())
                self.assertTrue(indicator.kind.strip())
                self.assertTrue(indicator.marts)
                self.assertTrue(indicator.sql.strip())

    def test_keys_are_unique(self):
        keys = [i.key for i in PI.INDICATORS]
        self.assertEqual(len(keys), len(set(keys)))

    def test_every_indicator_declares_at_least_one_caveat(self):
        for indicator in PI.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.caveats,
                                f"{indicator.key} declares no caveat at all")
                for caveat in indicator.caveats:
                    self.assertGreater(
                        len(caveat.strip()), 60,
                        f"{indicator.key}: caveat too short to explain a failure "
                        f"mode — {caveat!r}",
                    )

    def test_cited_marts_exist_as_models_on_disk(self):
        on_disk = {
            os.path.splitext(filename)[0].upper()
            for filename in os.listdir(MART_DIR) if filename.endswith(".sql")
        }
        self.assertTrue(on_disk)
        for indicator in PI.INDICATORS:
            for mart in indicator.marts:
                self.assertIn(mart, on_disk, f"{indicator.key} cites {mart}, which does not exist")

    def test_panel_uses_exactly_the_two_declared_marts(self):
        """The positive property in place of the bidirectional one in
        `test_indicators_operations.py`: this panel is exclusive by design, so the right
        scope is NOT "every mart used" — it is "only these two, and no other slipped in by
        accident"."""
        used = {m for i in PI.INDICATORS for m in i.marts}
        self.assertEqual(used, EXPECTED_MARTS)


class BoundParameterTest(unittest.TestCase):
    def test_date_filter_is_bound_and_never_concatenated(self):
        for indicator in PI.INDICATORS:
            if not indicator.dated:
                continue
            with self.subTest(indicator.key):
                self.assertIn("%(start)s", indicator.sql)
                self.assertIn("%(end)s", indicator.sql)

    def test_warehouse_filter_is_bound_and_never_concatenated(self):
        for indicator in PI.INDICATORS:
            if not indicator.dated:
                continue
            with self.subTest(indicator.key):
                self.assertIn("%(warehouses)s", indicator.sql)

    def test_product_filter_is_bound_when_used(self):
        """`ilike %(term)s`: the `%` wildcard lives in the bound VALUE (built in
        streamlit/prices_app.py), never inside the SQL text — otherwise a product name typed
        by the user would go straight into the query."""
        using_term = [i for i in PI.INDICATORS if "%(term)s" in i.sql]
        self.assertGreaterEqual(len(using_term), 2, "expected produtos_buscaveis + historico")
        for indicator in using_term:
            with self.subTest(indicator.key):
                self.assertIn(PI.PRODUCT_FILTER, indicator.sql)

    def test_no_query_uses_format_or_value_f_string(self):
        for indicator in PI.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn("{", indicator.sql)
                self.assertNotIn("}", indicator.sql)

    def test_panel_reads_only_the_mart_schema(self):
        for indicator in PI.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn('"GOLD"', indicator.sql)
                self.assertNotIn('"STAGE"', indicator.sql)
                self.assertIn(f"{PI.MART}.", indicator.sql)

    def test_no_query_writes(self):
        forbidden = ("insert ", "update ", "delete ", "merge ", "create ", "drop ",
                     "alter ", "truncate ", "grant ", "copy into")
        for indicator in PI.INDICATORS:
            lowered = indicator.sql.lower()
            for verb in forbidden:
                with self.subTest(f"{indicator.key}:{verb.strip()}"):
                    self.assertNotIn(verb, lowered)


class ContractInSyncTest(unittest.TestCase):
    def test_contract_on_disk_is_BYTE_FOR_BYTE_what_the_generator_produces(self):
        path = PAGE
        self.assertTrue(os.path.exists(path),
                        "docs/indicators/prices.md does not exist — run `make indicators-contract`")
        with open(path, encoding="utf-8") as handle:
            on_disk = handle.read()
        self.assertEqual(
            on_disk, C.render_prices(),
            "docs/indicators/prices.md is out of sync with indicators/prices.py. "
            "Run `make indicators-contract`.",
        )

    def test_header_carries_the_source_sha256_and_no_date(self):
        header = next(l for l in C.render_prices().splitlines()[:12] if "sha256" in l)
        self.assertIn(C.source_sha256(PI), header)
        self.assertNotRegex(C.render_prices(),
                            r"Generated by `make indicators-contract` on \d{4}-")

    def test_contract_carries_every_indicator_sql(self):
        generated = C.render_prices()
        for indicator in PI.INDICATORS:
            with self.subTest(indicator.key):
                self.assertIn(indicator.sql.strip(), generated)
                self.assertIn(indicator.title, generated)

    def test_contract_carries_every_caveat(self):
        generated = C.render_prices()
        for indicator in PI.INDICATORS:
            for caveat in indicator.caveats:
                with self.subTest(indicator.key):
                    self.assertIn(caveat, generated)


if __name__ == "__main__":
    unittest.main()
