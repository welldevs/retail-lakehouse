"""The panel's indicators, checked without a warehouse and without network.

IT LIVES IN platform/tests/ ON PURPOSE, not in streamlit/. The panel shares the platform
venv (the UI dependencies are an `optional-dependencies` of the same pyproject), so
`make test` already reaches it — and what matters is that these checks ALWAYS run, along with
the rest, not in a separate target someone has to remember to call.

WHAT THIS SUITE PROTECTS, and what it deliberately does not attempt.

It does not try to check NUMBERS: that needs a live warehouse, and the suite is offline by
repository invariant. The smoke test that actually runs the app is `make dashboard-check`,
which requires a live account.

What it protects is the property the whole panel rests on: **docs/indicators/operations.md
is derived from operations.py, so it cannot diverge from it**. If someone tweaks a SQL and
doesn't regenerate the document, the review keeps "passing" — because nobody reads a SQL and
a text side by side looking for disagreement. This test turns regeneration into a verified
obligation, and is the direct analogue of the STAGE DDL being derived from the cut instead of
hand-written.

And it protects the parameter boundary: every query with a date axis must BIND the filter
(`%(start)s`), never build it by concatenation. The interface selector is user input.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "platform", "src"))

from retail_platform.indicators import contract as C  # noqa: E402
from retail_platform.indicators import operations as I  # noqa: E402

PAGE = os.path.join(ROOT, "docs", "indicators", "operations.md")

MART_DIR = os.path.join(ROOT, "platform", "dbt", "models", "warehouse", "mart")


class StructureTest(unittest.TestCase):
    def test_every_indicator_declares_the_review_fields(self):
        """An indicator without a declared grain is not reviewable: the grain is what says
        whether a sum is legitimate. Without a kind, nobody knows whether the number can be
        read as a measurement of the world."""
        for indicator in I.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.title.strip())
                self.assertTrue(indicator.question.strip().endswith("?"),
                                "the question has to be a question")
                self.assertTrue(indicator.grain.strip())
                self.assertTrue(indicator.kind.strip())
                self.assertTrue(indicator.marts)
                self.assertTrue(indicator.sql.strip())

    def test_keys_are_unique(self):
        keys = [i.key for i in I.INDICATORS]
        self.assertEqual(len(keys), len(set(keys)))

    def test_every_indicator_declares_at_least_one_caveat(self):
        """Not bureaucracy. Each of these indicators aggregates, and aggregation is where the
        obvious measure produces a plausible and wrong number — a percentile that doesn't
        sum, a count that isn't additive, a swapped denominator. An indicator with no caveat
        at all was probably not thought through as someone who will rebuild it in another
        tool."""
        for indicator in I.INDICATORS:
            with self.subTest(indicator.key):
                self.assertTrue(indicator.caveats,
                                f"{indicator.key} declares no caveat at all")
                # The tuple existing is not enough. An empty entry — or a three-word
                # placeholder — passed `assertTrue` and warns nobody of anything. Found
                # while proving this very test can fail: the injection replaced the caveat
                # with an empty string and ONLY the contract sync test caught it.
                for caveat in indicator.caveats:
                    self.assertGreater(
                        len(caveat.strip()), 60,
                        f"{indicator.key}: caveat too short to explain a failure "
                        f"mode — {caveat!r}",
                    )

    def test_cited_marts_exist_as_models_on_disk(self):
        """A wrong name here only fails at run time, against the live account — and the
        contract would already have been published with the wrong source."""
        on_disk = {
            os.path.splitext(filename)[0].upper()
            for filename in os.listdir(MART_DIR) if filename.endswith(".sql")
        }
        self.assertTrue(on_disk)
        for indicator in I.INDICATORS:
            for mart in indicator.marts:
                self.assertIn(mart, on_disk, f"{indicator.key} cites {mart}, which does not exist")

    def test_every_mart_on_disk_is_used_by_some_indicator(self):
        """The inverse, and it is what catches the NEW mart: whoever creates a mart and
        doesn't expose it leaves a table with no consumer, which is how a mart dies silently."""
        used = {m for i in I.INDICATORS for m in i.marts}
        for filename in os.listdir(MART_DIR):
            if not filename.endswith(".sql"):
                continue
            name = os.path.splitext(filename)[0].upper()
            self.assertIn(name, used, f"{name} does not appear in any indicator")


class BoundParameterTest(unittest.TestCase):
    def test_date_filter_is_bound_and_never_concatenated(self):
        for indicator in I.INDICATORS:
            if not indicator.dated:
                continue
            with self.subTest(indicator.key):
                self.assertIn("%(start)s", indicator.sql)
                self.assertIn("%(end)s", indicator.sql)

    def test_warehouse_filter_is_bound_and_never_concatenated(self):
        """The interface multiselect is user input. `split(%(warehouses)s, ',')` binds the
        value; building an `in (...)` by concatenation does not."""
        for indicator in I.INDICATORS:
            if not indicator.dated:
                continue
            with self.subTest(indicator.key):
                self.assertIn("%(warehouses)s", indicator.sql)

    def test_no_query_uses_format_or_value_f_string(self):
        """The only interpolations allowed are the module constants (DB, filters). A
        leftover `{`/`}` in a query means someone tried to build a value into the string."""
        for indicator in I.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn("{", indicator.sql)
                self.assertNotIn("}", indicator.sql)

    def test_panel_reads_only_the_mart_schema(self):
        """The `retail_reader` role boundary, asserted in code too: a query citing GOLD or
        STAGE would be refused by the engine, but would fail at run time and against the
        live account. Here it fails earlier.

        Quoted `"GOLD"`/`"STAGE"` (not `.GOLD.`/`.STAGE.`): Postgres has no three-part
        database prefix like Snowflake, only the schema — and that, quoted in uppercase, is
        what appears in the real SQL (see I.MART in model.py)."""
        for indicator in I.INDICATORS:
            with self.subTest(indicator.key):
                self.assertNotIn('"GOLD"', indicator.sql)
                self.assertNotIn('"STAGE"', indicator.sql)
                self.assertIn(f"{I.MART}.", indicator.sql)

    def test_no_query_writes(self):
        forbidden = ("insert ", "update ", "delete ", "merge ", "create ", "drop ",
                     "alter ", "truncate ", "grant ", "copy into")
        for indicator in I.INDICATORS:
            lowered = indicator.sql.lower()
            for verb in forbidden:
                with self.subTest(f"{indicator.key}:{verb.strip()}"):
                    self.assertNotIn(verb, lowered)


class ContractInSyncTest(unittest.TestCase):
    """docs/indicators/operations.md is derived from operations.py. This test is what
    prevents divergence.

    Without it, tweaking a SQL and forgetting to run `make indicators-contract` leaves the
    review document describing a query that no longer exists — and the review keeps
    "passing", because nobody compares a SQL with a text looking for disagreement. Same
    reason the STAGE DDL is derived from the cut.
    """

    def test_contract_on_disk_is_BYTE_FOR_BYTE_what_the_generator_produces(self):
        """EXACT comparison, with no exempted line.

        Until the 2026-09-01 revision the header carried the generation date, and this test
        had to drop that line before comparing. An exemption inside the sync test itself is
        a blind spot: anything that landed on that line stopped being checked. The header
        now carries the sha256 of the catalog module — a function of the source, not of the
        clock — so the comparison can be total, and `make indicators-contract` over an
        up-to-date contract changes no byte.
        """
        path = PAGE
        self.assertTrue(os.path.exists(path),
                        "operations.md does not exist — run `make indicators-contract`")
        with open(path, encoding="utf-8") as handle:
            on_disk = handle.read()

        self.assertEqual(
            on_disk, C.render_operations(),
            "operations.md is out of sync with operations.py. "
            "Run `make indicators-contract`.",
        )

    def test_header_carries_the_source_sha256_and_no_date(self):
        """If the stamp goes back to being a clock, the generated file stays forever dirty
        in git and the test above has to exempt a line again. Guarded here so the regression
        shows up as a failure, not as noise in the diff."""
        header = next(l for l in C.render_operations().splitlines()[:12] if "sha256" in l)
        self.assertIn(C.source_sha256(I), header)
        self.assertNotRegex(C.render_operations(),
                            r"Generated by `make indicators-contract` on \d{4}-")

    def test_contract_carries_every_indicator_sql(self):
        generated = C.render_operations()
        for indicator in I.INDICATORS:
            with self.subTest(indicator.key):
                self.assertIn(indicator.sql.strip(), generated)
                self.assertIn(indicator.title, generated)

    def test_contract_carries_every_caveat(self):
        """The caveat is the reason the document exists. One left out of the contract would
        be written for nobody."""
        generated = C.render_operations()
        for indicator in I.INDICATORS:
            for caveat in indicator.caveats:
                with self.subTest(indicator.key):
                    self.assertIn(caveat, generated)

    def test_contract_declares_what_the_panel_does_not_show(self):
        generated = C.render_operations()
        self.assertIn("What the catalog does NOT answer", generated)
        for title, reason, trigger in I.OUT_OF_SCOPE:
            with self.subTest(title):
                self.assertIn(title, generated)
                self.assertIn(trigger, generated)

    def test_generator_imports_nothing_that_connects(self):
        """The contract has to be reviewable without credentials and without network — same
        property as `make warehouse-ddl`.

        The check is by AST and not by text search: the word "snowflake" legitimately
        appears in the BODY of the generated document, which describes authentication. What
        cannot exist is the IMPORT — the difference between talking about the connection
        and opening one.
        """
        with open(C.__file__, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".")[0])
        # Relative imports (`from . import operations`) are the catalog itself.
        allowed = {"os", "sys", "hashlib", "__future__"}
        self.assertEqual(
            imported - allowed, set(),
            "the contract generator may only import stdlib and the indicators package",
        )


class CountInProseTest(unittest.TestCase):
    """The documentation says how many indicators the panel has. That number is copied by
    hand, and it has already been wrong three times: it said 16 when there were 18, 19
    queries when there were 21 — and, in Phase 7, the README said 16 when there were 22,
    because this test only watched ARCHITECTURE.

    THE THIRD TIME IS THE LESSON: guarding ONE file doesn't guard the sentence, it guards the
    file. The same count lived in two documents and only one had a test — so the other rotted
    exactly as if no test existed. Now both go through the same list, and adding a third
    document with the sentence is one line here.

    Automating every count written in prose is not worth it — for most of them the remedy
    was to stop writing the number. This one stays because the sentence loses its meaning
    without it, and because adding an indicator is exactly the moment nobody remembers the
    documentation.
    """

    DOCUMENTS = ("ARCHITECTURE.md", "README.md")

    def _text(self, name: str) -> str:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, name), encoding="utf-8") as handle:
            return handle.read()

    def test_indicator_count_matches_in_every_document_that_cites_it(self):
        groups = len({indicator.group for indicator in I.INDICATORS})
        sentence = f"{len(I.INDICATORS)}\nindicators in {groups} groups"
        sentence_one_line = f"{len(I.INDICATORS)} indicators in {groups} groups"
        for name in self.DOCUMENTS:
            with self.subTest(name):
                text = self._text(name)
                self.assertTrue(
                    sentence in text or sentence_one_line in text,
                    f"{name} describes a different number of indicators/groups",
                )

    def test_query_count_in_architecture_matches(self):
        total = len(I.INDICATORS) + 3  # FRESHNESS, WINDOW, WAREHOUSES
        self.assertIn(f"the {total} queries", self._text("ARCHITECTURE.md"))


class OutOfScopeTest(unittest.TestCase):
    def test_every_item_declares_reason_AND_trigger(self):
        """Absence without a trigger is an excuse; with a trigger it is a decision. Same
        discipline as the table of technologies not adopted in ARCHITECTURE.md."""
        self.assertTrue(I.OUT_OF_SCOPE)
        for title, reason, trigger in I.OUT_OF_SCOPE:
            with self.subTest(title):
                self.assertTrue(title.strip())
                self.assertGreater(len(reason.strip()), 40)
                self.assertGreater(len(trigger.strip()), 20)

    def test_customer_by_order_gap_is_declared(self):
        """The most actionable item on the list and the one most people will ask for:
        repurchase, LTV, cohort. No mart joins customer with order, and that needs to be
        written where whoever builds the Power BI reads it BEFORE promising the indicator."""
        text = " ".join(m for _, m, _ in I.OUT_OF_SCOPE)
        self.assertIn("NO MART JOINS CUSTOMER WITH ORDER", text)


if __name__ == "__main__":
    unittest.main()
