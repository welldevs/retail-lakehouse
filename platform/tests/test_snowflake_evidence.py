"""The EVIDENCE, checked without Snowflake and without a credential.

The report exists because the account is a trial and the Snowflake half of the project is
not reproducible offline. That makes it the only artifact in the repository that cannot be
regenerated later — and so the failure mode that matters is not "it broke", it is "it wrote
something plausible and wrong". The tests below aim at that: that no number is written by
hand, that broken isolation shows up instead of vanishing, and that a sample that failed
does not disguise itself as an empty sample.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.snowflake_evidence import SAMPLES, render  # noqa: E402
from retail_platform.snowflake_load import ROLES  # noqa: E402


def _data(objects=None, isolation=None, samples=None, roles_run=None):
    return {
        "identidade": {
            "conta": "ACC123",
            "usuario": "SOMEONE",
            "papel": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH",
            "capturado_em": "2026-08-27 13:00:00 +00:00",
            "database": "RETAIL",
        },
        "objetos": objects if objects is not None else [
            ("STAGE", "STG_CUSTOMER", "RETAIL_LOADER", 20000),
            ("GOLD", "DIM_CUSTOMER", "RETAIL_TRANSFORMER", 20000),
            ("MART", "MART_CUSTOMER_BASE", "RETAIL_TRANSFORMER", 20000),
        ],
        "amostras": samples if samples is not None else {
            "MART_CUSTOMER_BASE": (["CUSTOMER_ID"], [("cust_bcn1_000000",)]),
        },
        "papeis_em_execucao": roles_run if roles_run is not None else (
            ["ROLE_NAME", "QUERY_TYPE", "QUERIES", "ULTIMA"],
            [
                ("RETAIL_LOADER", "COPY", 135, "2026-09-01 07:01"),
                ("RETAIL_TRANSFORMER", "CREATE_TABLE_AS_SELECT", 346, "2026-09-01 07:02"),
                ("RETAIL_READER", "SELECT", 732, "2026-09-01 06:43"),
            ],
        ),
        "isolamento": isolation or [],
    }


class RolesInExecutionTest(unittest.TestCase):
    """The isolation matrix proves what each role CAN read; this section proves what each
    one DID. It was the distinction that cost four defects in Phase 2 — the three roles
    existed, verified, and no run went through them.

    Replaces the list of screenshots that lived in docs/warehouse-evidence/PRINTS.md: five
    of the six items on that list were already covered by this page, and the sixth was this.
    """

    def test_each_roles_verb_is_published(self):
        text = render(_data())
        self.assertIn("CREATE_TABLE_AS_SELECT", text)
        self.assertIn("`RETAIL_READER`", text)
        self.assertIn("732", text)

    def test_empty_history_declares_the_absence_instead_of_an_empty_table(self):
        """A table with no rows reads as "the roles did nothing", which is
        indistinguishable from "there is no role separation". They are different things:
        Snowflake's query_history keeps no more than seven days, and an idle week erases
        the evidence without erasing the property."""
        text = render(_data(roles_run=(["ROLE_NAME"], [])))
        self.assertIn("No `RETAIL%` role executed", text)
        self.assertIn("seven days", text)

    def test_error_reading_the_history_shows_up_as_an_error(self):
        """Same rule as the samples: a failure that disguises itself as empty is the only
        thing worse than a failure."""
        text = render(_data(roles_run=(["error"], [("Insufficient privileges",)])))
        self.assertIn("Could not read the history", text)
        self.assertIn("Insufficient privileges", text)
        self.assertNotIn("No `RETAIL%` role executed", text)


class ReportTest(unittest.TestCase):
    def test_the_capture_is_dated_but_exposes_neither_account_nor_user(self):
        """Evidence without a date proves nothing: it exists to outlive the destination it
        describes. But the repository is public — account and user stay out (CR-009)."""
        text = render(_data())
        for expected in ("COMPUTE_WH", "2026-08-27", "<snowflake-account>"):
            self.assertIn(expected, text)
        for leaked in ("ACC123", "SOMEONE"):
            self.assertNotIn(leaked, text)

    def test_each_objects_ownership_is_published(self):
        """Ownership IS the proof that the role was worn — without it the report cannot
        tell this state apart from everything running as administrator."""
        text = render(_data())
        self.assertIn("RETAIL_LOADER", text)
        self.assertIn("RETAIL_TRANSFORMER", text)

    def test_the_total_is_summed_not_written(self):
        text = render(_data(objects=[
            ("STAGE", "A", "RETAIL_LOADER", 7),
            ("GOLD", "B", "RETAIL_TRANSFORMER", 35),
        ]))
        self.assertIn("42 rows", text)

    def test_table_without_a_count_neither_blows_up_nor_becomes_a_silent_zero(self):
        text = render(_data(objects=[("GOLD", "V", "RETAIL_TRANSFORMER", None)]))
        self.assertIn("`V`", text)

    def test_broken_isolation_is_highlighted(self):
        """The worst possible outcome would be a pretty report about a leaky control."""
        text = render(_data(isolation=["RETAIL_READER: should NOT read GOLD and did"]))
        self.assertIn("does NOT check out", text)
        self.assertIn("RETAIL_READER", text)

    def test_intact_isolation_is_declared_as_such(self):
        text = render(_data())
        self.assertIn("checks out completely", text)
        self.assertNotIn("does NOT check out", text)

    def test_the_published_matrix_covers_the_three_roles(self):
        text = render(_data())
        for role in ROLES:
            self.assertIn(role, text)

    def test_only_fact_ingestion_run_becomes_a_table(self):
        text = render(_data(samples={
            "MART_CUSTOMER_BASE": (["CUSTOMER_ID"], [("cust_bcn1_000000",)]),
            "FACT_INGESTION_RUN": (["SOURCE_NAME"], [("mercadona_catalog_api",)]),
        }))
        self.assertIn("### `FACT_INGESTION_RUN`", text)
        self.assertNotIn("cust_bcn1_000000", text)

    def test_a_failed_sample_does_not_disguise_itself_as_an_empty_sample(self):
        """A sample query that broke must show up as an error. Silenced, the reader would
        conclude that the mart is empty."""
        text = render(_data(samples={
            "MART_X": (["error"], [("SQL compilation error: invalid identifier",),]),
        }))
        self.assertIn("invalid identifier", text)


# The marts WILL GROW, and a list hard-coded here ages silently: adding a mart without adding
# the sample would leave a table with no record at all once the account expires, and nothing
# would fail. Deriving from disk makes the test fail in the commit that created the mart,
# which is when the cost of remembering is zero. That is how the three Orders marts were caught.
MART_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dbt", "models", "warehouse", "mart",
)


class SamplesTest(unittest.TestCase):
    def test_there_is_a_sample_for_each_mart(self):
        on_disk = {
            os.path.splitext(name)[0].upper()
            for name in os.listdir(MART_DIR) if name.endswith(".sql")
        }
        self.assertTrue(on_disk, f"no mart found in {MART_DIR}")
        # FACT_INGESTION_RUN is the ONLY deliberate exception: it is a GOLD fact, not a
        # MART, that came in via CR-005 to make latency/throughput queryable without a new
        # collector (BACKLOG.md, Observability). Named here so the exception is an
        # assertion, not a silent gap that a future SAMPLES entry would hide.
        outside_mart = {"FACT_INGESTION_RUN"}
        self.assertTrue(outside_mart <= set(SAMPLES), "expected exception disappeared")
        self.assertEqual(set(SAMPLES) - outside_mart, on_disk)

    def test_every_sample_is_limited_and_parameterized_by_the_database(self):
        """Without limit, the 'sample' would become an extract; without {db}, the report
        would only work for the database called RETAIL — and the destination is swappable
        on purpose."""
        for name, sql in SAMPLES.items():
            self.assertIn("limit", sql, f"{name} without limit")
            self.assertIn("{db}", sql, f"{name} with a hard-coded database")


if __name__ == "__main__":
    unittest.main()
