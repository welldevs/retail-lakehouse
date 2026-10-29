"""The TRANSPORT, checked without Snowflake and without a credential.

These tests exist because of two defects that only showed up running against the real
account, and that the cut's suite had no way to catch because they do not generate invalid
SQL — they generate VALID SQL pointing at the wrong place:

  1. `@%TABLE` resolves against the session's CURRENT schema, and Snowflake's `create
     schema` CHANGES the current schema. Since ensure_schemas creates STAGE, GOLD and MART
     in that order, the session ended up in MART and the PUT looked for
     RETAIL.MART.%STG_PRODUCT_PRICE.
  2. Without OVERWRITE on the PUT, the table stage keeps the file between runs and the next
     COPY would read the old one along with the new one — counts would double on the second
     load, not the first, which is the worst possible moment to find out.

Neither fails in an obvious way if it regresses, and that is why they became tests.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.warehouse_cut import SPECS  # noqa: E402
from retail_platform.snowflake_load import (  # noqa: E402
    DEFAULT_LOAD_ROLE,
    ROLES,
    SCHEMAS,
    SnowflakeLoadError,
    bootstrap,
    qualified,
    require_schemas,
    require_warehouse,
    transfer_statements,
    verify,
)


class QualificationTest(unittest.TestCase):
    def setUp(self):
        self.put, self.copy = transfer_statements(
            "STG_PRODUCT_PRICE", "/tmp/STG_PRODUCT_PRICE.parquet", "RETAIL"
        )

    def test_the_table_stage_is_qualified_with_database_and_schema(self):
        """Without this, the PUT follows the session's current schema — which `create
        schema` has just changed to MART."""
        self.assertIn("@RETAIL.STAGE.%STG_PRODUCT_PRICE", self.put)
        self.assertIn("@RETAIL.STAGE.%STG_PRODUCT_PRICE", self.copy)

    def test_never_uses_the_relative_form(self):
        for statement in (self.put, self.copy):
            self.assertNotIn("@%STG", statement, "the form relative to the current schema came back")

    def test_the_target_table_is_also_qualified(self):
        self.assertIn("copy into RETAIL.STAGE.STG_PRODUCT_PRICE", self.copy)

    def test_honours_a_different_database(self):
        put, copy = transfer_statements("STG_CUSTOMER", "/tmp/x.parquet", "OTHER_DB")
        self.assertIn("@OTHER_DB.STAGE.%STG_CUSTOMER", put)
        self.assertIn("copy into OTHER_DB.STAGE.STG_CUSTOMER", copy)

    def test_qualified_is_the_same_form_used_in_the_recheck(self):
        self.assertEqual(qualified("STG_CUSTOMER", "RETAIL"), "RETAIL.STAGE.STG_CUSTOMER")


class TransportTest(unittest.TestCase):
    def test_the_put_overwrites_the_previous_file_in_the_stage(self):
        """Without overwrite, the SECOND load would double the counts — not the first."""
        put, _ = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("overwrite = true", put)

    def test_the_put_does_not_recompress_the_parquet(self):
        put, _ = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("auto_compress = false", put)

    def test_the_copy_tells_snowflake_to_read_the_parquet_logical_type(self):
        """Without this, EVERY timestamp arrives 56 million years in the future — and nothing fails.

        DuckDB annotates the unit only in the modern LogicalType and leaves the legacy
        ConvertedType as NONE; Snowflake's reader, by default, ignores the former, falls back
        to the latter and assumes MILLISECONDS. 1.787e15 microseconds become the year
        56,648,666.

        The load does not fail (the row count is right), dbt builds everything green (the
        durations become big numbers, not errors) and a `count_if(x is not null)` funnel stays
        exact. This test is cheap precisely because the defect is not.
        """
        _, copy = transfer_statements("STG_ORDER", "/tmp/a.parquet", "RETAIL")
        self.assertIn("use_logical_type = true", copy)

    def test_the_copy_matches_by_column_NAME_not_by_position(self):
        """Adding a column in the middle of the cut would silently load data into the wrong
        column if matching were positional — and the result would still look plausible."""
        _, copy = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("match_by_column_name = case_insensitive", copy)

    def test_the_copy_aborts_on_the_first_error(self):
        _, copy = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("on_error = abort_statement", copy)

    def test_the_file_path_is_absolute(self):
        """A PUT with a relative path depends on the process cwd — in a DAG that is another
        directory."""
        put, _ = transfer_statements("STG_CATEGORY", "relative/a.parquet", "RETAIL")
        self.assertIn(f"file://{os.path.abspath('relative/a.parquet')}", put)

    def test_there_is_a_statement_for_every_spec_of_the_cut(self):
        for spec in SPECS:
            put, copy = transfer_statements(spec["name"], f"/tmp/{spec['name']}.parquet", "RETAIL")
            self.assertIn(spec["name"], put)
            self.assertIn(spec["name"], copy)


class SchemasTest(unittest.TestCase):
    def test_the_three_layers_are_created_with_a_comment(self):
        self.assertEqual(set(SCHEMAS), {"STAGE", "GOLD", "MART"})
        for name, comment in SCHEMAS.items():
            self.assertTrue(comment.strip(), f"{name} without a comment")
            self.assertNotIn("'", comment, f"{name}: a quote would break the DDL")


class _FakeCursor:
    """Returns agreed counts, to exercise `verify` without a network."""

    def __init__(self, counts):
        self.counts = counts
        self.last = None

    def execute(self, sql):
        self.last = sql
        for name, value in self.counts.items():
            if name in sql:
                if isinstance(value, Exception):
                    raise value
                self._value = value
                return self
        raise AssertionError(f"unexpected query: {sql}")

    def fetchone(self):
        return (self._value,)

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class RecheckTest(unittest.TestCase):
    """Loading without rechecking is trusting the transport — the same discipline as
    verify-landing, one level up."""

    def counts(self, override=None):
        base = {spec["name"]: 10 for spec in SPECS}
        base.update(override or {})
        return base

    def test_silence_when_everything_matches(self):
        con = _FakeConnection(_FakeCursor(self.counts()))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        self.assertEqual(verify(con, "/tmp", expected, "RETAIL"), [])

    def test_flags_a_count_mismatch(self):
        con = _FakeConnection(_FakeCursor(self.counts({"STG_CUSTOMER": 9})))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        problems = verify(con, "/tmp", expected, "RETAIL")
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_CUSTOMER", problems[0])
        self.assertIn("10", problems[0])
        self.assertIn("9", problems[0])

    def test_flags_a_table_the_export_did_not_declare(self):
        """A new spec that nobody exported cannot pass as if it were right."""
        con = _FakeConnection(_FakeCursor(self.counts()))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS if spec["name"] != "STG_WAREHOUSE"}
        problems = verify(con, "/tmp", expected, "RETAIL")
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_WAREHOUSE", problems[0])

    def test_flags_a_table_missing_at_the_destination_instead_of_blowing_up(self):
        con = _FakeConnection(_FakeCursor(
            self.counts({"STG_GEOGRAPHY": RuntimeError("does not exist")})
        ))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        problems = verify(con, "/tmp", expected, "RETAIL")
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_GEOGRAPHY", problems[0])


class _RecordingCursor:
    """Keeps every SQL statement executed, to check bootstrap without touching Snowflake."""

    def __init__(self, responses=None):
        self.sqls = []
        self.responses = responses or {}

    def execute(self, sql):
        self.sqls.append(sql)
        self._rows = []
        for key, value in self.responses.items():
            if key in sql:
                self._rows = value
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else (None,)

    def close(self):
        pass

    def emitted(self, *fragments):
        return [s for s in self.sqls if all(f in s for f in fragments)]


class GovernanceTest(unittest.TestCase):
    """The four defects that only showed up when WEARING the roles.

    While the pipeline ran as ACCOUNTADMIN none of them existed — the administrator has
    everything. None fails in an obvious way if it regresses: the first three give a
    privilege error far from the cause, and the fourth gives no error at all, it just hides
    the objects from whoever administers the account.
    """

    def setUp(self):
        self.cursor = _RecordingCursor()
        bootstrap(self.cursor, "RETAIL", grant_to_user="SOMEONE", warehouse="COMPUTE_WH")

    def test_the_three_roles_get_usage_on_the_warehouse(self):
        """Data does not move without compute. Without this grant, the load dies with 'No
        active warehouse selected' — an error that points at the session, not at the
        missing grant."""
        for role in ROLES:
            self.assertTrue(
                self.cursor.emitted("grant usage on warehouse COMPUTE_WH", role),
                f"{role} was left without usage on the warehouse",
            )

    def test_table_ownership_goes_to_the_role_that_rewrites_them(self):
        """`grant all` grants the APPLICABLE privileges, and ownership is not one of them.
        Without ownership, `create or replace table` fails — measured: 8 models at once."""
        self.assertTrue(self.cursor.emitted(
            "grant ownership on all tables in schema RETAIL.STAGE", "RETAIL_LOADER"))
        for layer in ("GOLD", "MART"):
            self.assertTrue(self.cursor.emitted(
                f"grant ownership on all tables in schema RETAIL.{layer}",
                "RETAIL_TRANSFORMER"), f"{layer} without ownership transfer")

    def test_the_ownership_transfer_keeps_existing_grants(self):
        """Without `copy current grants`, moving MART's ownership would revoke the READER's
        select — wearing the role would silence the consumer, with no error along the way."""
        for statement in self.cursor.emitted("grant ownership"):
            self.assertIn("copy current grants", statement)

    def test_the_roles_hang_under_sysadmin(self):
        """After ownership left ACCOUNTADMIN and secondary roles were turned off, the
        administrator stopped seeing the objects: it did not inherit those roles.
        Inheritance goes up, never down — isolation does not change."""
        for role in ROLES:
            self.assertTrue(
                self.cursor.emitted(f"grant role {role} to role SYSADMIN"),
                f"{role} outside the administrative hierarchy",
            )

    def test_the_users_secondary_roles_are_turned_off(self):
        """DEFAULT_SECONDARY_ROLES = ('ALL') is the default on modern accounts, and with it
        the session activates ALL of the user's roles besides the primary one. Without
        turning it off, wearing the role is decorative — and dbt has nowhere to run
        `use secondary roles none`."""
        self.assertTrue(self.cursor.emitted(
            "alter user SOMEONE set default_secondary_roles = ()"))

    def test_without_a_user_no_user_is_touched(self):
        cursor = _RecordingCursor()
        bootstrap(cursor, "RETAIL")
        self.assertEqual(cursor.emitted("alter user"), [])
        self.assertEqual(cursor.emitted("grant role RETAIL_LOADER to user"), [])

    def test_a_comment_with_an_apostrophe_does_not_break_the_ddl(self):
        """"dbt's role" interpolated raw closed the literal: bootstrap died after the first
        role and the load failed for lack of a grant."""
        statements = self.cursor.emitted("create role if not exists RETAIL_TRANSFORMER")
        self.assertTrue(statements)
        for statement in statements:
            self.assertIn("dbt''s role", statement)

    def test_bootstrap_is_the_only_one_that_creates_infrastructure(self):
        self.assertTrue(self.cursor.emitted("create database if not exists RETAIL"))
        for name in SCHEMAS:
            self.assertTrue(self.cursor.emitted(f"create schema if not exists RETAIL.{name}"))


class LoadRoleTest(unittest.TestCase):
    def test_the_load_does_not_run_as_administrator(self):
        """The load role overrides the connection's — the connection points at the
        administrative one because it is the one that runs bootstrap."""
        self.assertEqual(DEFAULT_LOAD_ROLE, "RETAIL_LOADER")

    def test_a_session_without_compute_is_flagged_before_touching_data(self):
        cursor = _RecordingCursor({"current_warehouse": [(None,)]})
        with self.assertRaises(SnowflakeLoadError) as error:
            require_warehouse(cursor)
        self.assertIn("warehouse", str(error.exception))

    def test_a_session_with_compute_passes(self):
        cursor = _RecordingCursor({"current_warehouse": [("COMPUTE_WH",)]})
        require_warehouse(cursor)

    def test_the_load_checks_only_stage(self):
        """Checking all three schemas would fail the load for a RIGHT reason:
        information_schema returns only what the role can see, and RETAIL_LOADER does not
        see GOLD or MART. A check broader than the need turns a working control into a run
        failure."""
        cursor = _RecordingCursor({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, "RETAIL")

    def test_missing_stage_says_to_run_bootstrap(self):
        cursor = _RecordingCursor({"information_schema.schemata": [("GOLD",), ("MART",)]})
        with self.assertRaises(SnowflakeLoadError) as error:
            require_schemas(cursor, "RETAIL")
        self.assertIn("STAGE", str(error.exception))
        self.assertIn("warehouse-bootstrap", str(error.exception))

    def test_the_load_never_creates_a_database_or_schema(self):
        """Being able to create a database was the symptom of running as administrator."""
        cursor = _RecordingCursor({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, "RETAIL")
        self.assertEqual(cursor.emitted("create"), [])


if __name__ == "__main__":
    unittest.main()
