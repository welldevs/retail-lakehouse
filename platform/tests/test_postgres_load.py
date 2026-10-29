"""The loader of the second warehouse target (local Postgres), checked without a real Postgres.

Same discipline as `test_snowflake_load.py`: everything here runs against FAKE
cursors/connections, with no network and no server up — the `warehouse-postgres` container
is opt-in (its own compose profile) and should not be a prerequisite of `make test`.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.warehouse_cut import SPECS  # noqa: E402
from retail_platform.postgres_load import (  # noqa: E402
    DEFAULT_LOAD_ROLE,
    GOLD,
    MART,
    ROLES,
    SCHEMAS,
    STAGE,
    PostgresLoadError,
    bootstrap,
    connect,
    ddl_for,
    qualified,
    require_schemas,
    verify,
)


class DDLTest(unittest.TestCase):
    def test_ddl_quotes_schema_and_table_in_uppercase(self):
        """UPPERCASE AND QUOTED, as in Snowflake: `sources.yml`/`dbt_project.yml` are
        shared by both targets and use literal STAGE/GOLD/MART — without quotes, Postgres
        would fold to lowercase and dbt would never find the table."""
        ddl = ddl_for({"name": "STG_CATEGORY"}, [("category_id", "INTEGER")], schema=STAGE)
        self.assertIn('create table "STAGE"."STG_CATEGORY"', ddl)

    def test_decimal_becomes_numeric_keeping_precision(self):
        ddl = ddl_for({"name": "X"}, [("value", "DECIMAL(18,6)")], schema=STAGE)
        self.assertIn("numeric(18,6)", ddl)

    def test_unknown_type_fails_instead_of_becoming_text(self):
        """Same criterion as Snowflake's `_TYPE_MAP`: a type with no entry must FAIL, not
        silently lose precision by becoming text."""
        with self.assertRaises(PostgresLoadError):
            ddl_for({"name": "X"}, [("c", "BLOB")], schema=STAGE)

    def test_timestamp_with_time_zone_becomes_timestamptz(self):
        ddl = ddl_for({"name": "X"}, [("t", "TIMESTAMP WITH TIME ZONE")], schema=STAGE)
        self.assertIn("timestamptz", ddl)


class QualificationTest(unittest.TestCase):
    def test_qualified_quotes_both_identifiers(self):
        self.assertEqual(qualified("STG_CUSTOMER", "STAGE"), '"STAGE"."STG_CUSTOMER"')

    def test_qualified_is_the_same_form_used_in_ddl_and_in_the_recheck(self):
        ddl = ddl_for({"name": "STG_CUSTOMER"}, [("customer_id", "INTEGER")], schema=STAGE)
        self.assertIn(qualified("STG_CUSTOMER", STAGE), ddl)


class SchemasTest(unittest.TestCase):
    def test_the_three_layers_are_the_same_literal_names_as_dbt(self):
        self.assertEqual(set(SCHEMAS), {STAGE, GOLD, MART})
        self.assertEqual({STAGE, GOLD, MART}, {"STAGE", "GOLD", "MART"})
        for name, comment in SCHEMAS.items():
            self.assertTrue(comment.strip(), f"{name} without a comment")
            # The comment goes inline into the DDL (see bootstrap) because `comment on
            # schema` does not accept a bound parameter — a quote would break the SQL.
            self.assertNotIn("'", comment, f"{name}: a quote would break the DDL")


class _FakeCursor:
    """Returns agreed counts, to exercise `verify` without a network."""

    def __init__(self, counts):
        self.counts = counts

    def execute(self, sql, params=None):
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
    """Loading without rechecking is trusting the transport — same discipline as Snowflake."""

    def counts(self, override=None):
        base = {spec["name"]: 10 for spec in SPECS}
        base.update(override or {})
        return base

    def test_silence_when_everything_matches(self):
        con = _FakeConnection(_FakeCursor(self.counts()))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        self.assertEqual(verify(con, "/tmp", expected), [])

    def test_flags_a_count_mismatch(self):
        con = _FakeConnection(_FakeCursor(self.counts({"STG_CUSTOMER": 9})))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        problems = verify(con, "/tmp", expected)
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_CUSTOMER", problems[0])
        self.assertIn("10", problems[0])
        self.assertIn("9", problems[0])

    def test_flags_a_table_the_export_did_not_declare(self):
        con = _FakeConnection(_FakeCursor(self.counts()))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS if spec["name"] != "STG_WAREHOUSE"}
        problems = verify(con, "/tmp", expected)
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_WAREHOUSE", problems[0])

    def test_flags_a_table_missing_at_the_destination_instead_of_blowing_up(self):
        con = _FakeConnection(_FakeCursor(
            self.counts({"STG_GEOGRAPHY": RuntimeError("relation does not exist")})
        ))
        expected = {spec["name"]: {"rows": 10} for spec in SPECS}
        problems = verify(con, "/tmp", expected)
        self.assertEqual(len(problems), 1)
        self.assertIn("STG_GEOGRAPHY", problems[0])


class _RecordingCursor:
    """Keeps every SQL statement (and parameters) executed, to check bootstrap without Postgres."""

    def __init__(self, responses=None):
        self.calls: list[tuple[str, tuple | None]] = []
        self.responses = responses or {}

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._rows = []
        for key, value in self.responses.items():
            if key in sql:
                self._rows = value
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass

    def emitted(self, *fragments):
        return [sql for sql, _ in self.calls if all(f in sql for f in fragments)]


class GovernanceTest(unittest.TestCase):
    """The three roles and the automatic grants on future tables — the Postgres equivalent
    of Snowflake's 'grant on future tables', without which the full refresh (drop+create)
    would silence the next role on every load."""

    def setUp(self):
        self.cursor = _RecordingCursor()
        bootstrap(self.cursor, grant_to_user="someone")

    def test_the_three_schemas_are_created_quoted_and_commented(self):
        for name in SCHEMAS:
            self.assertTrue(self.cursor.emitted(f'create schema if not exists "{name}"'))
            self.assertTrue(self.cursor.emitted(f'comment on schema "{name}"'))

    def test_the_three_roles_are_created(self):
        for name in ROLES:
            self.assertTrue(self.cursor.emitted(f"create role {name}"))

    def test_loader_only_has_usage_and_create_on_stage(self):
        self.assertTrue(self.cursor.emitted(f'grant usage, create on schema "{STAGE}"', "retail_loader"))

    def test_transformer_gets_automatic_select_on_future_stage_tables(self):
        """Without `alter default privileges`, the TRANSFORMER would not see the table the
        LOADER recreates on every load — a new `create table` inherits no grant at all."""
        self.assertTrue(self.cursor.emitted(
            f'alter default privileges for role retail_loader in schema "{STAGE}"',
            "grant select on tables to retail_transformer",
        ))

    def test_reader_gets_automatic_select_on_future_mart_tables(self):
        self.assertTrue(self.cursor.emitted(
            f'alter default privileges for role retail_transformer in schema "{MART}"',
            "grant select on tables to retail_reader",
        ))

    def test_reader_gets_no_future_privilege_on_gold(self):
        """Only MART. The same isolation Snowflake proves with `check_isolation`."""
        self.assertFalse(self.cursor.emitted(
            f'alter default privileges for role retail_transformer in schema "{GOLD}"',
            "retail_reader",
        ))

    def test_grant_to_user_grants_the_three_roles(self):
        for name in ROLES:
            self.assertTrue(self.cursor.emitted(f"grant {name} to someone"))

    def test_without_a_user_no_role_is_granted_to_anyone(self):
        cursor = _RecordingCursor()
        bootstrap(cursor)
        self.assertEqual(cursor.emitted("grant retail_loader to"), [])


class RequireSchemasTest(unittest.TestCase):
    def test_passes_when_the_schema_exists(self):
        cursor = _RecordingCursor({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, (STAGE,))

    def test_flags_and_suggests_bootstrap_when_missing(self):
        cursor = _RecordingCursor({"information_schema.schemata": [("GOLD",), ("MART",)]})
        with self.assertRaises(PostgresLoadError) as error:
            require_schemas(cursor, (STAGE,))
        self.assertIn(STAGE, str(error.exception))
        # The cited target has to exist: the message once told people to run
        # `warehouse-postgres-bootstrap`, which never existed in the Makefile.
        self.assertIn("`make warehouse-bootstrap`", str(error.exception))


class LoadRoleTest(unittest.TestCase):
    def test_the_load_does_not_run_as_administrator(self):
        """The load role is explicit, not the container's administrative user."""
        self.assertEqual(DEFAULT_LOAD_ROLE, "retail_loader")

    def test_unknown_role_is_refused_before_opening_a_connection(self):
        """`set role` does not accept a bound parameter — the name must be validated BEFORE
        reaching the SQL, and the validation runs before any network attempt."""
        with self.assertRaises(PostgresLoadError) as error:
            connect(role="evil_admin")
        self.assertIn("unknown role", str(error.exception))


if __name__ == "__main__":
    unittest.main()
