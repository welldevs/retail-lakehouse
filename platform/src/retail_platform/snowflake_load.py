"""Transport of the cut to Snowflake: DDL, PUT into an internal stage, COPY INTO.

Separate from `warehouse_cut.py` by the same criterion that separates `land.py` from
`customers_reference.py`: one module PRODUCES the artifact, the other MOVES it. Testing the
cut should not require a credential, and moving bytes should not require DuckDB.

INTERNAL STAGE, NOT EXTERNAL. A managed Snowflake cannot see a MinIO on localhost:
`COPY INTO` from an external stage would require real S3 plus a storage integration with
IAM. The internal table stage (`@%TABLE`) reverses the direction — the one pushing the
bytes is this process, which sees both sides — and needs no infrastructure at all. For
~1 MB of parquet it is the obviously proportional choice. Trigger to switch to an external
stage: RAW starting to live in a real S3, or the volume growing to the point where the
upload from the local machine becomes the bottleneck.

CREDENTIAL: no line of this module reads, stores or carries a secret. Everything comes from
`~/.snowflake/config.toml` by connection name, which the connector itself resolves —
including `private_key_file`. KEY-PAIR authentication, not password: it is the method
Snowflake recommends for programmatic access, and the only one that works for a DAG, which
has no way to open a browser or type a password.
"""

from __future__ import annotations

import os

from .warehouse_cut import SPECS, STAGE_SCHEMA, describe, ddl_for

DEFAULT_CONNECTION = os.environ.get("RETAIL_SNOWFLAKE_CONNECTION", "spark_retail")
DEFAULT_DATABASE = os.environ.get("RETAIL_SNOWFLAKE_DATABASE", "RETAIL")

# THE LOADER'S ROLE, not the connection's. The ~/.snowflake/config.toml connection points
# to the administrative role because it is the one that runs the bootstrap; the daily load
# overrides it to RETAIL_LOADER. Without this, we would create three roles and keep loading
# as ACCOUNTADMIN — which was the state until now: the control existed, was verified, and
# was not used.
DEFAULT_LOAD_ROLE = os.environ.get("RETAIL_SNOWFLAKE_LOAD_ROLE", "RETAIL_LOADER")

# The warehouse (compute) is an object SEPARATE from the database, with its own grant.
# While everything ran as ACCOUNTADMIN this went unnoticed, because an administrator sees
# every warehouse; the first `load` wearing RETAIL_LOADER failed with "No active warehouse
# selected" — the connection asked for COMPUTE_WH, the role had no USAGE, and Snowflake just
# leaves the session without compute instead of refusing the connection.
DEFAULT_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# Schemas that dbt will write. Created here because `dbt run` neither creates a schema with
# a comment nor guarantees order; it is infrastructure DDL, not model DDL.
SCHEMAS = {
    STAGE_SCHEMA: "1:1 mirror of the Silver cut. No business rule. Full refresh.",
    "GOLD": "Conformed dimensional model: DIM_* and FACT_*. Written by dbt-snowflake.",
    "MART": "One table per question, with the grain declared in each model header.",
}


class SnowflakeLoadError(Exception):
    """The destination refused the DDL, the upload or the copy."""


def connect(connection_name: str = DEFAULT_CONNECTION, role: str | None = None):
    """Connection by the name defined in ~/.snowflake/config.toml. Imported on demand.

    `role` overrides the connection's role — used by check_isolation to open one session
    per role without needing one configuration file per role.
    """
    try:
        import snowflake.connector
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise SnowflakeLoadError(
            "snowflake-connector-python is not installed in this venv. "
            "Run `make venv` to reinstall the platform dependencies."
        ) from exc

    extra = {"role": role} if role else {}
    try:
        return snowflake.connector.connect(connection_name=connection_name, **extra)
    except Exception as exc:
        raise SnowflakeLoadError(
            f"could not connect using connection '{connection_name}' from "
            f"~/.snowflake/config.toml: {exc}"
        ) from exc


def require_warehouse(cursor) -> None:
    """Checks that the session has compute. Runs BEFORE any data query.

    `current_warehouse()` is a session function and needs no warehouse to answer — which is
    why it works as a probe. When the role has no USAGE on the warehouse the connection asks
    for, Snowflake does not refuse the connection: it hands over a session without compute,
    and the error only shows up on the first data query, pointing at the session instead of
    the grant.
    """
    current = cursor.execute("select current_warehouse()").fetchone()[0]
    if not current:
        raise SnowflakeLoadError(
            "the session has no active warehouse. This connection's role probably lacks "
            "`usage` on the warehouse — run `make warehouse-bootstrap`, which grants "
            f"usage on warehouse {DEFAULT_WAREHOUSE} to the three roles."
        )


def require_schemas(cursor, database: str, schemas=(STAGE_SCHEMA,)) -> None:
    """Checks that the bootstrap has already run. Creates NOTHING — that is the difference.

    Checks ONLY the schemas the caller needs, and the default is just STAGE. Checking all
    three would fail the load for a correct reason: `information_schema.schemata` returns
    only what the role can see, and RETAIL_LOADER sees neither GOLD nor MART — which is
    precisely the isolation asked for. A check broader than the need turns a working
    control into an execution failure.

    Before the roles were worn, `load` ran `create database if not exists` and
    `create schema if not exists`. It worked because everything ran as ACCOUNTADMIN, and
    that was precisely the symptom: the loader could create a database. RETAIL_LOADER cannot,
    and should not — creating infrastructure is a one-off job, with administrative
    privilege, and is already what `bootstrap` does.

    Checking instead of creating also improves the error: "run make warehouse-bootstrap"
    says what to do, and "Insufficient privileges to operate on database" does not.
    """
    try:
        rows = cursor.execute(
            f"select schema_name from {database}.information_schema.schemata"
        ).fetchall()
    except Exception as exc:
        raise SnowflakeLoadError(
            f"database {database} does not exist or this role cannot see it ({exc}). "
            f"Run `make warehouse-bootstrap` once per account before loading."
        ) from exc

    existing = {row[0].upper() for row in rows}
    missing = [name for name in schemas if name.upper() not in existing]
    if missing:
        raise SnowflakeLoadError(
            f"schema missing in {database}: {', '.join(missing)}. "
            f"Run `make warehouse-bootstrap` once per account before loading."
        )


# What each role CAN and CANNOT read. Written as a matrix so it is executable: an
# unverified grant is an intention, not a control.
ISOLATION_MATRIX = {
    "RETAIL_READER": {
        "allow": ["{db}.MART.MART_CUSTOMER_BASE"],
        "deny": ["{db}.GOLD.DIM_CUSTOMER", "{db}.STAGE.STG_CUSTOMER"],
    },
    "RETAIL_TRANSFORMER": {
        "allow": ["{db}.STAGE.STG_CUSTOMER", "{db}.GOLD.DIM_CUSTOMER"],
        "deny": [],
    },
    "RETAIL_LOADER": {
        "allow": ["{db}.STAGE.STG_CUSTOMER"],
        "deny": ["{db}.MART.MART_CUSTOMER_BASE", "{db}.GOLD.DIM_CUSTOMER"],
    },
}


def check_isolation(connect_as_role, database: str = DEFAULT_DATABASE) -> list[str]:
    """Proves the matrix above by opening one session per role. Returns the violations.

    `USE SECONDARY ROLES NONE` IS NOT A DETAIL — it is what makes this test valid.
    Modern Snowflake accounts come with DEFAULT_SECONDARY_ROLES = ('ALL'), and in that mode
    the session activates ALL of the user's roles besides the primary one. Measured on this
    account: with secondary roles active, RETAIL_READER read GOLD and STAGE without trouble,
    because the same user also has ACCOUNTADMIN — and `show grants to role RETAIL_READER`
    kept showing only MART, that is, the configuration looked right and the behavior was
    not. An RBAC check made from an admin session without turning this off passes by
    mistake, always.

    In production, real isolation comes from one SERVICE USER per role, without
    ACCOUNTADMIN. Here there is a single user, so turning off secondary roles is the way to
    prove the grants are correct.
    """
    problems = []
    for role, expected in ISOLATION_MATRIX.items():
        try:
            connection = connect_as_role(role)
        except Exception as exc:
            problems.append(f"{role}: could not open a session ({exc})")
            continue
        cursor = connection.cursor()
        try:
            cursor.execute("use secondary roles none")
            for target in expected["allow"]:
                table = target.format(db=database)
                try:
                    cursor.execute(f"select count(*) from {table}").fetchone()
                except Exception:
                    problems.append(f"{role}: SHOULD read {table} and was denied")
            for target in expected["deny"]:
                table = target.format(db=database)
                try:
                    cursor.execute(f"select count(*) from {table}").fetchone()
                    problems.append(f"{role}: should NOT read {table} and did")
                except Exception:
                    pass
        finally:
            cursor.close()
            connection.close()
    return problems


def qualified(table: str, database: str = DEFAULT_DATABASE) -> str:
    return f"{database}.{STAGE_SCHEMA}.{table}"


def transfer_statements(table: str, parquet_path: str, database: str = DEFAULT_DATABASE) -> list[str]:
    """The two commands that move a parquet into a STAGE table.

    A function of its own, and not two f-strings inside the `load` loop, so the suite can
    check them WITHOUT Snowflake. Each of the two decisions below cost one real run, and
    neither would fail in an obvious way if reverted.
    """
    target = qualified(table, database)

    # TABLE STAGE ALWAYS QUALIFIED. `@%TABLE` resolves against the session's CURRENT schema,
    # and Snowflake's `create schema` CHANGES the current schema — measured: ensure_schemas
    # ends on MART, and the PUT started looking for RETAIL.MART.%STG_PRODUCT_PRICE, which
    # does not exist. Qualifying makes the command independent of session state, which is
    # what you want in a loader.
    stage = f"@{database}.{STAGE_SCHEMA}.%{table}"

    return [
        # OVERWRITE = TRUE: the table stage keeps the file between runs, and without this a
        # following COPY would read the old file along with the new one and double the rows.
        # AUTO_COMPRESS = FALSE: the parquet already comes out of DuckDB zstd-compressed;
        # letting PUT wrap it all in gzip would only make Snowflake unwrap twice.
        f"put file://{os.path.abspath(parquet_path)} {stage} "
        f"overwrite = true auto_compress = false",

        # MATCH_BY_COLUMN_NAME removes the need for the column order to match between the
        # parquet and the table. Without it, adding a column in the middle of the cut would
        # silently load data into the wrong column — the one error in this layer no
        # downstream test would catch, because the result is still a plausible number.
        f"copy into {target} from {stage} "
        # USE_LOGICAL_TYPE = TRUE. Without this, EVERY timestamp crosses the boundary 56
        # MILLION YEARS in the future, and nothing fails.
        #
        # DuckDB records the timestamp unit only in parquet's modern LogicalType
        # (Timestamp(timeUnit=microseconds)) and leaves the legacy ConvertedType as NONE.
        # Snowflake's reader ignores LogicalType by default, falls back to the legacy one,
        # finds no unit and assumes MILLISECONDS: 1.787e15 microseconds become 1.787e15
        # milliseconds, that is, the year 56,648,666.
        #
        # WHY THIS ONLY SHOWED UP NOW: it was the first time a TIMESTAMP crossed the
        # boundary. Up to Milestone 6 the whole cut only had DATE (ingestion_date,
        # snapshot_date, valid_from), and DATE travels as date32 with no unit ambiguity.
        #
        # AND WHY NOTHING CAUGHT IT: the loader's recheck compares row COUNTS, and the count
        # was right. dbt built the 166 nodes green — the durations became large numbers, not
        # nulls or errors, and a funnel made of `count_if(x is not null)` stays exact when
        # `x` is shifted 56 million years. What flagged it was reading the SLA mart and
        # seeing 80,000,060 picking minutes. That is why
        # assert_order_milestones_are_plausible_against_the_order_date now exists, failing
        # if a milestone falls outside the order's own window.
        f"file_format = (type = parquet use_logical_type = true) "
        f"match_by_column_name = case_insensitive "
        f"on_error = abort_statement",
    ]


# Three roles, one per pipeline VERB — not one per person. Whoever loads does not model,
# whoever models does not read RAW, and whoever consumes writes nowhere. That is what makes
# "governed layer" a verifiable claim instead of a slogan: without the grants, Snowflake
# here would be an expensive DuckDB.
ROLES = {
    "RETAIL_LOADER": "Loads the Silver cut into STAGE. Does not read GOLD or MART.",
    "RETAIL_TRANSFORMER": "Reads STAGE and writes GOLD and MART. This is dbt's role.",
    "RETAIL_READER": "Read-only on MART. This is the BI and third-party role.",
}


def _literal(text: str) -> str:
    """SQL string with the single quote doubled. The RETAIL_TRANSFORMER comment says
    "dbt's role": interpolated raw, the apostrophe closed the literal and the bootstrap died
    on syntax right after creating RETAIL_LOADER — leaving a role with no grant at all and a
    load that failed with "must have CREATE TABLE", an error that points at the grant and
    not at the cause."""
    return "'" + text.replace("'", "''") + "'"


def bootstrap(
    cursor,
    database: str,
    grant_to_user: str | None = None,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> list[str]:
    """Database, schemas, roles and grants. Idempotent, and separate from `load` on purpose.

    Creating a role requires ACCOUNTADMIN and is one-off infrastructure; mixing it into the
    daily load would mean asking for ACCOUNTADMIN every day to move parquet. The loader runs
    as RETAIL_LOADER after this.
    """
    done = []

    def run(sql, description):
        cursor.execute(sql)
        done.append(description)

    # The whole cut has ~146 thousand rows and `dbt build` finishes in ~12 s. X-Small with
    # fast suspension is the proportional choice; the 300 s default would bill five minutes
    # of idle compute on every run, which on a trial account is the difference between
    # demonstrating the flow several times and not being able to.
    run(
        f"alter warehouse {warehouse} set warehouse_size = 'XSMALL' "
        f"auto_suspend = 60 auto_resume = true",
        f"warehouse {warehouse}: xsmall, auto_suspend 60s",
    )

    run(f"create database if not exists {database}", f"database {database}")
    for name, comment in SCHEMAS.items():
        run(
            f"create schema if not exists {database}.{name} comment = {_literal(comment)}",
            f"schema {name}",
        )
    for name, comment in ROLES.items():
        run(f"create role if not exists {name} comment = {_literal(comment)}", f"role {name}")
        # Custom role hung under SYSADMIN — Snowflake's own convention, and here it stopped
        # being cosmetic. As soon as table OWNERSHIP moved to the project roles and secondary
        # roles were turned off, ACCOUNTADMIN stopped seeing the objects in
        # information_schema: it did not inherit those roles. Without this line, wearing the
        # role would cost the account's administrative visibility — and isolation does not
        # change, because inheritance goes up (SYSADMIN now sees MART), never down
        # (RETAIL_READER still has no GOLD).
        run(f"grant role {name} to role SYSADMIN", f"{name} -> SYSADMIN")

    # USAGE on the database is a prerequisite for anything; without it the role cannot even
    # see the schemas it is allowed to read. USAGE on the WAREHOUSE is the other
    # prerequisite, and it is easy to forget because ACCOUNTADMIN never needs it: the three
    # roles read and write DATA, and data does not move without compute. Without this line,
    # `load` dies with "No active warehouse selected" — an error that points at the session,
    # not at the missing grant.
    for name in ROLES:
        run(f"grant usage on database {database} to role {name}", f"usage db -> {name}")
        run(f"grant usage on warehouse {warehouse} to role {name}", f"usage wh -> {name}")

    # LOADER: writes STAGE, and nothing else.
    run(f"grant usage on schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: usage stage")
    run(f"grant create table on schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: create table")
    run(f"grant all on all tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: tables")
    run(f"grant all on future tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: future tables")

    # OWNERSHIP, not just privilege. `ddl_for` emits `create or replace table`, and replacing
    # a table requires OWNERSHIP of the existing one — `grant all` grants every APPLICABLE
    # privilege and ownership is not one of them. On a new account this is harmless (the
    # LOADER creates the tables and is born the owner); on an account where the load ran as
    # ACCOUNTADMIN before, it is what lets it stop doing so. `copy current grants` keeps the
    # TRANSFORMER's select, which would otherwise be revoked along with ownership.
    run(
        f"grant ownership on all tables in schema {database}.{STAGE_SCHEMA} "
        f"to role RETAIL_LOADER copy current grants",
        "loader: ownership of stage tables",
    )

    # TRANSFORMER: READS stage (never writes to it) and writes GOLD and MART.
    run(f"grant usage on schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: usage stage")
    run(f"grant select on all tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: select stage")
    run(f"grant select on future tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: select future stage")
    # `grant all on SCHEMA` gives privilege on the schema (usage, create table), NOT on the
    # tables that already exist inside it — those belong to whoever created them. Without
    # the two table grants below, the TRANSFORMER can create new tables in GOLD and cannot
    # READ the ones already there. This went unnoticed until check_isolation failed.
    for layer in ("GOLD", "MART"):
        run(f"grant all on schema {database}.{layer} to role RETAIL_TRANSFORMER",
            f"transformer: schema {layer}")
        run(f"grant all on all tables in schema {database}.{layer} to role RETAIL_TRANSFORMER",
            f"transformer: tables {layer}")
        run(f"grant all on future tables in schema {database}.{layer} to role RETAIL_TRANSFORMER",
            f"transformer: future tables {layer}")
        # OWNERSHIP. Same reason as STAGE, one level up: dbt materializes with
        # `create or replace table`, and replacing requires OWNERSHIP of the existing table.
        # Measured: with `grant all` and no ownership, `dbt build` failed 8 models with
        # "must have OWNERSHIP granted on TABLE". Harmless on a new account, where the
        # TRANSFORMER itself creates and is born the owner. `copy current grants` keeps the
        # READER's select on MART — without it, wearing the role would silence the consumer.
        for object_kind in ("tables", "views"):
            run(
                f"grant ownership on all {object_kind} in schema {database}.{layer} "
                f"to role RETAIL_TRANSFORMER copy current grants",
                f"transformer: ownership of {object_kind} in {layer}",
            )

    # READER: only MART, read-only. Sees neither STAGE nor GOLD — whoever consumes does not
    # need to know how the number was made, and should not be able to read the intermediate.
    run(f"grant usage on schema {database}.MART to role RETAIL_READER", "reader: usage mart")
    run(f"grant select on all tables in schema {database}.MART to role RETAIL_READER", "reader: select mart")
    run(f"grant select on future tables in schema {database}.MART to role RETAIL_READER", "reader: select future mart")

    if grant_to_user:
        for name in ROLES:
            run(f"grant role {name} to user {grant_to_user}", f"{name} -> {grant_to_user}")

        # WITHOUT THIS, WEARING THE ROLE MEANS NOTHING. Modern Snowflake accounts come with
        # DEFAULT_SECONDARY_ROLES = ('ALL'): the session activates ALL of the user's roles
        # besides the primary one. Measured on this account before the change:
        #   current_role() = ACCOUNTADMIN
        #   current_secondary_roles() = ORGADMIN, RETAIL_READER, RETAIL_TRANSFORMER, RETAIL_LOADER
        # That is: connecting as RETAIL_LOADER would keep carrying ACCOUNTADMIN underneath,
        # and every permission test would pass by mistake. Turning it off on the USER makes
        # the primary role hold for every connection — dbt's included, which has nowhere to
        # run a `use secondary roles none`.
        run(
            f"alter user {grant_to_user} set default_secondary_roles = ()",
            f"secondary roles turned off for {grant_to_user}",
        )

    return done


def load(connection, duck_connection, stage_dir: str, database: str = DEFAULT_DATABASE) -> dict:
    """Creates the STAGE tables and loads the parquet files from stage_dir. Returns the summary.

    `duck_connection` is used only to DESCRIBE the cut — the table schema comes from the
    query itself, never from a hand-written DDL next to it (see warehouse_cut). No data row
    goes through DuckDB here: the bytes are already in the parquet.
    """
    cursor = connection.cursor()
    require_warehouse(cursor)
    require_schemas(cursor, database)

    summary = {}
    for spec in SPECS:
        name = spec["name"]
        path = os.path.join(stage_dir, f"{name}.parquet")
        if not os.path.exists(path):
            raise SnowflakeLoadError(
                f"missing parquet: {path}. Run `make warehouse-export` before loading."
            )

        columns = describe(duck_connection, spec["sql"].strip())
        cursor.execute(ddl_for(spec, columns, database))
        for statement in transfer_statements(name, path, database):
            cursor.execute(statement)

        target = qualified(name, database)
        loaded = cursor.execute(f"select count(*) from {target}").fetchone()[0]
        summary[name] = {"rows": loaded, "bytes": os.path.getsize(path)}

    cursor.close()
    return summary


def verify(connection, stage_dir: str, expected: dict, database: str = DEFAULT_DATABASE) -> list[str]:
    """Rechecks each table's count against what the export declared.

    Same discipline as `verify-landing`: loading without rechecking is trusting the
    transport. Returns the list of divergences — empty when everything matches.
    """
    cursor = connection.cursor()
    problems = []
    for spec in SPECS:
        name = spec["name"]
        target = qualified(name, database)
        try:
            destination = cursor.execute(f"select count(*) from {target}").fetchone()[0]
        except Exception as exc:
            problems.append(f"{name}: could not count at the destination ({exc})")
            continue
        source = expected.get(name, {}).get("rows")
        if source is None:
            problems.append(f"{name}: the export declared no count")
        elif source != destination:
            problems.append(f"{name}: source {source} != destination {destination}")
    cursor.close()
    return problems
