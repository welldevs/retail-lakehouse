"""Transport of the cut into the local Postgres: DDL, COPY FROM STDIN, native roles.

Second warehouse target, next to `snowflake_load.py` — same producer/transporter split
(testing the cut should not require a credential, and moving bytes should not require
DuckDB). It exists because the Snowflake account used in this project is a TRIAL, with an
expiry date: a local Postgres proves that the physical boundary at L2->L3 (COPY/`source()`,
never `ref()`) also changes ENGINE, not just account. See DECISIONS.md.

NO INTERNAL STAGE. Snowflake's table stage solves a problem that does not exist here — a
managed Postgres would not see the local MinIO, but THIS Postgres runs on the same host
where the parquet was already written. The transport is direct: read the parquet with
DuckDB and send `COPY ... FROM STDIN`, row by row, with no intermediate file and no PUT.

FULL REFRESH BY DROP + CREATE, not `create or replace table`: Postgres has no such syntax
for tables. Since whoever recreates is always the same role (`retail_loader` in STAGE,
`retail_transformer` in GOLD/MART via dbt), ownership never needs to be transferred —
unlike Snowflake, where `copy current grants` exists precisely for that.

CREDENTIAL: local dev, no real secret — same criterion as MinIO and the simulated OLTP
(fixed user/password, documented in .env.example). There is no key pair and no file outside
the repo: it is a Postgres that the compose itself brings up, under an opt-in profile.

ROLES WITHOUT THE SECONDARY-ROLES TRAP. Snowflake needed `use secondary roles none`
because every modern account is born with `DEFAULT_SECONDARY_ROLES = ('ALL')` and activates
all of the user's roles underneath the primary one. Postgres has no such concept: a granted
role is inherited automatically (INHERIT is the default), and `set role` switches the
session's ACTIVE role without any "hidden" role still applying behind it.
`connect(..., role=...)` here connects as the container's administrative user and does
`set role` — the same pattern as Snowflake's `connect(connection_name, role=...)`, without
the caveat.
"""

from __future__ import annotations

import os

from .warehouse_cut import SPECS, STAGE_SCHEMA, describe

DEFAULT_HOST = os.environ.get("WAREHOUSE_PG_HOST", "localhost")
DEFAULT_PORT = int(os.environ.get("WAREHOUSE_PG_PORT", "5434"))
DEFAULT_DBNAME = os.environ.get("WAREHOUSE_PG_DB", "retail")
# The container's ADMINISTRATIVE user (POSTGRES_USER) — who runs bootstrap and what `set
# role` uses as its base. The three business roles (retail_loader/transformer/reader) have
# no LOGIN of their own in this round: they are roles for GRANT and for `set role`, not
# separate accounts. Unlike Snowflake, which needs one service user per role for production
# isolation — here the boundary that matters is the same one dbt proves (STAGE/GOLD/MART),
# and duplicating login accounts just for a development Postgres does not pay for itself.
DEFAULT_USER = os.environ.get("WAREHOUSE_PG_USER", "retail")
DEFAULT_PASSWORD = os.environ.get("WAREHOUSE_PG_PASSWORD", "retail")
DEFAULT_LOAD_ROLE = os.environ.get("RETAIL_WAREHOUSE_PG_LOAD_ROLE", "retail_loader")

STAGE = STAGE_SCHEMA
GOLD = "GOLD"
MART = "MART"

# UPPERCASE AND QUOTED, as in Snowflake — not lowercase. `sources.yml` (schema: STAGE) and
# `dbt_project.yml` (+schema: GOLD / MART) are SHARED by both dbt targets and use these
# three literal names; dbt always quotes the identifier (`"GOLD"`), so a schema created here
# as lowercase `gold` would never be found ("schema GOLD does not exist", measured). Quoting
# in every query is the price of not duplicating the two shared trees.
SCHEMAS = {
    STAGE: "1:1 mirror of the Silver cut. No business rule. Full refresh.",
    GOLD: "Conformed dimensional model: dim_* and fact_*. Written by dbt-postgres.",
    MART: "One table per question, with the grain declared in each model header.",
}

# The same three verbs as Snowflake — loads STAGE / reads STAGE and writes GOLD+MART / only
# reads MART — modelled as native Postgres roles.
ROLES = {
    "retail_loader": "Loads the Silver cut into stage. Does not read gold or mart.",
    "retail_transformer": "Reads stage and writes gold and mart. It is the dbt role.",
    "retail_reader": "Read-only on mart. It is the role of the panel and of third parties.",
}


class PostgresLoadError(Exception):
    """The destination refused the DDL or the copy, or the connection does not exist."""


def connect(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    dbname: str = DEFAULT_DBNAME,
    user: str = DEFAULT_USER,
    password: str = DEFAULT_PASSWORD,
    role: str | None = None,
):
    """Connection to the local Postgres. Imports `psycopg` on demand, like `snowflake_load.connect`.

    `role` runs `set role` after connecting — it switches the session's ACTIVE role without
    requiring a separate login account per role. Validated against ROLES before reaching the
    SQL: `set role` does not accept a bound parameter, and the name should never come from
    untrusted input.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise PostgresLoadError(
            "psycopg is not installed in this venv. Run `make venv` to reinstall the "
            "platform dependencies."
        ) from exc

    if role is not None and role not in ROLES:
        raise PostgresLoadError(f"unknown role: {role!r}. Expected one of {sorted(ROLES)}.")

    try:
        connection = psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except Exception as exc:
        raise PostgresLoadError(
            f"could not connect to {host}:{port}/{dbname} as {user!r}: {exc}"
        ) from exc

    if role is not None:
        with connection.cursor() as cursor:
            cursor.execute(f"set role {role}")

    return connection


def _qi(name: str) -> str:
    """Quoted identifier. Every schema/table reference goes through here: without quotes,
    Postgres would fold STAGE/GOLD/MART to lowercase and not find what bootstrap created."""
    return f'"{name}"'


def qualified(table: str, schema: str = STAGE) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def require_schemas(cursor, schemas: tuple[str, ...] = (STAGE,)) -> None:
    """Checks that bootstrap has already run. Creates NOTHING — same reason as Snowflake:
    failing here is more readable than a privilege error coming from the middle of the DDL."""
    cursor.execute(
        "select schema_name from information_schema.schemata where schema_name = any(%s)",
        (list(schemas),),
    )
    existing = {row[0] for row in cursor.fetchall()}
    missing = [name for name in schemas if name not in existing]
    if missing:
        raise PostgresLoadError(
            f"missing schema: {', '.join(missing)}. Run "
            f"`make warehouse-bootstrap` once before loading."
        )


# Type mapping, deliberately short for the same reason as Snowflake's `_TYPE_MAP`: a type
# with no entry here must FAIL, not become `text` by guesswork.
_TYPE_MAP = {
    "BOOLEAN": "boolean",
    "TINYINT": "bigint",
    "SMALLINT": "bigint",
    "INTEGER": "bigint",
    "BIGINT": "bigint",
    "HUGEINT": "numeric(38,0)",
    "UBIGINT": "numeric(20,0)",
    "UINTEGER": "bigint",
    "FLOAT": "double precision",
    "DOUBLE": "double precision",
    "VARCHAR": "text",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
}


def _postgres_type(duckdb_type: str) -> str:
    kind = duckdb_type.upper().strip()
    if kind.startswith("DECIMAL"):
        # DECIMAL(10,2) -> numeric(10,2). Same reason as Snowflake: decimal precision does
        # not become floating point.
        return "numeric" + kind[len("DECIMAL"):]
    if kind in _TYPE_MAP:
        return _TYPE_MAP[kind]
    raise PostgresLoadError(
        f"DuckDB type with no Postgres mapping: {duckdb_type!r}. "
        f"Add it to _TYPE_MAP instead of letting the cut fall into a text."
    )


def ddl_for(spec: dict, columns: list[tuple[str, str]], schema: str = STAGE) -> str:
    """`create table` derived from the cut's own schema — same discipline as Snowflake:
    the DDL is never hand-written alongside."""
    body = ",\n".join(f"    {name} {_postgres_type(kind)}" for name, kind in columns)
    return f"create table {qualified(spec['name'], schema)} (\n{body}\n)"


def bootstrap(cursor, grant_to_user: str | None = None) -> list[str]:
    """Schemas, roles and grants (incl. `default privileges` for future tables).
    Idempotent, and separate from `load` on purpose — same reason as Snowflake: creating
    infrastructure is one-off work, and the daily load should not repeat it.
    """
    done = []

    def run(sql, description):
        cursor.execute(sql)
        done.append(description)

    for name, comment in SCHEMAS.items():
        run(f"create schema if not exists {_qi(name)}", f"schema {name}")
        # `comment on schema` is utility DDL: Postgres's extended protocol does not accept
        # `$1` in that position ("syntax error at or near $1"), unlike a plain
        # `select`/`insert`. Inlined directly, as Snowflake already does in `ddl_for` —
        # safe because SCHEMAS is a module constant, never external input.
        run(f"comment on schema {_qi(name)} is '{comment}'", f"comment {name}")

    for name in ROLES:
        cursor.execute("select 1 from pg_roles where rolname = %s", (name,))
        if cursor.fetchone() is None:
            run(f"create role {name}", f"role {name}")
        else:
            done.append(f"role {name} already existed")

    # LOADER: creates and reads STAGE, and that is all.
    run(f"grant usage, create on schema {_qi(STAGE)} to retail_loader", "loader: usage+create stage")

    # TRANSFORMER: READS stage (never writes to it) and writes GOLD and MART.
    run(f"grant usage on schema {_qi(STAGE)} to retail_transformer", "transformer: usage stage")
    # ALTER DEFAULT PRIVILEGES is the Postgres equivalent of Snowflake's "grant on future
    # tables": every table retail_loader creates in STAGE from then on is born visible to
    # the TRANSFORMER, even after the next day's full refresh (drop + create).
    run(
        f"alter default privileges for role retail_loader in schema {_qi(STAGE)} "
        f"grant select on tables to retail_transformer",
        "transformer: automatic select on future stage tables",
    )
    for layer in (GOLD, MART):
        run(f"grant usage, create on schema {_qi(layer)} to retail_transformer",
            f"transformer: usage+create {layer}")

    # READER: only MART, read-only — including what does not exist yet, by the same mechanism.
    run(f"grant usage on schema {_qi(MART)} to retail_reader", "reader: usage mart")
    run(
        f"alter default privileges for role retail_transformer in schema {_qi(MART)} "
        f"grant select on tables to retail_reader",
        "reader: automatic select on future mart tables",
    )

    if grant_to_user:
        for name in ROLES:
            run(f"grant {name} to {grant_to_user}", f"{name} -> {grant_to_user}")

    return done


def load(connection, duck_connection, stage_dir: str, schema: str = STAGE) -> dict:
    """Recreates the STAGE tables (drop + create) and loads them via COPY FROM STDIN.

    `duck_connection` is used to DESCRIBE the cut (the table schema) and to READ the parquet
    row by row — no Postgres credential goes through it. Same return contract as
    `snowflake_load.load`: one dict per spec with `rows` and `bytes`.
    """
    cursor = connection.cursor()
    require_schemas(cursor, (schema,))

    summary = {}
    for spec in SPECS:
        spec_name = spec["name"]
        target = qualified(spec_name, schema)
        path = os.path.join(stage_dir, f"{spec_name}.parquet")
        if not os.path.exists(path):
            raise PostgresLoadError(
                f"missing parquet: {path}. Run `make warehouse-export` before loading."
            )

        columns = describe(duck_connection, spec["sql"].strip())
        column_names = [name for name, _ in columns]

        # DROP + CREATE, not `create or replace`: Postgres has no such syntax for tables.
        # CASCADE drops dependencies (views/FKs) — none are expected here, STAGE is an
        # isolated mirror by design.
        cursor.execute(f"drop table if exists {target} cascade")
        cursor.execute(ddl_for(spec, columns, schema))

        columns_sql = ", ".join(column_names)
        rows = duck_connection.execute(
            f"select {columns_sql} from read_parquet('{path}')"
        ).fetchall()
        with cursor.copy(f"copy {target} ({columns_sql}) from stdin") as copy:
            for row in rows:
                copy.write_row(row)

        cursor.execute(f"select count(*) from {target}")
        loaded = cursor.fetchone()[0]
        summary[spec_name] = {"rows": loaded, "bytes": os.path.getsize(path)}

    connection.commit()
    cursor.close()
    return summary


def verify(connection, stage_dir: str, expected: dict, schema: str = STAGE) -> list[str]:
    """Rechecks each table's count against what the export declared. Same discipline as
    `snowflake_load.verify` — returns the list of mismatches, empty when everything matches."""
    cursor = connection.cursor()
    problems = []
    for spec in SPECS:
        spec_name = spec["name"]
        try:
            cursor.execute(f"select count(*) from {qualified(spec_name, schema)}")
            destination = cursor.fetchone()[0]
        except Exception as exc:
            problems.append(f"{spec_name}: could not count at the destination ({exc})")
            continue
        source = expected.get(spec_name, {}).get("rows")
        if source is None:
            problems.append(f"{spec_name}: the export did not declare a count")
        elif source != destination:
            problems.append(f"{spec_name}: source {source} != destination {destination}")
    cursor.close()
    return problems
