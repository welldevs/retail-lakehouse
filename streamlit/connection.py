"""Panel connection to the local Postgres — wearing retail_reader, and only that.

WHY THE ROLE MATTERS HERE, and is not a configuration detail. The repository created three
roles, one per pipeline VERB: `retail_loader` writes STAGE, `retail_transformer` reads
STAGE and writes GOLD/MART, `retail_reader` reads MART and nothing else. This panel is the
THIRD verb, and is still the only consumer that actually wears the read role.

ENGINE: local Postgres, not Snowflake. The trial account used by this project expired (see
DECISIONS.md, CR-007) — this file swaps only the TRANSPORT. MART/GOLD/STAGE, the three
roles and the isolation guarantee stay the same, because the models/warehouse/ tree is
engine-neutral (see ARCHITECTURE.md).

NO `use secondary roles none`. That call existed to disarm Snowflake's
`DEFAULT_SECONDARY_ROLES = ('ALL')`, which re-enables all of the user's roles underneath
the primary one. Postgres has no such concept: `set role` switches the session's ACTIVE role
with no "hidden" role still applying behind it — see the header of
`retail_platform/postgres_load.py`.

CREDENTIAL: local dev, no real secret — same criterion as MinIO and the simulated OLTP. There
is no config file outside the repo: the four `WAREHOUSE_PG_*` have defaults built into
`postgres_load.py` itself, and the root `.env` only matters to whoever overrides the default.
"""

from __future__ import annotations

import os
import sys

# The panel runs from inside streamlit/, and the platform lives in platform/src. Without this,
# `import retail_platform` fails with a message that does not say why.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PLATFORM_SRC = os.path.join(_ROOT, "platform", "src")
if _PLATFORM_SRC not in sys.path:
    sys.path.insert(0, _PLATFORM_SRC)

HOST = os.environ.get("WAREHOUSE_PG_HOST", "localhost")
PORT = int(os.environ.get("WAREHOUSE_PG_PORT", "5434"))
DATABASE = os.environ.get("WAREHOUSE_PG_DB", "retail")
SCHEMA = '"MART"'
ROLE = os.environ.get("RETAIL_DASHBOARD_ROLE", "retail_reader")


class DashboardError(Exception):
    """The target refused, or the connection does not exist."""


def _dotenv() -> None:
    """Loads the root `.env` if it exists. Does NOT override what is already in the environment."""
    path = os.path.join(_ROOT, ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def open_session():
    """A new session, with the read role.

    Two lines after `connect()`, and both exist for the SAME reason: `set role`
    (inside `connect`) is transactional in Postgres — a later `rollback()` would silently undo
    it, handing the session back to the administrative user underneath the read role.
    `commit()` seals the switch before anything can undo it; `autocommit = True` guarantees
    that NO subsequent query (including one refused by `probe_isolation`) opens a transaction
    to undo. The panel only READS — there is no write atomicity to protect here.
    """
    _dotenv()
    from retail_platform.postgres_load import PostgresLoadError, connect

    try:
        conn = connect(host=HOST, port=PORT, dbname=DATABASE, role=ROLE)
    except PostgresLoadError as exc:
        raise DashboardError(
            f"could not connect as {ROLE}: {exc}\n\n"
            f"Check that the container is running (`make warehouse-postgres-up`) and that "
            f"`make warehouse-bootstrap` has run once."
        ) from exc
    conn.commit()
    conn.autocommit = True
    return conn


def identity(conn) -> dict:
    """Who is reading, from where. Goes in the panel footer — evidence, not decoration.

    The returned keys (`conta`, `usuario`, `papel`, `lido_em`, ...) are read by both panels;
    they are part of the interface and stay as they are.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "select current_database(), session_user, current_user, "
            "inet_server_addr(), inet_server_port(), "
            "to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')"
        )
        database, login, role, host, port, now = cursor.fetchone()
    finally:
        cursor.close()
    return {
        # inet_server_addr() returns NULL on a Unix-socket connection; here it is always
        # TCP (explicit host/port in connect()), but the fallback avoids a "None:None".
        "conta": f"{host or HOST}:{port or PORT}", "usuario": login, "papel": role,
        "warehouse": None, "regiao": None, "lido_em": now,
        "database": database, "schema": SCHEMA,
    }


def run(conn, sql: str, params: dict | None = None):
    """Executes and returns a pandas DataFrame, with UPPERCASE column names.

    The UPPER() is what lets the catalog and the panels not change a single line.
    Snowflake uppercases every unquoted identifier by default — which is why the panel reads
    `r["PEDIDOS_COLOCADOS"]` from SQL that writes `as pedidos_colocados`. Postgres does the
    OPPOSITE (folds to lowercase), so without this upper() every query would return the
    same columns under another name, and the whole panel would break on dictionary access —
    not on wrong SQL. One place fixes the panel's ~150 aliases at once.
    """
    import pandas as pd

    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or {})
        columns = [d[0].upper() for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()


def probe_isolation(conn) -> list[tuple[str, bool, str]]:
    """Checks, live, that this role does NOT reach GOLD or STAGE.

    It exists so that the claim "the panel reads only the MART" can be verified on screen,
    instead of being a sentence in the README. A panel that claims to respect a boundary and
    does not demonstrate it is asking for trust — which is exactly what this repository refuses.

    STAGE is quoted in uppercase (`"STG_ORDER"`) because the tables are created by
    `postgres_load.py`, which keeps the cut's name as Snowflake knows it; GOLD and
    MART are created by dbt from the model FILE name, which is already lowercase.
    """
    targets = [
        ('"MART".mart_order_funnel', True),
        ('"GOLD".fact_order', False),
        ('"STAGE"."STG_ORDER"', False),
    ]
    result = []
    for target, should_read in targets:
        cursor = conn.cursor()
        try:
            cursor.execute(f"select count(*) from {target}")
            cursor.fetchone()
            result.append((target, should_read is True, "read"))
        except Exception as exc:
            first_line = str(exc).splitlines()[0]
            result.append((target, should_read is False, f"refused ({first_line[:40]})"))
        finally:
            cursor.close()
    return result
