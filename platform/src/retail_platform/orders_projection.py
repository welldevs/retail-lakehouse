"""The live projection in Iceberg: two writers, one reader, and the monotonic merge.

WHY ICEBERG, AND WHY ONLY NOW.

The written trigger was *"a second engine needs to **write** the same table"*. It had not
fired until now, and it is on record that it had not. It fires at this milestone because
`live_order_state` now has two writers by design:

    orders-project              streaming consumer, event by event
    orders-rebuild-projection   batch rebuild, from RAW

and one concurrent reader (DuckDB, via `silver_live_order_state`). **The trigger fired on
CONCURRENCY, not on volume** — at this phase's volume a parquet rewritten with an atomic
`os.replace` would work. What Iceberg buys here is snapshot isolation between two writers
and one reader, plus time travel on the projection. Writing this down is the difference
between a decision and a fad.

WHAT THE CLOSED EXPERIMENT (removed in CR-012; see git history) MEASURED BEFORE THIS MODULE EXISTED:

  - the SQL catalog on top of Postgres works, and so does `upsert(join_cols=["order_id"])`;
  - two concurrent writers do NOT silently overwrite each other: the one starting from a
    stale snapshot gets `CommitFailedException`. That is optimistic concurrency working — if
    it went through silently, it would be a lost update;
  - DuckDB reads through `metadata_location`, and ONLY through it: it refuses to guess the
    current metadata by scanning storage, because that could read uncommitted metadata.

THE THIRD FINDING CHANGED THE DESIGN. The dbt model cannot point at a fixed path: the one
that knows which metadata is current is the CATALOG, and that is who gets asked. `make silver`
resolves the `metadata_location` and passes it as a var. The shortcut exists (`SET
unsafe_enable_version_guessing = true`, measured and working) and was rejected: reading
uncommitted metadata is exactly what a concurrent read must not do.

RETRY IS NOT ENOUGH — THE MERGE MUST BE MONOTONIC. Reconciling a conflict by reloading and
trying again resolves the COMMIT conflict, and still loses data: if the other writer
stored order X at `sequence_no` 7 and our attempt carries 5, a blind retry writes 5 on top of
it. The commit succeeds, the table regresses. That is why every attempt re-reads the state of
the affected keys and drops its own rows that do not advance — the same
`last_sequence_no` guard that protects the OLTP and the consumer, now protecting concurrent writes.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from .orders_stream import DEFAULT_SEEDS_DIR, fold_event, read_sla_minutes

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "retail")
CATALOG_DB = os.environ.get("ICEBERG_CATALOG_DB", "iceberg_catalog")
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "projection")
TABLE_NAME = f"{NAMESPACE}.live_order_state"
WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE", "s3://retail-lakehouse/iceberg"
)
MAX_COMMIT_ATTEMPTS = 20

WRITER_STREAM = "stream"
WRITER_REBUILD = "rebuild"


class OrdersProjectionError(Exception):
    """The catalog, the table or the reconciliation refused."""


# --------------------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------------------
#
# `written_by` is PROVENANCE, not a second copy of the state: it records which of the two
# writers touched the row last. Without it, "both writers really wrote" would be a claim
# about logs, not a fact you can query in the table itself.

COLUMNS = (
    "order_id", "wh", "order_date", "customer_id", "status", "last_sequence_no",
    "events_applied", "placed_at", "updated_at", "confirmed_at", "picking_started_at",
    "picked_at", "dispatched_at", "terminal_at", "line_count", "picked_line_count",
    "substituted_lines", "removed_lines", "gross_amount", "net_amount", "returned_amount",
    "delivered_within_slot", "picking_minutes", "sla_breached", "written_by",
)

_TIMESTAMPS = ("placed_at", "updated_at", "confirmed_at", "picking_started_at",
               "picked_at", "dispatched_at", "terminal_at")
_MONEY = ("gross_amount", "net_amount", "returned_amount")
_INTS = ("last_sequence_no", "events_applied", "line_count", "picked_line_count",
         "substituted_lines", "removed_lines")


def arrow_schema():
    import pyarrow as pa

    fields = {
        "order_id": pa.string(), "wh": pa.string(), "order_date": pa.date32(),
        "customer_id": pa.string(), "status": pa.string(), "written_by": pa.string(),
        "delivered_within_slot": pa.bool_(), "sla_breached": pa.bool_(),
        "picking_minutes": pa.decimal128(10, 2),
    }
    for name in _TIMESTAMPS:
        fields[name] = pa.timestamp("us", tz="UTC")
    for name in _MONEY:
        fields[name] = pa.decimal128(12, 2)
    for name in _INTS:
        fields[name] = pa.int32()
    # `order_id` is the upsert key and cannot be null; the rest can, and nullness carries
    # meaning (see `net_amount`, which is undetermined until picking).
    return pa.schema([
        pa.field(name, fields[name], nullable=(name != "order_id")) for name in COLUMNS
    ])


def _normalize(state: dict, writer: str) -> dict:
    """Fold state -> Iceberg types. Decimal for money, never float."""
    row = {}
    for name in COLUMNS:
        value = state.get(name)
        if name == "written_by":
            value = writer
        elif name in _MONEY and value is not None:
            value = Decimal(str(value))
        elif name == "picking_minutes" and value is not None:
            value = Decimal(str(value))
        elif name in _TIMESTAMPS and isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif name == "order_date" and isinstance(value, str):
            value = date.fromisoformat(value)
        row[name] = value
    return row


# --------------------------------------------------------------------------------------
# Catalog
# --------------------------------------------------------------------------------------

def catalog(config=None, *, dsn: str = None, warehouse: str = None):
    """SQL catalog on top of the OLTP Postgres. Imports pyiceberg on demand.

    `py-io-impl=PyArrowFileIO` and NOT s3fs: pyiceberg's `s3fs` extra drags in an
    `aiobotocore` that pins an old botocore and breaks the `boto3` the platform uses for
    RAW. pip accepts the install and the damage shows up later, in another module. Measured.
    """
    from .config import from_env

    config = config or from_env()
    try:
        from pyiceberg.catalog.sql import SqlCatalog
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise OrdersProjectionError(
            "pyiceberg is not installed in this venv. Run `make venv`."
        ) from exc

    port = os.environ.get("OLTP_PORT", "5433")
    uri = dsn or os.environ.get(
        "ICEBERG_CATALOG_URI",
        f"postgresql+psycopg://oltp:oltp@localhost:{port}/{CATALOG_DB}",
    )
    try:
        return SqlCatalog(CATALOG_NAME, **{
            "uri": uri,
            "warehouse": warehouse or WAREHOUSE,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": config.endpoint,
            "s3.access-key-id": config.access_key,
            "s3.secret-access-key": config.secret_key,
            "s3.region": config.region,
        })
    except Exception as exc:
        raise OrdersProjectionError(
            f"could not open the Iceberg catalog at {uri}: {exc}\n"
            "The stream plane does not come up with `make up`. Run `make stream-up`."
        ) from exc


def ensure_catalog_database(port: str = None) -> bool:
    """Creates the catalog database if missing. Returns True if it created it."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise OrdersProjectionError("psycopg is not installed in this venv.") from exc
    port = port or os.environ.get("OLTP_PORT", "5433")
    admin = f"postgresql://oltp:oltp@localhost:{port}/postgres"
    with psycopg.connect(admin, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (CATALOG_DB,))
            if cur.fetchone():
                return False
            cur.execute(f'create database "{CATALOG_DB}"')
    return True


def ensure_table(cat=None):
    cat = cat or catalog()
    cat.create_namespace_if_not_exists(NAMESPACE)
    if not cat.table_exists(TABLE_NAME):
        cat.create_table(TABLE_NAME, schema=arrow_schema())
    return cat.load_table(TABLE_NAME)


def metadata_location(cat=None) -> str:
    """Where the CURRENT metadata is, according to the catalog.

    This is the function `make silver` calls. DuckDB refuses to discover this on its own —
    and it is right to refuse: scanning storage for the newest metadata can find one that
    has not been committed yet. The catalog is the one that knows.
    """
    cat = cat or catalog()
    if not cat.table_exists(TABLE_NAME):
        raise OrdersProjectionError(
            f"{TABLE_NAME} does not exist in the catalog. Run `make orders-projection-iceberg-init`."
        )
    return cat.load_table(TABLE_NAME).metadata_location


# --------------------------------------------------------------------------------------
# The sink: the SAME small interface as PostgresProjection
# --------------------------------------------------------------------------------------

@dataclass
class CommitStats:
    attempts: int = 0
    conflicts: int = 0
    rows_written: int = 0
    rows_dropped_as_stale: int = 0


class IcebergProjection:
    """Iceberg sink. `load` / `save` / `commit` / `rollback` / `count` / `digest`.

    The interface is identical to `PostgresProjection` on purpose: it was the seam declared in
    Milestone 5, and swapping the storage does not change one line of the consumer. If this
    class needed one extra method for `orders-project` to work, the seam would have leaked.

    The semantic difference is hidden where it belongs: in Postgres, `save` writes and
    `commit` closes the transaction; here, `save` buffers and `commit` does the upsert —
    because Iceberg has no open transaction, every commit is a new snapshot.
    """

    def __init__(self, table, *, writer: str = WRITER_STREAM, cat=None):
        self._table = table
        self._catalog = cat
        self._writer = writer
        self._buffer: dict = {}
        self.stats = CommitStats()

    # ---- reading -----------------------------------------------------------
    def _current(self, order_ids=None) -> dict:
        """Current state, re-read from the newest snapshot.

        Scans the whole table and filters in memory instead of building an `In(...)` with
        thousands of values: at this volume (~6,400 rows) scanning two columns is trivial,
        and a giant filter pushed down into the scan is not. If the projection grows by
        orders of magnitude, this is the spot to change — and it is said here so it does
        not become a discovery.
        """
        rows = self._table.scan(selected_fields=COLUMNS).to_arrow().to_pylist()
        if order_ids is None:
            return {row["order_id"]: row for row in rows}
        wanted = set(order_ids)
        return {r["order_id"]: r for r in rows if r["order_id"] in wanted}

    def load(self, order_ids) -> dict:
        if not order_ids:
            return {}
        return self._current(order_ids)

    def save(self, states) -> None:
        for state in states:
            self._buffer[state["order_id"]] = state

    def rollback(self) -> None:
        self._buffer.clear()

    def commit(self) -> CommitStats:
        """Upsert with retry AND MONOTONIC MERGE. See the module header.

        Retry alone would resolve the commit conflict and still lose data: if the other
        writer already stored a higher `sequence_no` for the same order, writing over it makes
        the table REGRESS — and the commit would succeed, because from Iceberg's point of view
        nothing is wrong. That is why every attempt re-reads and drops its own rows that do
        not advance.
        """
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        if not self._buffer:
            return self.stats

        stats = CommitStats()
        for attempt in range(1, MAX_COMMIT_ATTEMPTS + 1):
            stats.attempts = attempt
            current = self._current(self._buffer.keys())
            candidates = [
                state for order_id, state in sorted(self._buffer.items())
                if order_id not in current
                or state["last_sequence_no"] > current[order_id]["last_sequence_no"]
            ]
            stats.rows_dropped_as_stale = len(self._buffer) - len(candidates)
            if not candidates:
                # Another writer already stored an equal or newer state for every key.
                # Not writing is the CORRECT outcome, not a failure.
                self._buffer.clear()
                self.stats = stats
                return stats
            batch = pa.Table.from_pylist(
                [_normalize(state, self._writer) for state in candidates],
                schema=arrow_schema(),
            )
            try:
                self._table.upsert(batch, join_cols=["order_id"])
                stats.rows_written = len(candidates)
                self._buffer.clear()
                self.stats = stats
                return stats
            except CommitFailedException:
                stats.conflicts += 1
                self._refresh()
        raise OrdersProjectionError(
            f"{MAX_COMMIT_ATTEMPTS} commit attempts without success: the table is under "
            f"more contention than this writer knows how to handle"
        )

    def _refresh(self) -> None:
        """Reloads ITS OWN table, not a hard-coded name.

        The first version reloaded `TABLE_NAME`. While only one table existed, that worked —
        and in the first test that used a probe table, a conflict made the probe writer
        reload the PRODUCTION table and write into it. Four synthetic rows landed in
        `live_order_state`, and the reconciliation was what caught it.

        A hard-coded identifier in a refresh function is always this: it works until a
        second object exists, and then it writes to the wrong place without any error.
        """
        if self._catalog is not None:
            self._table = self._catalog.load_table(self._table.name())
        else:
            self._table.refresh()

    # ---- observation --------------------------------------------------------
    def count(self) -> int:
        return self._table.scan(selected_fields=("order_id",)).to_arrow().num_rows

    def digest(self) -> str:
        """Fingerprint, comparable with the one from `PostgresProjection`.

        `written_by` IS LEFT OUT: it is provenance, not state. Including it would make the
        digest change when the same content was written by the other writer — and the
        property being measured is precisely that both produce the same state.
        """
        columns = [c for c in COLUMNS if c != "written_by"]
        rows = self._table.scan(selected_fields=tuple(columns)).to_arrow().to_pylist()
        sha = hashlib.sha256()
        for row in sorted(rows, key=lambda r: r["order_id"]):
            sha.update(json.dumps([str(row.get(c)) for c in columns],
                                  ensure_ascii=False).encode("utf-8"))
            sha.update(b"\n")
        return sha.hexdigest()

    def writers_seen(self) -> dict:
        rows = self._table.scan(selected_fields=("written_by",)).to_arrow().to_pylist()
        out: dict = {}
        for row in rows:
            out[row["written_by"]] = out.get(row["written_by"], 0) + 1
        return out

    def snapshots(self) -> list:
        return [s.snapshot_id for s in self._table.metadata.snapshots]


# --------------------------------------------------------------------------------------
# The second writer: batch rebuild from RAW
# --------------------------------------------------------------------------------------

@dataclass
class RebuildResult:
    partitions: int = 0
    events: int = 0
    orders: int = 0
    commits: int = 0
    conflicts: int = 0
    dropped_as_stale: int = 0
    partitions_read: list = field(default_factory=list)


def reset_table(cat=None):
    """Drops and recreates `live_order_state`. It exists because of ONE specific condition.

    The projection merge is MONOTONIC: a row with a `last_sequence_no` lower than the stored
    one is dropped as stale, and that is how the two writers coexist without trampling each
    other. The merge assumes, without saying so, that an `order_id` always refers to the SAME
    order.

    That premise breaks when the Source is regenerated with another `seed`, another
    reference, other premises or another `demand_model_version` — the four non-additive
    conditions of the CONTRACT. Then order `ord_mad1_20260824_000001` is a DIFFERENT order
    with the same id, and the monotonic merge mixes the two: MEASURED on 2026-08-31, a
    rebuild after the recalibration dropped 4,028 rows as "equal or newer state" and left the
    projection with a mix of two universes. `orders-reconcile` caught it.

    Reset is destructive on purpose and never happens on its own — only with an explicit --reset.
    """
    cat = cat or catalog()
    if cat.table_exists(TABLE_NAME):
        cat.drop_table(TABLE_NAME)
    return ensure_table(cat)


def rebuild(cat, partitions, *, sla_minutes: int = None,
            seeds_dir: str = DEFAULT_SEEDS_DIR, batch_size: int = 500,
            reset: bool = False) -> RebuildResult:
    """Rebuilds the projection from the on-disk log. It is the SECOND WRITER.

    IT SHARES `fold_event` WITH THE CONSUMER, AND THAT IS DELIBERATE — but it needs a
    caveat, or the reading goes wrong: agreement between this path and streaming is NOT
    evidence of correctness, it is evidence that the two writers do not trample each other.
    Evidence of correctness comes from `orders-reconcile`, which compares against
    `silver_order` — a genuinely independent implementation, a window function over the
    whole log.

    Confusing the two would be the same defect this project has already recorded twice:
    a check that compares something with itself and passes.
    """
    from .orders_oltp import read_log, verify_log

    sla = sla_minutes if sla_minutes is not None else read_sla_minutes(seeds_dir)
    table = reset_table(cat) if reset else ensure_table(cat)
    projection = IcebergProjection(table, writer=WRITER_REBUILD, cat=cat)
    result = RebuildResult()
    states: dict = {}

    for path in partitions:
        verify_log(path)
        result.partitions += 1
        result.partitions_read.append(path)
        for event, _line in read_log(path):
            result.events += 1
            current = states.get(event["order_id"])
            states[event["order_id"]] = fold_event(current, event, sla_minutes=sla)

    result.orders = len(states)
    ordered = [states[k] for k in sorted(states)]
    if reset:
        # ONE APPEND, not batched upserts (debt item 5, closed in CR-017). After a reset the
        # table is EMPTY and this writer is alone on it, so there is nothing to merge against
        # — the monotonic merge exists for a second writer, and here there is none yet. Each
        # batched upsert scanned the whole table it had already written: 414 commits and
        # ~55 min for 206,523 orders (2026-09-01), and the streaming sink on an empty table
        # reached only 38,300 of 211,367 orders in 80 minutes (2026-10-30).
        import pyarrow as pa

        batch = pa.Table.from_pylist(
            [_normalize(state, WRITER_REBUILD) for state in ordered], schema=arrow_schema()
        )
        table.append(batch)
        result.commits = 1
        return result
    for start in range(0, len(ordered), batch_size):
        projection.save(ordered[start:start + batch_size])
        stats = projection.commit()
        result.commits += 1
        result.conflicts += stats.conflicts
        result.dropped_as_stale += stats.rows_dropped_as_stale
    return result


# --------------------------------------------------------------------------------------
# Reconciliation: three independent folds
# --------------------------------------------------------------------------------------

_COMPARE = ("status", "last_sequence_no", "line_count", "picked_line_count",
            "gross_amount", "net_amount", "substituted_lines", "removed_lines")


def _as_key(value):
    """Normalizes for comparison across three engines without confusing TYPE with DIVERGENCE.

    NORMALIZES THE SCALE, NEVER ROUNDS. The first version formatted with `f"{v:.2f}"`, and
    that made `10.001` and `10.00` compare EQUAL — a reconciliation that rounds away the
    difference it came to check. Today all three sources use `decimal(12,2)` and the defect
    would not have shown up; it would show up the day one of them changed scale, which is
    the worst possible day to discover it.

    `Decimal.normalize()` strips trailing zeros without touching the value: `10.00` and `10`
    become the same key, `10.001` does not.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return f"num:{value.normalize()}"
    if isinstance(value, float):
        return f"num:{Decimal(str(value)).normalize()}"
    if isinstance(value, int):
        return f"num:{Decimal(value).normalize()}"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _rows_from_iceberg(cat) -> dict:
    table = ensure_table(cat)
    rows = table.scan(selected_fields=("order_id",) + _COMPARE).to_arrow().to_pylist()
    return {r["order_id"]: {c: _as_key(r.get(c)) for c in _COMPARE} for r in rows}


def _rows_from_silver(config) -> dict:
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    cursor = connection.execute(
        """
        select order_id, order_status, last_sequence_no, line_count_placed,
               line_count_picked, gross_amount_placed, net_amount,
               substituted_lines, removed_lines
          from silver_order
        """
    )
    out = {}
    for row in cursor.fetchall():
        out[row[0]] = dict(zip(_COMPARE, [_as_key(v) for v in row[1:]]))
    return out


def _rows_from_oltp(dsn=None) -> dict:
    from .orders_oltp import connect

    with connect(dsn) as connection:
        with connection.cursor() as cur:
            cur.execute(
                """
                select o.order_id, o.status, o.last_sequence_no, o.line_count,
                       o.picked_line_count, o.gross_amount, o.net_amount,
                       count(*) filter (where l.status = 'substituted'),
                       count(*) filter (where l.status = 'removed')
                  from orders o left join order_line l on l.order_id = o.order_id
                 group by o.order_id
                """
            )
            return {
                row[0]: dict(zip(_COMPARE, [_as_key(v) for v in row[1:]]))
                for row in cur.fetchall()
            }


def reconcile(config=None, *, cat=None, dsn=None, limit: int = 10) -> dict:
    """Compares the THREE folds. Any divergence fails — no rounding, no ignoring.

    `silver_order` is the only one of the three implementations that shares no code with
    any other: a SQL window function over the whole log. It is against it that the
    comparison counts as verification; agreement between projection and OLTP counts as
    evidence of transport.
    """
    from .config import from_env

    config = config or from_env()
    cat = cat or catalog(config)
    sources = {
        "iceberg": _rows_from_iceberg(cat),
        "silver": _rows_from_silver(config),
        "oltp": _rows_from_oltp(dsn),
    }
    keys = {name: set(rows) for name, rows in sources.items()}
    every_key = set().union(*keys.values())

    missing = {
        name: sorted(every_key - present)[:limit] for name, present in keys.items()
        if every_key - present
    }
    divergences = []
    common = set.intersection(*keys.values()) if keys else set()
    for order_id in sorted(common):
        for column in _COMPARE:
            values = {name: sources[name][order_id][column] for name in sources}
            if len(set(map(repr, values.values()))) > 1:
                # The "coluna" key is read by cli.py (`orders-reconcile` output): keep it.
                divergences.append({"order_id": order_id, "coluna": column, **values})
    return {
        "orders": {name: len(rows) for name, rows in sources.items()},
        "compared": len(common),
        "missing": missing,
        "divergences": divergences[:limit],
        "divergence_count": len(divergences),
        "ok": not missing and not divergences,
    }


def partitions_of(root: str = "data/orders", *, through: str = None) -> list:
    """Order partitions on disk, in deterministic order.

    `through` limits to `ingestion_date <= through`, which is the natural cut of the lambda
    pair: the batch layer covers settled history, and streaming covers the live tail.
    Without it, both layers recompute exactly the same thing — which works, and is wasteful.
    """
    out = []
    if not os.path.isdir(root):
        return out
    for day in sorted(os.listdir(root)):
        if not day.startswith("ingestion_date="):
            continue
        if through is not None and day.split("=", 1)[1] > through:
            continue
        day_path = os.path.join(root, day)
        for axis in sorted(os.listdir(day_path)):
            path = os.path.join(day_path, axis)
            if os.path.exists(os.path.join(path, "_SUCCESS")):
                out.append(path)
    return out
