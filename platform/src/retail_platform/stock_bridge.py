"""The bridge between Silver and the Spark stock job, in both directions.

    make stock-consumption      Silver  -> operations.stock_consumption   (pyiceberg)
    make stock-ledger           consumption -> operations.stock_ledger    (Spark)
    make silver                 ledger  -> silver_stock_ledger            (DuckDB)

WHY SPARK DOES NOT READ THE SILVER PARQUET DIRECTLY, AND WHY THAT IS A DECISION AND NOT A DETOUR.

Reading `s3://retail-lakehouse/silver/silver_order_line.parquet` from Spark would require
`hadoop-aws` plus the AWS SDK v1 bundle in the image — about 200 MB of jars and a second
integration with the object storage, with its own set of failure modes. Spark already talks
to MinIO through one path: Iceberg's `S3FileIO`, which Milestone A measured.

So consumption crosses over as an ICEBERG TABLE. The gain is not only the jar that stays out:
the same property that justified Iceberg — interop between engines — is now exercised by
the production path, and not only by an experiment. The full route has three engines and
two languages:

    DuckDB (Silver)  ->  pyiceberg writes  ->  Spark reads, computes, writes  ->  DuckDB reads

CONSUMPTION IS OBSERVED, NOT A PREMISE. It comes from `silver_order_line`, which comes from the
event log. The stock premises (`stock_premises_seed`) say how many DAYS to cover; how many
units that is worth per series is a consequence of what the orders consumed.

THREE SEMANTIC DECISIONS that change the number and are therefore written down:

  1. IT CONSUMES THE DELIVERED PRODUCT, not the ordered one. A substituted line takes stock
     from the SUBSTITUTE — hence the key is `fulfilled_source_product_id`. Aggregating by the
     original product would credit consumption to a shelf nobody touched, and the total would
     still add up.
  2. IT CONSUMES AT PICKING, not at placement. The instant is the order's `picked_at`, not the
     day partition. An order placed at 23h and picked early the next morning takes stock from
     the next day. Using the partition would be simpler and wrong at the edges — and the
     edges are exactly where stockouts show up.
  3. A REMOVED LINE DOES NOT CONSUME. `line_status = 'removed'` is the line picking did not
     find; it does not leave the shelf. Neither does `not_picked`: those are orders cancelled
     or refused at payment, which never reached picking.

THE LIMIT THIS LEAVES, DECLARED. The generator's `removal_rate` produces lines with reason
"unavailable" at a FIXED, random rate, without looking at any balance. The stockout this ledger
computes is independent of it: neither causes the other. Making them coherent would require the
generator to READ the ledger, and that would invert the dependency of the whole project — today
orders generate stock, and there would be a cycle. Trigger to change: a second-pass generator
that rereads the previous day's balance. It is out of scope, and the alternative (pretending
the fixed rate is a stockout) would be worse than the absence.
"""

from __future__ import annotations

import os

NAMESPACE = os.environ.get("STOCK_NAMESPACE", "operations")
CONSUMPTION_TABLE = f"{NAMESPACE}.stock_consumption"
LEDGER_TABLE = f"{NAMESPACE}.stock_ledger"


class StockLedgerError(RuntimeError):
    """Failure moving consumption or the ledger between Silver and the catalog."""


def catalog(config=None):
    """The SAME catalog as the live projection. There is no second one.

    One catalog per domain would look tidier and would create two places where a table
    can live — and `silver_gate` would have to ask both. The namespace already separates
    `projection` from `operations`.
    """
    from .orders_projection import catalog as open_catalog

    return open_catalog(config)


def consumption_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("wh", pa.string(), nullable=False),
        pa.field("source_product_id", pa.string(), nullable=False),
        pa.field("stock_date", pa.date32(), nullable=False),
        pa.field("units_consumed", pa.int64(), nullable=False),
        # PROVENANCE, as in live_order_state: without it, "who wrote this row" would be
        # a claim about a log instead of a queryable fact.
        pa.field("written_by", pa.string(), nullable=False),
    ])


CONSUMPTION_SQL = """
    select
        l.wh                                    as wh,
        l.fulfilled_source_product_id           as source_product_id,
        cast(o.picked_at as date)               as stock_date,
        cast(sum(l.quantity) as bigint)         as units_consumed
    from silver_order_line l
    join silver_order o using (order_id)
    where o.picked_at is not null
      and l.line_status in ('fulfilled', 'substituted')
      and l.fulfilled_source_product_id is not null
    group by 1, 2, 3
"""


def export_consumption(config=None, *, cat=None) -> dict:
    """Reads consumption from Silver and (re)writes it entirely into `operations.stock_consumption`.

    FULL OVERWRITE, not append: this table is a PROJECTION of Silver, not a log. If the
    order window is regenerated, the old consumption must go with it — an append would
    leave the ledger summing two captures and the total would reveal nothing.
    """
    import pyarrow as pa

    from .config import from_env
    from .query import connect_lakehouse

    config = config or from_env()
    connection = connect_lakehouse(config)
    try:
        # `.arrow()` returns a RecordBatchReader in duckdb 1.5, not a Table.
        # `fetch_arrow_table()` materializes — the aggregate has tens of thousands of
        # rows, not millions, so there is nothing to stream.
        arrow_table = connection.execute(CONSUMPTION_SQL).fetch_arrow_table()
    except Exception as exc:
        raise StockLedgerError(
            f"could not read consumption from Silver: {exc}\n"
            "Run `make silver` first — this verb reads silver_order_line and silver_order."
        ) from exc

    schema = consumption_schema()
    columns = [arrow_table.column(field.name).cast(field.type)
               for field in schema if field.name != "written_by"]
    columns.append(pa.array(["platform"] * arrow_table.num_rows, type=pa.string()))
    batch = pa.Table.from_arrays(columns, schema=schema)

    cat = cat or catalog(config)
    cat.create_namespace_if_not_exists(NAMESPACE)
    if not cat.table_exists(CONSUMPTION_TABLE):
        cat.create_table(CONSUMPTION_TABLE, schema=schema)
    table = cat.load_table(CONSUMPTION_TABLE)
    table.overwrite(batch)

    days = arrow_table.column("stock_date").to_pylist()
    return {
        "linhas": batch.num_rows,
        "series": len({(w, p) for w, p in zip(arrow_table.column("wh").to_pylist(),
                                              arrow_table.column("source_product_id").to_pylist())}),
        "unidades": sum(arrow_table.column("units_consumed").to_pylist()),
        "de": min(days).isoformat() if days else None,
        "ate": max(days).isoformat() if days else None,
        "metadata_location": cat.load_table(CONSUMPTION_TABLE).metadata_location,
    }


def metadata_location(table: str = LEDGER_TABLE, cat=None) -> str | None:
    """Where the table's CURRENT metadata is, according to the catalog — or None if it does not exist.

    None is not an error: Spark is OPTIONAL in this project. A freshly cloned repository, or a
    machine where nobody ran `make stock-ledger`, must build Silver in peace. The one that
    decides what to do with None is `silver_gate`, in a single place.

    It never scans the storage for the newest metadata: DuckDB refuses to do that and the
    refusal is right — the most recent file may be a commit that never happened.
    """
    try:
        cat = cat or catalog()
        if not cat.table_exists(table):
            return None
        return cat.load_table(table).metadata_location
    except Exception:
        return None
