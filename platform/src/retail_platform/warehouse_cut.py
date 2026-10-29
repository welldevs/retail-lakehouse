"""Cuts Silver down for the warehouse (Postgres or Snowflake): local parquet that both
loaders transport — COPY FROM STDIN on Postgres, PUT + COPY INTO on Snowflake.

WHY THIS MODULE EXISTS
----------------------
Snowflake is the FOURTH consumer that cannot reach the Lakehouse — the other three are the
FROZEN Sources. Two independent reasons, and both already have precedent in the repo:

  1. Network: a managed Snowflake cannot see a MinIO on localhost. `COPY INTO` from an
     external stage would require real S3 and a storage integration; the INTERNAL stage
     (`PUT file://...`) works without any of that, because the one pushing the bytes is
     this process, which sees both sides.
  2. Volume: copying all of Silver makes no sense. Measured on 2026-08-27 —
     3,796,213 rows in Silver, of which 3,094,992 (81.5%) are
     `silver_ine_population_series`, national, with 57,072 rows (1.8%) in the real scope
     of the 4 provinces. Loading that would mean paying storage for 27x the useful data.

That is why this module is a direct sibling of `customers_reference.py`: it materializes a
CUT of Silver for whoever cannot speak DuckDB. The only differences are the output format
(parquet instead of JSON) and the destination (a table instead of a file read by the
stdlib).

THE CUT IS DELIBERATELY DUMB
----------------------------
Geographic scope filter, latest-ingestion filter, grain deduplication. NO business rule.
If a domain `case when` shows up here, it is in the wrong place: modeling belongs to dbt,
on the Snowflake side. That is what prevents the same logic from existing in two engines
and silently diverging.

THE ONLY DECLARED EXCEPTION is excluding aggregates that the source mixes with the detail
(`sex_label = 'Total'`). It is not a business rule, it is avoiding double counting: summing
the three labels gives twice the population. It is exactly the trap that already cost a
2.03x inflation in the Phase 1 age pyramid, with `age_label`. An aggregate loaded next to
the detail is not "extra data": it is a wrong fact waiting for someone to sum it.

THE SCHEMA OF THE STAGE TABLES COMES FROM THE QUERY ITSELF
----------------------------------------------------------
The DDL is generated from the type DuckDB returns, not hand-written in a file next to it.
A manual DDL is a second place where the schema lives, and the two diverge on the first day
someone adds a column to the cut. Since STAGE is a 1:1 mirror, its schema IS the query
result — deriving it is what keeps both in sync by construction. The generated DDL can be
inspected without any connection (`dump_ddl`), so it stays reviewable.

STAGE IS FULL REFRESH (`create or replace` + `COPY INTO`). There is no merge or incremental
here on purpose: it is ~407 thousand rows, and history/versioning are the responsibility
of the GOLD SCD2 dimensions, not of the mirror. Trigger to revisit: the full load no longer
fitting in a window where redoing it is cheaper than reasoning about partial state.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

# Snowflake-side schemas. STAGE is the only one this module writes.
STAGE_SCHEMA = "STAGE"

DEFAULT_OUT = os.path.join("data", "warehouse-cut")


class WarehouseCutError(Exception):
    """The cut cannot be produced, or the destination refused it."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------------
# The cut, table by table
# --------------------------------------------------------------------------------
# Each entry is (STAGE table name, what the cut does and why, SQL). The "why" comments
# live in the SQL itself, next to the line they enforce.

SPECS: list[dict] = [
    {
        "name": "STG_PRODUCT_PRICE",
        "grain": "(ingestion_date, warehouse, source_product_id)",
        "sql": """
            -- CATEGORY-AXIS DEDUP. The Silver grain is
            -- (date, wh, category_id, subgroup_id, product): a product appears in more than
            -- one category and the source repeats the whole row. Measured: the PRICE does not
            -- diverge across those appearances (0 cases), so reducing by product is safe and
            -- hides no conflict — the same premise silver_price_change already assumes and
            -- that assert_price_is_consistent_across_appearances protects.
            --
            -- The `warehouse` axis STAYS in the grain, and that is measured, not convention:
            -- 187 products have a different unit_price across the 4 warehouses on the same
            -- day, and 0 diverge within the same warehouse. A price fact without `wh` would
            -- measure an average that exists nowhere.
            select
                ingestion_date,
                warehouse,
                source_product_id,
                any_value(display_name)                     as display_name,

                -- Primary category chosen by MIN, which is deterministic, with the count
                -- next to it so the multi-category case is a visible number and not a
                -- detail erased by the reduction (measured: max 2, mean 1.06).
                min(category_id)                            as primary_category_id,
                count(distinct category_id)                 as category_appearances,
                any_value(product_level1_category_id)       as product_level1_category_id,

                any_value(unit_price)                       as unit_price,

                -- THE PURCHASABLE-UNIT PRICE, next to the raw source value. It differs
                -- only on `bunch` rows, where the API returns reference_price * 99 — the
                -- ceiling of the weight selector, not a consumer price (obligation 5 of
                -- the Mercadona contract; 10 product x warehouse combinations, measured).
                -- BOTH cross over: FACT_ORDER_ITEM reconciles against the first, and the
                -- second is the evidence that the correction exists and how big it is.
                any_value(purchasable_unit_price)           as purchasable_unit_price,
                any_value(price_basis)                      as price_basis,
                any_value(net_content_kg_l)                 as net_content_kg_l,

                any_value(bulk_price)                       as bulk_price,
                any_value(reference_price)                  as reference_price,
                any_value(reference_format)                 as reference_format,
                any_value(min_bunch_amount)                 as min_bunch_amount,
                any_value(previous_unit_price)              as previous_unit_price,
                any_value(tax_percentage)                   as tax_percentage,

                any_value(is_pack)                          as is_pack,
                any_value(pack_size)                        as pack_size,
                any_value(unit_size)                        as unit_size,
                any_value(size_format)                      as size_format,
                any_value(unit_name)                        as unit_name,
                any_value(total_units)                      as total_units,
                any_value(selling_method)                   as selling_method,
                any_value(published)                        as published,
                any_value(is_new_arrival)                   as is_new_arrival
            from silver_product_price
            group by 1, 2, 3
            order by 1, 2, 3
        """,
    },
    {
        "name": "STG_PRICE_CHANGE",
        "grain": "(ingestion_date, previous_ingestion_date, warehouse, source_product_id)",
        "sql": """
            -- LOADED, NOT RE-DERIVED. It could be derived from STG_PRODUCT_PRICE with a window
            -- function, but that would create a second implementation of the same gap-aware
            -- lag() that Silver already has tested — and the two would diverge on the first
            -- tweak. Duplicating 43 thousand rows costs less than duplicating logic.
            --
            -- name_seen_before comes along because it is the input to the SCD2 decision of
            -- DIM_PRODUCT: a new id with a name that already existed is identity ambiguity,
            -- not "new product".
            select *
            from silver_price_change
            order by ingestion_date, warehouse, source_product_id
        """,
    },
    {
        "name": "STG_POPULATION_MUNICIPALITY",
        "grain": "(province_code, municipality_code, sex_label, year)",
        "sql": """
            -- WITHOUT sex_label = 'Total'. The source mixes the aggregate with the detail in
            -- the same column: loading the three labels makes any sum give twice the
            -- population. Same trap as age_label in Phase 1 (2.03x inflation, measured). A
            -- test on the Snowflake side rechecks Hombres + Mujeres = source Total.
            --
            -- Scope: only the 370 municipalities of the AUFs. Silver is national (74,898
            -- rows); outside the AUFs there is no customer, no warehouse, no question.
            select
                p.province_code,
                p.municipality_code,
                p.municipality_name,
                a.wh,
                p.sex_label,
                p.year,
                p.reference_date,
                p.population_value,
                p.is_secret
            from silver_ine_population_by_municipality p
            join warehouse_service_area a
              on  a.province_code     = p.province_code
             and  a.municipality_code = p.municipality_code
            where p.is_latest_ingestion
              and p.sex_label <> 'Total'
            order by 1, 2, 5, 6
        """,
    },
    {
        "name": "STG_CATEGORY",
        "grain": "(category_id)",
        "sql": """
            -- GLOBAL, with no warehouse or date axis. Measured: the tree has 151 level-2
            -- categories in EACH of the 4 warehouses, identical. Loading 2,114 rows
            -- (14 partitions x 151) to serve a dimension of 151 would be pure duplication.
            -- Cut by the latest ingestion so the choice is deterministic, not an
            -- any_value over different dates.
            select
                category_id,
                any_value(category_name)            as category_name,
                any_value(category_order)           as category_order,
                any_value(parent_category_id)       as parent_category_id,
                any_value(parent_category_name)     as parent_category_name,
                any_value(parent_category_order)    as parent_category_order
            from silver_category
            where ingestion_date = (select max(ingestion_date) from silver_category)
            group by 1
            order by 1
        """,
    },
    {
        "name": "STG_CUSTOMER",
        "grain": "(ingestion_date, customer_id)",
        "sql": """
            -- ALL ingestion_dates, not just the current one: DIM_CUSTOMER is SCD2 and needs
            -- the history to version. `is_latest_ingestion` travels along so the Snowflake
            -- side knows which base is current without recomputing a max().
            select *
            from silver_customer
            order by ingestion_date, customer_id
        """,
    },
    {
        "name": "STG_GEOGRAPHY",
        "grain": "(province_code, municipality_code, postal_code)",
        "sql": """
            -- THE ROLLUP THAT SURVIVES FROM THE 304,952 TRAMOS: 699 rows.
            --
            -- postal_code ALONE IS NOT A KEY — measured: 29 postal codes cross a municipality
            -- boundary. The pair (municipality, postal code) is the smallest grain that
            -- answers both "where the customer lives" and "where delivery goes". Also
            -- measured: 0 postal codes cross warehouses, so `wh` is a function of the pair
            -- and routing by postal code is unambiguous.
            --
            -- THE TWO MUNICIPALITY NAMES travel together, with distinct labels, because the
            -- two INE products spell the same municipality differently in 370 of 370 cases
            -- ("BRUC (EL)" versus "Bruc, El"). Keeping only one would hide the divergence;
            -- keeping both under the same label would be a trap. What is compared across
            -- sources is always the CODE.
            with tramos as (
                select * from silver_callejero_tramos where is_latest_ingestion
            ),
            municipality_callejero as (
                select province_code, municipality_code,
                       max(municipality_name) as municipality_name
                from silver_callejero_population_units
                where is_latest_ingestion and is_municipality_aggregate
                group by 1, 2
            ),
            -- CURRENT-YEAR population as a size attribute of the dimension. The whole
            -- series (1996-2025) lives in STG_POPULATION_MUNICIPALITY, which is a fact:
            -- this is a convenience of the cut, not the series.
            population as (
                select province_code, municipality_code,
                       max(municipality_name) as municipality_name,
                       sum(population_value)  as population_total
                from silver_ine_population_by_municipality
                where is_latest_ingestion
                  and sex_label <> 'Total'
                  and year = (select max(year) from silver_ine_population_by_municipality
                              where is_latest_ingestion)
                group by 1, 2
            )
            select
                t.province_code,
                t.municipality_code,
                t.postal_code,
                a.wh,
                a.is_home_municipality,
                mc.municipality_name                        as municipality_name_callejero,
                pp.municipality_name                        as municipality_name_ine,
                pp.population_total                         as municipality_population,
                count(*)                                    as tramo_count,
                count(distinct t.section_code)              as section_count,
                count(distinct t.street_code)
                    filter (where t.street_id <> '00000')   as street_count
            from tramos t
            join warehouse_service_area a
              on  a.province_code     = t.province_code
             and  a.municipality_code = t.municipality_code
            left join municipality_callejero mc
              on  mc.province_code     = t.province_code
             and  mc.municipality_code = t.municipality_code
            left join population pp
              on  pp.province_code     = t.province_code
             and  pp.municipality_code = t.municipality_code
            group by 1, 2, 3, 4, 5, 6, 7, 8
            order by 1, 2, 3
        """,
    },
    {
        "name": "STG_SERVICE_AREA",
        "grain": "(wh, province_code, municipality_code)",
        "sql": """
            -- 370 rows. The AUF (INE Functional Urban Area) of each warehouse: which
            -- municipalities it serves. It is what prevents a future order from being born
            -- outside the area.
            select * from warehouse_service_area
            order by wh, province_code, municipality_code
        """,
    },
    {
        "name": "STG_WAREHOUSE",
        "grain": "(wh)",
        "sql": """
            select * from warehouse_province_map order by wh
        """,
    },
    {
        "name": "STG_INGESTION_RUN",
        "grain": "(source_name, ingestion_date, wh)",
        "sql": """
            -- 18 ROWS THAT KEEP A MART FROM LYING. They distinguish "there was no price" from
            -- "there was no observation": the days 2026-08-17 to 08-23 do not exist in the
            -- catalog and CANNOT be recovered (the API only serves today's price). Without
            -- this table, a price fact with a gap is indistinguishable from a product that
            -- disappeared, and any time series silently interpolates.
            --
            -- The sources with a warehouse axis enter in the same shape; the union is by
            -- NAMED COLUMN, not by position.
            select
                source_name,
                ingestion_date,
                warehouse                       as wh,
                run_id,
                complete,
                declared_product_rows           as declared_rows,
                failure_count,
                anomaly_count,
                started_at_utc,
                finished_at_utc,
                duration_seconds
            from raw_manifest

            union all by name

            select
                source_name,
                ingestion_date,
                wh,
                run_id,
                complete,
                declared_customer_rows          as declared_rows,
                failure_count,
                anomaly_count,
                started_at_utc,
                finished_at_utc,
                duration_seconds
            from silver_oltp_manifest

            union all by name

            -- ORDERS DECLARES EVENTS, NOT ROWS, and the mapping is to `declared_rows`
            -- because that is what the partition file contains: one NDJSON line per event.
            -- Mapping `declared_order_rows` here would make the "file rows x declared rows"
            -- reconciliation compare two different quantities and pass by mistake at
            -- 6,400 versus 44,456.
            select
                source_name,
                ingestion_date,
                wh,
                run_id,
                complete,
                declared_event_rows             as declared_rows,
                failure_count,
                anomaly_count,
                started_at_utc,
                finished_at_utc,
                duration_seconds
            from silver_orders_manifest

            order by source_name, ingestion_date, wh
        """,
    },
    {
        "name": "STG_ORDER",
        "grain": "(order_id)",
        "sql": """
            -- THE ALREADY-FOLDED FOLD, loaded whole. `select *` on purpose: silver_order is
            -- the only Silver model whose grain is already exactly what the warehouse wants,
            -- so projecting column by column here would only create a second place to
            -- forget to add one.
            --
            -- WITHOUT `is_latest_ingestion`, and not by oversight: each ingestion_date is an
            -- OPERATING DAY, not a re-ingestion of the same day. Filtering by the latest
            -- would leave 4,800 of the 6,400 orders out of the warehouse.
            select *
            from silver_order
            order by ingestion_date, wh, order_id
        """,
    },
    {
        "name": "STG_ORDER_LINE",
        "grain": "(order_id, line_no)",
        "sql": """
            -- THE PLACED LINE AND THE FULFILLED LINE IN THE SAME PHYSICAL ROW.
            -- `source_product_id` is what the customer ordered, `fulfilled_source_product_id`
            -- is what they received, and both cross the boundary: loading only the second
            -- would erase the substitution, which is the one fact the event model exists to
            -- record.
            select *
            from silver_order_line
            order by order_id, line_no
        """,
    },
    {
        "name": "STG_ORDER_EVENT",
        "grain": "(event_id)",
        "sql": """
            -- THE WHOLE LOG, 44,456 rows. It is the only STAGE table that carries a
            -- semi-structured body, and the decision deserves to be written down.
            --
            -- WHY THE PAYLOAD CROSSES OVER. Every typed field in it has already been folded
            -- into STG_ORDER and STG_ORDER_LINE — decline_reason, cancelled_by, picked_amount.
            -- Without the payload, this fact only answers "an event of type X occurred at
            -- instant T", and there is no question only it answers. With the payload, it is
            -- the auditable log inside the warehouse: anyone who wants to check the fold
            -- against its origin can, without going back to RAW. It costs 17.5 MB of text
            -- before compression. It is NOT a second derivation of the fold — it is its
            -- ORIGIN, which is exactly the difference between audit and true duplication.
            --
            -- EXPLICIT CAST TO VARCHAR, not a JSON falling into the type map. _TYPE_MAP
            -- refuses what it does not know precisely so as not to silently guess VARCHAR;
            -- here the choice is visible in the query itself, and the column name carries
            -- the type. Whoever wants VARIANT parses it on the Snowflake side, where there
            -- is an engine that understands it.
            --
            -- `partition_wh` IS LEFT OUT. It is a Silver audit column — it exists so that
            -- assert_order_partition_axis_matches_record can compare the file path with the
            -- record content. Carrying it across the boundary would be loading a PROOF, not
            -- a fact, and the proof has already run on the other side.
            select
                ingestion_date,
                wh,
                event_id,
                event_type,
                event_version,
                order_id,
                sequence_no,
                producer,
                occurred_at,
                event_date,
                crosses_order_date,
                cast(payload as varchar)        as payload_json
            from silver_order_event
            order by ingestion_date, wh, order_id, sequence_no
        """,
    },
    {
        "name": "STG_ORDER_PREMISE",
        "grain": "(premise_key)",
        "sql": """
            -- THE DECLARED PREMISES, WHICH KEEP A MART FROM MEASURING AGAINST A THRESHOLD
            -- NOBODY USED. No count on this line on purpose: the seed grows.
            -- Direct sibling of STG_INGESTION_RUN: metadata, not a measure, promoted because
            -- without it the downstream mart has no way to be honest.
            --
            -- MART_FULFILLMENT_SLA counts violations of `sla_minutes_picking`. That number
            -- has an owner: the seed the generator read, whose sha256 is in the manifest of
            -- each partition. Rewriting it as a dbt var would create the second copy that
            -- diverges on the first edit — and nothing would fail, because counting zero
            -- violations against the wrong threshold looks exactly like counting zero
            -- against the right one.
            select *
            from order_premises
            order by premise_key
        """,
    },
    {
        "name": "STG_STOCK_PREMISE",
        "grain": "(premise_key)",
        "sql": """
            -- THE STOCK POLICY, kept apart from the order premises on purpose: those govern
            -- what a customer DOES, these what the warehouse HAS. MART_STOCK_HEALTH compares
            -- cover against `reorder_point_days`, and the number has to be the SAME one the
            -- Spark job used — not a copy of it in a dbt var.
            select *
            from stock_premises
            order by premise_key
        """,
    },
    {
        "name": "STG_STOCK_LEDGER",
        "grain": "(wh, source_product_id, stock_date)",
        "sql": """
            -- THE ONLY CUT THAT DOES NOT COME FROM A SOURCE. Stock is not observed anywhere
            -- in this project: this ledger is COMPUTED by the Spark job from the consumption
            -- measured in the orders plus the declared policy. `written_by` travels along so
            -- that whoever queries the warehouse finds that out without opening the docs —
            -- the same discipline that makes Silver never invent a currency.
            --
            -- IT IS NOT FILTERED BY WINDOW here: the ledger is already born with the orders'
            -- cut, because the consumption feeding it came from them. One more filter would
            -- create two places where the window lives.
            select
                wh,
                source_product_id,
                stock_date,
                opening_balance,
                units_received,
                units_demanded,
                units_fulfilled,
                units_short,
                closing_balance,
                reorder_units,
                reorder_eta,
                mean_daily_demand,
                days_of_cover,
                written_by
            from silver_stock_ledger
            order by wh, source_product_id, stock_date
        """,
    },
]


# --------------------------------------------------------------------------------
# DuckDB -> parquet local
# --------------------------------------------------------------------------------

# Type mapping. Deliberately short: if the cut produces a type that is not here, it must
# FAIL, not guess a VARCHAR and silently lose precision.
_TYPE_MAP = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "NUMBER(38,0)",
    "SMALLINT": "NUMBER(38,0)",
    "INTEGER": "NUMBER(38,0)",
    "BIGINT": "NUMBER(38,0)",
    "HUGEINT": "NUMBER(38,0)",
    "UBIGINT": "NUMBER(38,0)",
    "UINTEGER": "NUMBER(38,0)",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "VARCHAR": "VARCHAR",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
}


def _snowflake_type(duckdb_type: str) -> str:
    type_name = duckdb_type.upper().strip()
    if type_name.startswith("DECIMAL"):
        # DECIMAL(10,2) -> NUMBER(10,2). Decimal precision in currency does not become FLOAT.
        return "NUMBER" + type_name[len("DECIMAL"):]
    if type_name in _TYPE_MAP:
        return _TYPE_MAP[type_name]
    raise WarehouseCutError(
        f"DuckDB type with no Snowflake mapping: {duckdb_type!r}. "
        f"Add it to _TYPE_MAP instead of letting the cut fall into a VARCHAR."
    )


def describe(connection, sql: str) -> list[tuple[str, str]]:
    """Columns and types of the cut, without materializing any row."""
    rows = connection.execute(f"describe {sql}").fetchall()
    return [(r[0], r[1]) for r in rows]


def ddl_for(spec: dict, columns: list[tuple[str, str]], database: str) -> str:
    """`create or replace table` derived from the cut's own schema."""
    body = ",\n".join(
        f"    {name} {_snowflake_type(type_name)}" for name, type_name in columns
    )
    return (
        f"create or replace table {database}.{STAGE_SCHEMA}.{spec['name']} (\n"
        f"{body}\n"
        f")\ncomment = '1:1 mirror of the Silver cut. Grain {spec['grain']}.'"
    )


def build(connection, out_dir: str) -> dict:
    """Writes one parquet per spec into out_dir. Returns the summary of each.

    Separate from `load` so the suite exercises every cut against real DuckDB fixtures,
    without Snowflake and without network — the queries are where silent errors live
    (the 'Total' aggregate summed with the detail, the forgotten geographic scope), not
    the transport.
    """
    os.makedirs(out_dir, exist_ok=True)
    summary = {}
    for spec in SPECS:
        sql = spec["sql"].strip()
        columns = describe(connection, sql)
        target = os.path.join(out_dir, f"{spec['name']}.parquet")

        # Atomic write for the same reason as the rest of the repo: a file truncated by an
        # interruption must not look complete to the PUT that comes after.
        handle, temp_path = tempfile.mkstemp(dir=out_dir, prefix=".tmp-", suffix=".parquet")
        os.close(handle)
        try:
            connection.execute(
                f"copy ({sql}) to '{temp_path}' (format parquet, compression zstd)"
            )
            os.replace(temp_path, target)
        except BaseException:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

        row_count = connection.execute(f"select count(*) from ({sql})").fetchone()[0]
        summary[spec["name"]] = {
            "rows": row_count,
            "columns": len(columns),
            "bytes": os.path.getsize(target),
            "path": target,
            "grain": spec["grain"],
        }
    return summary


def dump_ddl(connection, database: str) -> str:
    """All of the STAGE DDL, with no Snowflake connection. For review and versioning."""
    parts = [
        f"-- Generated by retail_platform.warehouse_cut at {_utc_now()}.",
        "-- DO NOT edit by hand: the STAGE schema is derived from the cut in SPECS, and an",
        "-- edited DDL becomes a second place where the schema lives.",
        "",
        f"create database if not exists {database};",
        f"create schema if not exists {database}.{STAGE_SCHEMA};",
        "",
    ]
    for spec in SPECS:
        columns = describe(connection, spec["sql"].strip())
        parts.append(ddl_for(spec, columns, database) + ";")
        parts.append("")
    return "\n".join(parts)


def export(config, out_dir: str) -> dict:
    """Cuts Silver to local parquet. Does not talk to Snowflake."""
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    try:
        return build(connection, out_dir)
    finally:
        connection.close()
