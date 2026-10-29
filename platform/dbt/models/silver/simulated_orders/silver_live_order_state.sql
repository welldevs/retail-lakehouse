-- THE LIVE PROJECTION, read from the Iceberg table that two processes write.
--
-- GRAIN: order_id. It is the same grain as `silver_order`, on purpose: this model exists
-- ONLY so that the reconciliation between the streaming path and the batch path is a
-- query, and not a loose script. It adds no fact at all to the Lakehouse.
--
-- WHY THE METADATA PATH COMES FROM A VAR, AND IS NOT HARD-CODED HERE.
--
-- DuckDB **refuses** to discover on its own which is the current metadata of an Iceberg
-- table. Its message is explicit: "globbing the filesystem to locate the latest version is
-- disabled by default as this is considered unsafe and could result in reading uncommitted
-- data". There is a shortcut — `SET unsafe_enable_version_guessing = true` — that was
-- measured, works, and was REJECTED: reading uncommitted metadata is exactly what a
-- concurrent read with two writers must not do.
--
-- The one who knows which metadata is current is the CATALOG. `make silver` asks it
-- (`retail-platform iceberg-metadata`) and passes the answer as a var. The authority stays
-- in a single place, with no unsafe flag and without reimplementing a catalog convention.
--
-- The model stays OUT of `dbt build` while the table does not exist, through the same
-- `has-data` guard as the other sources: a fresh clone of the repository must not break
-- because of a stream plane nobody started.
--
-- `written_by` is PROVENANCE: which of the two writers touched the row last. Without it,
-- "both writers really wrote" would be a claim about a log, and not a queryable fact.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_live_order_state.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    order_id,
    wh,
    order_date,
    customer_id,
    status                                             as order_status,
    last_sequence_no,
    events_applied,

    placed_at,
    confirmed_at,
    picking_started_at,
    picked_at,
    dispatched_at,
    terminal_at,
    updated_at,

    line_count                                         as line_count_placed,
    picked_line_count                                  as line_count_picked,
    substituted_lines,
    removed_lines,

    cast(gross_amount as decimal(12, 2))               as gross_amount_placed,
    cast(net_amount   as decimal(12, 2))               as net_amount,
    cast(returned_amount as decimal(12, 2))            as returned_amount,
    delivered_within_slot,
    cast(picking_minutes as decimal(10, 2))            as picking_minutes,
    sla_breached,

    written_by

from iceberg_scan('{{ var("live_order_state_metadata") }}')
