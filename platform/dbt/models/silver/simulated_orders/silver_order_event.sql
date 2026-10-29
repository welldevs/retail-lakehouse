-- FAITHFUL PROJECTION of the order event log. No interpretation beyond typing the envelope.
--
-- GRAIN: event_id. Deterministic by contract — sha256(order_id|sequence_no) — and therefore
-- unique across the whole window, not just within the partition.
--
-- THE PAYLOAD STAYS AS JSON, ON PURPOSE, and this is the most important decision in this
-- model. Letting DuckDB infer the payload produces a STRUCT with the UNION of the fields of
-- the 12 types — measured, 32 fields — and that union depends on what sampling saw. A day
-- with no returns would not have `returned_amount` in the struct, and a downstream model
-- referencing it would stop COMPILING depending on the draw. It is the same trap that made
-- this Source's fingerprint cover the declared vocabulary instead of the observed one.
--
-- So the schema is DECLARED in `columns =`, never inferred, and the payload passes through
-- as JSON. Whoever needs a field extracts it by name, and extracting a missing field
-- returns NULL instead of breaking the build. The two tables that fold the log
-- (silver_order, silver_order_line) do exactly that.
--
-- NO is_latest_ingestion, and the reason is the same one already written for the Mercadona
-- models: here the multiple dates ARE the product, not a side effect of re-extraction. Each
-- ingestion_date is a day of operation, and stacking days is the point.
--
-- ingestion_date IS THE ORDER DATE, not the event date (CONTRACT.md section 1). An order
-- placed at 22:40 and delivered at 9:00 the next day leaves ALL its events in the partition
-- of the day it was placed. That is why this model ALSO exposes `event_date`: the two
-- questions — "orders placed on day D" and "events that occurred on day D" — are legitimate
-- and different.
--
-- partition_wh EXISTS TO BE RECONCILED, for the same reason as silver_customer: the
-- record's `wh` field and the hive segment `wh=` have the same name and DuckDB
-- deduplicates them into a single column, with the record's value winning. Without a
-- second column, a partition landed under the wrong directory would go unnoticed.
--
-- This model assumes at least one landed file (read_json fails on an empty glob).
-- When the source has not landed anything yet, `make silver` excludes it from the build via
-- `retail-platform has-data simulated_orders` — the same treatment as the other four.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_order_event.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh,
    regexp_extract(filename, 'wh=([^/]+)', 1) as partition_wh,

    event_id,
    event_type,
    event_version,
    order_id,
    sequence_no,
    producer,

    occurred_at,
    cast(occurred_at as date)                 as event_date,
    -- An event may occur after midnight of the order day. Flagging that is more honest
    -- than letting the consumer find out by comparing the two columns.
    cast(occurred_at as date) <> ingestion_date as crosses_order_date,

    payload

from read_json(
    '{{ var("orders_raw_prefix") }}/ingestion_date=*/wh=*/order_events.jsonl',
    format = 'newline_delimited',
    hive_partitioning = 1,
    filename = true,
    -- DECLARED, never inferred. See the header.
    columns = {
        event_id: 'VARCHAR',
        event_type: 'VARCHAR',
        event_version: 'INTEGER',
        occurred_at: 'TIMESTAMP',
        order_id: 'VARCHAR',
        sequence_no: 'INTEGER',
        wh: 'VARCHAR',
        producer: 'VARCHAR',
        payload: 'JSON'
    }
)
