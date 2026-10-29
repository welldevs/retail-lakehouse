-- 24 ROWS THAT KEEP A MART FROM LYING.
--
-- GRAIN: (source_name, ingestion_date, wh). TYPE: observed (operational metadata).
--
-- WHAT IT ANSWERS, and no other table answers: the difference between "there was no price"
-- and "THERE WAS NO OBSERVATION". Days 2026-08-17 to 08-23 do not exist in the catalog,
-- and not because nothing happened — because nobody looked, and the API only serves
-- today's price, so it will never be able to look. Without this table, a product missing
-- on a day is indistinguishable from a whole day missing, and any time series silently
-- interpolates the gap.
--
-- It is metadata promoted to a fact on purpose. It costs 24 rows; what it buys is the
-- ability of a mart to say "I don't know" instead of guessing. CR-005 added 6 of them to
-- promote started_at_utc/finished_at_utc/duration_seconds, which already existed two
-- layers below.
{{ config(materialized = 'table') }}

select
    {{ date_key('ingestion_date') }} as date_key,
    source_name,
    ingestion_date,
    wh,
    run_id,
    complete,
    declared_rows,
    failure_count,
    anomaly_count,

    -- CR-005: they already existed two layers below (Silver) and were dropped only by a fixed
    -- list of columns in the STAGE projection — not missing data, a forgotten projection.
    started_at_utc,
    finished_at_utc,
    duration_seconds,

    -- A run is only a valid observation if it completed and recorded no failure. Anything
    -- else is a suspect partition, and the consumer needs to know without having to read two
    -- columns and remember the rule.
    (complete and failure_count = 0)                        as is_valid_observation
from {{ source('stage', 'STG_INGESTION_RUN') }}
