-- PRICE TRANSITION between EXISTING snapshots.
--
-- GRAIN: (snapshot_date, previous_snapshot_date, wh, source_product_id). 43,204 rows.
-- TYPE: observed (derived by diff, but from observed data).
--
-- WHY IT EXISTS, and it is not for convenience. The source's `price_decreased` field is
-- FALSE in 100% of rows while 152 prices changed between two partitions: the field the
-- source offers to flag a change flags nothing. The only way to detect a change is to diff
-- snapshots, and that fix has already been done and tested in Silver.
--
-- WHY IT IS NOT RE-DERIVED HERE from FACT_PRICE_SNAPSHOT with a window function: that
-- would create a SECOND implementation of the same gap-aware lag(), on another engine,
-- and the two would diverge at the first adjustment. Duplicating 43 thousand rows costs
-- less than duplicating logic. Besides, the grain is genuinely different — transition,
-- not snapshot.
--
-- previous_snapshot_date IS THE PREVIOUS EXISTING PARTITION, not "yesterday". There is an
-- 8-day gap between 08-16 and 08-24, and days_between_snapshots makes that visible: a
-- price change over 8 days is not comparable to one over 1 day, and a mart that ignored
-- the difference would be comparing different things.
--
-- price_delta/price_delta_pct ARE RAW (over unit_price), preserved for fidelity — same
-- criterion as silver_product_price. purchasable_price_delta/_pct, next to them, are the
-- price a consumer actually pays (see silver_price_change), and change_type already
-- classifies "changed" against IT, not against the raw one. Whoever reads price changes
-- to decide something (promotion, trend) wants the purchasable_* columns, not the raw ones.
{{ config(materialized = 'table') }}

select
    {{ date_key('c.ingestion_date') }} as date_key,
    c.ingestion_date                                        as snapshot_date,
    c.previous_ingestion_date                               as previous_snapshot_date,
    {{ datediff("'day'", "c.previous_ingestion_date", "c.ingestion_date") }} as days_between_snapshots,

    c.warehouse                                             as wh,
    c.source_product_id,
    d.product_sk,

    c.previous_unit_price,
    c.unit_price,
    c.price_delta,
    case
        when c.previous_unit_price is null or c.previous_unit_price = 0 then null
        else round(100 * c.price_delta / c.previous_unit_price, 4)
    end                                                     as price_delta_pct,

    c.previous_purchasable_unit_price,
    c.purchasable_unit_price,
    c.purchasable_price_delta,
    case
        when c.previous_purchasable_unit_price is null or c.previous_purchasable_unit_price = 0
            then null
        else round(100 * c.purchasable_price_delta / c.previous_purchasable_unit_price, 4)
    end                                                     as purchasable_price_delta_pct,

    c.change_type,
    c.catalog_appearances,

    -- IDENTITY REVIEW QUEUE, not a detail. A new id whose display_name already existed in the
    -- previous partition: the source does not say whether it is the same item re-keyed or an
    -- item withdrawn and another launched. This fact POINTS OUT the cases; it does not decide
    -- for them.
    c.name_seen_before
from {{ source('stage', 'STG_PRICE_CHANGE') }} c
left join {{ ref('dim_product') }} d
  on  d.source_product_id = c.source_product_id
 and  c.ingestion_date   >= d.valid_from
 and  (d.valid_to is null or c.ingestion_date < d.valid_to)
