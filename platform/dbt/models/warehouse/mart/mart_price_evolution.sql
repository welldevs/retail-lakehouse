-- GRAIN: (snapshot_date, wh, source_product_id) — one row per product, warehouse and
--        OBSERVED day. Never per calendar day: see days_since_previous_snapshot.
--
-- QUESTION IT ANSWERS: how each product's price moved, in each warehouse, and how much
-- time passed since the previous observation.
--
-- THE COLUMN THAT PREVENTS THE WRONG READING is days_since_previous_snapshot. A change of
-- 0.10 EUR over 8 days (the real gap between 08-16 and 08-24) is not comparable to one of
-- 0.10 EUR in 1 day. Without that column, any average of daily change would mix the
-- two and nobody would notice.
--
-- TWO FAMILIES OF PRICE/CHANGE, ON PURPOSE. unit_price/price_delta/price_delta_pct
-- are RAW, inherited from fact_price_snapshot/fact_price_change for fidelity. In ~10
-- product x warehouse combinations (selling_method loose without unit_size), unit_price is the
-- CEILING of the weight selector (reference_price * 99) — 3,663.00 EUR for 150 g of prawns,
-- not a price a household pays (see silver_product_price.sql and DECISIONS.md, "Demand
-- calibration against MAPA 2025"). purchasable_unit_price/purchasable_price_delta/_pct are
-- the comparable price, and it is against THEM that change_type already classifies "changed". WHOEVER
-- MEASURES PRICE MOVEMENT OR PROMOTIONS MUST USE THE purchasable_* COLUMNS, never the raw ones.
{{ config(materialized = 'table') }}

select
    f.snapshot_date,
    f.date_key,
    d.year,
    d.year_month,
    f.wh,
    w.province_name,

    f.source_product_id,
    p.display_name,
    p.identity_ambiguous,
    c.category_name,
    c.parent_category_name,

    f.unit_price,
    f.purchasable_unit_price,
    f.unit_price_ex_tax,
    f.tax_percentage,

    ch.previous_unit_price,
    ch.price_delta,
    ch.price_delta_pct,
    ch.previous_purchasable_unit_price,
    ch.purchasable_price_delta,
    ch.purchasable_price_delta_pct,
    ch.change_type,
    ch.days_between_snapshots                               as days_since_previous_snapshot,

    -- New id with a name that already existed: it is not a "new product" until someone decides it is.
    -- The flag travels to the mart so that the decision belongs to whoever queries.
    coalesce(ch.name_seen_before, false)                    as identity_review_needed
from {{ ref('fact_price_snapshot') }} f
join {{ ref('dim_date') }} d       on d.date_key = f.date_key
join {{ ref('dim_warehouse') }} w  on w.wh = f.wh
left join {{ ref('dim_product') }} p  on p.product_sk = f.product_sk
left join {{ ref('dim_category') }} c on c.category_id = f.primary_category_id
left join {{ ref('fact_price_change') }} ch
       on  ch.snapshot_date     = f.snapshot_date
      and  ch.wh                = f.wh
      and  ch.source_product_id = f.source_product_id
