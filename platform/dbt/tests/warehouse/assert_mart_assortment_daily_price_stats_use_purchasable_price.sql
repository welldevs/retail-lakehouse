-- MART_ASSORTMENT_DAILY.min/max_unit_price MATCH THE MINIMUM/MAXIMUM OF
-- fact_price_snapshot.purchasable_unit_price AT THE SAME GRAIN — never the RAW unit_price.
--
-- WHY THIS TEST EXISTS. Until this fix, mart_assortment_daily aggregated the raw
-- fact_price_snapshot.unit_price. In ~10 product x warehouse combinations sold
-- loose without unit_size, the API returns unit_price = reference_price * 99 — the CEILING of the
-- weight selector (see assert_purchasable_price_is_anchored_in_the_bunch_amount.sql and
-- DECISIONS.md, "Demand calibration against MAPA 2025"), not a consumer price. That
-- inflated the maximum of the "Marisco y pescado" category to 3,663.00 EUR — an internally
-- consistent number, which is why none of the existing tests failed: the defect
-- only showed up when reading the mart on a panel, not when running `dbt build`.
--
-- Rebuilds the minimum/maximum FROM OUTSIDE, straight from fact_price_snapshot with the right column,
-- and fails if the mart does not match — the same discipline as
-- assert_order_funnel_totals_match_fact_order.sql, only for price instead of counts.
--
-- Fails with one row per divergent (snapshot_date, wh, category_id).
with esperado as (

    select
        f.snapshot_date,
        f.wh,
        f.primary_category_id                               as category_id,
        min(f.purchasable_unit_price)                        as min_esperado,
        max(f.purchasable_unit_price)                        as max_esperado
    from {{ ref('fact_price_snapshot') }} f
    group by 1, 2, 3

)
select
    m.snapshot_date, m.wh, m.category_id,
    m.min_unit_price, e.min_esperado,
    m.max_unit_price, e.max_esperado
from {{ ref('mart_assortment_daily') }} m
join esperado e
       on  e.snapshot_date = m.snapshot_date
      and  e.wh            = m.wh
      and  e.category_id   = m.category_id
where m.min_unit_price <> e.min_esperado
   or m.max_unit_price <> e.max_esperado
