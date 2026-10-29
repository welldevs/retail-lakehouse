-- GRAIN: (snapshot_date, wh, category_id) — one row per level-2 category, warehouse and
--        OBSERVED day.
--
-- QUESTION IT ANSWERS: how much catalog each warehouse has in each category, at what
-- price, and how much of it is exclusive to that warehouse.
--
-- ASSORTMENT IS DERIVED FROM THE ROW'S PRESENCE in the price fact, not from a table of its own:
-- a row in FACT_PRICE_SNAPSHOT asserts that the product was in that warehouse's catalog
-- on that day. Measured that this really varies — 4,283 to 4,335 products per warehouse, only
-- 3,859 in all four.
--
-- products_in_all_warehouses is the honest denominator for any comparison across
-- warehouses: comparing the average price of different catalogs measures the catalog
-- difference, not the price difference.
--
-- THE FOUR min/max/avg/median_unit_price ARE OVER purchasable_unit_price, NOT over raw
-- unit_price. Measured: in ~10 product x warehouse combinations sold loose without
-- unit_size, the API returns unit_price = reference_price * 99 (the ceiling of the weight
-- selector, never a consumer price) — a 37 EUR/kg frozen item inflated the category MAXIMUM
-- of "Marisco y pescado" to 3,663.00 EUR. purchasable_unit_price is the same value
-- on every other row and only diverges on those ~10 (see silver_product_price.sql and
-- DECISIONS.md, "Demand calibration against MAPA 2025") — using it here is the same correction
-- fact_price_snapshot.unit_price_ex_tax already applies, not a second decision.
{{ config(materialized = 'table') }}

with universal as (

    -- Products present in ALL FOUR warehouses on that day. Computed per day, and not
    -- once, because the catalog changes: a product may be universal on one date and not on
    -- another, and freezing that into a fixed list would lie about the older dates.
    select snapshot_date, source_product_id
    from {{ ref('fact_price_snapshot') }}
    group by 1, 2
    having count(distinct wh) = (select count(*) from {{ ref('dim_warehouse') }})

)

select
    f.snapshot_date,
    f.date_key,
    d.year_month,
    f.wh,

    f.primary_category_id                                   as category_id,
    c.category_name,
    c.parent_category_id,
    c.parent_category_name,

    count(*)                                                as products,
    {{ count_if('u.source_product_id is not null') }}         as products_in_all_warehouses,
    {{ count_if('u.source_product_id is null') }}             as products_exclusive_here,

    min(f.purchasable_unit_price)                           as min_unit_price,
    max(f.purchasable_unit_price)                           as max_unit_price,
    round(avg(f.purchasable_unit_price), 4)                 as avg_unit_price,
    {{ median('f.purchasable_unit_price') }}                  as median_unit_price,

    {{ count_if('f.is_new_arrival') }}                        as new_arrivals,
    {{ count_if('f.is_pack') }}                               as packs
from {{ ref('fact_price_snapshot') }} f
join {{ ref('dim_date') }} d          on d.date_key = f.date_key
left join {{ ref('dim_category') }} c on c.category_id = f.primary_category_id
left join universal u
       on  u.snapshot_date     = f.snapshot_date
      and  u.source_product_id = f.source_product_id
group by 1, 2, 3, 4, 5, 6, 7, 8
