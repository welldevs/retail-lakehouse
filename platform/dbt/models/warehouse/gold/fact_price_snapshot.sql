-- OBSERVED PRICE — and the PRESENCE OF THE ROW IS THE ASSORTMENT.
--
-- GRAIN: (snapshot_date, wh, source_product_id). 60,384 rows.
-- TYPE: observed.
--
-- IT IS CALLED SNAPSHOT, NOT DAILY, and the difference is not stylistic. There are
-- measured gaps: days 2026-08-17 to 08-23 do not exist and CANNOT be recovered — the
-- Mercadona API only serves today's price, and backfill is impossible for this source.
-- "Daily" would promise a continuity the source does not have, and a mart that assumed it
-- would silently interpolate the gap. Whoever needs to know whether a day was observed
-- queries FACT_INGESTION_RUN.
--
-- THE `wh` AXIS STAYS IN THE GRAIN, and that is measured: 187 products have a different
-- unit_price across the 4 warehouses on the same day, and 0 diverge within the same
-- warehouse. A price fact without `wh` would measure an average that exists in no
-- warehouse.
--
-- THERE IS NO FACT_ASSORTMENT, on purpose: the presence of a row here ALREADY is the
-- statement "this product was in this warehouse's catalog on this day". Measured that the
-- catalogs diverge (4,283 to 4,335 products per warehouse, only 3,859 in all four), so the
-- information is real — but a separate table would be the same information in a second
-- copy.
--
-- CAREFUL WHEN READING ABSENCE: a missing row may mean "product out of the catalog"
-- OR "day not observed". Only the cross with FACT_INGESTION_RUN distinguishes the two.
{{ config(materialized = 'table') }}

select
    {{ date_key('p.ingestion_date') }} as date_key,
    p.ingestion_date                                        as snapshot_date,
    p.warehouse                                             as wh,
    p.source_product_id,

    -- Points to the dimension VERSION in effect on the snapshot day, not the current version:
    -- it is what links an old price to the name the product had at the time.
    d.product_sk,
    p.primary_category_id,

    p.unit_price,

    -- THE PURCHASABLE-UNIT PRICE, next to the raw value. They are equal in 99.9% of rows; in
    -- the 10 product x warehouse combinations sold loose without `unit_size`, the API returns
    -- `reference_price * 99` — the weight selector's ceiling, and not a consumer price. It is
    -- against THIS column that FACT_ORDER_ITEM closes: the order charged the portion, not the
    -- ceiling.
    p.purchasable_unit_price,
    p.price_basis,
    p.net_content_kg_l,
    p.min_bunch_amount,

    p.bulk_price,
    p.reference_price,
    p.reference_format,
    p.previous_unit_price,
    p.tax_percentage,

    -- CONSUMER PREMISE, NOT SOURCE DATA. Mercadona does not declare a currency in any field
    -- (CONTRACT.md 4.6), and Silver does not invent one. The var has existed in dbt_project
    -- since day one precisely for Gold; materializing it here makes the premise travel with
    -- the number, instead of sitting only in a configuration file nobody reads when looking at
    -- a dashboard. Changing currency is editing the var, not hunting down 'EUR' scattered
    -- across models.
    '{{ var("currency") }}'                                 as currency,

    -- Pre-tax price, derived from the PURCHASABLE price. tax_percentage comes from the
    -- source; the division is this model's, which is why the column has its own name instead
    -- of overwriting unit_price. Deriving from the raw value would give 3,663.00 pre-tax for
    -- 150 g of langostino.
    case
        when p.tax_percentage is null or p.tax_percentage = 0 then null
        else round(p.purchasable_unit_price / (1 + p.tax_percentage / 100), 4)
    end                                                     as unit_price_ex_tax,

    p.is_pack,
    p.pack_size,
    p.unit_size,
    p.unit_name,
    p.selling_method,
    p.published,
    p.is_new_arrival
from {{ source('stage', 'STG_PRODUCT_PRICE') }} p
left join {{ ref('dim_product') }} d
  on  d.source_product_id = p.source_product_id
 and  p.ingestion_date   >= d.valid_from
 and  (d.valid_to is null or p.ingestion_date < d.valid_to)
