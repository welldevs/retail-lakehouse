-- ORDER LINE — and `unit_price_paid` is a DEGENERATE MEASURE, not a dimension attribute.
--
-- GRAIN: (order_id, line_no). 120,693 rows. TYPE: synthetic (the line) over observed
--       (product, price and catalog date, all copied from the real source).
--
-- WHY THE PRICE PAID IS A MEASURE AND NOT A DIM_PRODUCT ATTRIBUTE. Price varies by
-- warehouse and by day (187 products diverge across warehouses on the same day, measured),
-- so it could never be an attribute of a global product dimension — dim_product.sql already
-- records this: "tracking price here would create a new product version at every
-- fluctuation and the dimension would become the fact". The price lives in
-- FACT_PRICE_SNAPSHOT, and what this line carries is the price THIS order paid, which may
-- differ due to substitution.
--
-- MEASURED: `unit_price_paid` matches the catalog of (wh, price_as_of, FULFILLED product)
-- in 112,864 of 112,864 fulfilled or substituted lines — zero divergences, zero misses.
-- The closure against FACT_PRICE_SNAPSHOT is a test, and it joins on the FULFILLED product:
-- on a substituted line the customer paid the SUBSTITUTE's price, and checking against the
-- ordered product would make the test fail for comparing the wrong thing.
--
-- TWO product_sk, ON PURPOSE. `product_sk` is what was ordered; `fulfilled_product_sk` is
-- what was delivered. In the 4,670 substituted lines they differ, and that difference is
-- the only record that there was a substitution — carrying only one of the two would erase
-- the fact the event model exists to keep.
--
-- line_amount IS ZERO, NOT NULL, for a removed line or one of an order never picked. Zero
-- is what the line contributed; summing the column per order closes with net_amount
-- without any consumer-side filter, and a consumer that needs to filter is a consumer that
-- will forget.
{{ config(materialized = 'table') }}

with linhas as (

    select * from {{ source('stage', 'STG_ORDER_LINE') }}

),

produto as (

    select product_sk, source_product_id, valid_from, valid_to
    from {{ ref('dim_product') }}

)

select
    {{ date_key('l.ingestion_date') }} as date_key,
    l.ingestion_date                                        as order_date,
    l.order_id,
    l.line_no,
    l.wh,
    l.customer_id,

    l.line_status,

    -- WHAT WAS ORDERED. The dimension version in effect at price_as_of, and not the current
    -- one: it is what links the line to the name the product had on the day it was bought.
    l.source_product_id,
    ped.product_sk,
    l.category_id,
    l.subgroup_id,
    -- DEMAND GROUP stamped on the event by the Source. It crosses the boundary because it is
    -- the axis on which the basket was CALIBRATED: grouping by category_id in the mart measures
    -- Mercadona's tree, and grouping by this measures the demand decision.
    l.demand_group,

    -- WHAT WAS DELIVERED. Null when the line was removed or the order never reached picking:
    -- null here means "does not exist", not "unknown".
    l.fulfilled_source_product_id,
    cum.product_sk                                          as fulfilled_product_sk,
    (l.fulfilled_source_product_id is distinct from l.source_product_id
        and l.fulfilled_source_product_id is not null)      as was_substituted,

    l.quantity,
    -- Catalog price of the ORDERED product, on the order date.
    l.unit_price,
    -- DEGENERATE MEASURE: the price actually paid for the line. Equal to unit_price on a
    -- fulfilled line; the substitute's price on a substituted line; null when nothing was
    -- delivered.
    l.fulfilled_unit_price                                  as unit_price_paid,
    l.line_amount,
    l.line_amount_placed,
    l.line_amount_placed - l.line_amount                    as line_amount_delta,
    '{{ var("currency") }}'                                 as currency,

    -- THE PREMISE TRAVELS WITH THE NUMBER. Without price_as_of and price_source, comparing
    -- revenue across days would compare prices of different vintages without warning.
    l.price_as_of,
    l.price_source,

    l.placed_at,
    l.substituted_at,
    l.removed_at,
    l.removal_reason
from linhas l
left join produto ped
  on  ped.source_product_id = l.source_product_id
 and  l.price_as_of        >= ped.valid_from
 and  (ped.valid_to is null or l.price_as_of < ped.valid_to)
left join produto cum
  on  cum.source_product_id = l.fulfilled_source_product_id
 and  l.price_as_of        >= cum.valid_from
 and  (cum.valid_to is null or l.price_as_of < cum.valid_to)
