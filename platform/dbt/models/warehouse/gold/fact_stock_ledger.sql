-- STOCK — PERIODIC SNAPSHOT FACT, and this is the warehouse's third fact pattern.
--
-- GRAIN: (stock_date, wh, source_product_id). One row per series per day of the window,
--       including days with no sales.
-- TYPE: synthetic (the coverage policy) over observed (consumption, which came from the
--       orders, which came from the real catalog).
--
-- WHY PERIODIC SNAPSHOT AND NOT TRANSACTIONAL. FACT_ORDER_EVENT is transactional (one row
-- per event); FACT_ORDER is accumulating (one row that fills in). This is the third case:
-- the BALANCE is neither an event nor a life cycle — it is the state of a shelf at an
-- instant, and the natural question about it ("how much was there on day X") requires the
-- day with no movement to ALSO have a row. A transactional fact of movements would leave
-- the reader to rebuild the balance with a window function, which is exactly the
-- computation this project measured NOT to be enough.
--
-- THE ROWS FOR IDLE DAYS ARE NOT PADDING. They are where replenishment arrives: a product
-- that has not sold for three days has a balance on those three days, and an arrival on
-- one of them changes the following balance. Without them, `days_of_cover` could not mean
-- anything.
--
-- ADDITIVITY, and each column's name says which. The MOVEMENT columns
-- (units_received/demanded/fulfilled/short, reorder_units) are additive on any axis.
-- The BALANCE ones (opening_balance, closing_balance) are semi-additive: they sum across
-- products and warehouses, they do NOT sum across days — summing Monday's balance with
-- Tuesday's counts the same stock twice. `days_of_cover` is not additive on any axis: it
-- is a ratio, and its average over products is an average of ratios, not the ratio of the
-- sums.
--
-- THIS FACT'S WINDOW IS NOT FACT_ORDER'S, and the difference is real: consumption is
-- dated by PICKING, and an order placed late on the last day is picked the next day. The
-- ledger sees one day more than the orders. Equalizing the two windows would require
-- discarding observed consumption to make two tables look symmetric.
{{ config(materialized = 'table') }}

with produto as (

    select product_sk, source_product_id, valid_from, valid_to
    from {{ ref('dim_product') }}

),

ledger as (

    select * from {{ source('stage', 'STG_STOCK_LEDGER') }}

)

select
    -- ---- axes -------------------------------------------------------------------
    {{ date_key('l.stock_date') }} as date_key,
    l.stock_date,
    -- `wh` IS DIM_WAREHOUSE'S KEY, with no surrogate: the warehouse does not version and the
    -- natural key is a real Mercadona API parameter. Inventing a warehouse_sk here would create
    -- a vocabulary the rest of Gold does not use.
    l.wh,
    l.source_product_id,
    p.product_sk,

    -- ---- movement (additive) ----------------------------------------------------
    l.units_received,
    l.units_demanded,
    l.units_fulfilled,
    -- STOCKOUT: what demand asked for and the shelf did not have. It never became a negative
    -- balance, and the invariant that guarantees that is verified in Silver.
    l.units_short,
    l.reorder_units,
    l.reorder_eta,

    -- ---- balance (SEMI-additive: do not sum across days) ------------------------
    l.opening_balance,
    l.closing_balance,

    -- ---- ratios (NOT additive) --------------------------------------------------
    l.mean_daily_demand,
    l.days_of_cover,

    -- PROVENANCE all the way to Gold. It is how a warehouse query finds out that this column
    -- came from an engine outside Python — the property that justified Iceberg.
    l.written_by

from ledger l
-- PRODUCT BY THE VERSION IN EFFECT ON THE DATE, as FACT_ORDER_ITEM does. The product
-- changes name and label over the window; pointing to the current version would make the
-- 24/08 balance carry the 01/09 attribute.
left join produto p
       on  p.source_product_id = l.source_product_id
      and  l.stock_date       >= p.valid_from
      and  (p.valid_to is null or l.stock_date < p.valid_to)
