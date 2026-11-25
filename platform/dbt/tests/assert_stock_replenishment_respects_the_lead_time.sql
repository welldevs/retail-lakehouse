-- EVERY ARRIVAL HAS AN ORDER THAT EXPLAINS IT, ISSUED `supplier_lead_days` DAYS EARLIER.
--
-- IT IS THE FEEDBACK-LOOP TEST, which is why it exists apart from balance conservation. Conservation
-- checks the arithmetic WITHIN a day; this one checks the loop ACROSS days, which
-- is the only reason this job is not SQL. A `sum() over (...)` would pass conservation —
-- it would just never produce an arrival.
--
-- THE THREE THINGS IT TIES TOGETHER:
--
--   1. No arrival without an order. `units_received > 0` on day D requires a row of the SAME
--      series on day D - lead time with equal `reorder_units`.
--   2. No order arrives early. `reorder_eta` must be exactly
--      `stock_date + supplier_lead_days`.
--   3. No order gets lost WITHIN the window. An order whose arrival would fit in the window
--      must have arrived. Those arriving AFTER the last day are legitimate and stay
--      out: an order in transit at the end of the period is a real property of any ledger, and
--      demanding it disappear would be demanding the model lie. (That was exactly the
--      wrong assertion that failed spike question S7 on its first run.)
--
-- THE LEAD TIME IS NOT A CONSTANT OF THIS FILE: it comes from `stock_premises`, which projects the seed the
-- job read. Hard-coding 2 here would create a second copy, and the day the seed changed this test
-- would start checking a loop no row went through.
with lead_time as (

    select cast(value as integer) as supplier_lead_days
    from {{ ref('stock_premises') }}
    where premise_key = 'supplier_lead_days'

),

horizon as (

    select max(stock_date) as last_day from {{ ref('silver_stock_ledger') }}

),

ledger as (

    select * from {{ ref('silver_stock_ledger') }}

),

-- 1. orphan arrival: received without anything having been ordered on the right day
arrival_without_order as (

    select
        c.wh, c.source_product_id, c.stock_date,
        'arrival without an order that explains it ' || p.supplier_lead_days || ' day(s) earlier' as reason
    from ledger c
    cross join lead_time p
    left join ledger o
           on o.wh = c.wh
          and o.source_product_id = c.source_product_id
          and o.stock_date = c.stock_date - p.supplier_lead_days
          and o.reorder_units = c.units_received
    where c.units_received > 0
      and o.wh is null

),

-- 2. eta outside the declared lead time
eta_errada as (

    select
        l.wh, l.source_product_id, l.stock_date,
        'reorder_eta <> stock_date + supplier_lead_days' as reason
    from ledger l
    cross join lead_time p
    where l.reorder_units > 0
      and l.reorder_eta is distinct from l.stock_date + p.supplier_lead_days

),

-- 3. an order that would fit in the window and never arrived
lost_order as (

    select
        o.wh, o.source_product_id, o.stock_date,
        'order with arrival inside the window that never arrived' as reason
    from ledger o
    cross join lead_time p
    cross join horizon j
    left join ledger c
           on c.wh = o.wh
          and c.source_product_id = o.source_product_id
          and c.stock_date = o.reorder_eta
          and c.units_received = o.reorder_units
    where o.reorder_units > 0
      and o.reorder_eta <= j.last_day
      and c.wh is null

)

select * from arrival_without_order
union all
select * from eta_errada
union all
select * from lost_order
