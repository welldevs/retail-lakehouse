-- THE STOCK LEDGER COVERS THE WINDOW OF THE ORDERS THAT FED IT.
--
-- THE CONCRETE DEFECT THIS CATCHES, and it really happened on 2026-09-01.
--
-- `silver_stock_ledger` stays OUT of `dbt build` when the Iceberg table does not exist — and
-- that is deliberate, it is what keeps Spark optional. But the Silver PARQUET survives the
-- exclusion: it is what the last successful build left behind. So a `make silver` that
-- skips the model, followed by a `make warehouse`, publishes to Snowflake a ledger for a
-- window that no longer exists — and nothing breaks, because each row stays internally
-- consistent and every total keeps closing.
--
-- That is exactly what was measured: the regeneration took orders from 4 to 9 days, the ledger
-- stayed on the old 5 days, and MART_STOCK_HEALTH described a period different from all the
-- other marts without a single test failing.
--
-- IT IS A CROSS-DOMAIN FRESHNESS DEFECT, which no invariant WITHIN a domain catches —
-- the same family as the Phase 6 allocation defect. That is why the test lives here, in the
-- warehouse, where the two domains finally meet.
--
-- THE WINDOW COMPARED IS THE PICKING WINDOW, not the order window, because consumption is
-- dated by PICKING (stock_bridge.py: `cast(picked_at as date)`). Until CR-017 an order was
-- picked on the day it was placed, so the two windows nearly coincided and this test compared
-- order dates. Since CR-017 picking is scheduled backwards from the delivery slot — next
-- operating day at the earliest — so the first consumption is the day AFTER the first order,
-- and comparing order dates flagged a gap that is the model working as declared.
--
-- THE COMPARISON IS `>=`, NOT `=`: the ledger may see days the picking window does not (a
-- series with no demand on a day still has a balance). Demanding equality would force
-- discarding observed consumption to make two tables look symmetric.
with orders_window as (

    select min(cast(picked_at as date)) as from_date, max(cast(picked_at as date)) as to_date
    from {{ ref('fact_order') }}
    where picked_at is not null

),

stock_window as (

    select min(stock_date) as from_date, max(stock_date) as to_date
    from {{ ref('fact_stock_ledger') }}

)

select
    p.from_date   as orders_from,
    p.to_date  as orders_to,
    e.from_date   as stock_from,
    e.to_date  as stock_to,
    case
        when e.to_date < p.to_date
            then 'the ledger ends BEFORE the orders: stale stock published over a new window'
        when e.from_date > p.from_date
            then 'the ledger starts AFTER the orders: consumption at the start of the window without a balance'
    end    as reason
from orders_window p
cross join stock_window e
where e.to_date < p.to_date
   or e.from_date  > p.from_date
