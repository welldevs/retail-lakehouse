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
with pedidos as (

    select min(cast(picked_at as date)) as de, max(cast(picked_at as date)) as ate
    from {{ ref('fact_order') }}
    where picked_at is not null

),

estoque as (

    select min(stock_date) as de, max(stock_date) as ate
    from {{ ref('fact_stock_ledger') }}

)

select
    p.de   as pedidos_de,
    p.ate  as pedidos_ate,
    e.de   as estoque_de,
    e.ate  as estoque_ate,
    case
        when e.ate < p.ate
            then 'o ledger termina ANTES dos pedidos: estoque velho publicado sobre janela nova'
        when e.de > p.de
            then 'o ledger comeca DEPOIS dos pedidos: consumo do inicio da janela sem saldo'
    end    as violacao
from pedidos p
cross join estoque e
where e.ate < p.ate
   or e.de  > p.de
