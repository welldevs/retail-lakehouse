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
-- THE COMPARISON IS `>=`, NOT `=`, and the difference is real: consumption is dated by PICKING,
-- and an order placed late on the last day is picked the next day. The ledger sees one
-- day MORE than the orders. Demanding equality would force discarding observed consumption to
-- make two tables look symmetric.
with pedidos as (

    select min(order_date) as de, max(order_date) as ate
    from {{ ref('fact_order') }}

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
