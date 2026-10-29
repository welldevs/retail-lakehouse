-- THE FUNNEL CLOSES AGAINST THE FACT, AND COVERS EVERY DAY-WAREHOUSE THAT EXISTS IN THE FACT.
--
-- WHAT IT CATCHES, and why the obvious variant would not. The obvious variant would compare
-- the mart's `sum(orders_placed)` with the fact's `count(*)`. It passes by mistake in two real
-- and opposite cases:
--
--   * a day-warehouse VANISHING from the mart (the `cross join` of a missing threshold emptying the
--     aggregation, an inner join with DIM_DATE losing a date outside the calendar) and
--     another day INFLATING from the same fanout — the two errors cancel out in the total;
--   * the mart counting deliveries by `order_status` instead of by milestone. Measured: 5,985
--     against 6,046, the 61 returned orders that WERE ALSO delivered. A grand total
--     keeps matching on `orders_placed` while `orders_delivered` lies.
--
-- That is why the test is a row-by-row FULL OUTER JOIN, recomputing each funnel stage
-- straight from FACT_ORDER: a row missing on one side fails, an extra row fails, and each
-- stage is checked separately against the count by MILESTONE — which is the definition the
-- mart declares it uses.
--
-- Fails with one row per (day, warehouse) that does not close.
with esperado as (

    select
        order_date,
        wh,
        count(*)                                            as orders_placed,
        {{ count_if('confirmed_at is not null') }}            as orders_confirmed,
        {{ count_if('picked_at is not null') }}               as orders_picked,
        {{ count_if('dispatched_at is not null') }}           as orders_dispatched,
        {{ count_if('delivered_at is not null') }}            as orders_delivered,
        {{ count_if('returned_at is not null') }}             as orders_returned
    from {{ ref('fact_order') }}
    group by 1, 2

)

select
    coalesce(m.order_date, e.order_date)                    as order_date,
    coalesce(m.wh, e.wh)                                    as wh,
    e.orders_placed                                         as esperado_colocados,
    m.orders_placed                                         as no_mart_colocados,
    e.orders_delivered                                      as esperado_entregues,
    m.orders_delivered                                      as no_mart_entregues,
    case
        when m.wh is null then 'dia-armazem ausente do mart'
        when e.wh is null then 'dia-armazem que o fato nao tem'
        else 'contagem de etapa diverge'
    end                                                     as motivo
from esperado e
full outer join {{ ref('mart_order_funnel') }} m
             on m.order_date = e.order_date and m.wh = e.wh
where m.wh is null
   or e.wh is null
   or m.orders_placed      <> e.orders_placed
   or m.orders_confirmed   <> e.orders_confirmed
   or m.orders_picked      <> e.orders_picked
   or m.orders_dispatched  <> e.orders_dispatched
   or m.orders_delivered   <> e.orders_delivered
   or m.orders_returned    <> e.orders_returned
