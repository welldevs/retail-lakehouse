-- O FUNIL FECHA CONTRA O FATO, E COBRE TODO DIA-ARMAZEM QUE EXISTE NO FATO.
--
-- O QUE PEGA, e por que a variante obvia nao pegaria. A variante obvia compararia
-- `sum(orders_placed)` do mart com `count(*)` do fato. Ela passa por engano em dois casos
-- reais e opostos:
--
--   * um dia-armazem SUMINDO do mart (o `cross join` de um limiar ausente esvaziando a
--     agregacao, um join interno com DIM_DATE perdendo uma data fora do calendario) e
--     outro dia INFLANDO pelo mesmo fanout — os dois erros se cancelam no total;
--   * o mart contando entregas por `order_status` em vez de por marco. Medido: sao 5.985
--     contra 6.046, os 61 pedidos devolvidos que TAMBEM foram entregues. Um total geral
--     continua batendo em `orders_placed` enquanto `orders_delivered` mente.
--
-- Por isso o teste e um FULL OUTER JOIN linha a linha, recalculando cada etapa do funil
-- direto de FACT_ORDER: linha faltando de um lado reprova, linha sobrando reprova, e cada
-- etapa e conferida separadamente contra a contagem por MARCO — que e a definicao que o
-- mart declara usar.
--
-- Falha com uma linha por (dia, armazem) que nao fecha.
with esperado as (

    select
        order_date,
        wh,
        count(*)                                            as orders_placed,
        count_if(confirmed_at is not null)                  as orders_confirmed,
        count_if(picked_at is not null)                     as orders_picked,
        count_if(dispatched_at is not null)                 as orders_dispatched,
        count_if(delivered_at is not null)                  as orders_delivered,
        count_if(returned_at is not null)                   as orders_returned
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
