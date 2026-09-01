-- GRAO: (order_date, wh, buyer_age_band, demand_group) — o que cada coorte de comprador
-- leva de cada grupo de demanda, por armazem e dia.
--
-- PERGUNTA QUE RESPONDE: a Fase 5 mudou alguma coisa? O agregado NAO se move de proposito
-- — esse e o criterio de aceitacao do IPF — entao nenhum total deste mart difere do que
-- MART_BASKET_DAILY ja mostrava. O que muda esta na CONDICIONAL, e este e o unico lugar do
-- warehouse onde ela e consultavel.
--
-- COMO LER `share_within_band`. E a fatia que aquele grupo ocupa DENTRO da coorte, e nao no
-- total. Comparar a mesma linha entre duas faixas etarias e a leitura util: se vinho pesa
-- 4x mais em GE65 que em LT35, e porque o informe mede 48,9% do volume domestico de vinho
-- em lares cujo responsavel tem 65 anos ou mais, contra 1,6% nos de menos de 35.
--
-- POR QUE A FAIXA VEM DO PEDIDO E NAO DO CLIENTE. `buyer_age_band` foi carimbada no evento
-- `order_placed` pela Source, e e ela que escolheu o vetor de pesos daquela cesta. Derivar
-- a faixa aqui, de `dim_customer.birth_year`, daria a faixa de HOJE e reimplementaria os
-- limites das faixas numa segunda linguagem — que divergiria da primeira no primeiro
-- ajuste, e a divergencia produziria um mix plausivel.
--
-- NAO ADITIVO ENTRE FAIXAS NA COLUNA DE PEDIDOS. `orders_in_cohort` conta pedidos
-- distintos daquela coorte que tocaram o grupo; somar as quatro faixas de um grupo NAO da
-- o total de pedidos do dia, pelo mesmo motivo que `orders_touching_category` nao soma
-- entre categorias em MART_BASKET_DAILY. Linhas, unidades e valor sao aditivos.
{{ config(materialized = 'table') }}

with linhas as (

    select
        i.order_date,
        i.date_key,
        i.wh,
        o.buyer_age_band,
        i.demand_group,
        i.order_id,
        i.line_status,
        i.quantity,
        i.line_amount,
        i.line_amount_placed
    from {{ ref('fact_order_item') }} i
    join {{ ref('fact_order') }} o on o.order_id = i.order_id

),

por_coorte as (

    select
        order_date,
        wh,
        buyer_age_band,
        count(*) as lines_in_band
    from linhas
    group by 1, 2, 3

)

select
    l.order_date,
    l.date_key,
    d.year_month,
    l.wh,
    l.buyer_age_band,
    l.demand_group,

    count(distinct l.order_id)                              as orders_in_cohort,

    count(*)                                                as lines_placed,
    count_if(l.line_status in ('fulfilled', 'substituted'))  as lines_fulfilled,
    sum(l.quantity)                                         as units_placed,

    sum(l.line_amount_placed)                               as revenue_placed,
    sum(l.line_amount)                                      as revenue_fulfilled,
    '{{ var("currency") }}'                                 as currency,

    -- A COLUNA QUE A FASE EXISTE PARA PRODUZIR. Denominador e a propria coorte naquele
    -- (dia, armazem), e nao o total: e assim que a comparacao entre faixas isola a
    -- propensao do tamanho da coorte.
    round(div0(count(*), max(b.lines_in_band)), 6)          as share_within_band
from linhas l
join {{ ref('dim_date') }} d on d.date_key = l.date_key
join por_coorte b
  on b.order_date = l.order_date
 and b.wh = l.wh
 and b.buyer_age_band = l.buyer_age_band
group by 1, 2, 3, 4, 5, 6
