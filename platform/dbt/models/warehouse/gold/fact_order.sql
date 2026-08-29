-- PEDIDO — ACCUMULATING SNAPSHOT, e este padrao so existe porque ha eventos.
--
-- GRAO: order_id. 6.400 linhas. TIPO: synthetic (o pedido) sobre observed (quem compra,
--       o que se compra, quanto custa e onde mora).
--
-- POR QUE ACCUMULATING SNAPSHOT E NAO FATO TRANSACIONAL. Uma linha por pedido que se
-- PREENCHE conforme o pedido avanca: cada marco tem sua coluna de instante, e a duracao
-- entre marcos e medida, nao estimada. Isso e impossivel de construir a partir de uma
-- fotografia de estado — uma tabela `orders` com `status` diria onde o pedido esta, nunca
-- quanto tempo levou para chegar la. As duracoes desta tabela sao a resposta concreta a
-- pergunta "o que o log de eventos comprou que a fotografia nao compraria".
--
-- NULL AQUI SIGNIFICA "NAO ALCANCADO", NUNCA "DESCONHECIDO". Um pedido cancelado tem
-- picked_at nulo porque a separacao nao aconteceu; para os 6.400 pedidos desta janela,
-- todos terminais, nulo significa "nunca aconteceu e nunca vai acontecer". Quando a janela
-- passar a conter pedidos em voo, nulo passara a significar tambem "ainda nao" — e a
-- coluna `is_terminal` e o que distingue os dois casos sem adivinhacao.
--
-- CUSTOMER_SK RESOLVE A VERSAO VIGENTE NA DATA DO PEDIDO, que e literalmente o que
-- dim_customer.sql ja mandava fazer e ate aqui nenhum fato fazia. E o que finalmente faz o
-- SCD2 pagar por si: DIM_CUSTOMER tem 40.000 linhas (duas geracoes, 08-24 e 08-27) e os
-- pedidos de 08-24 a 08-26 apontam para a primeira, os de 08-27 para a segunda.
--
-- MEDIDO, E O NUMERO IMPORTA: a versao resolvida por este range join coincide com
-- `customer_ingestion_date` — que a Source gravou no proprio evento order_placed — nos
-- 6.400 casos. Sao dois caminhos independentes (a escolha do gerador em Python e uma
-- juncao por intervalo em SQL) chegando ao mesmo lugar, e um teste dbt vigia a coincidencia
-- em vez de confiar nela.
--
-- net_amount E NULO PARA QUEM NUNCA FOI SEPARADO, e isso e uma decisao corrigida, nao um
-- descuido: 298 pedidos (cancelados ou com pagamento recusado) morreram antes de alguem
-- tocar na cesta. Preencher com gross_amount_placed afirmaria um valor que ninguem apurou.
{{ config(materialized = 'table') }}

with pedidos as (

    select * from {{ source('stage', 'STG_ORDER') }}

),

-- A versao de cliente vigente na data do pedido. Intervalo fechado-aberto, o mesmo de
-- fact_price_snapshot: `valid_to` e o valid_from da PROXIMA versao, entao `<` e nao `<=`.
cliente as (

    select customer_sk, customer_id, valid_from, valid_to
    from {{ ref('dim_customer') }}

)

select
    cast(to_char(o.ingestion_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    o.ingestion_date                                        as order_date,
    o.order_id,
    o.wh,

    c.customer_sk,
    o.customer_id,
    -- A geracao que a Source declarou ter usado. Viaja junto de customer_sk para que a
    -- coincidencia entre os dois caminhos seja CONSULTAVEL, e nao so testada.
    o.customer_ingestion_date,
    c.valid_from                                            as customer_version_from,

    -- Mesmo hash natural de dim_customer e dim_geography. Junta por codigo, nunca por
    -- nome: os dois produtos do INE grafam municipio diferente em 370 de 370 casos.
    md5(concat_ws('|', o.province_code, o.municipality_code, o.postal_code)) as geography_sk,
    o.province_code,
    o.municipality_code,
    o.postal_code,

    o.order_status,
    o.is_terminal,
    -- DELIVERED nao e terminal: uma devolucao ainda pode vir depois. 61 dos 6.046 pedidos
    -- entregues viraram RETURNED, e e por isso que o funil se apoia em marco e nao em
    -- status (ver mart_order_funnel).
    o.last_event_type,

    -- ---- os marcos --------------------------------------------------------------
    o.placed_at,
    o.confirmed_at,
    o.payment_failed_at,
    o.cancelled_at,
    o.picking_started_at,
    o.picked_at,
    o.dispatched_at,
    o.delivered_at,
    o.delivery_failed_at,
    o.returned_at,
    o.last_event_at,

    -- ---- as duracoes, que so existem porque os marcos existem ---------------------
    -- Cada uma mede o intervalo entre DOIS marcos declarados, e e nula quando qualquer um
    -- dos dois nao ocorreu. Nunca coalesce para zero: zero minuto e uma medicao, ausencia
    -- de medicao nao e.
    datediff('minute', o.placed_at,          o.confirmed_at)   as minutes_to_confirm,
    datediff('minute', o.confirmed_at,       o.picking_started_at) as minutes_to_picking,
    datediff('minute', o.picking_started_at, o.picked_at)      as minutes_to_pick,
    datediff('minute', o.picked_at,          o.dispatched_at)  as minutes_to_dispatch,
    datediff('minute', o.dispatched_at,      o.delivered_at)   as minutes_to_deliver,
    datediff('minute', o.placed_at,          o.delivered_at)   as minutes_placed_to_delivered,
    o.lifecycle_minutes,

    -- ---- a promessa comercial ----------------------------------------------------
    -- Janela de entrega, nao rota: o Callejero nao tem coordenada nem adjacencia, entao
    -- tempo de rota seria inventado. O que se mede aqui e o cumprimento de uma promessa.
    o.delivery_slot_start,
    o.delivery_slot_end,
    o.delivered_within_slot,

    -- ---- as medidas --------------------------------------------------------------
    o.line_count_placed,
    o.line_count_picked,
    o.substituted_lines,
    o.removed_lines,

    o.gross_amount_placed,
    o.net_amount,
    -- A diferenca que so existe porque ha eventos: a cesta muda DEPOIS da colocacao.
    -- Positiva quando encolheu na separacao; negativa quando um substituto saiu mais caro.
    o.amount_delta,
    o.returned_amount,
    '{{ var("currency") }}'                                 as currency,

    o.payment_method,
    o.decline_reason,
    o.cancelled_by,
    o.cancellation_reason,
    o.delivery_failure_reason,

    o.price_as_of,
    o.price_source,
    o.event_count,
    o.last_sequence_no
from pedidos o
left join cliente c
  on  c.customer_id     = o.customer_id
 and  o.ingestion_date >= c.valid_from
 and  (c.valid_to is null or o.ingestion_date < c.valid_to)
