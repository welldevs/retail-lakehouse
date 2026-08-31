-- LINHA DE PEDIDO — e o `unit_price_paid` e MEDIDA DEGENERADA, nao atributo de dimensao.
--
-- GRAO: (order_id, line_no). 120.693 linhas. TIPO: synthetic (a linha) sobre observed
--       (produto, preco e data de catalogo, todos copiados da fonte real).
--
-- POR QUE O PRECO PAGO E MEDIDA E NAO ATRIBUTO DE DIM_PRODUCT. Preco varia por armazem e
-- por dia (187 produtos divergem entre armazens no mesmo dia, medido), entao ele nunca
-- poderia ser atributo de uma dimensao global de produto — dim_product.sql ja registra
-- isso: "rastrear preco aqui criaria uma versao nova de produto a cada oscilacao e a
-- dimensao viraria o fato". O preco vive em FACT_PRICE_SNAPSHOT, e o que esta linha
-- carrega e o preco que ESTE pedido pagou, que pode diferir por substituicao.
--
-- MEDIDO: `unit_price_paid` bate com o catalogo de (wh, price_as_of, produto CUMPRIDO) em
-- 112.864 de 112.864 linhas cumpridas ou substituidas — zero divergencias, zero ausencias.
-- O fecho contra FACT_PRICE_SNAPSHOT e um teste, e ele junta pelo produto CUMPRIDO: numa
-- linha substituida o cliente pagou o preco do SUBSTITUTO, e conferir contra o produto
-- pedido faria o teste reprovar por estar comparando a coisa errada.
--
-- DOIS product_sk, DE PROPOSITO. `product_sk` e o que foi pedido; `fulfilled_product_sk` e
-- o que foi entregue. Nas 4.670 linhas substituidas eles diferem, e essa diferenca e o
-- unico registro de que houve substituicao — carregar so um dos dois apagaria o fato que o
-- modelo de eventos existe para guardar.
--
-- line_amount E ZERO, NAO NULO, para linha removida ou de pedido nunca separado. Zero e o
-- que a linha contribuiu; somar a coluna por pedido fecha com net_amount sem nenhum filtro
-- do consumidor, e um consumidor que precise filtrar e um consumidor que vai esquecer.
{{ config(materialized = 'table') }}

with linhas as (

    select * from {{ source('stage', 'STG_ORDER_LINE') }}

),

produto as (

    select product_sk, source_product_id, valid_from, valid_to
    from {{ ref('dim_product') }}

)

select
    cast(to_char(l.ingestion_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    l.ingestion_date                                        as order_date,
    l.order_id,
    l.line_no,
    l.wh,
    l.customer_id,

    l.line_status,

    -- O QUE FOI PEDIDO. A versao da dimensao vigente em price_as_of, e nao a corrente:
    -- e o que liga a linha ao nome que o produto tinha no dia em que foi comprado.
    l.source_product_id,
    ped.product_sk,
    l.category_id,
    l.subgroup_id,
    -- GRUPO DE DEMANDA carimbado no evento pela Source. Atravessa a fronteira porque e o
    -- eixo em que a cesta foi CALIBRADA: agrupar por category_id no mart mede a arvore da
    -- Mercadona, e agrupar por aqui mede a decisao de demanda.
    l.demand_group,

    -- O QUE FOI ENTREGUE. Nulo quando a linha foi removida ou o pedido nunca chegou a
    -- separacao: nulo aqui significa "nao existe", nao "desconhecido".
    l.fulfilled_source_product_id,
    cum.product_sk                                          as fulfilled_product_sk,
    (l.fulfilled_source_product_id is distinct from l.source_product_id
        and l.fulfilled_source_product_id is not null)      as was_substituted,

    l.quantity,
    -- Preco de catalogo do produto PEDIDO, na data do pedido.
    l.unit_price,
    -- MEDIDA DEGENERADA: o preco efetivamente pago pela linha. Igual a unit_price em linha
    -- cumprida; o preco do substituto em linha substituida; nulo quando nada foi entregue.
    l.fulfilled_unit_price                                  as unit_price_paid,
    l.line_amount,
    l.line_amount_placed,
    l.line_amount_placed - l.line_amount                    as line_amount_delta,
    '{{ var("currency") }}'                                 as currency,

    -- A PREMISSA VIAJA JUNTO DO NUMERO. Sem price_as_of e price_source, comparar receita
    -- entre dias compararia precos de vintages diferentes sem aviso.
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
