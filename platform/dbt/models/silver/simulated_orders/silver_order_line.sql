-- Estado FINAL de cada linha do pedido, depois de substituicoes e remocoes.
--
-- GRAO: (order_id, line_no). `line_no` e a posicao na cesta COLOCADA e nunca e reaproveitado:
-- uma linha substituida mantem o mesmo line_no e troca o produto, uma linha removida mantem o
-- line_no e sai da conta. E o que permite reconstruir o que foi pedido e o que foi entregue
-- sem perder a correspondencia entre os dois.
--
-- POR QUE A LINHA COLOCADA E A LINHA FINAL CONVIVEM NA MESMA TABELA. `source_product_id` e o
-- que o cliente pediu; `fulfilled_source_product_id` e o que ele recebeu. Guardar so o segundo
-- apagaria a substituicao — que e exatamente o fato que o modelo de eventos existe para
-- registrar. Guardar so o primeiro faria a receita nao fechar.
--
-- `line_status`: placed | fulfilled | substituted | removed | not_picked.
--
-- `not_picked` EXISTE PORQUE O OLTP O EXIGIU. Ate o Marco 4, este modelo rotulava `fulfilled`
-- toda linha que nao tivesse sido substituida nem removida — INCLUSIVE as de pedido cancelado
-- ou com pagamento recusado, que nunca chegaram a separacao. Medido: 298 pedidos, 5.508 linhas
-- afirmando terem sido cumpridas por um pedido que morreu antes de alguem tocar nele.
--
-- O log nunca disse isso. `order_picked` e o unico evento que declara separacao, e ele nao
-- ocorre nesses pedidos; `else 'fulfilled'` era uma afirmacao do modelo, nao da fonte. O
-- defeito so apareceu ao replicar o mesmo log num OLTP, onde a linha nasce `placed` e so vira
-- outra coisa quando um evento a muda — dois folds independentes discordando e a unica forma
-- barata de achar isto.
--
-- `placed` cobre o pedido ainda em voo (nem separado, nem terminal). Nao ocorre na janela
-- atual — todos os 6.400 pedidos sao terminais — e existe para que o vocabulario seja
-- completo em vez de completo por sorte.
--
-- PRECO E O OBSERVADO, e a Source ja provou isso: `validate` reconfere, linha a linha, que o
-- unit_price bate com o catalogo daquele (armazem, price_as_of). Aqui ele so e tipado. O teste
-- dbt que refaz a conferencia contra silver_product_price existe porque esta e a primeira vez
-- que o numero atravessa RAW e parquet — a mesma logica de
-- assert_customer_lives_in_its_warehouse_service_area ser o terceiro check da Fase 1.
--
-- price_as_of E price_source VIAJAM JUNTO DO NUMERO. Sem eles, comparar receita entre dias
-- compararia precos de vintages diferentes sem aviso — a mesma disciplina da var `currency`,
-- que o Gold materializa "para que a premissa viaje junto do numero".
{{ config(
    location = 's3://retail-lakehouse/silver/silver_order_line.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with colocadas as (

    select
        ingestion_date,
        wh,
        order_id,
        occurred_at as placed_at,
        json_extract_string(payload, '$.customer_id')               as customer_id,
        cast(json_extract_string(payload, '$.price_as_of') as date) as price_as_of,
        json_extract_string(payload, '$.price_source')              as price_source,
        unnest(
            from_json(
                payload -> '$.lines',
                '[{"line_no":"INTEGER","source_product_id":"VARCHAR","category_id":"INTEGER",
                   "subgroup_id":"INTEGER","demand_group":"VARCHAR","quantity":"INTEGER",
                   "unit_price":"VARCHAR"}]'
            )
        ) as linha
    from {{ ref('silver_order_event') }}
    where event_type = 'order_placed'

),

substituicoes as (

    select
        order_id,
        cast(json_extract_string(payload, '$.line_no') as integer)  as line_no,
        json_extract_string(payload, '$.substitute_source_product_id')
                                                                    as substitute_source_product_id,
        cast(json_extract_string(payload, '$.substitute_unit_price') as decimal(10, 2))
                                                                    as substitute_unit_price,
        occurred_at                                                 as substituted_at
    from {{ ref('silver_order_event') }}
    where event_type = 'order_line_substituted'

),

remocoes as (

    select
        order_id,
        cast(json_extract_string(payload, '$.line_no') as integer)  as line_no,
        json_extract_string(payload, '$.reason')                    as removal_reason,
        occurred_at                                                 as removed_at
    from {{ ref('silver_order_event') }}
    where event_type = 'order_line_removed'

),

-- O QUE ACONTECEU COM O PEDIDO, para decidir o que aconteceu com a linha. So dois fatos
-- interessam, e os dois vem de evento declarado: houve separacao, e houve fim antes dela.
marcos_do_pedido as (

    select
        order_id,
        max(case when event_type = 'order_picked' then 1 else 0 end)  as foi_separado,
        max(case when event_type in ('order_cancelled', 'order_payment_failed')
                 then 1 else 0 end)                                   as morreu_antes
    from {{ ref('silver_order_event') }}
    group by order_id

)

select
    c.ingestion_date,
    c.wh,
    c.order_id,
    c.linha.line_no                                        as line_no,
    c.customer_id,
    c.placed_at,

    c.price_as_of,
    c.price_source,

    c.linha.source_product_id                              as source_product_id,
    c.linha.category_id                                    as category_id,
    c.linha.subgroup_id                                    as subgroup_id,
    -- Carimbado no evento pela Source, resolvido pela plataforma contra o de-para
    -- versionado. Vem do evento e nao de um join com o catalogo aqui, pelo mesmo motivo de
    -- `unit_price`: o que importa e o grupo que valia NO MOMENTO DO PEDIDO, e nao o que o
    -- de-para diria hoje.
    c.linha.demand_group                                   as demand_group,
    c.linha.quantity                                       as quantity,
    cast(c.linha.unit_price as decimal(10, 2))             as unit_price,

    case
        when r.line_no is not null       then 'removed'
        when s.line_no is not null       then 'substituted'
        when m.foi_separado = 1          then 'fulfilled'
        when m.morreu_antes = 1          then 'not_picked'
        else 'placed'
    end                                                    as line_status,

    -- O que o cliente recebeu. NULL quando a linha foi removida: null aqui significa "nao
    -- existe", nao "desconhecido" — mesma disciplina de house_number na Fase 1.
    case
        when r.line_no is not null then null
        when s.line_no is not null then s.substitute_source_product_id
        when m.foi_separado = 1    then c.linha.source_product_id
        else null
    end                                                    as fulfilled_source_product_id,
    case
        when r.line_no is not null then null
        when s.line_no is not null then s.substitute_unit_price
        when m.foi_separado = 1    then cast(c.linha.unit_price as decimal(10, 2))
        else null
    end                                                    as fulfilled_unit_price,

    -- Valor que a linha efetivamente contribuiu. Zero para linha removida, para que a soma
    -- por pedido feche com net_amount sem nenhum filtro do consumidor.
    case
        when r.line_no is not null then cast(0 as decimal(12, 2))
        when s.line_no is not null then cast(s.substitute_unit_price * c.linha.quantity as decimal(12, 2))
        when m.foi_separado = 1    then cast(cast(c.linha.unit_price as decimal(10, 2)) * c.linha.quantity as decimal(12, 2))
        -- Linha de pedido que nunca foi separado contribui ZERO, pelo mesmo motivo que a
        -- removida contribui zero: `line_amount` e o que a linha entregou, e ela nao
        -- entregou nada. O valor pedido continua em `line_amount_placed`, ao lado.
        else cast(0 as decimal(12, 2))
    end                                                    as line_amount,

    cast(cast(c.linha.unit_price as decimal(10, 2)) * c.linha.quantity as decimal(12, 2))
                                                           as line_amount_placed,

    s.substituted_at,
    r.removed_at,
    r.removal_reason

from colocadas c
left join substituicoes s on c.order_id = s.order_id and c.linha.line_no = s.line_no
left join remocoes      r on c.order_id = r.order_id and c.linha.line_no = r.line_no
join marcos_do_pedido   m on c.order_id = m.order_id
