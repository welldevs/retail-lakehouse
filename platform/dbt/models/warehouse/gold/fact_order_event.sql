-- O LOG DENTRO DO WAREHOUSE — fato transacional, um evento por linha.
--
-- GRAO: event_id. 44.456 linhas. TIPO: synthetic (o evento) — mas o event_id e
--       DETERMINISTICO, sha256(order_id|sequence_no), nunca uuid4: a Source nao tem
--       relogio nem entropia, e reexecutar o mesmo dia produz os mesmos ids.
--
-- POR QUE ELE EXISTE SE FACT_ORDER JA TEM OS MARCOS. FACT_ORDER guarda ATE 11 instantes
-- por pedido, um por marco previsto. Este fato guarda os eventos que NAO cabem numa coluna
-- de marco porque ocorrem N vezes: 4.670 substituicoes e 2.321 remocoes de linha, que sao
-- justamente as que fazem a cesta mudar depois da colocacao. Sem esta tabela, "quantas
-- linhas foram trocadas neste pedido" e uma contagem; "quando cada uma foi trocada, e em
-- que ordem" e impossivel.
--
-- DUAS DATAS, E A DIFERENCA E MEDIDA. `date_key` e o dia do PEDIDO — o eixo de particao,
-- imutavel, que mantem o fold inteiro num lugar so. `event_date_key` e o dia em que o
-- evento OCORREU. Elas divergem em 4.567 dos 44.456 eventos, e ha 1.156 eventos ocorridos
-- em 08-28 e 08-29, fora da janela de pedidos (08-24 a 08-27). Um fato com uma unica data
-- teria de escolher entre "onde o pedido mora" e "quando a coisa aconteceu", e qualquer
-- das duas escolhas mentiria para metade das perguntas. DIM_DATE cobre 2020-2030 de
-- proposito, entao as duas chaves resolvem.
--
-- O PAYLOAD CHEGA COMO VARIANT, E O PARSE ACONTECE AQUI. O STAGE transporta texto porque
-- transporte nao deve tipar nada; o GOLD tipa porque e onde ha um motor que entende
-- VARIANT. `try_parse_json` e nao `parse_json`: uma linha malformada deve virar nulo
-- visivel numa coluna, nao derrubar a construcao de 44 mil linhas — e um teste conta
-- quantos nulos ha, para que "visivel" nao dependa de alguem olhar.
--
-- minutes_since_previous_event ORDENA POR sequence_no, NAO POR occurred_at. O contrato
-- garante sequence_no contiguo por pedido (1..N, sem buracos, testado no Silver); dois
-- eventos podem compartilhar o mesmo instante, e ai ordenar por tempo produziria uma ordem
-- arbitraria que muda entre execucoes.
{{ config(materialized = 'table') }}

with eventos as (

    select * from {{ source('stage', 'STG_ORDER_EVENT') }}

)

select
    cast(to_char(e.ingestion_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    cast(to_char(e.event_date, 'YYYYMMDD') as number(38, 0))     as event_date_key,
    e.ingestion_date                                        as order_date,
    e.event_date,
    e.crosses_order_date,

    e.event_id,
    e.order_id,
    e.wh,
    e.sequence_no,
    e.event_type,
    e.event_version,
    e.producer,
    e.occurred_at,

    -- Repetida em vez de nomeada por `window w as (...)`: o Snowflake nao suporta a
    -- clausula WINDOW. O DuckDB suporta, e o repo ja pagou uma vez por essa diferenca.
    datediff('minute',
        lag(e.occurred_at) over (partition by e.order_id order by e.sequence_no),
        e.occurred_at)                                      as minutes_since_previous_event,
    (e.sequence_no = 1)                                     as is_first_event,
    (e.sequence_no = max(e.sequence_no) over (partition by e.order_id))
                                                            as is_last_event,

    -- O corpo do evento, tipado. Quem quiser conferir o fold contra a origem faz
    -- payload:picked_amount aqui, sem voltar ao RAW.
    try_parse_json(e.payload_json)                          as payload
from eventos e
