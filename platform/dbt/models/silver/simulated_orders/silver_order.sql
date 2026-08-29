-- O FOLD. Estado e medidas de cada pedido, derivados do log de eventos.
--
-- GRAO: order_id. O id ja carrega o armazem e o dia (ord_mad1_20260824_000042), entao nao ha
-- colisao entre particoes e nao e preciso compor a chave.
--
-- ESTE MODELO E A RAZAO DE A SOURCE NAO GRAVAR ESTADO. Nao existe `orders.json` no RAW: duas
-- representacoes da mesma verdade divergem. O estado mora aqui, e e sempre reconstruivel do
-- log — o mesmo principio que faz DIM_PRODUCT ser SCD2 DERIVADO da historia em vez de
-- acumulado por MERGE ("acumular criaria estado que so existe no destino").
--
-- O FOLD E NAO-TRIVIAL, e e isso que justifica o modelo de eventos existir. `net_amount` NAO
-- e derivavel de `gross_amount_placed`: substituicao e remocao de linha alteram a cesta depois
-- da colocacao. Medido na janela de 2026-08-24 a 08-27: colocado 879.449,74 contra separado
-- 821.121,93 — 58.327,81 de diferenca que o primeiro evento nao sabia. Se os dois fossem
-- sempre iguais, o log seria um carimbo de data, e um teste dbt invertido vigia exatamente
-- isso.
--
-- O ESTADO SAI DO TIPO DO ULTIMO EVENTO, e isso e correto e nao atalho: a maquina de estados
-- do contrato mapeia cada tipo a UM estado resultante (STATE_AFTER em events.py), entao o
-- estado depois do ultimo evento e o fold da cadeia inteira. Os dois eventos de linha mapeiam
-- para PICKING, que e tambem o estado de onde saem — eles mudam a CESTA, nao o estado.
--
-- `max(case when ...)` e nao `FILTER (WHERE ...)`: o DuckDB aceita os dois, o Snowflake so o
-- primeiro. Este modelo e duckdb-only pelo guard do dbt_project.yml, mas escrever o dialeto
-- portavel custa zero e o repo ja pagou uma vez por essa diferenca (ARCHITECTURE.md, "quatro
-- defeitos que so apareceram executando").
--
-- DINHEIRO E DECIMAL, NUNCA DOUBLE. Os valores atravessam o log como string exatamente para
-- que a conversao aconteca uma vez, aqui, com escala declarada.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_order.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with eventos as (

    select
        *,
        row_number() over (partition by order_id order by sequence_no desc) as recencia
    from {{ ref('silver_order_event') }}

),

colocacao as (

    select
        order_id,
        ingestion_date,
        wh,
        occurred_at                                                    as placed_at,
        json_extract_string(payload, '$.customer_id')                  as customer_id,
        cast(json_extract_string(payload, '$.customer_ingestion_date') as date)
                                                                       as customer_ingestion_date,
        json_extract_string(payload, '$.province_code')                as province_code,
        json_extract_string(payload, '$.municipality_code')            as municipality_code,
        json_extract_string(payload, '$.postal_code')                  as postal_code,
        cast(json_extract_string(payload, '$.price_as_of') as date)    as price_as_of,
        json_extract_string(payload, '$.price_source')                 as price_source,
        cast(json_extract_string(payload, '$.delivery_slot_start') as timestamp)
                                                                       as delivery_slot_start,
        cast(json_extract_string(payload, '$.delivery_slot_end') as timestamp)
                                                                       as delivery_slot_end,
        cast(json_extract_string(payload, '$.line_count') as integer)  as line_count_placed,
        cast(json_extract_string(payload, '$.gross_amount') as decimal(12, 2))
                                                                       as gross_amount_placed
    from {{ ref('silver_order_event') }}
    where event_type = 'order_placed'

),

marcos as (

    select
        order_id,
        count(*)                                                           as event_count,
        max(occurred_at)                                                   as last_event_at,
        max(sequence_no)                                                   as last_sequence_no,

        max(case when event_type = 'order_payment_authorized' then occurred_at end)
                                                                           as confirmed_at,
        max(case when event_type = 'order_payment_failed'     then occurred_at end)
                                                                           as payment_failed_at,
        max(case when event_type = 'order_cancelled'          then occurred_at end)
                                                                           as cancelled_at,
        max(case when event_type = 'order_picking_started'    then occurred_at end)
                                                                           as picking_started_at,
        max(case when event_type = 'order_picked'             then occurred_at end)
                                                                           as picked_at,
        max(case when event_type = 'order_dispatched'         then occurred_at end)
                                                                           as dispatched_at,
        max(case when event_type = 'order_delivered'          then occurred_at end)
                                                                           as delivered_at,
        max(case when event_type = 'order_delivery_failed'    then occurred_at end)
                                                                           as delivery_failed_at,
        max(case when event_type = 'order_returned'           then occurred_at end)
                                                                           as returned_at,

        -- CAST EXPLICITO PARA INTEGER, e a razao so aparece ao atravessar a fronteira.
        -- `sum()` sobre INTEGER devolve HUGEINT no DuckDB; o parquet nao tem INT128, entao
        -- a escrita rebaixa a coluna para DOUBLE em silencio e uma CONTAGEM DE EVENTOS
        -- chega ao warehouse como FLOAT. Nao corrompe estes valores (nenhum passa de 40),
        -- mas e a mesma classe de defeito que o repo recusa em dinheiro: o tipo passa a
        -- afirmar "isto pode ter parte fracionaria", que e falso. Quem achou foi o DDL
        -- derivado do proprio recorte — um DDL escrito a mao teria dito NUMBER(38,0) e a
        -- divergencia entre o que o arquivo tem e o que a tabela declara so apareceria no
        -- COPY INTO, ou nunca.
        cast(sum(case when event_type = 'order_line_substituted' then 1 else 0 end) as integer)
                                                                           as substituted_lines,
        cast(sum(case when event_type = 'order_line_removed'     then 1 else 0 end) as integer)
                                                                           as removed_lines,

        max(case when event_type = 'order_picked'
                 then cast(json_extract_string(payload, '$.picked_amount') as decimal(12, 2)) end)
                                                                           as net_amount,
        max(case when event_type = 'order_picked'
                 then cast(json_extract_string(payload, '$.picked_line_count') as integer) end)
                                                                           as line_count_picked,
        max(case when event_type = 'order_delivered'
                 then cast(json_extract_string(payload, '$.delivered_within_slot') as boolean) end)
                                                                           as delivered_within_slot,
        max(case when event_type = 'order_returned'
                 then cast(json_extract_string(payload, '$.returned_amount') as decimal(12, 2)) end)
                                                                           as returned_amount,
        max(case when event_type = 'order_payment_authorized'
                 then json_extract_string(payload, '$.payment_method') end)
                                                                           as payment_method,
        max(case when event_type = 'order_payment_failed'
                 then json_extract_string(payload, '$.decline_reason') end)
                                                                           as decline_reason,
        max(case when event_type = 'order_cancelled'
                 then json_extract_string(payload, '$.cancelled_by') end)  as cancelled_by,
        max(case when event_type = 'order_cancelled'
                 then json_extract_string(payload, '$.reason') end)        as cancellation_reason,
        max(case when event_type = 'order_delivery_failed'
                 then json_extract_string(payload, '$.reason') end)        as delivery_failure_reason
    from {{ ref('silver_order_event') }}
    group by order_id

),

ultimo as (

    select order_id, event_type as last_event_type
    from eventos
    where recencia = 1

)

select
    c.ingestion_date,
    c.wh,
    c.order_id,

    c.customer_id,
    c.customer_ingestion_date,
    c.province_code,
    c.municipality_code,
    c.postal_code,

    c.price_as_of,
    c.price_source,

    -- O tipo do ultimo evento determina o estado: ver o cabecalho.
    case u.last_event_type
        when 'order_placed'              then 'PLACED'
        when 'order_payment_authorized'  then 'CONFIRMED'
        when 'order_payment_failed'      then 'PAYMENT_FAILED'
        when 'order_cancelled'           then 'CANCELLED'
        when 'order_picking_started'     then 'PICKING'
        when 'order_line_substituted'    then 'PICKING'
        when 'order_line_removed'        then 'PICKING'
        when 'order_picked'              then 'PICKED'
        when 'order_dispatched'          then 'IN_TRANSIT'
        when 'order_delivered'           then 'DELIVERED'
        when 'order_delivery_failed'     then 'DELIVERY_FAILED'
        when 'order_returned'            then 'RETURNED'
    end                                                                as order_status,
    u.last_event_type,
    -- DELIVERED nao entra: uma devolucao ainda pode vir depois dele, e e por isso que ele
    -- nao encerra o agregado (CONTRACT.md secao 2.2).
    u.last_event_type in (
        'order_payment_failed', 'order_cancelled', 'order_delivery_failed', 'order_returned'
    )                                                                  as is_terminal,

    c.placed_at,
    m.confirmed_at,
    m.payment_failed_at,
    m.cancelled_at,
    m.picking_started_at,
    m.picked_at,
    m.dispatched_at,
    m.delivered_at,
    m.delivery_failed_at,
    m.returned_at,
    m.last_event_at,

    c.delivery_slot_start,
    c.delivery_slot_end,
    m.delivered_within_slot,

    c.line_count_placed,
    m.line_count_picked,
    m.substituted_lines,
    m.removed_lines,

    c.gross_amount_placed,
    m.net_amount,
    -- A diferenca que so existe porque ha eventos. Positiva quando a cesta encolheu na
    -- separacao; pode ser negativa quando um substituto e mais caro que o original.
    c.gross_amount_placed - m.net_amount                               as amount_delta,
    m.returned_amount,

    m.payment_method,
    m.decline_reason,
    m.cancelled_by,
    m.cancellation_reason,
    m.delivery_failure_reason,

    m.event_count,
    m.last_sequence_no,
    date_diff('minute', c.placed_at, m.last_event_at)                  as lifecycle_minutes

from colocacao c
join marcos m on c.order_id = m.order_id
join ultimo  u on c.order_id = u.order_id
