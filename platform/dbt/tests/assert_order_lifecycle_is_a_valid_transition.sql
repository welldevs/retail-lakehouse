-- Toda transicao entre eventos consecutivos de um pedido tem de existir na maquina de
-- estados do contrato, e nenhum evento pode vir depois de um estado terminal.
--
-- A TABELA DE PARES E A MESMA de events.py, escrita aqui de novo de proposito. Nao e
-- duplicacao ociosa: a Source valida o que EMITE, este teste valida o que CHEGOU ao parquet
-- depois de atravessar RAW e uma releitura. Sao os dois lados de uma fronteira, e o valor
-- esta em eles serem independentes — a mesma razao pela qual
-- assert_customer_lives_in_its_warehouse_service_area e o terceiro check da mesma regra.
--
-- Estados terminais (PAYMENT_FAILED, CANCELLED, DELIVERY_FAILED, RETURNED) nao aparecem como
-- estado de partida em nenhum par: por construcao, qualquer evento depois deles reprova.
-- DELIVERED aparece, e so como partida de order_returned.
with transicoes(estado_antes, tipo) as (
    values
        ('INICIO',     'order_placed'),
        ('PLACED',     'order_payment_authorized'),
        ('PLACED',     'order_payment_failed'),
        ('PLACED',     'order_cancelled'),
        ('CONFIRMED',  'order_cancelled'),
        ('CONFIRMED',  'order_picking_started'),
        ('PICKING',    'order_line_substituted'),
        ('PICKING',    'order_line_removed'),
        ('PICKING',    'order_picked'),
        ('PICKED',     'order_dispatched'),
        ('IN_TRANSIT', 'order_delivered'),
        ('IN_TRANSIT', 'order_delivery_failed'),
        ('DELIVERED',  'order_returned')
),

estados(tipo, estado_depois) as (
    values
        ('order_placed',             'PLACED'),
        ('order_payment_authorized', 'CONFIRMED'),
        ('order_payment_failed',     'PAYMENT_FAILED'),
        ('order_cancelled',          'CANCELLED'),
        ('order_picking_started',    'PICKING'),
        ('order_line_substituted',   'PICKING'),
        ('order_line_removed',       'PICKING'),
        ('order_picked',             'PICKED'),
        ('order_dispatched',         'IN_TRANSIT'),
        ('order_delivered',          'DELIVERED'),
        ('order_delivery_failed',    'DELIVERY_FAILED'),
        ('order_returned',           'RETURNED')
),

encadeado as (
    select
        e.order_id,
        e.sequence_no,
        e.event_type,
        coalesce(
            lag(s.estado_depois) over (partition by e.order_id order by e.sequence_no),
            'INICIO'
        ) as estado_antes
    from {{ ref('silver_order_event') }} e
    join estados s on e.event_type = s.tipo
)

select c.order_id, c.sequence_no, c.estado_antes, c.event_type
from encadeado c
left join transicoes t
       on c.estado_antes = t.estado_antes and c.event_type = t.tipo
where t.tipo is null
