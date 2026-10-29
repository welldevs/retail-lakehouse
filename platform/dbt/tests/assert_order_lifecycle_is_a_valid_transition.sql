-- Every transition between consecutive events of an order must exist in the contract's state
-- machine, and no event may come after a terminal state.
--
-- THE PAIR TABLE IS THE SAME as in events.py, written here again on purpose. It is not
-- idle duplication: the Source validates what it EMITS, this test validates what REACHED the parquet
-- after crossing RAW and a re-read. They are the two sides of a boundary, and the value
-- is in their being independent — the same reason
-- assert_customer_lives_in_its_warehouse_service_area is the third check of the same rule.
--
-- Terminal states (PAYMENT_FAILED, CANCELLED, DELIVERY_FAILED, RETURNED) do not appear as
-- a starting state in any pair: by construction, any event after them fails.
-- DELIVERED does appear, and only as the start of order_returned.
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
