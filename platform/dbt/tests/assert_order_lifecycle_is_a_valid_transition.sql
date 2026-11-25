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
with transitions(state_before, event_kind) as (
    values
        ('START',     'order_placed'),
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

states(event_kind, state_after) as (
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

chained as (
    select
        e.order_id,
        e.sequence_no,
        e.event_type,
        coalesce(
            lag(s.state_after) over (partition by e.order_id order by e.sequence_no),
            'START'
        ) as state_before
    from {{ ref('silver_order_event') }} e
    join states s on e.event_type = s.event_kind
)

select c.order_id, c.sequence_no, c.state_before, c.event_type
from chained c
left join transitions t
       on c.state_before = t.state_before and c.event_type = t.event_kind
where t.event_kind is null
