-- THE FOLD. State and measures of each order, derived from the event log.
--
-- GRAIN: order_id. The id already carries the warehouse and the day
-- (ord_mad1_20260824_000042), so there is no collision between partitions and no need to
-- compose the key.
--
-- THIS MODEL IS THE REASON THE SOURCE DOES NOT WRITE STATE. There is no `orders.json` in
-- RAW: two representations of the same truth diverge. The state lives here, and is always
-- rebuildable from the log — the same principle that makes DIM_PRODUCT an SCD2 DERIVED from
-- history instead of accumulated by MERGE ("accumulating would create state that only
-- exists in the destination").
--
-- THE FOLD IS NON-TRIVIAL, and that is what justifies the event model existing.
-- `net_amount` is NOT derivable from `gross_amount_placed`: line substitution and removal
-- change the basket after placement. Measured over the 2026-08-24 to 08-27 window: placed
-- 879,449.74 versus picked 821,121.93 — a 58,327.81 difference the first event did not
-- know about. If the two were always equal, the log would be a date stamp, and an inverted
-- dbt test watches exactly that.
--
-- THE STATE COMES FROM THE TYPE OF THE LAST EVENT, and that is correct and not a shortcut:
-- the contract's state machine maps each type to ONE resulting state (STATE_AFTER in
-- events.py), so the state after the last event is the fold of the whole chain. The two
-- line events map to PICKING, which is also the state they come from — they change the
-- BASKET, not the state.
--
-- `max(case when ...)` and not `FILTER (WHERE ...)`: DuckDB accepts both, Snowflake only
-- the first. This model is duckdb-only through the dbt_project.yml guard, but writing the
-- portable dialect costs nothing and the repo has already paid once for that difference
-- (ARCHITECTURE.md, "four defects that only showed up when running").
--
-- MONEY IS DECIMAL, NEVER DOUBLE. The values cross the log as strings precisely so that
-- the conversion happens once, here, with a declared scale.
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
        -- Buyer age band AT THE TIME OF THE ORDER, stamped on the event by the Source.
        -- It comes from the payload and not from a join with silver_customer on purpose, for the
        -- same reason as `demand_group` in silver_order_line: it is the band that chose that
        -- basket's weight vector, and deriving it today would give today's band. A customer whose
        -- birthday falls within the window has two orders in different bands, and that is right.
        json_extract_string(payload, '$.buyer_age_band')               as buyer_age_band,
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

        -- EXPLICIT CAST TO INTEGER, and the reason only shows up when crossing the boundary.
        -- `sum()` over INTEGER returns HUGEINT in DuckDB; parquet has no INT128, so the write
        -- silently downgrades the column to DOUBLE and an EVENT COUNT reaches the warehouse as
        -- FLOAT. It does not corrupt these values (none goes above 40), but it is the same class
        -- of defect the repo refuses for money: the type starts to claim "this may have a
        -- fractional part", which is false. What found it was the DDL derived from the cut itself
        -- — a hand-written DDL would have said NUMBER(38,0) and the divergence between what the
        -- file holds and what the table declares would only show up at COPY INTO, or never.
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
    c.buyer_age_band,

    c.price_as_of,
    c.price_source,

    -- The type of the last event determines the state: see the header.
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
    -- DELIVERED is not included: a return can still come after it, which is why it does not
    -- close the aggregate (CONTRACT.md section 2.2).
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
    -- The difference that only exists because there are events. Positive when the basket
    -- shrank at picking; can be negative when a substitute is more expensive than the original.
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
