-- ORDER — ACCUMULATING SNAPSHOT, and this pattern only exists because there are events.
--
-- GRAIN: order_id. TYPE: synthetic (the order) over observed (who buys,
--       what is bought, how much it costs and where they live).
--
-- WHY ACCUMULATING SNAPSHOT AND NOT A TRANSACTIONAL FACT. One row per order that FILLS IN
-- as the order progresses: each milestone has its own instant column, and the duration
-- between milestones is measured, not estimated. That is impossible to build from a state
-- snapshot — an `orders` table with a `status` would say where the order is, never how
-- long it took to get there. The durations in this table are the concrete answer to the
-- question "what did the event log buy that the snapshot would not".
--
-- NULL HERE MEANS "NOT REACHED", NEVER "UNKNOWN". A cancelled order has a null picked_at
-- because picking did not happen; as long as all orders in the window are terminal, null
-- means "never happened and never will". When the window starts containing orders in
-- flight, null will also mean "not yet" — and the `is_terminal` column is what
-- distinguishes the two cases without guessing.
--
-- CUSTOMER_SK RESOLVES THE VERSION IN EFFECT ON THE ORDER DATE, which is literally what
-- dim_customer.sql already called for and until now no fact did. It is what finally makes
-- the SCD2 pay for itself: DIM_CUSTOMER has two generations (08-24 and 08-27), and the
-- orders before 08-27 point to the customer's first version, those from 08-27 onwards to
-- the second.
--
-- MEASURED, AND THE NUMBER MATTERS: the version resolved by this range join matches
-- `customer_ingestion_date` — which the Source wrote in the order_placed event itself — in
-- ALL cases. They are two independent paths (the generator's choice in Python and an
-- interval join in SQL) arriving at the same place, and a dbt test watches the match
-- instead of trusting it.
--
-- net_amount IS NULL FOR WHAT WAS NEVER PICKED, and that is a corrected decision, not an
-- oversight: 298 orders (cancelled or with declined payment) died before anyone touched
-- the basket. Filling it with gross_amount_placed would claim a value nobody determined.
{{ config(materialized = 'table') }}

with pedidos as (

    select * from {{ source('stage', 'STG_ORDER') }}

),

-- The customer version in effect on the order date. Closed-open interval, the same as
-- fact_price_snapshot: `valid_to` is the valid_from of the NEXT version, so `<` and not `<=`.
cliente as (

    select customer_sk, customer_id, valid_from, valid_to
    from {{ ref('dim_customer') }}

)

select
    {{ date_key('o.ingestion_date') }} as date_key,
    o.ingestion_date                                        as order_date,
    o.order_id,
    o.wh,

    c.customer_sk,
    o.customer_id,
    -- The generation the Source declared it used. Travels with customer_sk so that the match
    -- between the two paths is QUERYABLE, and not just tested.
    o.customer_ingestion_date,
    c.valid_from                                            as customer_version_from,

    -- Same natural hash as dim_customer and dim_geography. Joins by code, never by
    -- name: the two INE products spell the municipality differently in 370 of 370 cases.
    md5(concat_ws('|', o.province_code, o.municipality_code, o.postal_code)) as geography_sk,
    o.province_code,
    o.municipality_code,
    o.postal_code,

    -- The age band that CHOSE THE WEIGHT VECTOR of this basket, stamped on the event. It sits
    -- next to customer_sk and not inside dim_customer on purpose: dim_customer versions who
    -- the customer is, and this column records which band they were in when they ordered.
    -- They are two questions, and only the second explains that order's mix.
    o.buyer_age_band,

    o.order_status,
    o.is_terminal,
    -- DELIVERED is not terminal: a return can still come afterwards. Some delivered orders
    -- became RETURNED, which is why the funnel relies on milestones and not on status (see
    -- mart_order_funnel).
    o.last_event_type,

    -- ---- the milestones ---------------------------------------------------------
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

    -- ---- the durations, which only exist because the milestones exist -------------
    -- Each one measures the interval between TWO declared milestones, and is null when either
    -- of the two did not occur. Never coalesce to zero: zero minutes is a measurement, absence
    -- of measurement is not.
    {{ datediff("'minute'", "o.placed_at",          "o.confirmed_at") }}       as minutes_to_confirm,
    {{ datediff("'minute'", "o.confirmed_at",       "o.picking_started_at") }} as minutes_to_picking,
    {{ datediff("'minute'", "o.picking_started_at", "o.picked_at") }}          as minutes_to_pick,
    {{ datediff("'minute'", "o.picked_at",          "o.dispatched_at") }}      as minutes_to_dispatch,
    {{ datediff("'minute'", "o.dispatched_at",      "o.delivered_at") }}       as minutes_to_deliver,
    {{ datediff("'minute'", "o.placed_at",          "o.delivered_at") }}       as minutes_placed_to_delivered,
    o.lifecycle_minutes,

    -- ---- the commercial promise -------------------------------------------------
    -- Delivery window, not route: the Callejero has no coordinates or adjacency, so route time
    -- would be invented. What is measured here is the fulfilment of a promise.
    o.delivery_slot_start,
    o.delivery_slot_end,
    o.delivered_within_slot,

    -- ---- the measures -----------------------------------------------------------
    o.line_count_placed,
    o.line_count_picked,
    o.substituted_lines,
    o.removed_lines,

    o.gross_amount_placed,
    o.net_amount,
    -- The difference that only exists because there are events: the basket changes AFTER
    -- placement. Positive when it shrank at picking; negative when a substitute cost more.
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
