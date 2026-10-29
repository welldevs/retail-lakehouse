-- GRAIN: (order_date, wh) — one row per warehouse per day of the window.
--
-- QUESTION IT ANSWERS: of every 100 orders placed at a warehouse on a day, how many were
-- confirmed, picked, dispatched and delivered — and where the ones that were not slipped away.
--
-- THE FUNNEL RESTS ON MILESTONES, NEVER ON STATUS, and that is the decision that makes the table
-- true. `order_status` holds the CURRENT state of the order, which is that of the LAST event.
-- A returned order has status RETURNED — and it was delivered. Measured in 2026-06, on the window
-- at the time: counting `order_status = 'DELIVERED'` gave 5,985 deliveries; counting
-- `delivered_at is not null` gave 6,046. The difference was the 61 returned orders, and a
-- funnel built on status would lose them at the delivery stage, producing a delivery rate
-- 1% lower than the real one without anything failing. The DATE matters: the count is from that
-- capture and does not hold again; what still holds is the reason the column is the instant.
--
-- A milestone is monotonic: once reached, it does not go back. Status is not. A funnel is by
-- definition a count of REACHED stages, so the right column is the instant.
--
-- THE EXITS DO NOT ADD UP WITH THE STAGES, and they should not. `orders_cancelled` counts those who
-- left through cancellation at any point before picking; `orders_returned` counts those who left
-- AFTER crossing the whole funnel. Adding both columns to the stages would count the
-- returned ones twice — which is why they live in their own block, with a name that says exit.
{{ config(materialized = 'table') }}

select
    f.order_date,
    f.date_key,
    d.year_month,
    f.wh,

    -- ---- the stages, by milestone reached -----------------------------------------
    count(*)                                                as orders_placed,
    {{ count_if('f.confirmed_at is not null') }}              as orders_confirmed,
    {{ count_if('f.picking_started_at is not null') }}        as orders_picking_started,
    {{ count_if('f.picked_at is not null') }}                 as orders_picked,
    {{ count_if('f.dispatched_at is not null') }}             as orders_dispatched,
    {{ count_if('f.delivered_at is not null') }}              as orders_delivered,

    -- ---- the exits, which do NOT add up with the stages above ---------------------
    {{ count_if('f.payment_failed_at is not null') }}         as orders_payment_failed,
    {{ count_if('f.cancelled_at is not null') }}               as orders_cancelled,
    {{ count_if('f.delivery_failed_at is not null') }}         as orders_delivery_failed,
    {{ count_if('f.returned_at is not null') }}                as orders_returned,

    -- ---- the rates, all with the same denominator ---------------------------------
    -- A single denominator (orders_placed) on purpose: stage-by-stage chained rates
    -- multiply and nobody can add up the funnel in their head. Here each rate answers
    -- "of everything that came in, how much made it this far", which is the question one asks of a funnel.
    round({{ count_if('f.confirmed_at is not null') }}   / count(*), 4) as confirm_rate,
    round({{ count_if('f.picked_at is not null') }}      / count(*), 4) as pick_rate,
    round({{ count_if('f.delivered_at is not null') }}   / count(*), 4) as delivery_rate,
    round({{ count_if('f.cancelled_at is not null') }}   / count(*), 4) as cancellation_rate,
    round({{ count_if('f.returned_at is not null') }}    / count(*), 4) as return_rate,

    -- ---- the money ----------------------------------------------------------------
    -- gross is what was PLACED; net is what was PICKED. The two are never equal, and it is
    -- that difference that justifies the event log existing: substitution and removal change
    -- the basket after placement. `net_amount` is null for those that died before picking,
    -- and sum() ignores nulls — which here is the right behavior: what was never picked does not
    -- contribute zero, it contributes nothing.
    sum(f.gross_amount_placed)                              as gross_amount_placed,
    sum(f.net_amount)                                       as net_amount_picked,
    sum(f.amount_delta)                                     as amount_delta,
    sum(f.returned_amount)                                  as returned_amount,
    max(f.currency)                                         as currency,

    sum(f.substituted_lines)                                as substituted_lines,
    sum(f.removed_lines)                                    as removed_lines
from {{ ref('fact_order') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
group by 1, 2, 3, 4
