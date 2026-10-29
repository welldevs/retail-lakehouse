-- GRAIN: (order_date, wh) — one row per warehouse per day of the window.
--
-- QUESTION IT ANSWERS: how long each stage takes, and how many orders exceed the
-- DECLARED picking threshold.
--
-- THE THRESHOLD IS NOT A CONSTANT OF THIS MODEL. It comes from FACT_ORDER_PREMISE, which came
-- from the seed the GENERATOR read, whose sha256 is in the manifest of every RAW partition.
-- Hard-coding 90 here (or in a dbt var) would create a second copy of the number, and the day
-- someone edits the seed without editing the copy, this mart starts counting breaches against
-- a threshold no order ever knew — without failing anything, because zero breaches against the
-- wrong threshold looks exactly like zero against the right one.
--
-- TWO FINDINGS THAT STAYED RECORDED HERE FOR THREE PHASES, AND WERE FIXED IN PHASE 7.
-- The text below tells both because the reason they took so long is worth more than
-- the fix.
--
-- FIRST. `sla_minutes_picking` was 90 and was UNREACHABLE by construction: picking
-- takes `minutes_per_line_picked` (2) x number of lines, and `basket_lines_max` is 40 — a ceiling
-- of 80 minutes. Measured in 2026-06 on the window at the time: p50 = 36, p90 = 62, MAXIMUM = 80.00,
-- ZERO breaches. And not because the operation was good: because the three premises did not
-- intersect.
--
-- SECOND. The promised delivery window was almost never the window in which delivery
-- happened — and the deviation was EARLY, not late. Measured on 2026-09-01 over 86,803
-- deliveries: 73,124 (84%) arrived BEFORE the window opened. The cause was arithmetic and lay
-- in the premises: `slot_lead_hours` drew the window start between 2h and 24h after
-- placement, while the sum of the milestones delivered within at most 8.5h.
--
-- WHY THEY STAYED RECORDED SO LONG, AND WHAT CHANGED. The rule this project
-- follows is to refuse premise tuning until the output pleases, and it is right. What was missing
-- was the distinction: changing a premise to improve a number is one thing; making two
-- premises MUTUALLY COHERENT is another. A window that opens 24h after an order the
-- model itself delivers within at most 8.5h is not an undesired result — it is an
-- internally contradictory model. The same goes for an alert above the arithmetic ceiling. Under
-- the old rule, both were untouchable; under the distinction, both were model defects.
--
-- HOW THE FIX PROTECTS ITSELF FROM BECOMING WHAT IT REFUSES. The three premises are now
-- DERIVED from the other rows of the same seed, and `assert_order_premises_are_internally_coherent`
-- checks the DERIVATION — never the result. It contains no assertion about how high
-- adherence came out. Whoever in the future tunes `slot_lead_hours_*` or `sla_minutes_picking` to fix
-- a KPI breaks a test, even with a value that fits the plausible range: measured, changing
-- the threshold from 60 to 75 fails.
--
-- WHAT THE MART STILL DOES THE SAME: it carries `sla_minutes`, `max_picking_minutes` and
-- `orders_breaching_sla` SIDE BY SIDE. Before, that served to understand a structural zero;
-- now it serves to let the reader see against which threshold the count was made. The reason has
-- not changed — a breach count without the threshold next to it cannot be checked.
--
-- WHAT CHANGES HERE IS WHAT GETS PUBLISHED. `orders_delivered_within_slot` alone says 8% and lets
-- the reader conclude "the operation runs late", which is the opposite of what happens. Arriving
-- early and arriving late are OPPOSITE operational problems, and collapsing both into "outside the
-- window" erases which one it is. That is why the three counts travel separately.
--
-- PERCENTILE OVER ORDERS THAT REACHED THE MILESTONE, not over all of them. `percentile_cont`
-- ignores nulls, and null here means "the stage did not happen". A delivery-time p90
-- that counted cancellations as zero would measure another company's operation. The counts
-- next to it say how many orders went into each percentile, so that no p90 is read
-- without knowing over how many it was computed.
{{ config(materialized = 'table') }}

with sla as (

    -- `max(case when ...)` and not `where premise_key = ...`: a filter that does not match returns
    -- ZERO rows, and the cross join below would empty the whole mart — the mart becomes 0 rows and
    -- no not_null test fails on an empty table. With max(), the CTE ALWAYS returns
    -- one row; if the premise disappears from the seed, the column becomes null and the
    -- not_null test on sla_minutes fails, which is the desired behavior.
    select
        max(case when premise_key = 'sla_minutes_picking'
                 then premise_value end)                    as sla_minutes_picking
    from {{ ref('fact_order_premise') }}

)

select
    f.order_date,
    f.date_key,
    d.year_month,
    f.wh,

    count(*)                                                as orders_placed,

    -- ---- how many orders reached each milestone (the denominator of each percentile) ----
    count(f.minutes_to_confirm)                             as orders_with_confirm,
    count(f.minutes_to_pick)                                as orders_with_pick,
    count(f.minutes_to_deliver)                             as orders_with_deliver,

    -- ---- p50 / p90 per stage --------------------------------------------------------
    percentile_cont(0.5) within group (order by f.minutes_to_confirm) as p50_minutes_to_confirm,
    percentile_cont(0.9) within group (order by f.minutes_to_confirm) as p90_minutes_to_confirm,
    percentile_cont(0.5) within group (order by f.minutes_to_pick)    as p50_minutes_to_pick,
    percentile_cont(0.9) within group (order by f.minutes_to_pick)    as p90_minutes_to_pick,
    percentile_cont(0.5) within group (order by f.minutes_to_dispatch) as p50_minutes_to_dispatch,
    percentile_cont(0.9) within group (order by f.minutes_to_dispatch) as p90_minutes_to_dispatch,
    percentile_cont(0.5) within group (order by f.minutes_to_deliver) as p50_minutes_to_deliver,
    percentile_cont(0.9) within group (order by f.minutes_to_deliver) as p90_minutes_to_deliver,
    percentile_cont(0.5) within group (order by f.minutes_placed_to_delivered)
                                                            as p50_minutes_placed_to_delivered,
    percentile_cont(0.9) within group (order by f.minutes_placed_to_delivered)
                                                            as p90_minutes_placed_to_delivered,

    -- ---- the declared threshold, and the distance to it ------------------------------
    max(s.sla_minutes_picking)                              as sla_minutes,
    max(f.minutes_to_pick)                                  as max_picking_minutes,
    {{ count_if('f.minutes_to_pick > s.sla_minutes_picking') }} as orders_breaching_sla,

    -- ---- the commercial promise, which is the SLA the operation actually has ---------
    -- Unlike the picking threshold: the delivery window is chosen per order, it is
    -- in the log, and the order_delivered event declares whether it was met. There is real variation here.
    count(f.delivered_within_slot)                          as orders_with_slot_outcome,
    {{ count_if('f.delivered_within_slot') }}                 as orders_delivered_within_slot,
    -- Early and late kept apart on purpose: they are opposite operational problems, and
    -- "outside the window" does not say which one is happening. The imbalance between the two
    -- is what exposed the incoherence of the premises on 2026-09-01: 73,124 early, or 84
    -- percent of the total. These two columns are the instrument that made it visible.
    {{ count_if("f.delivered_at is not null and f.delivered_at < f.delivery_slot_start") }}
                                                            as orders_delivered_before_slot,
    {{ count_if("f.delivered_at is not null and f.delivered_at > f.delivery_slot_end") }}
                                                            as orders_delivered_after_slot,
    round(
        {{ div0(count_if('f.delivered_within_slot'), 'count(f.delivered_within_slot)') }}, 4
    )                                                       as slot_adherence_rate
from {{ ref('fact_order') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
cross join sla s
group by 1, 2, 3, 4
