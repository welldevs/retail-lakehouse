-- THE ORDER PREMISES MUST NOT CONTRADICT EACH OTHER.
--
-- HISTORY THIS FREEZES. Measured on 2026-09-01: 84% of deliveries arrived BEFORE the
-- promised window opened, and `sla_minutes_picking` sat above the arithmetic ceiling of
-- picking, so it never fired (Phase 7). Measured again on 2026-10-30, after that fix: the
-- delivery window was still drawn independently of the delivery time, 77.5% of orders were
-- delivered on the day they were placed and 22% before 07:00 Madrid time, and every warehouse
-- placed the same number of orders every day (CR-017). Each was a premise disagreeing with
-- another, or with the calendar a real operator publishes.
--
-- WHAT THIS TEST CHECKS, AND WHAT IT DELIBERATELY DOES NOT CHECK.
--
-- It checks the DERIVATION, never the result. There is no assertion here about how high
-- on-time delivery or adherence came out: a test that looked at a KPI would BLESS whoever
-- tunes premises until the KPI looks nice. Whoever touches these numbers to fix a KPI breaks
-- a derivation here instead.
--
--   1. The customer CADENCE reproduces `daily_order_rate`: sum(weight / days) x (1 - skip)
--      within 2%. The cadence replaced the daily lottery; the declared rate is the anchor
--      the MAPA channel closure was built on, and the two must not drift apart.
--   2. Every SHARE vector sums to 1 (cadence, weekday, delivery lead day).
--   3. The delivery window can hold a slot: open < close, and open + slot_hours <= close.
--   4. The SLA threshold is derived from the declared basket triangle (Phase 7), and sits
--      strictly between the modal picking time and the arithmetic ceiling.
with p as (
    select premise_key, cast(value as double) as v
    from {{ ref('order_premises') }}
),
premise as (
    select
        max(case when premise_key = 'daily_order_rate'         then v end) as daily_rate,
        max(case when premise_key = 'order_skip_rate'          then v end) as skip_rate,
        max(case when premise_key = 'cadence_weight_7d'        then v end) as c7,
        max(case when premise_key = 'cadence_weight_14d'       then v end) as c14,
        max(case when premise_key = 'cadence_weight_21d'       then v end) as c21,
        max(case when premise_key = 'cadence_weight_28d'       then v end) as c28,
        sum(case when premise_key like 'order_weekday_weight_%' then v end) as weekday_weights,
        sum(case when premise_key like 'slot_lead_day_weight_%' then v end) as lead_day_weights,
        max(case when premise_key = 'delivery_open_hour'       then v end) as opens,
        max(case when premise_key = 'delivery_close_hour'      then v end) as closes,
        max(case when premise_key = 'slot_hours'               then v end) as slot_len,
        max(case when premise_key = 'minutes_per_line_picked'  then v end) as minutes_per_line,
        max(case when premise_key = 'basket_lines_min'         then v end) as lines_min,
        max(case when premise_key = 'basket_lines_mode'        then v end) as lines_mode,
        max(case when premise_key = 'basket_lines_max'         then v end) as lines_max,
        max(case when premise_key = 'sla_minutes_picking'      then v end) as sla_picking,
        max(case when premise_key = 'sla_picking_percentile'   then v end) as sla_percentile
    from p
),
derived as (
    select
        *,
        (c7 / 7 + c14 / 14 + c21 / 21 + c28 / 28) * (1 - skip_rate)          as cadence_rate,
        c7 + c14 + c21 + c28                                              as cadence_total,
        minutes_per_line * lines_max                                            as picking_ceiling,
        minutes_per_line * lines_mode                                           as picking_mode,
        floor((lines_max + 1)
              - sqrt((1 - sla_percentile) * ((lines_max + 1) - lines_min)
                                         * ((lines_max + 1) - lines_mode)))
              * minutes_per_line                                                 as sla_derived
    from premise
)
select
    'daily_order_rate' as premise,
    daily_rate        as declared,
    cadence_rate   as derived,
    'the declared cadence (sum(weight/days) x (1 - order_skip_rate)) must reproduce the daily rate '
      || 'within 2%' as reason
from derived
where abs(cadence_rate - daily_rate) > 0.02 * daily_rate
union all
select 'cadence_weight_*', cadence_total, 1, 'cadence weights sum to 1'
from derived where abs(cadence_total - 1) > 1e-9
union all
select 'order_weekday_weight_*', weekday_weights, 1, 'weekday weights sum to 1'
from derived where abs(weekday_weights - 1) > 1e-9
union all
select 'slot_lead_day_weight_*', lead_day_weights, 1, 'lead-day weights sum to 1'
from derived where abs(lead_day_weights - 1) > 1e-9
union all
select 'delivery_close_hour', closes, opens + slot_len,
       'the delivery slot fits between opening and closing'
from derived where not (opens < closes and opens + slot_len <= closes)
union all
select
    'sla_minutes_picking',
    sla_picking,
    picking_ceiling,
    'threshold >= arithmetic ceiling (basket_lines_max x minutes_per_line_picked): unexercisable'
from derived
where sla_picking >= picking_ceiling
union all
select
    'sla_minutes_picking',
    sla_picking,
    sla_derived,
    'derived from sla_picking_percentile over the declared triangular basket'
from derived
where sla_picking <> sla_derived
union all
select
    'sla_minutes_picking',
    sla_picking,
    picking_mode,
    'threshold <= modal picking time (basket_lines_mode x minutes_per_line_picked): constant alert'
from derived
where sla_picking <= picking_mode
