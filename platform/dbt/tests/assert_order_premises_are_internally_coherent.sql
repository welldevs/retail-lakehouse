-- THE ORDER PREMISES MUST NOT CONTRADICT EACH OTHER.
--
-- THE FINDING THIS FREEZES. Measured on 2026-09-01, over 86,803 delivered orders:
-- 73,124 of them — 84% — were DELIVERED BEFORE THE PROMISED WINDOW OPENED. And, in the same
-- seed, `sla_minutes_picking` was 90 minutes while the longest picking the
-- model can produce takes 80 (`basket_lines_max` x `minutes_per_line_picked`):
-- an alert above the ARITHMETIC CEILING, which never fires.
--
-- WHY THIS IS A MODEL FIX, AND NOT RESULT TUNING. The distinction matters more
-- than the number: changing a premise until the output pleases is what this project refuses; making
-- two premises mutually coherent is something else. A window that OPENS 24h after an
-- order the model itself delivers within at most 8.5h is not an undesired result — it is
-- an internally contradictory model, the same family as Phase 6's "true argument about the
-- wrong property". `slot_adherence_rate` showed a fixed 8.5% and
-- `orders_breaching_sla` was structurally zero: both indicators existed without measuring
-- anything.
--
-- WHAT THIS TEST CHECKS, AND WHAT IT DELIBERATELY DOES NOT CHECK.
--
-- It checks the DERIVATION, never the result. There is no assertion here about how high
-- adherence came out — and that is deliberate. After this fix it becomes trivial to keep tweaking
-- `slot_lead_hours_*` until adherence looks nice, and a test that looked at adherence
-- would BLESS that tweak instead of catching it. By checking the derivation, whoever in the future touches
-- these numbers to fix a KPI breaks a test. The resulting adherence is REPORTED,
-- not a target — the same rule as Phase 6's anchored rate.
--
-- THE DECLARED CHAIN, which this file does NOT repeat: the five stages between `order_placed` and
-- `order_delivered` already live in the seed, one row each, and the generator walks them in that order.
-- The cycle is their sum. Hard-coding 52 and 512 here would create a second place where the cycle lives,
-- and the two would diverge at the first adjustment of `minutes_to_picking_max`.
--
-- MEASURED WHEN WRITING THIS TEST, with the previous seed: FAIL 3. `slot_lead_hours_min` = 2
-- against a derived 1, `slot_lead_hours_max` = 24 against 8, and `sla_minutes_picking` = 90
-- against a ceiling of 80. The four clauses guard DIFFERENT failures, and none implies
-- another: window too early, window too late, unexercisable threshold and constant threshold.
with p as (

    select premise_key, cast(value as double) as v
    from {{ ref('order_premises') }}

),

premissa as (

    select
        max(case when premise_key = 'minutes_to_payment_min'   then v end) as pagamento_min,
        max(case when premise_key = 'minutes_to_payment_max'   then v end) as pagamento_max,
        max(case when premise_key = 'minutes_to_picking_min'   then v end) as separacao_inicio_min,
        max(case when premise_key = 'minutes_to_picking_max'   then v end) as separacao_inicio_max,
        max(case when premise_key = 'minutes_per_line_picked'  then v end) as por_linha,
        max(case when premise_key = 'basket_lines_min'         then v end) as linhas_min,
        max(case when premise_key = 'basket_lines_mode'        then v end) as linhas_moda,
        max(case when premise_key = 'basket_lines_max'         then v end) as linhas_max,
        max(case when premise_key = 'minutes_to_dispatch_min'  then v end) as despacho_min,
        max(case when premise_key = 'minutes_to_dispatch_max'  then v end) as despacho_max,
        max(case when premise_key = 'minutes_to_delivered_min' then v end) as entrega_min,
        max(case when premise_key = 'minutes_to_delivered_max' then v end) as entrega_max,
        max(case when premise_key = 'slot_lead_hours_min'      then v end) as janela_min,
        max(case when premise_key = 'slot_lead_hours_max'      then v end) as janela_max,
        max(case when premise_key = 'sla_minutes_picking'      then v end) as sla_separacao,
        max(case when premise_key = 'sla_picking_percentile'   then v end) as sla_percentil
    from p

),

ciclo as (

    select
        *,
        pagamento_min + separacao_inicio_min + (por_linha * linhas_min)
                      + despacho_min + entrega_min                        as ciclo_min,
        pagamento_max + separacao_inicio_max + (por_linha * linhas_max)
                      + despacho_max + entrega_max                        as ciclo_max,
        por_linha * linhas_max                                            as separacao_teto,
        por_linha * linhas_moda                                           as separacao_moda,
        -- The p-th percentile of triangular(a, b, m) on the branch above the mode. `b` is
        -- `basket_lines_max + 1` because that is how the generator draws
        -- (`rng.triangular(low, high + 1, mode)`), and `floor` because it truncates with
        -- `int()`. Copying the formula without the +1 would give 29 lines instead of 30 — a
        -- one-line error nobody would notice and that would make the threshold come out 2 minutes lower.
        floor((linhas_max + 1)
              - sqrt((1 - sla_percentil) * ((linhas_max + 1) - linhas_min)
                                         * ((linhas_max + 1) - linhas_moda)))
              * por_linha                                                 as sla_derivado
    from premissa

)

-- THE WINDOW OPENS EARLY ENOUGH TO FIT THE FASTEST DELIVERY THE MODEL PRODUCES.
select
    'slot_lead_hours_min' as premissa,
    janela_min            as declarado,
    ceil(ciclo_min / 60)  as derivado,
    'a menor antecedencia e o ciclo mais curto arredondado para cima: '
      || ciclo_min || ' min' as regra
from ciclo
where janela_min <> ceil(ciclo_min / 60)

union all

-- AND CLOSES EARLY ENOUGH FOR THE SLOWEST DELIVERY TO STILL REACH IT. This is the
-- limit that was violated: a window opening at 24h against a cycle of at most 8.5h
-- makes the promise impossible to keep by CONSTRUCTION, and adherence becomes a number about the
-- seed's arithmetic instead of about the operation.
select
    'slot_lead_hours_max',
    janela_max,
    floor(ciclo_max / 60),
    'a maior antecedencia e o ciclo mais longo arredondado para baixo: '
      || ciclo_max || ' min'
from ciclo
where janela_max <> floor(ciclo_max / 60)

union all

-- THE ALERT THRESHOLD LIVES BELOW THE ARITHMETIC CEILING. Above it the alert cannot be exercised:
-- no possible basket takes that long.
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_teto,
    'limiar >= teto aritmetico (basket_lines_max x minutes_per_line_picked): inexercivel'
from ciclo
where sla_separacao >= separacao_teto

union all

-- AND IT IS EXACTLY WHAT THE DECLARED POLICY PRODUCES. Without this clause the two previous ones
-- only define a RANGE — between 29 and 79 minutes — and choosing 79 to nearly zero out the alerts
-- would pass both. The policy (`sla_picking_percentile`) is what can be debated; the
-- threshold is its consequence. Whoever wants fewer alerts changes the policy, where the change
-- is visible as what it is.
select
    'sla_minutes_picking',
    sla_separacao,
    sla_derivado,
    'derivado de sla_picking_percentile sobre a triangular declarada da cesta'
from ciclo
where sla_separacao <> sla_derivado

union all

-- AND ABOVE THE MODAL PICKING TIME. Below it, the TYPICAL basket would already breach the SLA, and the alert
-- would stop distinguishing the problematic order from the ordinary one — which is the other way for a
-- threshold to measure nothing.
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_moda,
    'limiar <= separacao modal (basket_lines_mode x minutes_per_line_picked): alerta constante'
from ciclo
where sla_separacao <= separacao_moda
