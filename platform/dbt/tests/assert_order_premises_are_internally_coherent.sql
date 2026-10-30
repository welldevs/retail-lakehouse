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
premissa as (
    select
        max(case when premise_key = 'daily_order_rate'         then v end) as taxa_diaria,
        max(case when premise_key = 'order_skip_rate'          then v end) as pulo,
        max(case when premise_key = 'cadence_weight_7d'        then v end) as c7,
        max(case when premise_key = 'cadence_weight_14d'       then v end) as c14,
        max(case when premise_key = 'cadence_weight_21d'       then v end) as c21,
        max(case when premise_key = 'cadence_weight_28d'       then v end) as c28,
        sum(case when premise_key like 'order_weekday_weight_%' then v end) as dias_semana,
        sum(case when premise_key like 'slot_lead_day_weight_%' then v end) as antecedencia,
        max(case when premise_key = 'delivery_open_hour'       then v end) as abre,
        max(case when premise_key = 'delivery_close_hour'      then v end) as fecha,
        max(case when premise_key = 'slot_hours'               then v end) as janela,
        max(case when premise_key = 'minutes_per_line_picked'  then v end) as por_linha,
        max(case when premise_key = 'basket_lines_min'         then v end) as linhas_min,
        max(case when premise_key = 'basket_lines_mode'        then v end) as linhas_moda,
        max(case when premise_key = 'basket_lines_max'         then v end) as linhas_max,
        max(case when premise_key = 'sla_minutes_picking'      then v end) as sla_separacao,
        max(case when premise_key = 'sla_picking_percentile'   then v end) as sla_percentil
    from p
),
derivado as (
    select
        *,
        (c7 / 7 + c14 / 14 + c21 / 21 + c28 / 28) * (1 - pulo)          as taxa_da_cadencia,
        c7 + c14 + c21 + c28                                              as cadencia_total,
        por_linha * linhas_max                                            as separacao_teto,
        por_linha * linhas_moda                                           as separacao_moda,
        floor((linhas_max + 1)
              - sqrt((1 - sla_percentil) * ((linhas_max + 1) - linhas_min)
                                         * ((linhas_max + 1) - linhas_moda)))
              * por_linha                                                 as sla_derivado
    from premissa
)
select
    'daily_order_rate' as premissa,
    taxa_diaria        as declarado,
    taxa_da_cadencia   as derivado,
    'a cadencia declarada (sum(peso/dias) x (1 - order_skip_rate)) deve reproduzir a taxa '
      || 'diaria em 2%' as regra
from derivado
where abs(taxa_da_cadencia - taxa_diaria) > 0.02 * taxa_diaria
union all
select 'cadence_weight_*', cadencia_total, 1, 'os pesos de cadencia somam 1'
from derivado where abs(cadencia_total - 1) > 1e-9
union all
select 'order_weekday_weight_*', dias_semana, 1, 'os pesos por dia da semana somam 1'
from derivado where abs(dias_semana - 1) > 1e-9
union all
select 'slot_lead_day_weight_*', antecedencia, 1, 'os pesos de antecedencia somam 1'
from derivado where abs(antecedencia - 1) > 1e-9
union all
select 'delivery_close_hour', fecha, abre + janela,
       'a janela de entrega cabe entre abertura e fechamento'
from derivado where not (abre < fecha and abre + janela <= fecha)
union all
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_teto,
    'limiar >= teto aritmetico (basket_lines_max x minutes_per_line_picked): inexercivel'
from derivado
where sla_separacao >= separacao_teto
union all
select
    'sla_minutes_picking',
    sla_separacao,
    sla_derivado,
    'derivado de sla_picking_percentile sobre a triangular declarada da cesta'
from derivado
where sla_separacao <> sla_derivado
union all
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_moda,
    'limiar <= separacao modal (basket_lines_mode x minutes_per_line_picked): alerta constante'
from derivado
where sla_separacao <= separacao_moda
