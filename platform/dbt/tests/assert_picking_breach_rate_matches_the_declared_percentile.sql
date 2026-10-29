-- THE BASKET THE GENERATOR DRAWS IS THE BASKET THE SEED DECLARES.
--
-- READ THIS BEFORE THINKING IT IS A KPI TEST, because it looks like one and is not. The project refuses
-- tests that check a RESULT — that is why `assert_order_premises_are_internally_coherent`
-- checks the derivation and not adherence. This one does not check whether the operation is good: it checks whether the
-- OBSERVED DISTRIBUTION and the DECLARED DISTRIBUTION are the same.
--
-- THE CHAIN, which only closes if both ends agree:
--
--   the seed declares     basket_lines ~ triangular(min, max + 1, mode), truncated
--   the seed declares     sla_picking_percentile = 0.90
--   from which DERIVES    sla_minutes_picking = floor(p90) x minutes_per_line_picked
--   and the generator     picking_minutes = minutes_per_line_picked x basket lines
--   produces
--
--   so, if the observed basket really follows the declared triangular, the fraction of orders that
--   exceed the threshold must be 1 - sla_picking_percentile.
--
-- WHAT IT CATCHES THAT NOTHING ELSE DOES. `premises_sha256` guarantees the generator read THESE
-- values; no test verified that it used them with the declared SHAPE. Swapping
-- `rng.triangular` for `rng.uniform` while keeping the same three numbers would pass every
-- other test: the bounds would stay right, the totals would keep closing, the mode
-- would silently vanish — and every average-basket indicator would become plausible and wrong.
--
-- THE PROBABILITY IS COMPUTED, NOT HARD-CODED. For the triangular on the branch above the mode,
-- P(X >= x) = (b - x)^2 / ((b - a)(b - m)). Here `b` is `basket_lines_max + 1` because that is how
-- the generator draws, and the cut is `threshold/per_line + 1` because exceeding requires the
-- NEXT line. Measured on 2026-09-01: theoretical 0.09747, observed 0.09694 over 197,402 picked
-- orders — 5 ten-thousandths of difference.
--
-- THE TOLERANCE IS 1 PERCENTAGE POINT, and it is loose on purpose: with almost 200 thousand samples the
-- sampling error is around 0.07 pp, so 1 pp only fires if the SHAPE changes. Tightening it
-- further would turn a distribution test into a luck test.
with premissa as (

    select
        max(case when premise_key = 'basket_lines_min'        then cast(value as double) end) as a,
        max(case when premise_key = 'basket_lines_max'        then cast(value as double) end) + 1 as b,
        max(case when premise_key = 'basket_lines_mode'       then cast(value as double) end) as m,
        max(case when premise_key = 'minutes_per_line_picked' then cast(value as double) end) as por_linha,
        max(case when premise_key = 'sla_minutes_picking'     then cast(value as double) end) as limiar,
        max(case when premise_key = 'sla_picking_percentile'  then cast(value as double) end) as percentil
    from {{ ref('order_premises') }}

),

teorico as (

    select
        *,
        -- cut in LINES: exceeding the threshold requires the first line above it
        (limiar / por_linha) + 1                             as corte,
        power(b - ((limiar / por_linha) + 1), 2)
            / ((b - a) * (b - m))                            as p_estouro
    from premissa

),

observado as (

    select
        count(*)                                                          as separados,
        count(*) filter (
            where date_diff('minute', picking_started_at, picked_at) > (select limiar from premissa)
        )                                                                 as estouros
    from {{ ref('silver_order') }}
    where picked_at is not null

)

select
    o.separados,
    o.estouros,
    round(o.estouros::double / o.separados, 5) as taxa_observada,
    round(t.p_estouro, 5)                      as taxa_teorica,
    round(1 - t.percentil, 5)                  as taxa_declarada,
    'a cesta observada nao segue a triangular declarada no seed' as violacao
from observado o
cross join teorico t
where o.separados > 0
  and abs(o.estouros::double / o.separados - t.p_estouro) > 0.01
