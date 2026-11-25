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
with premise as (

    select
        max(case when premise_key = 'basket_lines_min'        then cast(value as double) end) as a,
        max(case when premise_key = 'basket_lines_max'        then cast(value as double) end) + 1 as b,
        max(case when premise_key = 'basket_lines_mode'       then cast(value as double) end) as m,
        max(case when premise_key = 'minutes_per_line_picked' then cast(value as double) end) as minutes_per_line,
        max(case when premise_key = 'sla_minutes_picking'     then cast(value as double) end) as threshold,
        max(case when premise_key = 'sla_picking_percentile'  then cast(value as double) end) as percentile
    from {{ ref('order_premises') }}

),

theoretical as (

    select
        *,
        -- cut in LINES: exceeding the threshold requires the first line above it
        (threshold / minutes_per_line) + 1                             as cutoff,
        power(b - ((threshold / minutes_per_line) + 1), 2)
            / ((b - a) * (b - m))                            as breach_p
    from premise

),

observed as (

    select
        count(*)                                                          as picked,
        count(*) filter (
            where date_diff('minute', picking_started_at, picked_at) > (select threshold from premise)
        )                                                                 as breaches
    from {{ ref('silver_order') }}
    where picked_at is not null

)

select
    o.picked,
    o.breaches,
    round(o.breaches::double / o.picked, 5) as observed_rate,
    round(t.breach_p, 5)                      as theoretical_rate,
    round(1 - t.percentile, 5)                  as declared_rate,
    'the observed basket does not follow the triangular declared in the seed' as reason
from observed o
cross join theoretical t
where o.picked > 0
  and abs(o.breaches::double / o.picked - t.breach_p) > 0.01
