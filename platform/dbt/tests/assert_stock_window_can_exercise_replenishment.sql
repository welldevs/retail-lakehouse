-- THE WINDOW MUST BE LONGER THAN THE OPENING COVER, OTHERWISE THE LEDGER MEASURES NOTHING.
--
-- THE FINDING THIS FREEZES, and it is against the author himself. The first run of the stock
-- job, on 2026-09-01, gave ZERO STOCKOUT over 86,520 rows. It was not a good operation:
-- `opening_days_of_demand` was 7 and the observed window had 5 days, so the OPENING
-- stock covered the whole period. Zero stockout was arithmetic, not measurement.
--
-- IT IS EXACTLY THE SAME FAMILY as the structurally zero `orders_breaching_sla` that Milestone B
-- had just fixed in the orders domain — recreated in the stock domain two
-- hours later, by the person who had written the fix. A threshold no basket reaches and a
-- stock no window consumes are the same thing: a number that can only give one
-- result. That is why the rule became a test instead of a paragraph.
--
-- WHAT IT DOES NOT REQUIRE, and the distinction matters: it does not require that there BE a stockout. An operation
-- that never runs out is a legitimate state of the world, and requiring stockouts would be asking the model to
-- produce the number that pleases. What it requires is that a stockout be POSSIBLE — that the
-- result depend on demand and not on the premises' arithmetic.
--
-- WHAT STAYS DECLARED AND IS NOT TESTED: with 7 days of cover, reorder point at 3 and lead time of 2, one
-- replenishment cycle closes in the 10-day window and the second does not. Lowering the cover until
-- two cycles fit would be choosing a premise for what it does to the chart, and the window's ceiling
-- is external — 2026-09-01 is as far as the observed catalog reaches.
with cobertura as (

    select cast(value as double) as opening_days_of_demand
    from {{ ref('stock_premises') }}
    where premise_key = 'opening_days_of_demand'

),

janela as (

    select
        count(distinct stock_date) as dias,
        min(stock_date)            as de,
        max(stock_date)            as ate
    from {{ ref('silver_stock_ledger') }}

)

select
    j.dias                        as dias_na_janela,
    c.opening_days_of_demand      as dias_de_cobertura_inicial,
    j.de,
    j.ate,
    'a janela nao consome o estoque inicial: ruptura fica zero por construcao' as violacao
from janela j
cross join cobertura c
where j.dias <= c.opening_days_of_demand
