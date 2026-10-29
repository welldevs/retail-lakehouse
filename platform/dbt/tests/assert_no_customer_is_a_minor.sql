-- NO ACCOUNT HOLDER IS A MINOR.
--
-- THE FINDING THIS FREEZES, and why it needed TWO tests. Measured on 2026-08-31:
-- 18.01% of customers were under 18 (3,602 of 20,000), with `age_at_ingestion` from
-- 0 to 100 — there were newborn account holders. Phase 5 handled this in the wrong domain:
-- it created `min_buyer_age` and filtered at ORDER time, leaving the customer records intact on the
-- argument that `silver_customer` was a faithful projection of the resident population. The argument
-- was right about what the Source delivers and wrong about what a customer register is — a customer
-- base is not a census. `assert_no_order_comes_from_a_minor` still exists and
-- is still right; it just was not enough, because it proved nobody BOUGHT while being a
-- minor, never that nobody EXISTED as a minor.
--
-- ALL ingestion_dates, not just the current one. A minor in an old generation is still
-- a minor in the base: `silver_customer` stacks the history on purpose, orders pin
-- `customer_ingestion_date` and the orders export resolves the MOST RECENT version of each
-- customer — filtering by `is_latest_ingestion` here would leave the back door open.
--
-- THE MINIMUM AGE IS NOT A CONSTANT IN THIS FILE: it comes from `customer_premises`, which projects the
-- seed itself. Repeating it here would create a second place to change it, and the two would diverge.
--
-- AND THE AGE IS `age_at_ingestion`, not today's age. It is the attribute the generator actually
-- sampled (`birth_year = year(ingestion_date) - age`), so the subtraction returns the exact
-- drawn value. Comparing against the clock would make this test change its answer on its own as
-- time passes, which is the opposite of what a test does.
with limite as (

    select cast(value as integer) as min_customer_age
    from {{ ref('customer_premises') }}
    where premise_key = 'min_customer_age'

)

select
    c.ingestion_date,
    c.wh,
    c.customer_id,
    c.birth_year,
    c.age_at_ingestion,
    l.min_customer_age
from {{ ref('silver_customer') }} c
cross join limite l
where c.age_at_ingestion < l.min_customer_age

union all

-- THE PREMISE'S OWN FLOOR. Same mechanism, same reason and same number as
-- `assert_no_order_comes_from_a_minor`: reading the premise proves the DATA obeys what was
-- declared, but on its own it does not keep the premise from being reverted — with the threshold at zero there
-- are no minors and the test would pass with a base of newborns. The seed may RAISE the minimum
-- age whenever it wants; below 18 it cannot go silently.
select
    current_date  as ingestion_date,
    'PREMISSA'    as wh,
    '-'           as customer_id,
    null          as birth_year,
    null          as age_at_ingestion,
    l.min_customer_age
from limite l
where l.min_customer_age < 18
