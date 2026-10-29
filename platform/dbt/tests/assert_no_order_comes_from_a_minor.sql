-- NO ORDER IS PLACED BY A MINOR.
--
-- THE FINDING THIS FREEZES. Measured on 2026-08-31: 18.01% of customers were under 18
-- — 3,602 of 20,000 — with `age_at_ingestion` ranging from 0 to 100. There were newborn
-- account holders. That was not a defect of the OLTP Source, which declares in its contract that
-- age comes from INE's provincial POPULATION distribution and delivers exactly that; what
-- had never been declared was the difference between a RESIDENT and WHOEVER PLACES AN ORDER.
--
-- WHY THIS BECOMES A TEST, and not only a premise. While age did nothing, a
-- three-year-old buyer was harmless. From the moment it governs demand,
-- 18% of the base would fall into MAPA's 'under 35' band while being children — and the calibration would be
-- wrong by construction, with no total breaking. A test here is what keeps the premise
-- from being silently reverted by someone who does not know why it exists.
--
-- THE MINIMUM AGE IS NOT A CONSTANT IN THIS FILE: it comes from `order_premises`, which projects the
-- seed itself. Repeating it here would create a second place to change it, and the two would diverge.
--
-- MEASURED WHEN WRITING THIS TEST: lowering `min_buyer_age` to 0 in the seed makes this test
-- pass, because with the threshold at zero there are no minors. Reading the premise is the right
-- behavior — the test checks that the DATA obeys what was declared — but on its own it does not
-- protect the premise from being reverted. Hence the second half below: a FLOOR, not a
-- copy of the value. The seed may raise the minimum age whenever it wants; below 18 it cannot
-- go silently, because that is exactly where the base had three-year-old account holders.
with limite as (

    select cast(value as integer) as min_buyer_age
    from {{ ref('order_premises') }}
    where premise_key = 'min_buyer_age'

)

select
    o.order_id,
    o.wh,
    o.ingestion_date,
    o.customer_id,
    c.birth_year,
    year(o.ingestion_date) - c.birth_year as idade_no_pedido,
    l.min_buyer_age
from {{ ref('silver_order') }} o
join {{ ref('silver_customer') }} c
  on c.customer_id = o.customer_id
 and c.ingestion_date = o.customer_ingestion_date
cross join limite l
where year(o.ingestion_date) - c.birth_year < l.min_buyer_age

union all

-- THE PREMISE'S OWN FLOOR. Returns a row — and therefore fails — if someone lowers
-- min_buyer_age below 18. The columns match those of the query above because `union all`
-- requires it; what matters is the last one.
select
    'PREMISSA'                                          as order_id,
    '-'                                                 as wh,
    current_date                                        as ingestion_date,
    '-'                                                 as customer_id,
    null                                                as birth_year,
    null                                                as idade_no_pedido,
    l.min_buyer_age
from limite l
where l.min_buyer_age < 18
