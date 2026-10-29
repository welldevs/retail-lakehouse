-- A PREMISE silver_price_change DEPENDS ON.
--
-- It reduces the repeated appearances of an id with any_value(unit_price). That is only safe
-- because the price does NOT diverge across appearances of the same id in the same partition (measured:
-- 0 divergences; only the categories[] array differs, in 198 and 199 cases). If the source
-- starts returning different prices per category, any_value would become an arbitrary and
-- silent choice — this test fails before that happens.
select
    ingestion_date,
    warehouse,
    source_product_id,
    count(distinct unit_price) as distinct_prices
from {{ ref('silver_product_price') }}
group by 1, 2, 3
having count(distinct unit_price) > 1
