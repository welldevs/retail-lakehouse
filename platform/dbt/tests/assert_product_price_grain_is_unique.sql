-- Uniqueness of the COMPOSITE GRAIN. Deliberately not `unique` on source_product_id:
-- CONTRACT.md 4.8 says the product repeats across categories and subgroups, and a uniqueness
-- test on the id alone would fail by the source's design, not by defect.
select
    ingestion_date,
    warehouse,
    category_id,
    subgroup_id,
    source_product_id,
    count(*) as n
from {{ ref('silver_product_price') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
