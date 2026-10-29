-- Referential integrity per partition: every category cited in silver_product_price
-- exists in the tree of the SAME partition. A join on category_id without the date would cross
-- partitions and hide a category that left the tree.
select
    p.ingestion_date,
    p.warehouse,
    p.category_id,
    count(*) as orphan_rows
from {{ ref('silver_product_price') }} p
left join {{ ref('silver_category') }} c
  on  c.ingestion_date = p.ingestion_date
 and  c.warehouse      = p.warehouse
 and  c.category_id    = p.category_id
where c.category_id is null
group by 1, 2, 3
