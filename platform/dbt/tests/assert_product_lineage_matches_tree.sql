-- The source delivers the level-1 lineage along TWO paths: the tree in
-- categories/categories.json and the categories[] array inside each product. Measured:
-- they agree on 100% of rows. The redundancy is the test — if they ever diverge, the shape
-- of the source changed in a way schema_fingerprint does not catch.
select
    p.ingestion_date,
    p.warehouse,
    p.category_id,
    p.source_product_id,
    p.product_level1_category_id,
    c.parent_category_id
from {{ ref('silver_product_price') }} p
join {{ ref('silver_category') }} c
  on  c.ingestion_date = p.ingestion_date
 and  c.warehouse      = p.warehouse
 and  c.category_id    = p.category_id
where p.product_level1_category_id <> c.parent_category_id
