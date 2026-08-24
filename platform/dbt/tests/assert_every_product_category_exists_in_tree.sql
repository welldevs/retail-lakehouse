-- Integridade referencial por particao: toda categoria citada em silver_product_price
-- existe na arvore da MESMA particao. Um join por category_id sem a data cruzaria
-- particoes e esconderia uma categoria que saiu da arvore.
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
