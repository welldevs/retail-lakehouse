-- A fonte entrega a linhagem de nivel 1 por DOIS caminhos: a arvore em
-- categories/categories.json e o array categories[] dentro de cada produto. Medido:
-- concordam em 100% das linhas. A redundancia e o teste — se um dia divergirem, a forma
-- da fonte mudou de um jeito que o schema_fingerprint nao pega.
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
