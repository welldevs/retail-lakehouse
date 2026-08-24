-- Unicidade do GRAO COMPOSTO. Deliberadamente nao e `unique` em source_product_id:
-- CONTRACT.md 4.8 diz que o produto repete entre categorias e subgrupos, e um teste de
-- unicidade no id isolado falharia por desenho da fonte, nao por defeito.
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
