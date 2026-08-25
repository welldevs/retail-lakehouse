-- Arvore de categorias: nivel 1 (26) > nivel 2 (151), por particao.
--
-- Verificado nos snapshots: a arvore tem EXATAMENTE dois niveis — nenhum no de nivel 2
-- carrega filhos. Por isso o achatamento aqui e final, e nao uma simplificacao.
--
-- CONTRACT.md secao 6: apenas ids de nivel 2 sao acessiveis em /api/categories/{id}/;
-- ids de nivel 1 respondem 404/410. O nivel 1 existe apenas nesta arvore.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_category',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite_or_ignore': 1}
) }}

with tree as (

    select
        ingestion_date,
        wh                              as warehouse,
        unnest(results)                 as level1
    from read_json(
        '{{ var("raw_prefix") }}/ingestion_date=*/wh=*/categories/categories.json',
        hive_partitioning = 1,
        union_by_name = true
    )

)

select
    ingestion_date,
    warehouse,
    level2.id                           as category_id,
    level2.name                         as category_name,
    level2.order                        as category_order,
    level1.id                           as parent_category_id,
    level1.name                         as parent_category_name,
    level1.order                        as parent_category_order
from (
    select ingestion_date, warehouse, level1, unnest(level1.categories) as level2
    from tree
)
