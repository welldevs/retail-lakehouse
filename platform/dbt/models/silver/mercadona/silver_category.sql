-- Category tree: level 1 (26) > level 2 (151), per partition.
--
-- Verified in the snapshots: the tree has EXACTLY two levels — no level-2 node carries
-- children. That is why the flattening here is final, and not a simplification.
--
-- CONTRACT.md section 6: only level-2 ids are reachable at /api/categories/{id}/;
-- level-1 ids respond 404/410. Level 1 exists only in this tree.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_category',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite': 1}
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
