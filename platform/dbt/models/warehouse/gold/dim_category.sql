-- THE CATEGORY TREE, flattened into two levels.
--
-- GRAIN: one level-2 category (151). KEY: category_id.
-- TYPE: observed.
--
-- GLOBAL, with no warehouse or date axis: measured that the tree has exactly 151
-- level-2 categories in EACH of the 4 warehouses, identical. Giving this dimension a `wh`
-- axis would create 604 rows to represent 151 things.
--
-- TWO LEVELS IS FINAL, not a simplification: verified in the snapshots that no level-2
-- node carries children. And level 1 (26 categories) exists ONLY in this tree — the API
-- responds 404/410 for level-1 ids (CONTRACT.md section 6), so it is not a queryable
-- entity, just a grouping. That is why it becomes a flattened attribute and not its own
-- dimension with a foreign key.
{{ config(materialized = 'table') }}

select
    md5(cast(category_id as varchar))       as category_sk,

    category_id,
    category_name,
    category_order,

    parent_category_id,
    parent_category_name,
    parent_category_order
from {{ source('stage', 'STG_CATEGORY') }}
