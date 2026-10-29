-- THE DEMAND GROUP IS STAMPED ON THE EVENT, which is why it can go stale.
--
-- `demand_group` reaches Silver from inside the `order_placed` payload, not from a join
-- with the catalog. That is deliberate and right: what matters is the group that held AT THE
-- TIME OF THE ORDER, not what the mapping would say today. But it has a price — the stamp does not
-- correct itself.
--
-- THE FAILURE MODE THIS CATCHES: a half-regenerated window. Someone edits
-- `demand_category_mapping_seed.csv`, re-exports the reference and runs `orders-refresh` on
-- two of the four warehouses. The other two keep the old stamp. The result is a
-- window in which the SAME product belongs to two groups, and any aggregation by
-- `demand_group` mixes two different calibrations — no error, no null, no broken
-- total. Exactly the most expensive class of defect in this repo.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT. `demand_group is not null` would pass easily: both
-- halves DO have a group, just different groups. And comparing against the mapping would require
-- reimplementing the wildcard resolution in SQL — a second implementation of the same rule,
-- which would diverge from the first at the first adjustment. Checking against the mapping is the job of
-- the Source's `validate`, which reads the same file the generator read; what is checked here is
-- the window's INTERNAL CONSISTENCY, which is the question only this layer can ask.
--
-- The grain is (product, category, subgroup) because that is the triple the mapping resolves: the
-- same product in two categories may legitimately fall into different groups.
select
    source_product_id,
    category_id,
    subgroup_id,
    count(distinct demand_group)                                  as grupos_distintos,
    string_agg(distinct demand_group, ' | ' order by demand_group) as grupos,
    min(ingestion_date)                                           as primeiro_dia,
    max(ingestion_date)                                           as ultimo_dia
from {{ ref('silver_order_line') }}
where demand_group is not null
group by 1, 2, 3
having count(distinct demand_group) > 1
