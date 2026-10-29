-- The partition axis must match what the record says, in the two models that expose the
-- two sides.
--
-- WHY THIS EXTRA COLUMN EXISTS. The event's `wh` field and the hive `wh=` segment have the
-- SAME name, and DuckDB deduplicates them into a single column — the record's value wins and the
-- path's disappears. Without `partition_wh`, a partition landed under the wrong directory would be
-- internally consistent and therefore invisible to every downstream test. The same defect
-- the invalid Mercadona API `wh` produces, which is already recorded in ARCHITECTURE.md.
select 'silver_order_event' as modelo, order_id as chave, wh, partition_wh
from {{ ref('silver_order_event') }}
where partition_wh is distinct from wh

union all

select 'silver_orders_manifest', run_id, wh, source_warehouse
from {{ ref('silver_orders_manifest') }}
where source_warehouse is distinct from wh
   or partition_wh is distinct from wh
