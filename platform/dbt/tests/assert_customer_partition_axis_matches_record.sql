-- THE PARTITION AXIS MUST AGREE WITH THE RECORD, in both of the source's models.
--
-- Measured: in customers.json the record's `wh` field and the hive `wh=` path segment
-- have the same name, and DuckDB's read_json deduplicates the two into a single column — the
-- record's value wins and the path's disappears. A partition landed under the wrong directory would be
-- invisible: the column would read "vlc1" (from the record) while the object lived in wh=svq1/.
-- silver_customer.partition_wh exists precisely to give the path a name of its own and
-- make that comparison possible.
--
-- In the manifest the problem does not exist (source.wh and hive wh have distinct names), but the
-- check is the same and holds for the same reason — it is the pattern raw_manifest.sql already uses
-- with warehouse/source_warehouse.
--
-- Fails with one row per divergence.
select 'silver_customer' as modelo, ingestion_date, wh, partition_wh as caminho, count(*) as linhas
from {{ ref('silver_customer') }}
where partition_wh is distinct from wh
group by 1, 2, 3, 4

union all

select 'silver_oltp_manifest', ingestion_date, source_warehouse, wh, count(*)
from {{ ref('silver_oltp_manifest') }}
where source_warehouse is distinct from wh
group by 1, 2, 3, 4
