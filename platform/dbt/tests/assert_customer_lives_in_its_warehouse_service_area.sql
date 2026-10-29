-- THIS SOURCE'S CENTRAL GUARANTEE, re-checked in Silver: every customer lives in a municipality
-- of their own warehouse's Functional Urban Area.
--
-- It is already verified twice before reaching here — in the generator (unit test) and in
-- `oltp-validate --strict`, which re-reads the reference. This test is the third, and the only one that
-- looks at the data AFTER it crosses RAW and parquet: a landing error that
-- mixed partitions, or a future join that swapped the axis, would only show up in this
-- layer. The guarantee only holds if it is verified where the consumer reads.
--
-- Joins by CODE, never by name: measured that the Callejero and INE 29005 spell the 370
-- municipalities differently in 370 of 370 cases.
--
-- Fails with one row per customer outside the area.
select
    c.ingestion_date,
    c.wh,
    c.customer_id,
    c.province_code,
    c.municipality_code,
    c.municipality_name,
    c.postal_code
from {{ ref('silver_customer') }} c
left join {{ ref('warehouse_service_area') }} a
  on  a.wh                = c.wh
 and  a.province_code     = c.province_code
 and  a.municipality_code = c.municipality_code
where a.wh is null
