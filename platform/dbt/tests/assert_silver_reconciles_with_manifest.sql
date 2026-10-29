-- THE TEST THAT CLOSES THE manifest -> RAW -> Silver LOOP.
--
-- The Source declares totals.product_rows and totals.unique_product_ids in the manifest.
-- Silver derived its rows by reading the catalog files from object storage, along a
-- completely different path. If the two numbers diverge, something got lost
-- between extraction and parquet — and it is better to find out here than on a dashboard.
--
-- Fails if any partition diverges.
with derived as (
    select
        ingestion_date,
        warehouse,
        count(*)                                as observed_rows,
        count(distinct source_product_id)       as observed_unique
    from {{ ref('silver_product_price') }}
    group by 1, 2
)

select
    m.ingestion_date,
    m.warehouse,
    m.declared_product_rows,
    d.observed_rows,
    m.declared_unique_product_ids,
    d.observed_unique
from {{ ref('raw_manifest') }} m
join derived d
  on  d.ingestion_date = m.ingestion_date
 and  d.warehouse      = m.warehouse
where m.declared_product_rows      <> d.observed_rows
   or m.declared_unique_product_ids <> d.observed_unique
