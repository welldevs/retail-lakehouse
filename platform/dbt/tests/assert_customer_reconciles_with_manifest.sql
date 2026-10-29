-- CLOSES THE manifest -> RAW -> Silver LOOP for the OLTP source, in the same spirit as
-- assert_silver_reconciles_with_manifest.sql (Mercadona).
--
-- The Source declared how many customers it generated, how many ended up without a house number and how many
-- municipalities it used. Silver reached the same numbers along a completely
-- different path: reading the landed JSON. A divergence here means something got lost or
-- duplicated between extraction and parquet.
--
-- house_number_null and municipalities_used come in alongside the count because they are the two
-- totals a PARTIAL loss would preserve: losing rows without a number would keep
-- customer_rows wrong but would also break these — and losing the rows of a whole municipality
-- would go unnoticed if only the count were checked.
--
-- Fails if any partition diverges.
with derived as (

    select
        ingestion_date,
        wh,
        count(*)                                              as observed_rows,
        count(*) filter (where house_number is null)          as observed_house_number_null,
        count(distinct province_code || municipality_code)    as observed_municipalities
    from {{ ref('silver_customer') }}
    group by 1, 2

)

select
    m.ingestion_date,
    m.wh,
    m.declared_customer_rows,
    d.observed_rows,
    m.declared_house_number_null,
    d.observed_house_number_null,
    m.declared_municipalities_used,
    d.observed_municipalities
from {{ ref('silver_oltp_manifest') }} m
join derived d
  on  d.ingestion_date = m.ingestion_date
 and  d.wh             = m.wh
where m.declared_customer_rows       <> d.observed_rows
   or m.declared_house_number_null   <> d.observed_house_number_null
   or m.declared_municipalities_used <> d.observed_municipalities
