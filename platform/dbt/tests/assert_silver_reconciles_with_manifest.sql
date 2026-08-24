-- O TESTE QUE FECHA O CICLO manifesto -> RAW -> Silver.
--
-- A Source declara totals.product_rows e totals.unique_product_ids no manifesto. O
-- Silver derivou suas linhas lendo os arquivos de catalogo do object storage, por um
-- caminho completamente diferente. Se os dois numeros divergem, alguma coisa se perdeu
-- entre a extracao e o parquet — e melhor descobrir aqui do que num dashboard.
--
-- Falha se qualquer particao divergir.
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
