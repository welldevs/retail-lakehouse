-- Preco zero ou negativo nao existe num catalogo de varejo. Medido nas tres particoes:
-- min 0.19, max 476.00.
select ingestion_date, warehouse, source_product_id, unit_price
from {{ ref('silver_product_price') }}
where unit_price is null or unit_price <= 0
