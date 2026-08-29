-- A GARANTIA CENTRAL DESTA SOURCE, reconferida no Silver: todo cliente mora num municipio
-- da Area Urbana Funcional do proprio armazem.
--
-- Ja e verificada duas vezes antes de chegar aqui — no gerador (teste unitario) e no
-- `oltp-validate --strict`, que rele a referencia. Este teste e a terceira, e a unica que
-- olha o dado DEPOIS de ele atravessar RAW e parquet: um erro de aterrissagem que
-- misturasse particoes, ou um join futuro que trocasse o eixo, so apareceria nesta
-- camada. A garantia so vale se for verificada onde o consumidor le.
--
-- Junta por CODIGO, nunca por nome: medido que Callejero e INE 29005 grafam os 370
-- municipios de forma diferente em 370 dos 370 casos.
--
-- Falha com uma linha por cliente fora da area.
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
