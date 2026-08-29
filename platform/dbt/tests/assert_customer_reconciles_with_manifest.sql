-- FECHA O CICLO manifesto -> RAW -> Silver para a source de OLTP, no mesmo espirito de
-- assert_silver_reconciles_with_manifest.sql (Mercadona).
--
-- A Source declarou quantos clientes gerou, quantos ficaram sem numero de casa e quantos
-- municipios usou. O Silver chegou aos mesmos numeros por um caminho completamente
-- diferente: lendo o JSON aterrissado. Divergencia aqui significa que algo se perdeu ou
-- se duplicou entre a extracao e o parquet.
--
-- house_number_null e municipalities_used entram junto com a contagem porque sao os dois
-- totais que uma perda PARCIAL preservaria: perder linhas sem numero manteria
-- customer_rows errado mas tambem quebraria estes — e perder linhas de um municipio
-- inteiro passaria despercebido se so a contagem fosse conferida.
--
-- Falha se qualquer particao divergir.
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
