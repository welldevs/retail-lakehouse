-- PSEUDOVIAS do Callejero (arquivo PSEU): pontos endereçaveis fora de uma rua
-- convencional -- caixas de correio agrupadas ("BUZON..."), estacionamentos, conjuntos
-- de edificios com nome proprio. Nao e auxiliar de VIAS: sao identidades e codigos
-- proprios, sem correspondencia 1:1 com nenhuma rua -- por isso vira modelo Silver
-- proprio, nao so uma coluna extra em silver_callejero_streets.
--
-- GRAO: (ingestion_date, pseudo_address_code). Codigo (10 digitos) ja e unico dentro de
-- uma ingestion_date.
--
-- Layout medido (CONTRACT.md da source, secao 2): linha de 127 chars, codigo em [0:10]
-- (provincia 2 + municipio 3 + id 5), nome em [10:63], data em [63:71], nome completo
-- em [77:127]. Sem campo de tipo de via -- pseudovias nao sao classificadas por tipo.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_pseudo_addresses.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/PSEU.*',
        columns = {'line': 'VARCHAR'},
        delim = E'\x01',
        header = false,
        encoding = 'latin-1',
        quote = '',
        hive_partitioning = 1,
        filename = true
    )

)

select
    ingestion_date,
    'PSEU'                                          as source_dataset,
    source_file,
    substr(line, 1, 10)                             as pseudo_address_code,
    province_code,
    substr(line, 3, 3)                               as municipality_code,
    substr(line, 6, 5)                               as pseudo_address_id,
    strptime(substr(line, 64, 8), '%Y%m%d')::date   as reference_date,
    trim(substr(line, 11, 53))                       as name_short,
    trim(substr(line, 78, 50))                       as name_full

from raw
