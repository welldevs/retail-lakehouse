-- RUAS do Callejero (arquivo VIAS), projetadas sem interpretacao alem de decompor o
-- codigo e extrair os campos de nome/tipo.
--
-- GRAO: (ingestion_date, street_code). street_code (10 digitos) ja e unico dentro de
-- uma ingestion_date.
--
-- Layout medido (CONTRACT.md da source, secao 2): linha de 132 chars, codigo em [0:10]
-- (provincia 2 + municipio 3 + id da via 5), nome curto em [10:38], data em [38:46],
-- tipo de via (CALLE/PLAZA/CMNO/PRAJE...) em [52:57], nome completo em [57:107], nome
-- curto alternativo em [107:132]. Validado contra enderecos reais: "Alcala" no
-- municipio 079 (Madrid), "Gran Via" em 3 municipios diferentes.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_streets.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/VIAS.*',
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
    'VIAS'                                         as source_dataset,
    source_file,
    substr(line, 1, 10)                            as street_code,
    province_code,
    substr(line, 3, 3)                              as municipality_code,
    substr(line, 6, 5)                              as street_id,
    strptime(substr(line, 39, 8), '%Y%m%d')::date  as reference_date,
    trim(substr(line, 53, 5))                       as street_type,
    trim(substr(line, 11, 28))                      as street_name_short,
    trim(substr(line, 58, 50))                      as street_name_full,
    trim(substr(line, 108, 25))                     as street_name_alt

from raw
