-- SECOES CENSITARIAS do Callejero (arquivo SECC), projetadas sem interpretacao alem de
-- decompor o codigo, que a fonte concatena sem separador.
--
-- GRAO: (ingestion_date, section_code). section_code (10 digitos) ja e unico dentro de
-- uma ingestion_date -- embute provincia+municipio+distrito+secao, entao nao ha colisao
-- entre provincias diferentes.
--
-- SECC nao tem nenhum outro campo alem do codigo -- medido contra os 4 arquivos reais
-- (CONTRACT.md da source, secao 2). Largura de linha e 11 (10 digitos + 1 espaco a
-- direita), nao 10 -- o espaco faz parte da linha, nao do codigo.
--
-- O arquivo e texto de largura fixa em ISO-8859-1 (Latin-1) com CRLF, nao JSON: le-se
-- cada linha inteira como uma unica coluna (delim que nunca aparece no dado) e extrai
-- por substr(), a mesma tecnica de silver_ine_population_series para "Nombre" -- aqui
-- aplicada a offsets de byte, nao a segmentos separados por ". ".
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_sections.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        trim(line)                                       as section_code
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/SECC.*',
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
    'SECC'                            as source_dataset,
    source_file,
    section_code,
    province_code,
    substr(section_code, 3, 3)        as municipality_code,
    substr(section_code, 6, 2)        as district_code,
    substr(section_code, 8, 3)        as section_number

from raw
