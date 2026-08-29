-- UNIDADES POPULACIONAIS (nucleos) do Callejero (arquivo UP), projetadas sem
-- interpretacao alem de decompor o codigo e extrair os dois nomes uteis.
--
-- GRAO: (ingestion_date, unit_code). unit_code (12 digitos) ja e unico dentro de uma
-- ingestion_date.
--
-- Layout medido (CONTRACT.md da source, secao 2), NAO documentado oficialmente pelo
-- INE no download: linha de 604 chars, codigo em [0:12] (provincia 2 + municipio 3 +
-- sufixo 7; sufixo "0000000" e a linha AGREGADA do municipio inteiro, as demais sao
-- nucleos/entidades dentro dele), data em [15:23], nome do MUNICIPIO em [94:314], nome
-- da UNIDADE POPULACIONAL (o nucleo -- ex. "*DISEMINADO*" para populacao dispersa) em
-- [459:529]. Existem mais repeticoes do mesmo texto em outras larguras (formas
-- abreviadas), nao extraidas aqui -- confirmado comparando o conteudo real, nao
-- assumido por semelhanca de posicao.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_population_units.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/UP.*',
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
    -- Uma reextracao da MESMA publicacao do INE cria outra ingestion_date com linhas
    -- equivalentes, e este modelo empilha todas (o historico e deliberado). Sem uma
    -- marca explicita, quem consultar sem filtrar conta em dobro — ja aconteceu com
    -- silver_ine_population_series, que ficou com 1.547.496 linhas em CADA uma de duas
    -- ingestion_date. `where is_latest_ingestion` e a forma certa de ler o estado atual;
    -- sem o filtro, le-se o historico inteiro, e isso passa a ser uma escolha e nao um
    -- acidente.
    ingestion_date = max(ingestion_date) over () as is_latest_ingestion,
    'UP'                                          as source_dataset,
    source_file,
    substr(line, 1, 12)                           as unit_code,
    province_code,
    substr(line, 3, 3)                             as municipality_code,
    substr(line, 6, 7)                             as entity_suffix,
    substr(line, 6, 7) = '0000000'                 as is_municipality_aggregate,
    strptime(substr(line, 16, 8), '%Y%m%d')::date  as reference_date,
    trim(substr(line, 95, 220))                    as municipality_name,
    trim(substr(line, 460, 70))                    as population_unit_name

from raw
