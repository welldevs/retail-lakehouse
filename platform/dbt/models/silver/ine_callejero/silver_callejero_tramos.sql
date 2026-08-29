-- TRAMOS de via do Callejero (arquivo TRAM): a unica das 4 fontes de geografia do INE
-- que liga, num so registro, secao censitaria + entidade/nucleo + via OU pseudovia +
-- CODIGO POSTAL + faixa de numeracao. E o que permite montar Rua+Cidade+CEP+Bairro
-- (nucleo, onde existir) numa unica consulta, cruzando com os outros 3 modelos desta
-- source por codigo (nao ha FK fisica nos arquivos, so os codigos coincidem).
--
-- GRAO: (ingestion_date, section_code, entity_suffix, street_id, pseudo_address_id,
-- postal_code, numbering_type, number_from, number_from_qualifier, number_to,
-- number_to_qualifier). Nao ha uma unica coluna de "id do tramo" nos bytes — grao
-- composto verificado sem excecao contra as 304.952 linhas reais das 4 provincias (ver
-- assert_callejero_tramos_grain_is_unique.sql).
--
-- Layout medido (CONTRACT.md da source, secao 2): linha de 273 chars. [0:10] codigo de
-- SECAO (mesmo formato de SECC) · [13:20] CUN, sufixo de entidade/nucleo (mesmo formato
-- de UP) · [20:25] CVIA, id de via (mesmo formato de VIAS; "00000" = nao se aplica) ·
-- [25:30] CPSVIA, id de pseudovia (mesmo formato de PSEU; "00000" = nao se aplica) —
-- CVIA e CPSVIA sao mutuamente exclusivos, confirmado sem excecao nas 4 provincias ·
-- [42:47] CPOS, codigo postal (5 digitos, sempre com o prefixo correto da provincia,
-- confirmado em 304.905/304.952 linhas — 47 excecoes, 0,015%, so em Barcelona) · [47:48]
-- TINUM, tipo de numeracao (0=sem numeracao, 1=impar, 2=par) · [48:52]+[52:53] EIN+CEIN,
-- extremo inferior da faixa + qualificador de duplicado · [53:57]+[57:58] ESN+CESN,
-- extremo superior + qualificador · [61:69] data de referencia. Confirmado contra o
-- "Diseno de registro" oficial do INE/IDA-Padron (nomeia os campos do "Tramero" na
-- mesma ordem observada nos bytes) — ver CONTRACT.md secao 6 para a referencia.
--
-- street_code/pseudo_address_code sao reconstruidos (provincia+municipio+id) para JOIN
-- direto com silver_callejero_streets/silver_callejero_pseudo_addresses; quando o id e
-- "00000" (sentinela de "nao se aplica"), o codigo reconstruido simplesmente nao bate
-- com nenhuma rua/pseudovia real — nenhum tratamento especial precisa disso.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_tramos.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/TRAM.*',
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
    'TRAM'                                                  as source_dataset,
    source_file,
    substr(line, 1, 10)                                     as section_code,
    province_code,
    substr(line, 3, 3)                                      as municipality_code,
    substr(line, 6, 2)                                      as district_code,
    substr(line, 8, 3)                                       as section_number,
    substr(line, 14, 7)                                     as entity_suffix,
    province_code || substr(line, 3, 3) || substr(line, 14, 7)  as unit_code,
    substr(line, 21, 5)                                     as street_id,
    province_code || substr(line, 3, 3) || substr(line, 21, 5)  as street_code,
    substr(line, 26, 5)                                     as pseudo_address_id,
    province_code || substr(line, 3, 3) || substr(line, 26, 5)  as pseudo_address_code,
    substr(line, 43, 5)                                     as postal_code,
    substr(line, 48, 1)                                     as numbering_type,
    substr(line, 49, 4)                                     as number_from,
    trim(substr(line, 53, 1))                               as number_from_qualifier,
    substr(line, 54, 4)                                     as number_to,
    trim(substr(line, 58, 1))                               as number_to_qualifier,
    strptime(substr(line, 62, 8), '%Y%m%d')::date          as reference_date

from raw
