-- POPULACAO por MUNICIPIO do INE (Tempus3, table_id 29005 — "Cifras oficiales del
-- padron por municipio"), projetada sem interpretacao alem de separar municipio/sexo,
-- que a fonte concatena em "Nombre", e resolver o CODIGO do municipio via o seed
-- ine_municipality_codes_seed — table_id=29005 nao traz codigo, so o nome por extenso
-- (ver CONTRACT.md da source, secao 2, e scripts/derive_municipality_codes.py).
--
-- GRAO: (ingestion_date, series_code, year, fk_periodo). fk_periodo=28 e o unico valor
-- medido no payload real (ao contrario de 31304, que publica 2 estimativas/ano —
-- fk_periodo 26 e 27); mantido no grao por simetria e como salvaguarda caso o INE
-- comece a publicar mais de uma estimativa/ano aqui no futuro, nao porque foi observado.
--
-- MEDIDO CONTRA O PAYLOAD REAL (amostra de 4.509 series completas — a resposta sem
-- filtro tem historico desde 1996, e grande o bastante para truncar na rede durante
-- esta investigacao; a amostra e o prefixo integro ate o corte):
--   * "Nombre" tem SEMPRE exatamente 4 segmentos separados por ". ":
--     "<Municipio>. <Sexo>. Total habitantes. Personas." — ao contrario de 31304, a
--     ORDEM AQUI E FIXA (nenhuma excecao na amostra), entao municipio e sexo sao
--     extraidos por POSICAO (split_part 1 e 2), nao por vocabulario fechado.
--   * sexo e sempre um de 'Total'/'Hombres'/'Mujeres' — vocabulario DIFERENTE de
--     31304, que usa 'Ambos sexos' para o mesmo papel. Fontes diferentes, nao
--     normalizado aqui: cada modelo expoe o vocabulario da propria fonte.
--   * Secreto=false em 100% dos pontos da amostra (nenhuma supressao por sigilo
--     estatistico observada) — exposto como coluna propria (is_secret) em vez de
--     transformado, porque essa ausencia NAO foi verificada exaustivamente (a amostra
--     nao cobre os ~8.200 municipios inteiros) e nao ha payload observado com
--     Secreto=true para saber o que a fonte poe em "Valor" nesse caso.
--
-- Nome de municipio NAO E CHAVE SEGURA em geral (18 nomes duplicados na Espanha entre
-- provincias diferentes — ver ine_municipality_codes_seed) — mas o seed ja vem escopado
-- a 08/28/41/46, onde nenhuma colisao foi medida, entao o INNER JOIN abaixo e seguro
-- sem risco de fanout. O INNER JOIN tambem e a interpretacao deliberada do escopo: RAW
-- preserva os ~8.200 municipios da Espanha inteira tal como a fonte devolve (nada e
-- filtrado na extracao), mas este modelo Silver so resolve codigo — e so serve consulta
-- cruzada com o Callejero — para o que esta plataforma realmente cobre, mesmo raciocinio
-- de escopo que warehouse_service_area ja aplica.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_ine_population_by_municipality.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw_series as (

    select
        ingestion_date,
        "COD"    as series_code,
        "Nombre" as series_name,
        "Data"   as data_points
    from read_json(
        '{{ var("ine_raw_prefix") }}/ingestion_date=*/tables/table_id=29005.json',
        hive_partitioning = 1,
        union_by_name = true
    )

),

segmented as (

    select
        ingestion_date,
        series_code,
        series_name,
        data_points,
        trim(split_part(series_name, '. ', 1)) as municipality_name,
        trim(split_part(series_name, '. ', 2)) as sex_label
    from raw_series

),

exploded as (

    select
        ingestion_date,
        series_code,
        series_name,
        municipality_name,
        sex_label,
        unnest(data_points) as point
    from segmented

)

select
    e.ingestion_date,
    e.series_code,
    e.series_name,
    c.province_code,
    c.municipality_code,
    e.municipality_name,
    e.sex_label,

    e.point."Anyo"                            as year,
    e.point."FK_Periodo"                      as fk_periodo,
    to_timestamp(e.point."Fecha" / 1000)::date as reference_date,
    e.point."Secreto"                         as is_secret,
    e.point."Valor"                           as population_value

from exploded e
inner join {{ ref('ine_municipality_codes_seed') }} c
    on e.municipality_name = c.municipality_name
