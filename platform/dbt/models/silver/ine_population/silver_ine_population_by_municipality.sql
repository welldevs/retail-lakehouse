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
-- Nome de municipio NAO E CHAVE SEGURA: 18 nomes se repetem na Espanha entre provincias
-- diferentes (ver ine_municipality_codes_seed). O seed esta escopado a 08/28/41/46 e nao
-- tem NENHUMA colisao interna — medido — mas isso sozinho NAO basta, e uma versao
-- anterior deste comentario concluia errado que bastava: o lado RAW e NACIONAL (~8.200
-- municipios, nada e filtrado na extracao), entao uma linha do seed casa tanto a serie da
-- nossa provincia quanto a homonima de fora. Fanout medido em 3 municipios:
--
--     "Torrent. Total. Total habitantes. Personas. " -> DPOP21778 = 90.928 (46244)
--     "Torrent. Total. Total habitantes. Personas. " -> DPOP7960  =    182 (17197, Girona)
--
-- O teste de grao (ingestion_date, series_code, year, fk_periodo) nao pegava, porque as
-- duas series tem series_code diferente — dai o teste adicional
-- assert_ine_population_by_municipality_has_one_series_per_municipality.
--
-- CORRECAO: `ine_ambiguous_series_seed` traz, para cada serie de nome ambiguo, o CODIGO
-- OFICIAL do municipio (provincia+municipio) obtido da propria API do INE
-- (VALORES_SERIE/{COD} — ver scripts/derive_ambiguous_series.py). Series de nome ambiguo
-- so passam quando o codigo oficial bate com o do seed; nomes nao ambiguos seguem
-- resolvidos por nome, como antes. Nao e heuristica de "fica o maior valor": e a fonte
-- dizendo qual serie e de qual municipio.
--
-- O INNER JOIN com o seed continua sendo a interpretacao deliberada do escopo: este
-- modelo Silver so resolve codigo — e so serve consulta cruzada com o Callejero — para o
-- que esta plataforma realmente cobre, mesmo raciocinio que warehouse_service_area aplica.
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
    -- Uma reextracao da MESMA publicacao do INE cria outra ingestion_date com linhas
    -- equivalentes, e este modelo empilha todas (o historico e deliberado). Sem uma
    -- marca explicita, quem consultar sem filtrar conta em dobro — ja aconteceu com
    -- silver_ine_population_series, que ficou com 1.547.496 linhas em CADA uma de duas
    -- ingestion_date. `where is_latest_ingestion` e a forma certa de ler o estado atual;
    -- sem o filtro, le-se o historico inteiro, e isso passa a ser uma escolha e nao um
    -- acidente.
    e.ingestion_date = max(e.ingestion_date) over () as is_latest_ingestion,
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
-- Só as series de nome ambiguo aparecem neste seed. Para elas, o codigo oficial da fonte
-- tem de bater com o do seed; para todas as demais, `a.series_code is null` e o nome ja
-- resolve sozinho.
left join {{ ref('ine_ambiguous_series_seed') }} a
    on e.series_code = a.series_code
where a.series_code is null
   or (a.province_code = c.province_code and a.municipality_code = c.municipality_code)
