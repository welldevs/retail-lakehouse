-- PROJECAO da serie de populacao por provincia, tal como o INE devolve. Nenhuma
-- interpretacao alem de separar os campos que "Nombre" ja concatena (idade, territorio,
-- sexo).
--
-- GRAO: (ingestion_date, table_id, series_code, year, fk_periodo). Medido contra o
-- payload real (table_id=31304, 16.377 series, 1.547.496 pontos): cada serie tem DOIS
-- pontos por ano, um por fk_periodo (27 = referencia 30/06, 26 = referencia 31/12 —
-- INE publica duas estimativas por ano). "year" sozinho NAO e unico dentro de uma serie;
-- confirmado (serie, Anyo, FK_Periodo) unico para as 16.377 series inteiras, 0 excecoes.
--
-- year (Anyo da fonte) e reference_date PODEM discordar em fk_periodo=26: medido que
-- Anyo=2022/fk_periodo=26 tem reference_date=2021-12-31, um ano ANTES. Nao e erro de
-- calculo daqui — e a convencao do INE para "populacao em 1 de janeiro de <Anyo>",
-- registrada com Fecha em 31/12 do ano anterior. year fica como o rotulo da fonte;
-- reference_date fica como a data exata derivada de Fecha; nenhum dos dois e "corrigido"
-- para bater com o outro.
--
-- table_id NAO vem como coluna hive: ao contrario de wh=/ingestion_date= da Mercadona,
-- que sao segmentos de DIRETORIO, "table_id=<id>" e o NOME DO ARQUIVO
-- (tables/table_id=31304.json) — hive_partitioning=1 do DuckDB so reconhece key=value em
-- diretorio, nunca em nome de arquivo. Medido: sem filename=true, a coluna nao aparece.
-- Por isso ele e extraido do caminho do arquivo com regexp, nao do particionamento hive.
--
-- O glob abaixo mira table_id=31304.json ESPECIFICAMENTE, nao table_id=*.json: desde que
-- table_id=29005 (populacao por MUNICIPIO, ver silver_ine_population_by_municipality)
-- passou a aterrissar na mesma particao, um glob generico misturaria as duas tabelas
-- neste classificador — que so sabe o vocabulario de 31304. Colisao real, medida: "Sevilla"
-- e ao mesmo tempo o nome de uma provincia (lista abaixo) E o nome do municipio capital
-- dessa provincia — uma serie de 29005 tipo "Sevilla. Total. Total habitantes. Personas."
-- casaria com province_name="Sevilla" por vocabulario, viraria uma linha fantasma nesta
-- serie de PROVINCIA com sex_label/age_label errados (o classificador nao reconhece
-- "Total habitantes"/"Personas" como sexo ou idade). Cada table_id com sua propria
-- projecao, cada uma so le o proprio arquivo — sem glob compartilhado entre as duas.
--
-- Provincia, idade e sexo vem concatenados como TEXTO dentro de "Nombre", nao como
-- colunas separadas nem codigos: "<Idade>. <Territorio>. <Sexo>. Población. Número.",
-- ex.: "Total. Albacete. Ambos sexos. Población. Número.". MEDIDO CONTRA O PAYLOAD REAL:
-- a ORDEM dos tres primeiros segmentos NAO e fixa —
--   * 15.912 series (97,2%): "Idade. Territorio. Sexo." (a ordem documentada)
--   * 156 series (o bucket "85 y más años" inteiro): "Sexo. Idade. Territorio."
--   * 309 series: sao o TOTAL NACIONAL, nao uma provincia — "Total Nacional. Idade.
--     Sexo." ou (so a faixa "100 y más años") "Idade. Total Nacional. Sexo."
-- Por isso territorio e sexo sao identificados por VOCABULARIO conhecido (lista fechada
-- de 52 provincias + "Total Nacional"; 3 valores de sexo), nao por posicao — extrair
-- por indice fixo (ex.: split_part(..., 2)) produz "province_name" errado para ~2,8%
-- das series (idade ou sexo, gravado como se fosse provincia). "Total Nacional" fica
-- como valor legitimo de province_name (nao e uma provincia real, mas nenhuma provincia
-- real se chama assim, entao um join futuro por nome de provincia o ignora sozinho).
-- Idade e o segmento que sobra depois de identificar territorio e sexo — nunca lido por
-- posicao fixa.
--
-- Este modelo assume que ha ao menos um arquivo aterrissado (read_json falha com "no
-- files found" sobre um glob vazio). Quando esta source ainda nao aterrissou nada — ex.:
-- clone novo do repositorio, antes do primeiro `make ine-refresh` — `make silver`
-- exclui este modelo do `dbt build` via `retail-platform has-data`, para nao derrubar o
-- build inteiro (que serve tambem a Mercadona) por causa de uma source vazia. Ver
-- Makefile, alvo `silver`.
--
-- location e PLANO sob silver/ (silver/silver_ine_population_series.parquet), nao
-- silver/ine_population/silver_ine_population_series.parquet: medido que
-- retail_platform/query.py deriva o nome da view do primeiro segmento apos "silver/" —
-- um arquivo sob um subdiretorio vira uma view particionada chamada pelo NOME DO
-- SUBDIRETORIO, nao pelo nome do arquivo. A organizacao em models/silver/ine_population/
-- (pasta do .sql, so afeta onde o dbt ACHA o arquivo fonte) e independente do layout de
-- saida no bucket, que segue a convencao ja em uso pelos modelos da Mercadona: um
-- arquivo .parquet por modelo, direto sob silver/.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_ine_population_series.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with provincias (province_name) as (

    -- As 52 provincias de Espanha (50 + Ceuta e Melilla), tal como aparecem em "Nombre".
    -- Lista fechada, medida contra o payload real de table_id=31304 — nao um catalogo
    -- geografico geral; se um table_id futuro usar outra grafia, este CTE precisa mudar.
    values
        ('Albacete'), ('Alicante/Alacant'), ('Almería'), ('Araba/Álava'), ('Asturias'),
        ('Ávila'), ('Badajoz'), ('Balears, Illes'), ('Barcelona'), ('Bizkaia'), ('Burgos'),
        ('Cáceres'), ('Cádiz'), ('Cantabria'), ('Castellón/Castelló'), ('Ceuta'),
        ('Ciudad Real'), ('Córdoba'), ('Coruña, A'), ('Cuenca'), ('Gipuzkoa'), ('Girona'),
        ('Granada'), ('Guadalajara'), ('Huelva'), ('Huesca'), ('Jaén'), ('León'),
        ('Lleida'), ('Lugo'), ('Madrid'), ('Málaga'), ('Melilla'), ('Murcia'),
        ('Navarra'), ('Ourense'), ('Palencia'), ('Palmas, Las'), ('Pontevedra'),
        ('Rioja, La'), ('Salamanca'), ('Santa Cruz de Tenerife'), ('Segovia'),
        ('Sevilla'), ('Soria'), ('Tarragona'), ('Teruel'), ('Toledo'),
        ('Valencia/València'), ('Valladolid'), ('Zamora'), ('Zaragoza')

),

raw_series as (

    select
        ingestion_date,
        regexp_extract(filename, 'table_id=([^.]+)\.json$', 1) as table_id,
        "COD"                                                  as series_code,
        "Nombre"                                                as series_name,
        "Data"                                                  as data_points
    from read_json(
        '{{ var("ine_raw_prefix") }}/ingestion_date=*/tables/table_id=31304.json',
        hive_partitioning = 1,
        union_by_name = true,
        filename = true
    )

),

segmented as (

    select
        *,
        trim(split_part(series_name, '. ', 1)) as seg1,
        trim(split_part(series_name, '. ', 2)) as seg2,
        trim(split_part(series_name, '. ', 3)) as seg3
    from raw_series

),

classified as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        data_points,
        seg1,
        seg2,
        seg3,

        -- territorio: bate com a provincia conhecida OU com "Total Nacional", em
        -- qualquer um dos 3 segmentos. coalesce pega o primeiro que casar.
        coalesce(
            case when seg1 = 'Total Nacional' or seg1 in (select province_name from provincias) then seg1 end,
            case when seg2 = 'Total Nacional' or seg2 in (select province_name from provincias) then seg2 end,
            case when seg3 = 'Total Nacional' or seg3 in (select province_name from provincias) then seg3 end
        ) as province_name,

        -- sexo: bate com um dos 3 valores fixos, em qualquer um dos 3 segmentos.
        coalesce(
            case when seg1 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg1 end,
            case when seg2 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg2 end,
            case when seg3 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg3 end
        ) as sex_label

    from segmented

),

dimensioned as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        data_points,
        province_name,
        sex_label,

        -- idade: o segmento que sobra depois de province_name e sex_label identificados.
        case
            when seg1 is distinct from province_name and seg1 is distinct from sex_label then seg1
            when seg2 is distinct from province_name and seg2 is distinct from sex_label then seg2
            else seg3
        end as age_label

    from classified

),

exploded as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        province_name,
        age_label,
        sex_label,
        unnest(data_points) as point
    from dimensioned

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
    table_id,
    series_code,
    series_name,
    province_name,
    age_label,
    sex_label,

    point."Anyo"                                                as year,
    point."FK_Periodo"                                          as fk_periodo,
    to_timestamp(point."Fecha" / 1000)::date                    as reference_date,
    point."FK_TipoDato"                                         as fk_tipo_dato,
    point."Valor"                                               as population_value

from exploded
