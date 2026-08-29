-- PRODUTO, SCD2 — e a dimensao NOMEIA uma ambiguidade em vez de escondê-la.
--
-- GRAO: uma versao de produto, (source_product_id, valid_from).
-- CHAVE: product_sk. Chave natural: source_product_id.
-- TIPO: observed.
--
-- O QUE ESTE SCD2 REALMENTE MODELA. Chavear em `source_product_id` modela o ciclo de vida
-- DA CHAVE DA FONTE, nao do item comercial — o CONTRACT.md 4.5 e o ARCHITECTURE.md ja
-- registram que a fonte nao oferece identidade de negocio. Nos casos medidos de
-- `name_seen_before` (id novo cujo display_name ja existia na particao anterior), o SCD2
-- produz "um produto morreu, outro nasceu". Pode estar certo, mas e uma ESCOLHA, e por
-- isso a flag identity_ambiguous e carregada para dentro da dimensao: quem consultar ve o
-- caso, em vez de herdar a decisao sem saber.
--
-- SCD2 DERIVADO DA HISTORIA, NAO ACUMULADO POR MERGE — e esta e a decisao de fundo.
-- O STAGE traz TODAS as particoes, e o RAW guarda todos os snapshots: a dimensao pode ser
-- reconstruida do zero a qualquer momento, sempre igual. Um `dbt snapshot` faria o oposto
-- — criaria estado que se acumula fora do RAW, que nao pode ser reconstruido e que pode
-- divergir dele em silencio. O repo inteiro se apoia em "o RAW e o ponto de nao-retorno,
-- tudo a jusante e reconstruivel"; derivar da historia e a unica forma coerente com isso.
--
-- CONSEQUENCIA HONESTA A REGISTRAR: o ARCHITECTURE.md dizia que o Iceberg entraria
-- "quando o Gold dim_product precisar de SCD2 por MERGE em tabela existente". Esse gatilho
-- NAO disparou — a historia completa tornou o MERGE desnecessario. O Snowflake continua
-- justificado por governanca (RBAC) e por ser camada servida, nao por MERGE.
--
-- ATRIBUTOS RASTREADOS (Tipo 2): display_name e a categoria primaria. Preco NAO — preco e
-- fato, e vive em FACT_PRICE_SNAPSHOT. Rastrear preco aqui criaria uma versao nova de
-- produto a cada oscilacao e a dimensao viraria o fato.
--
-- DIMENSAO GLOBAL, sem eixo de armazem: medido que display_name nao varia entre os 4
-- armazens (0 divergencias). O que varia entre armazens e preco e sortimento, e os dois
-- sao fatos.
{{ config(materialized = 'table') }}

with por_data as (

    -- Um produto por data, atravessando os armazens. `max` e seguro aqui pela medicao
    -- acima; se display_name passar a divergir entre armazens, o teste de unicidade de
    -- grao nao pega — mas o de contagem de versoes sim, porque a dimensao passaria a
    -- oscilar. Fica dito.
    select
        source_product_id,
        ingestion_date,
        max(display_name)               as display_name,
        max(primary_category_id)        as primary_category_id,
        max(category_appearances)       as category_appearances,
        max(pack_size)                  as pack_size,
        max(unit_size)                  as unit_size,
        max(unit_name)                  as unit_name,
        max(size_format)                as size_format,
        count(distinct warehouse)       as warehouses_listing
    from {{ source('stage', 'STG_PRODUCT_PRICE') }}
    group by 1, 2

),

-- A especificacao da janela e repetida em vez de nomeada por uma clausula `window`:
-- o Snowflake nao suporta `WINDOW <nome> AS (...)`. Repetir e feio e e o que compila.
marcado as (

    select
        *,
        case
            when lag(ingestion_date) over (
                partition by source_product_id order by ingestion_date) is null then 1
            when display_name is distinct from lag(display_name) over (
                partition by source_product_id order by ingestion_date) then 1
            when primary_category_id is distinct from lag(primary_category_id) over (
                partition by source_product_id order by ingestion_date) then 1
            else 0
        end as inicia_versao
    from por_data

),

agrupado as (

    select
        *,
        sum(inicia_versao) over (
            partition by source_product_id
            order by ingestion_date
            rows between unbounded preceding and current row
        ) as version_number
    from marcado

),

versoes as (

    select
        source_product_id,
        version_number,
        min(ingestion_date)             as valid_from,
        max(ingestion_date)             as last_seen_date,
        max(display_name)               as display_name,
        max(primary_category_id)        as primary_category_id,
        max(category_appearances)       as category_appearances,
        max(pack_size)                  as pack_size,
        max(unit_size)                  as unit_size,
        max(unit_name)                  as unit_name,
        max(size_format)                as size_format,
        max(warehouses_listing)         as warehouses_listing
    from agrupado
    group by 1, 2

),

-- Id novo cujo display_name ja existia na particao anterior: a fonte nao diz se e o mesmo
-- item rechaveado ou um item retirado e outro lancado. silver_price_change ja marca esses
-- casos; aqui a marca so viaja para a dimensao.
ambiguidade as (

    select distinct source_product_id
    from {{ source('stage', 'STG_PRICE_CHANGE') }}
    where name_seen_before

)

select
    md5(concat_ws('|', v.source_product_id, cast(v.valid_from as varchar))) as product_sk,

    v.source_product_id,
    v.version_number,

    v.valid_from,
    -- valid_to e o valid_from da PROXIMA versao, nao "o dia anterior a ela": as particoes
    -- tem lacunas (08-17 a 08-23 nao existem), entao subtrair um dia inventaria uma data
    -- em que nada foi observado. Intervalo fechado-aberto [valid_from, valid_to).
    lead(v.valid_from) over (
        partition by v.source_product_id order by v.version_number
    )                                                       as valid_to,
    lead(v.valid_from) over (
        partition by v.source_product_id order by v.version_number
    ) is null                                               as is_current,
    v.last_seen_date,

    v.display_name,
    v.primary_category_id,
    v.category_appearances,
    (v.category_appearances > 1)                            as is_multi_category,

    v.pack_size,
    v.unit_size,
    v.unit_name,
    v.size_format,
    v.warehouses_listing,

    coalesce(a.source_product_id is not null, false)        as identity_ambiguous
from versoes v
left join ambiguidade a on a.source_product_id = v.source_product_id
