-- PROJECAO FIEL E TIPADA de cada snapshot. Nenhuma leitura entre datas acontece aqui:
-- interpretacao temporal e do silver_price_change.
--
-- GRAO: (ingestion_date, warehouse, category_id, subgroup_id, source_product_id).
-- NAO e source_product_id. CONTRACT.md 4.8: um produto aparece em mais de uma categoria
-- e em mais de um subgrupo — ~270 linhas repetidas por particao. E semantica da fonte,
-- preservada de proposito.
--
-- CONTRACT.md 4.4: unit_price e string em 100% dos registros; converter para float
-- perderia precisao decimal em moeda. Daqui sai DECIMAL, nunca DOUBLE.
--
-- TRIM em todo preco: previous_unit_price vem da fonte com espaco a esquerda
-- ("       18.75") em 100% dos nao-nulos. O DuckDB tolera isso no CAST — medido — mas o
-- TRIM fica por PORTABILIDADE: o argumento do dbt e este mesmo SQL rodar depois em
-- Snowflake ou Spark, e nem todo motor e tolerante. Normalizar na origem custa nada.
--
-- CONTRACT.md 4.6: a fonte NAO declara moeda. Nao ha coluna currency aqui, e a var
-- do projeto e premissa para o Gold, nao dado da fonte.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_product_price',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite_or_ignore': 1}
) }}

with catalog_file as (

    select
        ingestion_date,
        wh                              as warehouse,
        id                              as category_id,
        name                            as category_name,
        categories                      as subgroups
    from read_json(
        '{{ var("raw_prefix") }}/ingestion_date=*/wh=*/catalog/category_id=*.json',
        hive_partitioning = 1,
        union_by_name = true
    )

),

subgroup as (

    select
        ingestion_date,
        warehouse,
        category_id,
        category_name,
        unnest(subgroups)               as sg
    from catalog_file

),

product as (

    select
        ingestion_date,
        warehouse,
        category_id,
        category_name,
        sg.id                           as subgroup_id,
        sg.name                         as subgroup_name,
        unnest(sg.products)             as p
    from subgroup

)

select
    ingestion_date,
    warehouse,
    category_id,
    category_name,
    subgroup_id,
    subgroup_name,

    -- CHAVE DA FONTE, nao identidade de negocio (CONTRACT.md 4.5). Consistente dentro
    -- do snapshot; sem garantia de estabilidade entre snapshots.
    p.id                                                    as source_product_id,
    p.display_name,

    -- Linhagem de nivel 1 vinda do proprio produto. Redundante com silver_category, e a
    -- redundancia e o teste: medido, concorda com a arvore em 100% das linhas.
    p.categories[1].id                                      as product_level1_category_id,
    p.categories[1].name                                    as product_level1_category_name,

    -- Precos. Strings na fonte; DECIMAL aqui.
    trim(p.price_instructions.unit_price)::decimal(10, 2)    as unit_price,
    trim(p.price_instructions.bulk_price)::decimal(10, 2)    as bulk_price,
    trim(p.price_instructions.reference_price)::decimal(12, 3) as reference_price,
    p.price_instructions.reference_format                    as reference_format,
    trim(p.price_instructions.previous_unit_price)::decimal(10, 2) as previous_unit_price,

    -- Medido: FALSO em 100% das linhas nas tres particoes, enquanto 152 precos mudaram
    -- entre 08-16 e 08-24. Preservado por fidelidade, mas NAO serve para detectar
    -- variacao — e por isso que silver_price_change existe.
    p.price_instructions.price_decreased                     as price_decreased,

    -- tax_percentage e o campo preenchido; `iva` e nulo em 100% das linhas.
    trim(p.price_instructions.tax_percentage)::decimal(6, 3) as tax_percentage,

    p.price_instructions.is_pack                             as is_pack,
    p.price_instructions.pack_size                           as pack_size,
    p.price_instructions.unit_size                           as unit_size,
    p.price_instructions.size_format                         as size_format,
    p.price_instructions.unit_name                           as unit_name,
    p.price_instructions.total_units                         as total_units,
    p.price_instructions.selling_method                      as selling_method,
    p.price_instructions.approx_size                         as approx_size,

    p.packaging,
    p.thumbnail,
    p.share_url,
    p.slug,
    p.published,
    p.is_new_arrival

from product
