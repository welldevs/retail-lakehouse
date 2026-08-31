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
--
-- CONTRACT.md 4.9 — `unit_price` NEM SEMPRE E PRECO DE UNIDADE COMPRAVEL.
-- Quando `selling_method = 1` (seletor de peso) e `unit_size` e nulo, a fonte devolve
-- `unit_price = reference_price * 99`: o preco do TETO do seletor, nao de nada que um
-- domicilio compre. Medido em 2026-08-31: o fator e exatamente 99,000 em 10 linhas
-- (max 3.663,00 EUR para um congelado a 37,00 EUR/kg), e 0,200 nas outras 21 do mesmo
-- selling_method — que sao a propria `unit_size` e estao corretas.
--
-- `unit_price` FICA INTACTO, porque projecao fiel da fonte e invariante deste modelo. O
-- que entra sao as colunas DERIVADAS ao lado, e elas nao inventam nada: `min_bunch_amount`
-- e `increment_bunch_amount` sempre estiveram no RAW e este modelo os descartava.
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

),

typed as (

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

    -- OS TRES CAMPOS QUE ESTE MODELO DESCARTAVA. Estao no RAW desde a primeira particao;
    -- nenhuma reingestao foi necessaria para traze-los. `min_bunch_amount` e a porcao
    -- minima comprável em `reference_format` (0,15 kg no congelado a granel), e e o unico
    -- caminho para um preco de unidade comprável quando `unit_price` e o teto do seletor.
    p.price_instructions.bunch_selector                      as bunch_selector,
    p.price_instructions.min_bunch_amount                    as min_bunch_amount,
    p.price_instructions.increment_bunch_amount              as increment_bunch_amount,

    p.packaging,
    p.thumbnail,
    p.share_url,
    p.slug,
    p.published,
    p.is_new_arrival

from product

)

select
    *,

    -- COMO O PRECO DESTA LINHA SE FORMA. Rotulo, nao regra de negocio: cada valor descreve
    -- uma convencao da fonte que foi MEDIDA, e a distincao existe porque as tres tem
    -- consequencias diferentes para quem monta uma cesta.
    --
    --   bunch  selling_method = 1 e unit_size nulo -> unit_price = reference_price * 99.
    --          E o UNICO caso em que unit_price nao serve. 10 linhas medidas.
    --   piece  peca inteira (presunto de 8 a 9,5 kg): unit_price = reference_price *
    --          unit_size, PRECO LEGITIMO. O que nao e plausivel ali e a incidencia de
    --          compra, e isso e problema do modelo de demanda, nao deste modelo.
    --   unit   embalagem de consumo. O caso normal.
    case
        when selling_method = 1 and unit_size is null then 'bunch'
        when packaging = 'Pieza'                      then 'piece'
        else 'unit'
    end                                                     as price_basis,

    -- A PORCAO COMPRAVEL, em `reference_format`. Para `bunch` e a porcao minima do seletor;
    -- para o resto e a propria `unit_size` da fonte (nula quando a fonte nao a declara).
    case
        when selling_method = 1 and unit_size is null then min_bunch_amount
        else unit_size
    end                                                     as purchasable_unit_size,

    -- O PRECO DA PORCAO COMPRAVEL. Difere de `unit_price` SO no caso `bunch`, e ali e
    -- reference_price * min_bunch_amount — dois campos observados, nenhuma estimativa.
    case
        when selling_method = 1 and unit_size is null
            then (reference_price * min_bunch_amount)::decimal(10, 2)
        else unit_price
    end                                                     as purchasable_unit_price,

    -- CONTEUDO EM KG OU LITRO da porcao comprável, derivado de preco / preco de referencia.
    -- Nulo de proposito quando `reference_format` nao e massa nem volume (`ud`, `dz`, `lv`,
    -- `dc`, `m`): converter ovos para kg exigiria um peso por ovo que NENHUMA fonte deste
    -- repo mede. Cobertura medida em 2026-08-31: 70,61% direto por kg/L, 11,88% via
    -- 100 g / 100 ml, 16,70% em `ud` e portanto nulo aqui.
    case
        when reference_format in ('kg', 'L') and reference_price > 0
            then ((case
                    when selling_method = 1 and unit_size is null
                        then (reference_price * min_bunch_amount)::decimal(10, 2)
                    else unit_price
                  end) / reference_price)::decimal(12, 4)
        when reference_format in ('100 g', '100 ml') and reference_price > 0
            then ((case
                    when selling_method = 1 and unit_size is null
                        then (reference_price * min_bunch_amount)::decimal(10, 2)
                    else unit_price
                  end) / reference_price * 0.1)::decimal(12, 4)
    end                                                     as net_content_kg_l

from typed
