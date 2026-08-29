-- PROJECAO FIEL dos clientes sinteticos. Nenhuma interpretacao alem de tipar e de marcar
-- qual ingestion_date e a corrente.
--
-- GRAO: (ingestion_date, customer_id). Nao e customer_id sozinho: cada reexecucao da
-- Source cria uma ingestion_date nova, e os mesmos ids convivem em datas diferentes. O
-- `wh` esta embutido no proprio id (cust_mad1_000042), entao nao ha colisao entre
-- armazens dentro de uma data — CONTRACT.md secao 3, garantia 3.
--
-- O CLIENTE E SINTETICO, O ENDERECO NAO. Municipio, via, CEP e faixa de numeracao vem
-- sempre de uma linha real do Callejero. Este modelo nao acrescenta nem corrige nada
-- disso: o que a Source gravou e o que sai daqui.
--
-- `wh` PRESERVADO, nao renomeado para `warehouse`. As duas tabelas com que este modelo se
-- junta (warehouse_service_area, warehouse_province_map) usam `wh`, e o vocabulario da
-- fonte e o que manda — o mesmo criterio que manteve `numbering_type` em vez de inventar
-- `address_precision` (ARCHITECTURE.md, "Escolhas de vocabulario: preservar, nao
-- renomear"). silver_product_price usa `warehouse` porque a Mercadona chama assim la.
--
-- partition_wh EXISTE PARA SER RECONCILIADO. Medido: o campo `wh` do registro e o
-- segmento hive `wh=` tem o MESMO nome, e o DuckDB deduplica os dois numa coluna so — o
-- valor do registro vence e o do caminho desaparece. Sem uma segunda coluna, uma particao
-- pousada sob o diretorio errado passaria despercebida porque nao haveria com o que
-- comparar. Extrair do filename com regexp devolve o eixo da particao com nome proprio;
-- o teste de igualdade esta em schema.yml. Mesmo motivo de raw_manifest.sql expor
-- `warehouse` (hive) e `source_warehouse` (do manifesto) lado a lado.
--
-- candidate_index e AUDITORIA, nao chave estrangeira. Aponta a linha exata de
-- address_candidates.json, que e insumo efemero do gerador e nao e pousado no RAW. So tem
-- significado contra a referencia identificada no manifesto (ver silver_oltp_manifest).
-- Resolve-lo de volta ao tramo exigiria rematerializar a referencia; o gatilho para fazer
-- isso e um mart precisar de section_code.
--
-- Este modelo assume ao menos um arquivo aterrissado (read_json falha sobre glob vazio).
-- Quando a source ainda nao aterrissou nada, `make silver` o exclui do build via
-- `retail-platform has-data simulated_oltp` — mesmo tratamento das outras tres.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_customer.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        wh,
        regexp_extract(filename, 'wh=([^/]+)', 1)       as partition_wh,
        customer_id,
        province_code,
        province_name,
        municipality_code,
        municipality_name,
        candidate_index,
        street_name,
        postal_code,
        numbering_type,
        house_number,
        first_name,
        last_name,
        sex_label,
        birth_year
    from read_json(
        '{{ var("oltp_raw_prefix") }}/ingestion_date=*/wh=*/customers.json',
        hive_partitioning = 1,
        union_by_name = true,
        filename = true
    )

)

select
    ingestion_date,

    -- Uma regeracao da base cria outra ingestion_date, e este modelo empilha todas (o
    -- historico e deliberado: e o que permite ver a base crescer). Sem uma marca
    -- explicita, quem consultar sem filtrar soma clientes de geracoes diferentes.
    -- `where is_latest_ingestion` e a forma certa de ler a base atual — mesma convencao
    -- dos 7 modelos de referencia do INE.
    ingestion_date = max(ingestion_date) over ()         as is_latest_ingestion,

    wh,
    partition_wh,
    customer_id,

    province_code,
    province_name,
    municipality_code,
    municipality_name,

    candidate_index,
    street_name,
    postal_code,
    numbering_type,
    house_number,

    first_name,
    last_name,
    sex_label,
    birth_year,

    -- A IDADE E O ATRIBUTO ESTAVEL, birth_year e o derivado. O gerador sorteia idade e
    -- grava `birth_year = year(ingestion_date) - idade` (customers_generator.py:189),
    -- entao esta subtracao devolve EXATAMENTE a idade amostrada, sem aproximacao. Importa
    -- porque e o que sobrevive a uma regeracao em outra data: a pessoa mantem a idade e
    -- muda o ano de nascimento (CONTRACT.md secao 3).
    year(ingestion_date) - birth_year                    as age_at_ingestion

from raw
