-- O MANIFESTO DA SOURCE DE OLTP COMO FONTE MODELADA. Irmao de raw_manifest.sql, com a
-- mesma finalidade: fazer de "o Silver perdeu linha?" um TESTE, e nao um script solto.
--
-- GRAO: (ingestion_date, wh) — um manifesto por particao.
--
-- Aqui ele carrega uma coisa a mais que o da Mercadona: a LINHAGEM DOS INSUMOS. Esta
-- source e derivada — consome o Silver que as outras tres produziram — entao "de qual
-- Callejero e de qual populacao estes clientes sairam" e uma pergunta legitima e a
-- resposta so existe no manifesto. O bloco `reference` e o unico lugar onde as tres
-- ingestion_date de insumo ficam registradas.
--
-- previous_runs conta o `history`, que a Source acumula a cada --overwrite guardando a
-- seed e o count de cada execucao anterior (partition.py:130-153). Sem isso, uma
-- regeracao com outra seed trocaria as pessoas por tras dos mesmos customer_id sem deixar
-- rastro. Com isso, "esta base foi regerada 3 vezes" e consultavel.
--
-- Desde a Fase 6 o escopo do CADASTRO viaja junto: idade minima, taxa de penetracao e regra
-- de alocacao. As tres tem a mesma natureza da seed — trocam as pessoas por tras dos mesmos
-- ids — e sem elas duas particoes com a mesma contagem seriam indistinguiveis. `customer_target`
-- ao lado de `requested_count` responde "a base tem o tamanho que a populacao pedia?" sem
-- refazer a conta.
--
-- CONTRACT.md secao 1: uma lista ausente equivale a lista vazia — dai o coalesce antes de
-- len(), mesmo padrao de raw_manifest.sql.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_oltp_manifest.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh,
    run_id,
    complete,

    source.name                             as source_name,
    source.wh                               as source_warehouse,
    source.generator_version                as generator_version,
    manifest_version,

    started_at_utc,
    finished_at_utc,
    duration_seconds,

    -- O que reproduz esta particao exatamente. seed + reference + ingestion_date + escopo do
    -- cadastro sao as QUATRO condicoes da garantia de reprodutibilidade (CONTRACT.md secao 3).
    config.seed                             as seed,
    config.count                            as requested_count,
    -- 'reference' quando o tamanho veio da populacao, 'cli' quando alguem passou --count.
    -- Sem esta coluna, uma base gerada com override manual seria indistinguivel de uma
    -- derivada da populacao, e a unica forma de descobrir seria refazer a conta.
    config.count_source                     as count_source,
    config.reference                        as reference_path,

    -- Linhagem dos insumos: de qual Silver estes clientes foram derivados.
    reference.callejero_ingestion_date       as callejero_ingestion_date,
    reference.population_ingestion_date      as population_ingestion_date,
    reference.population_series_ingestion_date as population_series_ingestion_date,
    reference.address_candidates             as reference_address_candidates,
    reference.orphan_tramos_excluded         as reference_orphan_tramos_excluded,
    reference.population_year                as population_year,
    reference.population_reference_date      as population_reference_date,
    reference.age_year                       as age_year,
    reference.age_fk_periodo                 as age_fk_periodo,
    reference.age_reference_date             as age_reference_date,

    -- O ESCOPO DO CADASTRO. As tres primeiras trocam as pessoas por tras dos mesmos
    -- customer_id tanto quanto a seed troca, e por isso viajam ao lado dela. Sem elas, duas
    -- particoes com a mesma contagem e o mesmo aspecto seriam indistinguiveis.
    reference.min_customer_age               as min_customer_age,
    reference.penetration_pct                as penetration_pct,
    reference.allocation_rule                as allocation_rule,
    reference.penetration_source             as penetration_source,
    reference.customer_target                as customer_target,

    totals.customer_rows                    as declared_customer_rows,
    totals.municipalities_used              as declared_municipalities_used,
    totals.house_number_null                as declared_house_number_null,
    totals.pseudo_address_rows              as declared_pseudo_address_rows,
    totals.bytes                            as declared_bytes,

    schema_fingerprint.sha256               as schema_fingerprint,
    len(coalesce(files, []))                as declared_files,
    len(coalesce(failures, []))             as failure_count,
    len(coalesce(anomalies, []))            as anomaly_count,
    len(coalesce(history, []))              as previous_runs

from read_json(
    '{{ var("oltp_raw_prefix") }}/ingestion_date=*/wh=*/_manifest.json',
    hive_partitioning = 1,
    union_by_name = true
)
