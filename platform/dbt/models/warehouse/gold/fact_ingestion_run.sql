-- 24 LINHAS QUE IMPEDEM UM MART MENTIR.
--
-- GRAO: (source_name, ingestion_date, wh). TIPO: observed (metadado operacional).
--
-- O QUE ELE RESPONDE, e nenhuma outra tabela responde: a diferenca entre "nao houve preco"
-- e "NAO HOUVE OBSERVACAO". Os dias 2026-08-17 a 08-23 nao existem no catalogo, e nao
-- porque nada aconteceu — porque ninguem olhou, e a API so serve o preco de hoje, entao
-- nunca podera olhar. Sem esta tabela, um produto ausente num dia e indistinguivel de um
-- dia inteiro ausente, e qualquer serie temporal interpola a lacuna em silencio.
--
-- E metadado promovido a fato de proposito. Custa 24 linhas; o que compra e a capacidade
-- de um mart dizer "nao sei" em vez de chutar. CR-005 acrescentou 6 delas para promover
-- started_at_utc/finished_at_utc/duration_seconds, que ja existiam duas camadas abaixo.
{{ config(materialized = 'table') }}

select
    cast(to_char(ingestion_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    source_name,
    ingestion_date,
    wh,
    run_id,
    complete,
    declared_rows,
    failure_count,
    anomaly_count,

    -- CR-005: ja existiam duas camadas abaixo (Silver) e eram descartados so por uma
    -- lista fixa de colunas na projecao do STAGE — nao dado ausente, projecao esquecida.
    started_at_utc,
    finished_at_utc,
    duration_seconds,

    -- Uma execucao so e observacao valida se completou e nao registrou falha. Qualquer
    -- outra coisa e particao suspeita, e o consumidor precisa saber sem ter de ler duas
    -- colunas e lembrar da regra.
    (complete and failure_count = 0)                        as is_valid_observation
from {{ source('stage', 'STG_INGESTION_RUN') }}
