-- 18 LINHAS QUE IMPEDEM UM MART MENTIR.
--
-- GRAO: (source_name, ingestion_date, wh). TIPO: observed (metadado operacional).
--
-- O QUE ELE RESPONDE, e nenhuma outra tabela responde: a diferenca entre "nao houve preco"
-- e "NAO HOUVE OBSERVACAO". Os dias 2026-08-17 a 08-23 nao existem no catalogo, e nao
-- porque nada aconteceu — porque ninguem olhou, e a API so serve o preco de hoje, entao
-- nunca podera olhar. Sem esta tabela, um produto ausente num dia e indistinguivel de um
-- dia inteiro ausente, e qualquer serie temporal interpola a lacuna em silencio.
--
-- E metadado promovido a fato de proposito. Custa 18 linhas; o que compra e a capacidade
-- de um mart dizer "nao sei" em vez de chutar.
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

    -- Uma execucao so e observacao valida se completou e nao registrou falha. Qualquer
    -- outra coisa e particao suspeita, e o consumidor precisa saber sem ter de ler duas
    -- colunas e lembrar da regra.
    (complete and failure_count = 0)                        as is_valid_observation
from {{ source('stage', 'STG_INGESTION_RUN') }}
