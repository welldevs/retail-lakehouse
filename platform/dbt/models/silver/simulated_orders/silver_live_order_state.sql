-- A PROJEÇÃO VIVA, lida da tabela Iceberg que dois processos escrevem.
--
-- GRÃO: order_id. É o mesmo grão de `silver_order`, e de propósito: este modelo existe
-- SOMENTE para que a reconciliação entre o caminho em streaming e o caminho em lote seja uma
-- consulta, e não um script solto. Ele não acrescenta fato nenhum ao Lakehouse.
--
-- POR QUE O CAMINHO DO METADADO VEM DE UMA VAR, E NÃO ESTÁ CRAVADO AQUI.
--
-- O DuckDB **recusa** descobrir sozinho qual é o metadado corrente de uma tabela Iceberg.
-- A mensagem dele é explícita: "globbing the filesystem to locate the latest version is
-- disabled by default as this is considered unsafe and could result in reading uncommitted
-- data". Existe um atalho — `SET unsafe_enable_version_guessing = true` — que foi medido,
-- funciona, e foi RECUSADO: ler metadado não commitado é exatamente o que uma leitura
-- concorrente com dois escritores não pode fazer.
--
-- Quem sabe qual metadado é o corrente é o CATÁLOGO. `make silver` pergunta a ele
-- (`retail-platform iceberg-metadata`) e passa a resposta como var. A autoridade continua
-- num lugar só, sem flag insegura e sem reimplementar uma convenção de catálogo.
--
-- O modelo fica FORA do `dbt build` enquanto a tabela não existir, pela mesma guarda
-- `has-data` das outras sources: um clone novo do repositório não pode quebrar por causa de
-- um plano de stream que ninguém subiu.
--
-- `written_by` é PROVENIÊNCIA: qual dos dois escritores tocou a linha por último. Sem ela,
-- "os dois escritores escreveram de verdade" seria afirmação sobre log, e não um fato
-- consultável.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_live_order_state.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    order_id,
    wh,
    order_date,
    customer_id,
    status                                             as order_status,
    last_sequence_no,
    events_applied,

    placed_at,
    confirmed_at,
    picking_started_at,
    picked_at,
    dispatched_at,
    terminal_at,
    updated_at,

    line_count                                         as line_count_placed,
    picked_line_count                                  as line_count_picked,
    substituted_lines,
    removed_lines,

    cast(gross_amount as decimal(12, 2))               as gross_amount_placed,
    cast(net_amount   as decimal(12, 2))               as net_amount,
    cast(returned_amount as decimal(12, 2))            as returned_amount,
    delivered_within_slot,
    cast(picking_minutes as decimal(10, 2))            as picking_minutes,
    sla_breached,

    written_by

from iceberg_scan('{{ var("live_order_state_metadata") }}')
