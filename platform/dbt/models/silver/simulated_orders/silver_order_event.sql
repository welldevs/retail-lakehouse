-- PROJECAO FIEL do log de eventos de pedido. Nenhuma interpretacao alem de tipar o envelope.
--
-- GRAO: event_id. Deterministico por contrato — sha256(order_id|sequence_no) — e por isso
-- unico em toda a janela, nao so dentro da particao.
--
-- O PAYLOAD FICA COMO JSON, DE PROPOSITO, e esta e a decisao mais importante deste modelo.
-- Deixar o DuckDB inferir o payload produz um STRUCT com a UNIAO dos campos dos 12 tipos —
-- medido, 32 campos — e essa uniao depende do que a amostragem viu. Um dia sem nenhuma
-- devolucao nao teria `returned_amount` no struct, e um modelo a jusante que o referenciasse
-- passaria a NAO COMPILAR dependendo do sorteio. E a mesma armadilha que fez a impressao
-- digital desta Source cobrir o vocabulario declarado em vez do observado.
--
-- Entao o schema e DECLARADO em `columns =`, nunca inferido, e o payload atravessa como JSON.
-- Quem precisa de um campo o extrai por nome, e a extracao de um campo ausente devolve NULL
-- em vez de quebrar o build. As duas tabelas que dobram o log (silver_order,
-- silver_order_line) fazem exatamente isso.
--
-- SEM is_latest_ingestion, e o motivo e o mesmo ja escrito para os modelos da Mercadona: aqui
-- as varias datas SAO o produto, nao efeito colateral de reextracao. Cada ingestion_date e um
-- dia de operacao, e empilhar dias e o ponto.
--
-- ingestion_date E A DATA DO PEDIDO, nao a do evento (CONTRACT.md secao 1). Um pedido das
-- 22h40 entregue as 9h do dia seguinte deixa TODOS os seus eventos na particao do dia em que
-- foi colocado. Por isso este modelo expoe TAMBEM `event_date`: as duas perguntas — "pedidos
-- colocados no dia D" e "eventos ocorridos no dia D" — sao legitimas e diferentes.
--
-- partition_wh EXISTE PARA SER RECONCILIADO, pelo mesmo motivo de silver_customer: o campo
-- `wh` do registro e o segmento hive `wh=` tem o mesmo nome e o DuckDB os deduplica numa
-- coluna so, com o valor do registro vencendo. Sem uma segunda coluna, uma particao pousada
-- sob o diretorio errado passaria despercebida.
--
-- Este modelo assume ao menos um arquivo aterrissado (read_json falha sobre glob vazio).
-- Quando a source ainda nao aterrissou nada, `make silver` o exclui do build via
-- `retail-platform has-data simulated_orders` — mesmo tratamento das outras quatro.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_order_event.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh,
    regexp_extract(filename, 'wh=([^/]+)', 1) as partition_wh,

    event_id,
    event_type,
    event_version,
    order_id,
    sequence_no,
    producer,

    occurred_at,
    cast(occurred_at as date)                 as event_date,
    -- Um evento pode ocorrer depois da meia-noite do dia do pedido. Marcar isso e mais
    -- honesto do que deixar o consumidor descobrir comparando as duas colunas.
    cast(occurred_at as date) <> ingestion_date as crosses_order_date,

    payload

from read_json(
    '{{ var("orders_raw_prefix") }}/ingestion_date=*/wh=*/order_events.jsonl',
    format = 'newline_delimited',
    hive_partitioning = 1,
    filename = true,
    -- DECLARADO, nunca inferido. Ver o cabecalho.
    columns = {
        event_id: 'VARCHAR',
        event_type: 'VARCHAR',
        event_version: 'INTEGER',
        occurred_at: 'TIMESTAMP',
        order_id: 'VARCHAR',
        sequence_no: 'INTEGER',
        wh: 'VARCHAR',
        producer: 'VARCHAR',
        payload: 'JSON'
    }
)
