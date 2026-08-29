-- PASSAGEM do seed para o object storage — mesmo motivo/mecanismo de
-- warehouse_service_area.sql (query.py so escaneia parquet sob silver/, nunca tabela
-- nativa do .duckdb local).
--
-- POR QUE AS PREMISSAS PRECISAM ATRAVESSAR ATE O WAREHOUSE. `sla_minutes_picking` e o
-- limiar contra o qual MART_FULFILLMENT_SLA conta violacoes, e e a MESMA premissa que o
-- gerador consumiu para produzir as duracoes. Se o warehouse guardasse esse 90 numa var
-- do dbt, existiriam dois lugares onde a premissa mora — e o dia em que alguem editar o
-- seed e nao a var, o mart passa a medir contra um limiar que nenhum pedido conheceu, sem
-- reprovar nada. A var `currency` e o caso oposto e por isso continua sendo var: a
-- Mercadona nao declara moeda em campo nenhum, entao a premissa nao tem outra casa.
--
-- NAO PERTENCE A simulated_orders/. Aquela pasta contem o que e derivado do RAW da
-- Source; isto e declaracao de entrada, e nao depende de nenhuma particao existir —
-- mesmo criterio que mantem warehouse_service_area direto sob silver/. Consequencia
-- pratica: `make silver` constroi este modelo mesmo sem nenhum pedido gerado.
--
-- O `label` viaja junto de cada linha, e e sempre 'synthetic'. E o unico jeito de quem
-- consultar o warehouse descobrir, sem abrir o CONTRACT, que estes numeros sao premissa
-- declarada e nao medicao — a mesma disciplina que faz o Silver nunca inventar moeda.
{{ config(
    location = 's3://retail-lakehouse/silver/order_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('order_premises_seed') }}
