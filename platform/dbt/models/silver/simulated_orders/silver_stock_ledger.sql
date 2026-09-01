-- O SALDO DE ESTOQUE, lido da tabela Iceberg que o SPARK escreve.
--
-- GRAO: (wh, source_product_id, stock_date). Uma linha por serie por dia da janela,
-- inclusive nos dias sem venda — sem elas o saldo "pularia" justamente os dias em que uma
-- reposicao chega, e cobertura em dias deixaria de significar alguma coisa.
--
-- POR QUE ELE ENTRA PELO ICEBERG E NAO PELO RAW. Este e o unico modelo do Silver que nao
-- deriva de uma Source: estoque nao e observado em lugar nenhum deste projeto. A Mercadona
-- nao publica saldo, o INE nao mede varejo e o MAPA mede consumo domiciliar. O ledger e
-- CALCULADO, a partir do consumo observado nos pedidos mais uma politica declarada em
-- `stock_premises_seed` — e quem calcula e um job Spark, porque a forma do calculo nao e SQL.
--
-- A FORMA, em uma frase: e uma soma corrida cujas ENTRADAS sao geradas por decisoes tomadas
-- a partir do proprio estado. O saldo cai abaixo do ponto, uma ordem e emitida, ela chega
-- dois dias depois e muda o saldo dos dias seguintes, que decide se ha nova ordem. Window
-- function le a partition inteira mas nao escreve de volta nela. Isso foi MEDIDO:
-- `make spike-spark-iceberg`, pergunta S7b, roda a mesma entrada pelas duas formas e a soma
-- corrida em SQL diverge em 14 dos 30 dias do caso, chegando a -70 de saldo.
--
-- O CAMINHO DO METADADO VEM DE UMA VAR, pelo mesmo motivo de `silver_live_order_state`: o
-- DuckDB RECUSA descobrir sozinho qual e o metadado corrente de uma tabela Iceberg, e a
-- recusa esta certa — varrer o storage pode encontrar um commit que nao aconteceu. Quem
-- sabe e o catalogo, e `make silver` pergunta a ele.
--
-- E ESTE MODELO FICA FORA DO BUILD ENQUANTO A TABELA NAO EXISTIR. Nao e tolerancia a falha:
-- e a propriedade que mantem o Spark OPCIONAL. Um clone novo do repositorio, onde ninguem
-- baixou a imagem do Spark, constroi o Silver inteiro — o portao de `silver_gate.py` tira
-- daqui o que depende de uma tabela ausente, e `test_O_SPARK_E_OPCIONAL_e_isto_e_o_que_prova`
-- guarda isso.
--
-- `written_by` e PROVENIENCIA. Nesta tabela ele e sempre 'spark', e continua valendo a pena:
-- e como uma consulta descobre que existe um terceiro escritor no catalogo, e o primeiro
-- fora do Python — que e a propriedade que justificou o Iceberg e estava so afirmada.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_stock_ledger.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    wh,
    source_product_id,
    stock_date,

    -- Os movimentos do dia, na ordem em que acontecem: chega, sai, sobra.
    cast(opening_balance  as bigint)          as opening_balance,
    cast(units_received   as bigint)          as units_received,
    cast(units_demanded   as bigint)          as units_demanded,
    cast(units_fulfilled  as bigint)          as units_fulfilled,
    -- RUPTURA: o que a demanda pediu e o saldo nao tinha. Registrada em vez de virar saldo
    -- negativo — saldo negativo fecharia a soma e mentiria sobre a prateleira.
    cast(units_short      as bigint)          as units_short,
    cast(closing_balance  as bigint)          as closing_balance,

    -- A reposicao e LANCAMENTO DESTA TABELA, e nao tabela separada. Um ledger com uma
    -- tabela de ordens ao lado exigiria juntar as duas para responder qualquer pergunta de
    -- saldo, e o grao ja e o dia.
    cast(reorder_units    as bigint)          as reorder_units,
    reorder_eta,

    cast(mean_daily_demand as decimal(12, 4)) as mean_daily_demand,
    cast(days_of_cover     as decimal(12, 4)) as days_of_cover,

    written_by

from iceberg_scan('{{ var("stock_ledger_metadata") }}')
