-- PASSAGEM do seed para o object storage — mesmo motivo/mecanismo de order_premises.sql.
--
-- POR QUE ESTAS PREMISSAS SAO SEPARADAS DAS DE PEDIDO. As de `order_premises` governam o
-- que um cliente FAZ; estas governam o que o ARMAZEM tem. Sao dominios diferentes e mudam
-- por razoes diferentes: a cesta muda quando se aprende algo sobre comportamento de compra,
-- a cobertura de estoque muda quando se aprende algo sobre a operacao. Junta-las num arquivo
-- so faria toda regeracao de pedidos parecer uma mudanca de politica de estoque, e vice-versa.
--
-- O QUE E SINTETICO AQUI, E O QUE NAO E. As quatro linhas sao POLITICA — quantos dias
-- cobrir, quando repor, ate onde repor, quanto o fornecedor demora. Nenhuma fonte deste
-- repositorio mede estoque, e o `label` diz `synthetic` em todas.
--
-- O que NAO e sintetico e o numero que elas multiplicam: a demanda media diaria por serie
-- (armazem x produto) vem do consumo MEDIDO em `silver_order_line`. Entao o nivel de
-- estoque de cada produto e observado; so a quantidade de dias de cobertura e declarada.
-- E a mesma forma da base de clientes, dimensionada pela populacao do INE: a estrutura vem
-- do dado, a politica vem daqui, e as duas ficam distinguiveis.
{{ config(
    location = 's3://retail-lakehouse/silver/stock_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('stock_premises_seed') }}
