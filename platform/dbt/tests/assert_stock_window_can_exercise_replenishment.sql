-- A JANELA TEM DE SER MAIOR QUE A COBERTURA INICIAL, SENAO O LEDGER NAO MEDE NADA.
--
-- O ACHADO QUE ISTO CONGELA, e ele e contra o proprio autor. A primeira execucao do job de
-- estoque, em 2026-09-01, deu RUPTURA ZERO em 86.520 linhas. Nao era operacao boa:
-- `opening_days_of_demand` valia 7 e a janela observada tinha 5 dias, entao o estoque
-- INICIAL cobria o periodo inteiro. Ruptura zero era aritmetica, nao medicao.
--
-- E EXATAMENTE A MESMA FAMILIA do `orders_breaching_sla` estruturalmente zero que o Marco B
-- tinha acabado de corrigir no dominio de pedidos — recriada no dominio de estoque duas
-- horas depois, por quem tinha escrito a correcao. Um limiar que nenhuma cesta alcanca e um
-- estoque que nenhuma janela consome sao a mesma coisa: um numero que so pode dar um
-- resultado. Por isso a regra virou teste em vez de virar paragrafo.
--
-- O QUE ELE NAO EXIGE, e a distincao importa: ele nao exige que HAJA ruptura. Uma operacao
-- que nao rompe e um estado legitimo do mundo, e exigir ruptura seria pedir que o modelo
-- produzisse o numero que agrada. O que ele exige e que a ruptura seja POSSIVEL — que o
-- resultado dependa da demanda e nao da aritmetica das premissas.
--
-- O QUE FICA DECLARADO E NAO E TESTADO: com cobertura de 7 dias, ponto em 3 e prazo de 2, um
-- ciclo de reposicao fecha na janela de 10 dias e o segundo nao. Cair a cobertura ate caber
-- dois ciclos seria escolher uma premissa pelo que ela faz com o grafico, e o teto da janela
-- e externo — 2026-09-01 e ate onde o catalogo observado alcanca.
with cobertura as (

    select cast(value as double) as opening_days_of_demand
    from {{ ref('stock_premises') }}
    where premise_key = 'opening_days_of_demand'

),

janela as (

    select
        count(distinct stock_date) as dias,
        min(stock_date)            as de,
        max(stock_date)            as ate
    from {{ ref('silver_stock_ledger') }}

)

select
    j.dias                        as dias_na_janela,
    c.opening_days_of_demand      as dias_de_cobertura_inicial,
    j.de,
    j.ate,
    'a janela nao consome o estoque inicial: ruptura fica zero por construcao' as violacao
from janela j
cross join cobertura c
where j.dias <= c.opening_days_of_demand
