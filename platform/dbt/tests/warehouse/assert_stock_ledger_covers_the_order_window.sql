-- O LEDGER DE ESTOQUE COBRE A JANELA DOS PEDIDOS QUE O ALIMENTARAM.
--
-- O DEFEITO CONCRETO QUE ISTO PEGA, e ele aconteceu de verdade em 2026-09-01.
--
-- `silver_stock_ledger` fica FORA do `dbt build` quando a tabela Iceberg nao existe — e
-- isso e deliberado, e o que mantem o Spark opcional. Mas o PARQUET do Silver sobrevive a
-- exclusao: ele e o que a ultima construcao bem-sucedida deixou. Entao um `make silver` que
-- pula o modelo, seguido de um `make warehouse`, publica no Snowflake um ledger de uma
-- janela que nao existe mais — e nada quebra, porque cada linha continua internamente
-- consistente e todo total continua fechando.
--
-- Foi exatamente o que se mediu: a regeracao levou os pedidos de 4 para 9 dias, o ledger
-- ficou nos 5 dias antigos, e MART_STOCK_HEALTH descreveu um periodo diferente de todos os
-- outros marts sem que um unico teste reprovasse.
--
-- E DEFEITO DE FRESCOR ENTRE DOMINIOS, que nenhuma invariante DENTRO de um dominio pega —
-- a mesma familia do defeito de reparticao da Fase 6. Por isso o teste mora aqui, no
-- warehouse, onde os dois dominios finalmente se encontram.
--
-- A COMPARACAO E `>=`, E NAO `=`, e a diferenca e real: o consumo e datado pela SEPARACAO,
-- e um pedido colocado tarde no ultimo dia e separado no dia seguinte. O ledger enxerga um
-- dia a MAIS que os pedidos. Exigir igualdade obrigaria a descartar consumo observado para
-- fazer duas tabelas parecerem simetricas.
with pedidos as (

    select min(order_date) as de, max(order_date) as ate
    from {{ ref('fact_order') }}

),

estoque as (

    select min(stock_date) as de, max(stock_date) as ate
    from {{ ref('fact_stock_ledger') }}

)

select
    p.de   as pedidos_de,
    p.ate  as pedidos_ate,
    e.de   as estoque_de,
    e.ate  as estoque_ate,
    case
        when e.ate < p.ate
            then 'o ledger termina ANTES dos pedidos: estoque velho publicado sobre janela nova'
        when e.de > p.de
            then 'o ledger comeca DEPOIS dos pedidos: consumo do inicio da janela sem saldo'
    end    as violacao
from pedidos p
cross join estoque e
where e.ate < p.ate
   or e.de  > p.de
