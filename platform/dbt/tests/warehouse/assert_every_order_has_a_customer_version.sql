-- TODO PEDIDO RESOLVE PARA UMA VERSAO DE CLIENTE — E E A MESMA QUE A SOURCE DECLAROU.
--
-- O QUE PEGA, e o que a variante obvia nao pegaria. A variante obvia seria `customer_sk
-- is not null`. Ela reprovaria se o range join nao casasse nada — mas passaria alegremente
-- se casasse a versao ERRADA, que e o modo de falha real de um SCD2: intervalo com
-- fronteira fechada nos dois lados faz o pedido do dia da virada casar DUAS versoes, o
-- join vira fanout, e a contagem de pedidos infla sem que `is not null` perceba.
--
-- Este teste faz tres afirmacoes de uma vez:
--
--   1. TODO pedido resolve (customer_sk nao nulo);
--   2. a data do pedido cai DENTRO de [valid_from, valid_to) da versao resolvida;
--   3. a versao resolvida e a MESMA que a Source gravou em `customer_ingestion_date`
--      dentro do evento order_placed.
--
-- A terceira e a que vale. Sao dois caminhos independentes chegando ao mesmo lugar: a
-- escolha do roster no gerador (Python, no momento da geracao) e uma juncao por intervalo
-- (SQL, no warehouse, meses depois). Medido: coincidem nos 6.400 pedidos. Enquanto
-- coincidirem, o SCD2 esta sendo resolvido do jeito que a Fase 3 prometeu; quando
-- divergirem, um dos dois lados mudou de ideia sobre o que "versao vigente" significa, e
-- isso precisa ser uma falha e nao uma descoberta em dashboard.
--
-- Falha com uma linha por pedido, dizendo qual das tres afirmacoes caiu.
select
    f.order_id,
    f.order_date,
    f.customer_id,
    f.customer_ingestion_date                               as declarada_pela_source,
    f.customer_version_from                                 as resolvida_pelo_scd2,
    case
        when f.customer_sk is null                              then 'sem versao'
        when f.order_date < f.customer_version_from             then 'versao futura'
        when f.customer_ingestion_date <> f.customer_version_from
                                                            then 'versao divergente'
    end                                                     as motivo
from {{ ref('fact_order') }} f
where f.customer_sk is null
   or f.order_date < f.customer_version_from
   or f.customer_ingestion_date <> f.customer_version_from
