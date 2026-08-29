-- O VALOR DO PEDIDO FECHA COM A SOMA DAS SUAS LINHAS — NOS DOIS SENTIDOS.
--
-- O QUE PEGA: fanout. FACT_ORDER_ITEM entra em DUAS juncoes com DIM_PRODUCT (o produto
-- pedido e o cumprido), cada uma por intervalo de validade. Um intervalo com fronteira
-- fechada dos dois lados, ou uma versao duplicada na dimensao, multiplica linhas — e
-- nenhuma soma reprova sozinha, so fica maior. Comparar contra um total calculado por
-- outro caminho e o unico jeito barato de ver isso.
--
-- POR QUE A VARIANTE OBVIA NAO PEGARIA. Comparar so `net_amount` deixaria passar o caso em
-- que o fanout atinge apenas linhas removidas (que contribuem zero) ou apenas pedidos nunca
-- separados (net_amount nulo). Por isso as DUAS somas sao comparadas: o valor COLOCADO
-- (que toda linha tem, inclusive removida) e o valor SEPARADO.
--
-- E POR QUE `net_amount` NULO NAO E ERRO. 298 pedidos morreram antes da separacao —
-- cancelamento ou pagamento recusado. Neles a soma das linhas e exatamente zero, porque
-- `line_amount` mede o que a linha entregou e elas nao entregaram nada. O teste exige essa
-- combinacao (nulo de um lado, zero do outro) em vez de ignora-la: preencher net_amount com
-- o valor colocado foi um defeito real, achado no Marco 4 comparando dois folds
-- independentes, e este teste e o que impede sua volta.
--
-- Falha com uma linha por pedido que nao fecha.
with por_pedido as (

    select
        order_id,
        count(*)                                            as linhas,
        sum(line_amount_placed)                             as soma_colocada,
        sum(line_amount)                                    as soma_entregue
    from {{ ref('fact_order_item') }}
    group by 1

)

select
    f.order_id,
    f.order_status,
    f.line_count_placed,
    i.linhas                                                as linhas_no_fato_de_item,
    f.gross_amount_placed,
    i.soma_colocada,
    f.net_amount,
    i.soma_entregue,
    case
        when i.order_id is null                             then 'pedido sem linhas'
        when f.line_count_placed <> i.linhas                then 'contagem de linhas diverge'
        when f.gross_amount_placed <> i.soma_colocada       then 'valor colocado diverge'
        when f.net_amount is null and i.soma_entregue <> 0  then 'nunca separado mas com valor'
        when f.net_amount is not null and f.net_amount <> i.soma_entregue
                                                            then 'valor separado diverge'
    end                                                     as motivo
from {{ ref('fact_order') }} f
left join por_pedido i on i.order_id = f.order_id
where i.order_id is null
   or f.line_count_placed <> i.linhas
   or f.gross_amount_placed <> i.soma_colocada
   or (f.net_amount is null     and i.soma_entregue <> 0)
   or (f.net_amount is not null and f.net_amount <> i.soma_entregue)
