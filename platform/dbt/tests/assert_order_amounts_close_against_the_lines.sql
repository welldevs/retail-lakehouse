-- A aritmetica do fold fecha, nos dois sentidos.
--
--   gross_amount_placed = soma das linhas COLOCADAS
--   net_amount          = soma das linhas EFETIVAS (removida contribui zero)
--
-- POR QUE ISTO NAO E REDUNDANTE COM O MANIFESTO. O teste de reconciliacao compara o fold do
-- Silver com o fold da Source. Este compara o fold do PEDIDO com o fold das LINHAS, dois
-- modelos do proprio Silver construidos por caminhos diferentes: silver_order soma o que os
-- eventos `order_picked` declararam, silver_order_line reconstroi linha a linha aplicando
-- substituicao e remocao. Se os dois concordam, a substituicao foi aplicada ao valor certo.
--
-- `net_amount` e nulo para pedido que nunca chegou a separacao. Esses ficam de fora do
-- segundo teste de propósito: nulo ali significa "nao houve separacao", nao "zero".
with das_linhas as (

    select
        order_id,
        cast(sum(line_amount_placed) as decimal(14, 2)) as soma_colocada,
        cast(sum(line_amount) as decimal(14, 2))        as soma_efetiva,
        count(*)                                        as linhas,
        -- Contar o COMPLEMENTO de 'removed' era equivalente enquanto so existiam tres
        -- valores. Com `not_picked` no vocabulario deixa de ser: uma linha de pedido
        -- cancelado nao foi removida e tambem nao foi cumprida.
        sum(case when line_status in ('fulfilled', 'substituted') then 1 else 0 end)
                                                        as linhas_cumpridas
    from {{ ref('silver_order_line') }}
    group by order_id

)

select
    o.order_id,
    o.gross_amount_placed,
    l.soma_colocada,
    o.net_amount,
    l.soma_efetiva,
    o.line_count_placed,
    l.linhas,
    o.line_count_picked,
    l.linhas_cumpridas
from {{ ref('silver_order') }} o
join das_linhas l on o.order_id = l.order_id
where o.gross_amount_placed is distinct from l.soma_colocada
   or o.line_count_placed   is distinct from l.linhas
   or (o.net_amount is not null and o.net_amount is distinct from l.soma_efetiva)
   or (o.line_count_picked is not null and o.line_count_picked is distinct from l.linhas_cumpridas)
