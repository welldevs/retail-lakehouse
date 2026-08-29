-- GRAO: (order_date, wh) — 16 linhas, 4 dias x 4 armazens.
--
-- PERGUNTA QUE RESPONDE: de cada 100 pedidos colocados num armazem num dia, quantos foram
-- confirmados, separados, despachados e entregues — e por onde escaparam os que nao foram.
--
-- O FUNIL SE APOIA EM MARCO, NUNCA EM STATUS, e esta e a decisao que faz a tabela ser
-- verdadeira. `order_status` guarda o estado ATUAL do pedido, que e o do ULTIMO evento.
-- Um pedido devolvido tem status RETURNED — e foi entregue. Medido nesta janela: contar
-- `order_status = 'DELIVERED'` da 5.985 entregas; contar `delivered_at is not null` da
-- 6.046. Sao os 61 pedidos devolvidos, e um funil montado sobre status os perderia na
-- etapa de entrega, produzindo uma taxa de entrega 1% menor que a real sem nada reprovar.
--
-- Marco e monotonico: uma vez alcancado, nao volta atras. Status nao e. Um funil e por
-- definicao uma contagem de etapas ALCANCADAS, entao a coluna certa e o instante.
--
-- AS SAIDAS NAO SOMAM COM AS ETAPAS, e nao deviam. `orders_cancelled` conta quem saiu por
-- cancelamento em qualquer ponto antes da separacao; `orders_returned` conta quem saiu
-- DEPOIS de ter atravessado o funil inteiro. Somar as duas colunas as etapas contaria os
-- devolvidos duas vezes — por isso elas vivem num bloco proprio, com nome que diz saida.
{{ config(materialized = 'table') }}

select
    f.order_date,
    f.date_key,
    d.year_month,
    f.wh,

    -- ---- as etapas, por marco alcancado ------------------------------------------
    count(*)                                                as orders_placed,
    count_if(f.confirmed_at is not null)                    as orders_confirmed,
    count_if(f.picking_started_at is not null)              as orders_picking_started,
    count_if(f.picked_at is not null)                       as orders_picked,
    count_if(f.dispatched_at is not null)                   as orders_dispatched,
    count_if(f.delivered_at is not null)                    as orders_delivered,

    -- ---- as saidas, que NAO somam com as etapas acima -----------------------------
    count_if(f.payment_failed_at is not null)               as orders_payment_failed,
    count_if(f.cancelled_at is not null)                    as orders_cancelled,
    count_if(f.delivery_failed_at is not null)              as orders_delivery_failed,
    count_if(f.returned_at is not null)                     as orders_returned,

    -- ---- as taxas, todas com o mesmo denominador ----------------------------------
    -- Denominador unico (orders_placed) de proposito: taxas encadeadas etapa-a-etapa se
    -- multiplicam e ninguem consegue somar o funil de cabeca. Aqui cada taxa responde
    -- "de tudo que entrou, quanto chegou ate aqui", que e a pergunta que se faz de um funil.
    round(count_if(f.confirmed_at is not null)   / count(*), 4) as confirm_rate,
    round(count_if(f.picked_at is not null)      / count(*), 4) as pick_rate,
    round(count_if(f.delivered_at is not null)   / count(*), 4) as delivery_rate,
    round(count_if(f.cancelled_at is not null)   / count(*), 4) as cancellation_rate,
    round(count_if(f.returned_at is not null)    / count(*), 4) as return_rate,

    -- ---- o dinheiro ---------------------------------------------------------------
    -- gross e o que foi COLOCADO; net e o que foi SEPARADO. Os dois nunca sao iguais, e e
    -- essa diferenca que justifica o log de eventos existir: substituicao e remocao mudam
    -- a cesta depois da colocacao. `net_amount` e nulo para quem morreu antes da separacao,
    -- e sum() ignora nulo — que aqui e o comportamento certo: quem nunca foi separado nao
    -- contribui com zero, contribui com nada.
    sum(f.gross_amount_placed)                              as gross_amount_placed,
    sum(f.net_amount)                                       as net_amount_picked,
    sum(f.amount_delta)                                     as amount_delta,
    sum(f.returned_amount)                                  as returned_amount,
    max(f.currency)                                         as currency,

    sum(f.substituted_lines)                                as substituted_lines,
    sum(f.removed_lines)                                    as removed_lines
from {{ ref('fact_order') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
group by 1, 2, 3, 4
