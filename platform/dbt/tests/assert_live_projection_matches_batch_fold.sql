-- O caminho em STREAMING e o caminho em LOTE chegam ao mesmo estado, pedido a pedido.
--
-- QUAIS DOIS CAMINHOS, EXATAMENTE — porque a resposta errada faz este teste valer nada.
--
--   silver_order            window function em SQL sobre o log inteiro
--   silver_live_order_state fold incremental, evento a evento, atravessando um broker
--
-- Sao implementacoes INDEPENDENTES: nao compartilham codigo, nao compartilham motor, e nem
-- sequer compartilham linguagem. E por isso que o acordo entre elas e evidencia.
--
-- O que NAO e evidencia, e precisa estar dito: o acordo entre `orders-project` e
-- `orders-rebuild-projection` — os dois escritores da tabela Iceberg. Eles compartilham
-- `fold_event`, entao concordarem so mostra que nao se atropelam. Confundir as duas coisas
-- seria o mesmo defeito que este projeto ja registrou duas vezes: uma verificacao que
-- compara algo consigo mesmo e passa.
--
-- POR QUE ESTE TESTE NAO E REDUNDANTE COM `orders-reconcile`. O verbo compara TRES folds
-- (inclusive o OLTP) e serve para quem opera; este roda dentro do `make silver` e reprova o
-- build. Um cobre a operacao, o outro cobre a integracao. Um pipeline em que a divergencia
-- so aparece quando alguem lembra de rodar um comando nao tem verificacao — tem habito.
--
-- `net_amount` e nulo para pedido que nunca chegou a separacao, nos DOIS lados. `is distinct
-- from` compara nulo com nulo como igual, que e o que se quer: nulo aqui significa "nao
-- houve separacao", e as duas implementacoes tem de concordar inclusive sobre isso.
select
    b.order_id,
    b.order_status              as status_lote,
    v.order_status              as status_stream,
    b.last_sequence_no          as seq_lote,
    v.last_sequence_no          as seq_stream,
    b.gross_amount_placed       as bruto_lote,
    v.gross_amount_placed       as bruto_stream,
    b.net_amount                as liquido_lote,
    v.net_amount                as liquido_stream,
    b.line_count_placed         as linhas_lote,
    v.line_count_placed         as linhas_stream,
    b.line_count_picked         as separadas_lote,
    v.line_count_picked         as separadas_stream,
    b.substituted_lines         as substituidas_lote,
    v.substituted_lines         as substituidas_stream,
    b.removed_lines             as removidas_lote,
    v.removed_lines             as removidas_stream
from {{ ref('silver_order') }} b
full outer join {{ ref('silver_live_order_state') }} v on b.order_id = v.order_id
where b.order_id is null
   or v.order_id is null
   or b.order_status        is distinct from v.order_status
   or b.last_sequence_no    is distinct from v.last_sequence_no
   or b.gross_amount_placed is distinct from v.gross_amount_placed
   or b.net_amount          is distinct from v.net_amount
   or b.line_count_placed   is distinct from v.line_count_placed
   or b.line_count_picked   is distinct from v.line_count_picked
   or b.substituted_lines   is distinct from v.substituted_lines
   or b.removed_lines       is distinct from v.removed_lines
