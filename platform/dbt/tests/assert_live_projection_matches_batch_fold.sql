-- The STREAMING path and the BATCH path reach the same state, order by order.
--
-- WHICH TWO PATHS, EXACTLY — because the wrong answer makes this test worth nothing.
--
--   silver_order            SQL window function over the whole log
--   silver_live_order_state incremental fold, event by event, through a broker
--
-- They are INDEPENDENT implementations: they share no code, no engine, and not
-- even a language. That is why their agreement is evidence.
--
-- What is NOT evidence, and needs to be said: the agreement between `orders-project` and
-- `orders-rebuild-projection` — the two writers of the Iceberg table. They share
-- `fold_event`, so their agreeing only shows they do not trample each other. Confusing the two things
-- would be the same defect this project has already recorded twice: a check that
-- compares something with itself and passes.
--
-- WHY THIS TEST IS NOT REDUNDANT WITH `orders-reconcile`. The command compares THREE folds
-- (including the OLTP) and serves the operator; this one runs inside `make silver` and fails the
-- build. One covers operations, the other covers integration. A pipeline in which the divergence
-- only shows up when someone remembers to run a command has no check — it has a habit.
--
-- `net_amount` is null for an order that never reached picking, on BOTH sides. `is distinct
-- from` treats null vs null as equal, which is what is wanted: null here means "there was
-- no picking", and both implementations must agree on that too.
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
