-- INVERTED TEST: fails when NOTHING went wrong.
--
-- Sibling of assert_geography_postal_code_is_not_a_key, and for the same reason. That one fails
-- when no postal code crosses municipalities, that is, when DIM_GEOGRAPHY's composite grain loses its
-- justification. This one fails when no basket changes between placement and picking — that
-- is, when `net_amount` becomes derivable from `gross_amount_placed`.
--
-- WHY THAT IS A DEFECT AND NOT A QUIET DAY. If the fold is trivial, the event log carries
-- no information an orders table would not carry, and the whole model —
-- the Source that writes a log instead of state, the Silver that folds it, the broker that carries it —
-- stops being justified. ARCHITECTURE.md already explicitly refused the decorative version
-- of this: "publishing the batch output itself to a topic and consuming it back would add a
-- broker to maintain and zero information".
--
-- Measured on the 2026-08-24 to 08-27 window: 4,670 substituted lines and 2,321 removed out of
-- 120,693, and 5,166 orders with a picked value different from the placed one.
select
    count(*)                                                        as pedidos_com_separacao,
    sum(case when substituted_lines > 0 or removed_lines > 0 then 1 else 0 end)
                                                                    as pedidos_com_cesta_alterada,
    sum(case when net_amount is distinct from gross_amount_placed then 1 else 0 end)
                                                                    as pedidos_com_valor_diferente
from {{ ref('silver_order') }}
where net_amount is not null
having sum(case when substituted_lines > 0 or removed_lines > 0 then 1 else 0 end) = 0
    or sum(case when net_amount is distinct from gross_amount_placed then 1 else 0 end) = 0
