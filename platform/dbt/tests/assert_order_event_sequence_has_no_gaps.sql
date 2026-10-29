-- sequence_no must be a contiguous 1..N within each order.
--
-- WHY THIS IS THE MOST BASIC TEST OF A LOG. An event lost between the Source and the parquet
-- changes no count the eye would notice: the order still has events, the fold
-- still produces a state, and the value still looks plausible. What changes is the MEANING —
-- an order that lost `order_line_removed` starts declaring revenue that did not happen.
--
-- Checking min=1, max=N and count=N catches all three forms of the defect at once: a gap in the middle,
-- a lost start and a duplicate. Comparing only `count(*) = max(sequence_no)` would let through a
-- log in which a duplicate offsets a gap.
select
    order_id,
    count(*)                as eventos,
    min(sequence_no)        as menor,
    max(sequence_no)        as maior,
    count(distinct sequence_no) as distintos
from {{ ref('silver_order_event') }}
group by order_id
having min(sequence_no) <> 1
    or max(sequence_no) <> count(*)
    or count(distinct sequence_no) <> count(*)
