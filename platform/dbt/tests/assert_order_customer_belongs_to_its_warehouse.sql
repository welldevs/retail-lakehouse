-- A customer of W only orders from W. Constraint 1 of the five the FAQ had already written before
-- Orders existed, and the easiest to break silently.
--
-- Measured in Phase 1: 0 postal codes cross warehouses — each one's service area is disjoint. So an
-- order whose customer belongs to another warehouse is not "a rare case", it is a defect.
--
-- THIS IS THE THIRD CHECK OF THE SAME RULE, and the three are independent on purpose: the generator
-- only chooses among that warehouse's customers, `orders-validate` re-checks against the reference,
-- and this one looks at the data AFTER it crosses RAW and parquet. Same ladder as
-- assert_customer_lives_in_its_warehouse_service_area.
--
-- THE JOIN IS BY ID, and the comparison is on `wh`. No name is involved: the lesson of the 370 of 370
-- municipalities spelled differently by INE's two sources applies here too.
select
    o.order_id,
    o.wh          as warehouse_do_pedido,
    c.wh          as warehouse_do_cliente,
    o.customer_id
from {{ ref('silver_order') }} o
left join (
    select distinct customer_id, wh from {{ ref('silver_customer') }}
) c on o.customer_id = c.customer_id
where c.customer_id is null
   or c.wh is distinct from o.wh
