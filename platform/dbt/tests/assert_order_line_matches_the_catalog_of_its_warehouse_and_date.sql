-- Constraints 2 and 3 of the FAQ's five, in a single test because they fail together:
--   2. only products from W's catalog on that date;
--   3. price = W's price on that date.
--
-- WHY BOTH IN THE SAME TEST. A product missing from the catalog and a divergent price are the
-- same question asked of the same join: "does this line correspond to a real row of
-- silver_product_price?". Separating them would force repeating the join and the dedup, and would make a
-- missing product show up as two failures.
--
-- 187 PRODUCTS DIVERGE IN PRICE ACROSS WAREHOUSES on the same day — measured in Phase 2. That is why the
-- join carries `warehouse` AND `ingestion_date`: matching by product alone would pass with the price of the
-- wrong warehouse.
--
-- DEDUP BEFORE THE JOIN. `silver_product_price` has more rows than products per partition
-- (4,581 rows for 4,311 products in mad1/2026-08-24): a product appears in more than one
-- category, and that is source semantics. Without `distinct` the join would multiply the order
-- line and the test would fail from fanout, not from a defect. The price is identical across
-- appearances — a guarantee that already has its own test (assert_price_is_consistent_across_appearances).
--
-- A REMOVED LINE also comes in: the ORDERED product had to exist even if it was not
-- delivered. What is not checked for it is `fulfilled_*`, which is null by contract.
--
-- THE COMPARISON IS AGAINST `purchasable_unit_price`, NOT AGAINST `unit_price`. Measured on
-- 2026-08-31: when `selling_method = 1` and `unit_size` is null, the source returns
-- `unit_price = reference_price * 99` — the weight selector's ceiling (obligation 5 of the
-- Mercadona contract). There are 10 product x warehouse combinations, and matching against `unit_price` would make
-- this test DEMAND that the order charge 1,084.05 EUR for 150 g of prawns. The raw value
-- travels in the failure report alongside, so the difference is visible when there is one.
with catalogo as (

    select distinct
        warehouse              as wh,
        ingestion_date         as price_as_of,
        source_product_id,
        purchasable_unit_price as unit_price,
        unit_price             as source_unit_price,
        price_basis
    from {{ ref('silver_product_price') }}

),

pedido as (

    select
        l.order_id,
        l.line_no,
        l.wh,
        l.price_as_of,
        l.line_status,
        l.source_product_id,
        l.unit_price,
        l.fulfilled_source_product_id,
        l.fulfilled_unit_price
    from {{ ref('silver_order_line') }} l

)

select
    p.order_id,
    p.line_no,
    p.line_status,
    p.source_product_id,
    p.unit_price,
    c.unit_price        as preco_no_catalogo,
    c.source_unit_price as preco_cru_da_fonte,
    c.price_basis,
    p.fulfilled_source_product_id,
    p.fulfilled_unit_price,
    f.unit_price   as preco_do_substituto_no_catalogo,
    case
        when c.source_product_id is null then 'produto pedido nao esta no catalogo deste (wh, data)'
        when p.unit_price is distinct from c.unit_price then 'preco pago diverge do observado'
        when p.fulfilled_source_product_id is not null and f.source_product_id is null
            then 'produto entregue nao esta no catalogo deste (wh, data)'
        else 'preco do produto entregue diverge do observado'
    end as motivo
from pedido p
left join catalogo c
       on p.wh = c.wh and p.price_as_of = c.price_as_of
      and p.source_product_id = c.source_product_id
left join catalogo f
       on p.wh = f.wh and p.price_as_of = f.price_as_of
      and p.fulfilled_source_product_id = f.source_product_id
where c.source_product_id is null
   or p.unit_price is distinct from c.unit_price
   or (p.fulfilled_source_product_id is not null and f.source_product_id is null)
   or (p.fulfilled_source_product_id is not null
       and p.fulfilled_unit_price is distinct from f.unit_price)
