-- THE CROSS-CHECK BETWEEN THE TWO SOURCES: the price paid on an order is the price Mercadona
-- published at that warehouse on that day.
--
-- IT IS THE HARDEST TEST TO FOOL IN THE WAREHOUSE, because it links two facts produced along
-- paths that never touch: FACT_PRICE_SNAPSHOT comes from the real API (observed);
-- FACT_ORDER_ITEM comes from a synthetic event log generated months later, which COPIED the
-- price from the exported reference. If the generator had invented a price, rounded it, or
-- picked a product from another warehouse, this is where it shows.
--
-- THE JOIN IS ON THE FULFILLED PRODUCT, NOT THE ORDERED ONE — and that is the part the obvious
-- variant gets wrong. On a substituted line the customer paid the SUBSTITUTE's price, which is another
-- source_product_id; joining on `source_product_id` would make the test compare product A's price
-- with the amount paid for product B and fail 4,670 correct lines. Joining on the
-- fulfilled product is what makes substitution verifiable instead of invisible.
--
-- (wh, price_as_of) GO INTO THE KEY, not just the product: 187 products cost differently
-- across the 4 warehouses on the same day. A test that ignored the warehouse axis would pass
-- comparing against the price somewhere else.
--
-- LINES WITHOUT A FULFILLED PRODUCT STAY OUT — removed ones, and those of orders that never reached
-- picking. There is no paid price to check, and demanding one would be demanding that the model
-- invent the number this test exists to forbid.
--
-- THE COMPARISON IS AGAINST `purchasable_unit_price`, not against the raw `unit_price`. In the 10
-- product x warehouse combinations sold loose without `unit_size`, the API returns
-- `reference_price * 99` — the weight selector's ceiling, not a consumer price. Matching against
-- the raw value would make this test DEMAND that the order had charged 1,084.05 EUR for 150 g of
-- prawns, that is, demand the defect back. The raw value travels in the failure report
-- alongside, so the difference is visible when there is one.
--
-- Fails with one row per divergence.
select
    i.order_id,
    i.line_no,
    i.wh,
    i.price_as_of,
    i.fulfilled_source_product_id,
    i.unit_price_paid,
    p.purchasable_unit_price                                as preco_no_catalogo,
    p.unit_price                                            as preco_cru_da_fonte,
    p.price_basis,
    case
        when p.source_product_id is null then 'produto cumprido ausente do catalogo daquele armazem e dia'
        else 'preco pago diverge do catalogo'
    end                                                     as motivo
from {{ ref('fact_order_item') }} i
left join {{ ref('fact_price_snapshot') }} p
       on  p.snapshot_date      = i.price_as_of
      and  p.wh                 = i.wh
      and  p.source_product_id  = i.fulfilled_source_product_id
where i.fulfilled_source_product_id is not null
  and (p.source_product_id is null or p.purchasable_unit_price <> i.unit_price_paid)
