-- Restricoes 2 e 3 das cinco do FAQ, num teste so porque falham juntas:
--   2. so produtos do catalogo de W naquela data;
--   3. preco = o preco de W naquela data.
--
-- POR QUE AS DUAS NO MESMO TESTE. Um produto ausente do catalogo e um preco divergente sao a
-- mesma pergunta feita ao mesmo join: "esta linha corresponde a uma linha real de
-- silver_product_price?". Separa-las obrigaria a repetir o join e a dedup, e faria um produto
-- ausente aparecer como duas falhas.
--
-- 187 PRODUTOS DIVERGEM DE PRECO ENTRE ARMAZENS no mesmo dia — medido na Fase 2. Por isso o
-- join carrega `warehouse` E `ingestion_date`: casar so por produto passaria com o preco do
-- armazem errado.
--
-- DEDUP ANTES DO JOIN. `silver_product_price` tem mais linhas que produtos por particao
-- (4.581 linhas para 4.311 produtos em mad1/2026-08-24): um produto aparece em mais de uma
-- categoria, e isso e semantica da fonte. Sem `distinct` o join multiplicaria a linha do
-- pedido e o teste reprovaria por fanout, nao por defeito. O preco e identico entre as
-- aparicoes — garantia que ja tem teste proprio (assert_price_is_consistent_across_appearances).
--
-- LINHA REMOVIDA tambem entra: o produto PEDIDO tinha de existir mesmo que nao tenha sido
-- entregue. O que nao se confere para ela e o `fulfilled_*`, que e nulo por contrato.
with catalogo as (

    select distinct
        warehouse         as wh,
        ingestion_date    as price_as_of,
        source_product_id,
        unit_price
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
    c.unit_price   as preco_no_catalogo,
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
