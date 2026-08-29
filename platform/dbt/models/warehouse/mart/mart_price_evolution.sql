-- GRAO: (snapshot_date, wh, source_product_id) — uma linha por produto, armazem e dia
--        OBSERVADO. Nunca por dia de calendario: ver days_since_previous_snapshot.
--
-- PERGUNTA QUE RESPONDE: como o preco de cada produto se moveu, em cada armazem, e quanto
-- tempo se passou desde a observacao anterior.
--
-- A COLUNA QUE IMPEDE A LEITURA ERRADA e days_since_previous_snapshot. Uma variacao de
-- 0,10 EUR ao longo de 8 dias (o vao real entre 08-16 e 08-24) nao e comparavel a uma de
-- 0,10 EUR em 1 dia. Sem essa coluna, qualquer media de variacao diaria misturaria as
-- duas e ninguem notaria.
{{ config(materialized = 'table') }}

select
    f.snapshot_date,
    f.date_key,
    d.year,
    d.year_month,
    f.wh,
    w.province_name,

    f.source_product_id,
    p.display_name,
    p.identity_ambiguous,
    c.category_name,
    c.parent_category_name,

    f.unit_price,
    f.unit_price_ex_tax,
    f.tax_percentage,

    ch.previous_unit_price,
    ch.price_delta,
    ch.price_delta_pct,
    ch.change_type,
    ch.days_between_snapshots                               as days_since_previous_snapshot,

    -- Id novo com nome que ja existia: nao e "produto novo" ate alguem decidir que e.
    -- A flag viaja ate o mart para que a decisao seja de quem consulta.
    coalesce(ch.name_seen_before, false)                    as identity_review_needed
from {{ ref('fact_price_snapshot') }} f
join {{ ref('dim_date') }} d       on d.date_key = f.date_key
join {{ ref('dim_warehouse') }} w  on w.wh = f.wh
left join {{ ref('dim_product') }} p  on p.product_sk = f.product_sk
left join {{ ref('dim_category') }} c on c.category_id = f.primary_category_id
left join {{ ref('fact_price_change') }} ch
       on  ch.snapshot_date     = f.snapshot_date
      and  ch.wh                = f.wh
      and  ch.source_product_id = f.source_product_id
