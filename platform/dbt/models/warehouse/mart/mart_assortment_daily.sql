-- GRAO: (snapshot_date, wh, category_id) — uma linha por categoria de nivel 2, armazem e
--        dia OBSERVADO.
--
-- PERGUNTA QUE RESPONDE: quanto de catalogo cada armazem tem em cada categoria, a que
-- preco, e o quanto disso e exclusivo dele.
--
-- SORTIMENTO E DERIVADO DA PRESENCA DA LINHA no fato de preco, nao de uma tabela propria:
-- uma linha em FACT_PRICE_SNAPSHOT afirma que o produto estava no catalogo daquele armazem
-- naquele dia. Medido que isso varia de verdade — 4.283 a 4.335 produtos por armazem, so
-- 3.859 nos quatro.
--
-- products_in_all_warehouses e o denominador honesto de qualquer comparacao entre
-- armazens: comparar preco medio de catalogos diferentes mede a diferenca de catalogo,
-- nao de preco.
{{ config(materialized = 'table') }}

with universal as (

    -- Produtos presentes nos QUATRO armazens naquele dia. Calculado por dia, e nao uma
    -- vez so, porque o catalogo muda: um produto pode ser universal numa data e nao em
    -- outra, e congelar isso numa lista fixa mentiria sobre as datas antigas.
    select snapshot_date, source_product_id
    from {{ ref('fact_price_snapshot') }}
    group by 1, 2
    having count(distinct wh) = (select count(*) from {{ ref('dim_warehouse') }})

)

select
    f.snapshot_date,
    f.date_key,
    d.year_month,
    f.wh,

    f.primary_category_id                                   as category_id,
    c.category_name,
    c.parent_category_id,
    c.parent_category_name,

    count(*)                                                as products,
    count_if(u.source_product_id is not null)               as products_in_all_warehouses,
    count_if(u.source_product_id is null)                   as products_exclusive_here,

    min(f.unit_price)                                       as min_unit_price,
    max(f.unit_price)                                       as max_unit_price,
    round(avg(f.unit_price), 4)                             as avg_unit_price,
    median(f.unit_price)                                    as median_unit_price,

    count_if(f.is_new_arrival)                              as new_arrivals,
    count_if(f.is_pack)                                     as packs
from {{ ref('fact_price_snapshot') }} f
join {{ ref('dim_date') }} d          on d.date_key = f.date_key
left join {{ ref('dim_category') }} c on c.category_id = f.primary_category_id
left join universal u
       on  u.snapshot_date     = f.snapshot_date
      and  u.source_product_id = f.source_product_id
group by 1, 2, 3, 4, 5, 6, 7, 8
