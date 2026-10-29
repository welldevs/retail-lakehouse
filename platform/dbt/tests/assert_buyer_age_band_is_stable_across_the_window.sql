-- THE SAME PERSON CANNOT BE IN TWO BANDS IN THE SAME WINDOW — unless they have a birthday
-- within it, and in that case the two bands must be NEIGHBORS and in the right order.
--
-- THE FAILURE MODE THIS CATCHES: a half-regenerated window. Someone edits the band
-- boundaries, re-exports the reference and runs `orders-refresh` on two of the four warehouses.
-- The result is a window in which the same customer appears as `35_49` one day and `GE65` the
-- next, and any per-cohort aggregation mixes two definitions — no error, no null, no
-- broken total. It is the sibling of `assert_demand_group_is_stable_across_the_window`, on the
-- buyer axis instead of the product one.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT. `assert_buyer_age_band_matches_the_customer_birth_year`
-- checks each order against the customer record and would pass: both halves are internally
-- correct, each under its own band definition. What only THIS layer can ask is
-- whether the window is coherent WITH ITSELF.
--
-- A BIRTHDAY IS LEGITIMATE, which is why the condition is not "a single band". The current window has
-- four days, but the model does not depend on that: what is required is that the transition be to the
-- next band and forward in time. Two non-adjacent bands, or a customer who
-- GETS YOUNGER over the window, are defects.
with por_cliente as (

    select
        customer_id,
        wh,
        buyer_age_band,
        min(ingestion_date) as primeiro_dia,
        max(ingestion_date) as ultimo_dia
    from {{ ref('silver_order') }}
    where buyer_age_band is not null
    group by 1, 2, 3

),

ordem as (

    select
        p.*,
        case buyer_age_band
            when 'LT35'  then 1
            when '35_49' then 2
            when '50_64' then 3
            when 'GE65'  then 4
        end as posicao
    from por_cliente p

),

extremos as (

    select
        customer_id,
        wh,
        count(*)                                            as faixas,
        min(posicao)                                        as primeira,
        max(posicao)                                        as ultima,
        string_agg(buyer_age_band, ' | ' order by posicao)  as faixas_vistas
    from ordem
    group by 1, 2

)

select
    e.customer_id,
    e.wh,
    e.faixas,
    e.faixas_vistas,
    inicial.ultimo_dia   as fim_da_faixa_mais_nova,
    final.primeiro_dia   as inicio_da_faixa_mais_velha
from extremos e
join ordem inicial
  on inicial.customer_id = e.customer_id and inicial.wh = e.wh
 and inicial.posicao = e.primeira
join ordem final
  on final.customer_id = e.customer_id and final.wh = e.wh
 and final.posicao = e.ultima
where e.faixas > 1
  and (
      -- a jump of more than one band: no birthday explains it
      e.ultima - e.primeira > 1
      -- or the older band starts BEFORE the younger one ends: the customer got younger
      or final.primeiro_dia < inicial.ultimo_dia
  )
