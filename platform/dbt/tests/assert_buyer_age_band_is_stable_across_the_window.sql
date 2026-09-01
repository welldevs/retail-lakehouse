-- A MESMA PESSOA NAO PODE ESTAR EM DUAS FAIXAS NA MESMA JANELA — salvo se fizer aniversario
-- dentro dela, e nesse caso as duas faixas precisam ser VIZINHAS e na ordem certa.
--
-- O MODO DE FALHA QUE ISTO PEGA: uma janela regerada pela metade. Alguem edita os limites
-- das faixas, reexporta a referencia e roda `orders-refresh` em dois dos quatro armazens.
-- O resultado e uma janela em que o mesmo cliente aparece como `35_49` num dia e `GE65` no
-- outro, e qualquer agregacao por coorte mistura duas definicoes — sem erro, sem nulo, sem
-- total quebrado. E o irmao de `assert_demand_group_is_stable_across_the_window`, no eixo
-- do comprador em vez do produto.
--
-- POR QUE A VARIANTE OBVIA NAO PEGARIA. `assert_buyer_age_band_matches_the_customer_birth_year`
-- confere cada pedido contra o cadastro e passaria: as duas metades estao internamente
-- corretas, cada uma sob a sua definicao de faixa. O que so ESTA camada consegue perguntar e
-- se a janela e coerente CONSIGO MESMA.
--
-- ANIVERSARIO E LEGITIMO, e por isso a condicao nao e "faixa unica". A janela atual tem
-- quatro dias, mas o modelo nao depende disso: o que se exige e que a transicao seja para a
-- faixa seguinte e para frente no tempo. Duas faixas nao adjacentes, ou um cliente que
-- REJUVENESCE ao longo da janela, sao defeito.
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
      -- salto de mais de uma faixa: nao ha aniversario que explique
      e.ultima - e.primeira > 1
      -- ou a faixa mais velha comeca ANTES de a mais nova terminar: o cliente rejuvenesceu
      or final.primeiro_dia < inicial.ultimo_dia
  )
