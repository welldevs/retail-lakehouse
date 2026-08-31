-- O DEFEITO MAIS CARO JA MEDIDO NESTE REPO, e o teste que impede sua volta.
--
-- Quando `selling_method = 1` e `unit_size` e nulo, a fonte devolve
-- `unit_price = reference_price * 99` — o preco do TETO do seletor de peso, e nao de nada
-- que um domicilio compre. Medido em 2026-08-31: 10 linhas, fator exatamente 99,000, com
-- `unit_price` chegando a 3.663,00 EUR para um congelado a 37,00 EUR/kg. Usadas por um
-- gerador de cesta indiferente ao preco, essas 10 linhas produziram 16,6% de TODA a
-- receita simulada, e nenhum dos 947 testes existentes reprovou.
--
-- POR QUE A VARIANTE OBVIA NAO PEGARIA
-- ------------------------------------
-- Um teto absoluto (`purchasable_unit_price < 600`) passaria em silencio no dia em que a
-- fonte trouxer um granel a 3,00 EUR/kg: 3,00 * 99 = 297, confortavelmente abaixo do teto,
-- e o defeito voltaria valendo 297 EUR por unidade. O limiar tem de ser RELACIONAL —
-- ancorado nos dois campos observados que definem a porcao — e nao um numero escolhido.
--
-- AS DUAS METADES, e por que ambas sao necessarias
-- ------------------------------------------------
--   1. toda linha `bunch` tem de valer `reference_price * min_bunch_amount`. Pega tanto a
--      volta do teto de 99 quanto qualquer "simplificacao" que faca
--      purchasable_unit_price = unit_price.
--   2. nenhuma linha que NAO e `bunch` pode ter preco diferente de `unit_price`. Delimita
--      o raio de acao da derivacao: ela corrige 10 linhas, e nao pode comecar a reescrever
--      as outras 18.550 sem que alguem note. Sem esta metade, um erro de sinal no CASE
--      passaria pela primeira.
with bunch_desancorado as (

    select
        warehouse,
        ingestion_date,
        source_product_id,
        price_basis,
        unit_price,
        reference_price,
        min_bunch_amount,
        purchasable_unit_price,
        (reference_price * min_bunch_amount)::decimal(10, 2) as esperado,
        'bunch fora da ancora' as motivo
    from {{ ref('silver_product_price') }}
    where price_basis = 'bunch'
      and (
            purchasable_unit_price is distinct from (reference_price * min_bunch_amount)::decimal(10, 2)
         or min_bunch_amount is null
         or min_bunch_amount <= 0
      )

),

nao_bunch_reescrito as (

    select
        warehouse,
        ingestion_date,
        source_product_id,
        price_basis,
        unit_price,
        reference_price,
        min_bunch_amount,
        purchasable_unit_price,
        unit_price as esperado,
        'nao-bunch com preco reescrito' as motivo
    from {{ ref('silver_product_price') }}
    where price_basis <> 'bunch'
      and purchasable_unit_price is distinct from unit_price

)

select * from bunch_desancorado
union all
select * from nao_bunch_reescrito
