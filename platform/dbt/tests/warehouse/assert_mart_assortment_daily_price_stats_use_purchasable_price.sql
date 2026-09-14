-- MART_ASSORTMENT_DAILY.min/max_unit_price BATEM COM O MINIMO/MAXIMO DE
-- fact_price_snapshot.purchasable_unit_price NO MESMO GRAO — nunca com unit_price CRU.
--
-- POR QUE ESTE TESTE EXISTE. Ate esta correcao, mart_assortment_daily agregava
-- fact_price_snapshot.unit_price cru. Em ~10 combinacoes produto x armazem vendidas a
-- granel sem unit_size, a API devolve unit_price = reference_price * 99 — o TETO do
-- seletor de peso (ver assert_purchasable_price_is_anchored_in_the_bunch_amount.sql e
-- DECISIONS.md, "Demand calibration against MAPA 2025"), nao um preco de consumo. Isso
-- inflava o maximo da categoria "Marisco y pescado" para 3.663,00 EUR — um numero
-- internamente consistente, e por isso nenhum dos testes existentes reprovava: o defeito
-- so apareceu ao ler o mart num painel, nao ao rodar `dbt build`.
--
-- Reconstroi o minimo/maximo POR FORA, direto de fact_price_snapshot com a coluna certa,
-- e reprova se o mart nao bater — a mesma disciplina de
-- assert_order_funnel_totals_match_fact_order.sql, so que para preco em vez de contagem.
--
-- Falha com uma linha por (snapshot_date, wh, category_id) divergente.
with esperado as (

    select
        f.snapshot_date,
        f.wh,
        f.primary_category_id                               as category_id,
        min(f.purchasable_unit_price)                        as min_esperado,
        max(f.purchasable_unit_price)                        as max_esperado
    from {{ ref('fact_price_snapshot') }} f
    group by 1, 2, 3

)
select
    m.snapshot_date, m.wh, m.category_id,
    m.min_unit_price, e.min_esperado,
    m.max_unit_price, e.max_esperado
from {{ ref('mart_assortment_daily') }} m
join esperado e
       on  e.snapshot_date = m.snapshot_date
      and  e.wh            = m.wh
      and  e.category_id   = m.category_id
where m.min_unit_price <> e.min_esperado
   or m.max_unit_price <> e.max_esperado
