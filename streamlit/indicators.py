"""The panel's indicators: the query AND the explanation, in the SAME place.

WHY THIS IS A MODULE AND NOT A DOCUMENT. This folder's `CONTRACT.md` is GENERATED from here
(`make dashboard-contract`). If the explanation lived in a hand-written markdown, it would be a
second place where the indicator lives, and the two would diverge at the first SQL tweak — with
the cruel detail that the review would keep "passing", because nobody reads a SQL query and a
text side by side looking for disagreement. It's the same reason the STAGE's DDL is derived from
the cut instead of hand-written, and why `docs/warehouse-evidence/` is generated.

WHAT EACH FIELD CARRIES, and why none of them is optional:

  `pergunta`  — what the indicator answers. If it doesn't fit in one sentence, the indicator is
                doing two things.
  `grao`      — the source's grain. It's what says whether a sum is legitimate: summing
                `orders_touching_category` across categories counts the same order multiple times.
  `tipo`      — `observado` (came from a real source), `sintetico` (was generated), `misto` or
                `derivado`. A panel that doesn't distinguish the two invites reading simulation
                density as market penetration.
  `armadilhas`— what goes wrong when rebuilding this in Power BI. It's not a footnote caveat:
                these are the cases where the obvious measure produces a PLAUSIBLE and wrong
                number, which is the only class of error no test catches.

PARAMETERS. Every query with a date axis accepts `%(inicio)s`, `%(fim)s` and `%(armazens)s`
(comma-separated list). The warehouse filter uses
`array_contains(wh::variant, split(%(armazens)s, ','))` — one line, with no SQL assembled by
concatenation, so there's no way to inject anything through the interface's selector.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DB = "RETAIL"
FILTRO_DATA = "order_date between %(inicio)s and %(fim)s"
FILTRO_DATA_SNAP = "snapshot_date between %(inicio)s and %(fim)s"
FILTRO_DATA_STOCK = "stock_date between %(inicio)s and %(fim)s"
FILTRO_WH = "array_contains(wh::variant, split(%(armazens)s, ','))"


@dataclass(frozen=True)
class Indicador:
    chave: str
    titulo: str
    grupo: str
    pergunta: str
    grao: str
    tipo: str
    marts: tuple[str, ...]
    sql: str
    armadilhas: tuple[str, ...] = field(default_factory=tuple)
    datado: bool = True


INDICADORES: tuple[Indicador, ...] = (

    # ================================================================= A. COMMERCIAL
    Indicador(
        chave="resumo_comercial",
        titulo="Commercial summary",
        grupo="A. Commercial",
        pergunta="How many orders came in, how many reached the customer, and how much revenue was realized?",
        grao="(order_date, wh) aggregated for the period total",
        tipo="synthetic (the order) over observed (customer, product, price)",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "AVERAGE TICKET has two possible denominators and they are NOT equivalent: "
            "revenue/orders_picked = 95.11 and revenue/orders_placed = 90.91. The second "
            "divides the revenue of whoever was picked by the total including whoever never "
            "reached picking — it measures something that doesn't exist. Use `orders_picked`.",
            "`net_amount_picked` is NULL for an order that died before picking, and `sum()` "
            "ignores null. This is correct and intentional: whoever was never picked doesn't "
            "contribute zero, it contributes nothing. In Power BI, a `SUM` over a null column "
            "does the same; a `COALESCE(...,0)` would invent a revenue realization that never "
            "happened.",
        ),
        sql=f"""
            select
                sum(orders_placed)                                  as pedidos_colocados,
                sum(orders_confirmed)                               as pedidos_confirmados,
                sum(orders_picked)                                  as pedidos_separados,
                sum(orders_delivered)                               as pedidos_entregues,
                sum(gross_amount_placed)                            as valor_colocado,
                sum(net_amount_picked)                              as receita_apurada,
                -- Denominador = separados. Ver as armadilhas.
                round(sum(net_amount_picked)
                      / nullif(sum(orders_picked), 0), 2)           as ticket_medio,
                round(sum(orders_delivered)
                      / nullif(sum(orders_placed), 0), 4)           as taxa_entrega,
                max(currency)                                       as moeda
            from {DB}.MART.MART_ORDER_FUNNEL
            where {FILTRO_DATA} and {FILTRO_WH}
        """,
    ),

    Indicador(
        chave="funil",
        titulo="Conversion funnel, by milestone reached",
        grupo="A. Commercial",
        pergunta="Out of every 100 orders placed, how many made it through each stage?",
        grao="(order_date, wh) aggregated; one row per STAGE",
        tipo="derived from milestone count",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "STAGES ARE COUNTED BY MILESTONE REACHED, never by status. `order_status` holds "
            "the state of the LAST event: a returned order has status RETURNED and WAS "
            "delivered. Measured on this base: counting status='DELIVERED' gives 193,789; "
            "counting delivered_at is not null gives 195,419 — the 1,630 returned ones. A "
            "funnel built on status publishes a delivery rate 1% lower than the real one, and "
            "nothing fails.",
            "Milestone is monotonic (once reached, it doesn't go back); status isn't. The mart "
            "already resolves this — the `orders_*` columns are milestone counts. In Power BI, "
            "do NOT rebuild the funnel from a status field.",
        ),
        sql=f"""
            with total as (
                select
                    sum(orders_placed)          as colocado,
                    sum(orders_confirmed)       as confirmado,
                    sum(orders_picking_started) as separacao_iniciada,
                    sum(orders_picked)          as separado,
                    sum(orders_dispatched)      as despachado,
                    sum(orders_delivered)       as entregue
                from {DB}.MART.MART_ORDER_FUNNEL
                where {FILTRO_DATA} and {FILTRO_WH}
            )
            select 1 as ordem, 'Placed'            as etapa, colocado            as pedidos, 1.0 as taxa from total
            union all select 2, 'Payment approved', confirmado,         round(confirmado/nullif(colocado,0),4)         from total
            union all select 3, 'Picking started',  separacao_iniciada, round(separacao_iniciada/nullif(colocado,0),4) from total
            union all select 4, 'Picked',            separado,           round(separado/nullif(colocado,0),4)           from total
            union all select 5, 'Dispatched',        despachado,         round(despachado/nullif(colocado,0),4)         from total
            union all select 6, 'Delivered',         entregue,           round(entregue/nullif(colocado,0),4)           from total
            order by ordem
        """,
    ),

    Indicador(
        chave="vazamento",
        titulo="Where the order leaks out",
        grupo="A. Commercial",
        pergunta="How many orders left the funnel, and for what reason?",
        grao="(order_date, wh) aggregated; one row per exit reason",
        tipo="derived from milestone count",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "THESE COUNTS DO NOT ADD UP WITH THE FUNNEL STAGES. `orders_returned` counts "
            "whoever left AFTER going through the entire funnel; adding exits to stages would "
            "count the returned orders twice. In Power BI, keep the two blocks separate and "
            "never build a 'total orders' by summing stages with exits.",
        ),
        sql=f"""
            select 'Payment declined' as motivo, sum(orders_payment_failed)  as pedidos,
                   round(sum(orders_payment_failed)/nullif(sum(orders_placed),0),4) as sobre_colocados
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Cancelled', sum(orders_cancelled),
                   round(sum(orders_cancelled)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Delivery failed', sum(orders_delivery_failed),
                   round(sum(orders_delivery_failed)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Returned (after delivery)', sum(orders_returned),
                   round(sum(orders_returned)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            order by pedidos desc
        """,
    ),

    Indicador(
        chave="decomposicao_perda",
        titulo="Value loss, decomposed by CAUSE",
        grupo="A. Commercial",
        pergunta="What was placed but not realized as revenue — and why?",
        grao="(order_date, wh) aggregated",
        tipo="derived",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "THIS IS THE EASIEST INDICATOR IN THE PANEL TO GET WRONG, and the error produces "
            "a plausible number. `SUM(gross) - SUM(net)` gives 1,278,522.89 and MIXES two "
            "losses with opposite causes. `SUM(amount_delta)` gives 393,757.28 — and that's "
            "not the same thing, nor is it wrong: `amount_delta` only exists for a PICKED "
            "order, and `sum()` ignores the null of the other 9,121.",
            "The correct decomposition, verified arithmetically (393,757.28 + 884,765.61 = "
            "1,278,522.89): loss at PICKING = sum(amount_delta), the basket shrank through "
            "removal and substitution; loss from DEAD ORDER = the remainder, the full value "
            "of whoever never reached picking. These are problems from different areas — one "
            "is store operations, the other is payment and cancellation — and a single number "
            "hides which one is happening.",
            "THE HYPOTHESIS THAT STOOD HERE DID NOT HOLD UP, and the record is that of the "
            "test that failed it, not of a deleted sentence. Over 6,400 orders (Phase 3), the "
            "dead order was worth on average 179.51 against 135.36 for the picked one, and "
            "the hypothesis was that a bigger basket takes longer to pick and offers a wider "
            "window for cancellation. Over 206,523 orders (Phase 7) the two values converged "
            "to 97.00 and 97.11 — practically equal, over 9,121 dead orders. Either the "
            "original difference was small-sample noise, or the effect doesn't exist. It "
            "CANNOT be confirmed from the MART — there is no order grain here — so the "
            "question remains open, and the earlier hypothesis is recorded as refuted, not "
            "as pending.",
        ),
        sql=f"""
            with t as (
                select sum(gross_amount_placed) as colocado,
                       sum(net_amount_picked)   as apurado,
                       sum(amount_delta)        as delta_separacao,
                       sum(orders_placed)       as pedidos,
                       sum(orders_picked)       as separados
                from {DB}.MART.MART_ORDER_FUNNEL
                where {FILTRO_DATA} and {FILTRO_WH}
            )
            select 1 as ordem, 'Loss at picking (basket shrank)' as causa,
                   delta_separacao as valor, separados as pedidos_afetados,
                   round(delta_separacao/nullif(separados,0),2) as por_pedido from t
            union all
            select 2, 'Loss from dead order (never picked)',
                   colocado - apurado - delta_separacao, pedidos - separados,
                   round((colocado - apurado - delta_separacao)
                         /nullif(pedidos - separados,0),2) from t
            union all
            select 3, 'TOTAL unfulfilled', colocado - apurado, pedidos - separados, null from t
            order by ordem
        """,
    ),

    Indicador(
        chave="serie_diaria",
        titulo="Daily series: orders and revenue",
        grupo="A. Commercial",
        pergunta="How do orders and revenue move day by day, by warehouse?",
        grao="(order_date, wh) — the mart's native grain",
        tipo="mixed",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "THE CURRENT WINDOW HAS 4 DAYS. There is no trend, seasonality or weekly "
            "comparison to extract from this — any trend line over 4 points is decoration. "
            "The axis exists so the series GROWS, and it grows every day the DAG runs.",
        ),
        sql=f"""
            select order_date, wh,
                   orders_placed, orders_delivered, net_amount_picked, amount_delta,
                   round(net_amount_picked/nullif(orders_picked,0),2) as ticket_medio,
                   delivery_rate
            from {DB}.MART.MART_ORDER_FUNNEL
            where {FILTRO_DATA} and {FILTRO_WH}
            order by order_date, wh
        """,
    ),

    # ================================================================= B. OPERATIONS
    Indicador(
        chave="sla_separacao",
        titulo="Picking SLA: threshold, maximum and violations",
        grupo="B. Operations",
        pergunta="How many orders breached the declared picking threshold?",
        grao="(order_date, wh) aggregated",
        tipo="derived from a declared assumption (synthetic)",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "THE THREE COLUMNS ONLY MEAN SOMETHING TOGETHER. Until Phase 7 "
            "`orders_breaching_sla` was 0 not because operations were good: the declared "
            "threshold was 90 minutes against an ARITHMETIC ceiling of 80 (`basket_lines_max` "
            "40 x `minutes_per_line_picked` 2), and no possible basket reached the threshold. "
            "Fixed in Phase 7: the threshold became DERIVED from the declared p90 of the "
            "basket distribution (`sla_picking_percentile` = 0.90), and today it's worth 60 — "
            "below the ceiling of 80, so a violation becomes possible again and starts "
            "measuring real operations. The observed maximum stays at 80: the arithmetic "
            "ceiling didn't change, only the threshold against it.",
            "The threshold comes from `FACT_ORDER_PREMISE`, which came from the seed the "
            "GENERATOR read, whose sha256 is in the manifest of every RAW partition. In "
            "Power BI, do NOT hardcode 90 into a measure: read the `sla_minutes` column. "
            "Hardcoding creates a second copy of the number, and on the day the seed changes "
            "the panel starts measuring against a threshold no order ever knew — without "
            "failing anything, because zero against the wrong threshold looks the same as "
            "zero against the right one.",
        ),
        sql=f"""
            select
                max(sla_minutes)            as limiar_declarado_min,
                max(max_picking_minutes)    as maximo_observado_min,
                sum(orders_breaching_sla)   as violacoes,
                sum(orders_with_pick)       as pedidos_com_separacao
            from {DB}.MART.MART_FULFILLMENT_SLA
            where {FILTRO_DATA} and {FILTRO_WH}
        """,
    ),

    Indicador(
        chave="percentis_etapa",
        titulo="Time per stage (p50 / p90)",
        grupo="B. Operations",
        pergunta="How long does each stage take, at the middle and at the tail?",
        grao="(order_date, wh); median of medians when aggregated — see gotchas",
        tipo="derived",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "PERCENTILE DOESN'T SUM AND DOESN'T AVERAGE. The mart holds p50/p90 by (day, "
            "warehouse); the panel shows the AVERAGE of these percentiles when there's more "
            "than one row, and that's an approximation, not the percentile of the whole set. "
            "For the true percentile of the period you'd need order grain, which lives in "
            "`FACT_ORDER` — out of `RETAIL_READER`'s reach, by design. The column label says "
            "`media_p90` precisely so it doesn't pass itself off as the p90.",
            "The percentiles are calculated over the orders that REACHED each milestone "
            "(`percentile_cont` ignores null). A delivery p90 that counted cancelled orders "
            "as zero would measure a different company's operations. The `orders_with_*` "
            "columns say over how many orders each percentile was calculated.",
        ),
        sql=f"""
            select 1 as ordem, 'Placed -> payment' as etapa,
                   round(avg(p50_minutes_to_confirm),1) as media_p50,
                   round(avg(p90_minutes_to_confirm),1) as media_p90,
                   sum(orders_with_confirm)             as pedidos
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 2, 'Picking (start -> end)', round(avg(p50_minutes_to_pick),1),
                   round(avg(p90_minutes_to_pick),1), sum(orders_with_pick)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 3, 'Picked -> dispatched', round(avg(p50_minutes_to_dispatch),1),
                   round(avg(p90_minutes_to_dispatch),1), sum(orders_with_pick)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 4, 'Dispatched -> delivered', round(avg(p50_minutes_to_deliver),1),
                   round(avg(p90_minutes_to_deliver),1), sum(orders_with_deliver)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 5, 'Total cycle (placed -> delivered)',
                   round(avg(p50_minutes_placed_to_delivered),1),
                   round(avg(p90_minutes_placed_to_delivered),1), sum(orders_with_deliver)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            order by ordem
        """,
    ),

    Indicador(
        chave="janela_entrega",
        titulo="Delivery window: before, within, after",
        grupo="B. Operations",
        pergunta="Did the delivery happen within the window promised to the customer?",
        grao="(order_date, wh) aggregated; one row per outcome",
        tipo="derived",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "ARRIVING EARLY AND ARRIVING LATE ARE OPPOSITE PROBLEMS, and a single 'adherence' "
            "rate erases which one is happening. Always publish the direction: low adherence "
            "invites the conclusion 'operations are running late', and this project has "
            "already measured the opposite case — BEFORE the fix below, on 2026-09-01, 84% of "
            "deliveries arrived BEFORE the window opened.",
            "THIS 84% MEASUREMENT WAS A MODEL DEFECT, not an operational one: "
            "`slot_lead_hours` drew the window's start between 2h and 24h after placement, "
            "while the sum of milestones delivered in at most 8.5h. The two assumptions were "
            "DECLARED SEPARATELY and never reconciled. Fixed in Phase 7: `slot_lead_hours_*` "
            "became DERIVED from the declared cycle in the same seed (1h to 8h, against 2h to "
            "24h), and the test `assert_order_premises_are_internally_coherent` checks the "
            "DERIVATION — never the adherence, so that no one tunes the number until the KPI "
            "looks good. After the fix: 42.5% before, 24.8% within, 32.7% after — the "
            "distortion fell to less than half and the direction of the deviation (early, not "
            "late) held.",
            "In Power BI, publish all THREE counts. If a single indicator is required, use "
            "'deliveries outside the window' with the direction detail alongside it.",
        ),
        sql=f"""
            select 1 as ordem, 'Before the window opens' as resultado,
                   sum(orders_delivered_before_slot) as entregas
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 2, 'Within the window', sum(orders_delivered_within_slot)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 3, 'After the window closes', sum(orders_delivered_after_slot)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            order by ordem
        """,
    ),

    # ================================================================= C. BASKET
    Indicador(
        chave="receita_categoria",
        titulo="Revenue by category",
        grupo="C. Basket and category",
        pergunta="Which categories account for the realized revenue?",
        grao="(order_date, wh, category_id) aggregated by category",
        tipo="mixed",
        marts=("MART_BASKET_DAILY",),
        armadilhas=(
            "`orders_touching_category` IS NOT ADDITIVE across categories: an order with milk "
            "and bread counts once in each. Adding up the 151 categories of a single day "
            "gives far more than the ~22,947 orders of that day — and it's always ~22,947, "
            "because `daily_order_rate` is fixed and there's no day-of-week effect. Lines, "
            "units and value ARE additive, because each line belongs to exactly one category. "
            "For order counts use MART_ORDER_FUNNEL, which has the right grain.",
            "`revenue_fulfilled` is what was DELIVERED; `revenue_placed` is what was ordered. "
            "The difference (`revenue_lost`) is removal plus orders never picked. Don't swap "
            "one for the other on a 'revenue' chart without saying which.",
            "THE CATEGORY IS THE ONE AT ORDER TIME, recorded in the event itself, not the one "
            "the product has today. This is the correct behavior for historical revenue, and "
            "it differs from a join against the current dimension.",
        ),
        sql=f"""
            select
                parent_category_name                as categoria_nivel1,
                category_name                       as categoria,
                sum(revenue_fulfilled)              as receita_apurada,
                sum(revenue_placed)                 as valor_pedido,
                sum(revenue_lost)                   as valor_perdido,
                sum(lines_placed)                   as linhas_pedidas,
                sum(units_fulfilled)                as unidades_entregues,
                count(distinct order_date)          as dias,
                max(currency)                       as moeda
            from {DB}.MART.MART_BASKET_DAILY
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1, 2
            order by receita_apurada desc nulls last
        """,
    ),

    Indicador(
        chave="substituicao_categoria",
        titulo="Substitution and removal, by category",
        grupo="C. Basket and category",
        pergunta="Where does the basket change the most between order and delivery?",
        grao="(order_date, wh, category_id) aggregated by category",
        tipo="derived from a declared assumption (synthetic)",
        marts=("MART_BASKET_DAILY",),
        armadilhas=(
            "THE RATES ARE AN ASSUMPTION, NOT AN OBSERVATION. `substitution_rate` (0.04) and "
            "`removal_rate` (0.02) were DECLARED in the generator's seed; no source in this "
            "repository measures availability. The variation across categories is sampling "
            "noise over a constant rate, not an assortment signal. A panel that ranks "
            "categories by 'stockout risk' with this data is making it up.",
            "`unavailable` is the recorded reason, and NOT `out_of_stock`: no stock fact "
            "exists on this platform. The vocabulary is deliberate — naming it as stock would "
            "promise data nobody measured.",
            "Recalculate the rate from the SUMS (lines_substituted / lines_placed). Averaging "
            "the rates by day-warehouse-category weighs each cell equally, regardless of "
            "size — the classic Simpson's paradox in a panel.",
        ),
        sql=f"""
            select
                category_name                                       as categoria,
                sum(lines_placed)                                   as linhas_pedidas,
                sum(lines_substituted)                              as linhas_substituidas,
                sum(lines_removed)                                  as linhas_removidas,
                sum(lines_never_picked)                             as linhas_nunca_separadas,
                round(sum(lines_substituted)/nullif(sum(lines_placed),0), 4) as taxa_substituicao,
                round(sum(lines_removed)    /nullif(sum(lines_placed),0), 4) as taxa_remocao
            from {DB}.MART.MART_BASKET_DAILY
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1
            having sum(lines_placed) >= 100
            order by taxa_substituicao desc
        """,
    ),

    Indicador(
        chave="perfil_por_faixa",
        titulo="Consumption profile by buyer age band",
        grupo="C. Basket and category",
        pergunta="What does each age band buy, and where does it differ most from the others?",
        grao="(order_date, wh, buyer_age_band, demand_group) aggregated by band and group",
        tipo="synthetic calibrated against benchmark (MAPA 2025)",
        marts=("MART_DEMAND_COHORT",),
        armadilhas=(
            "THE AGGREGATE DOESN'T CHANGE ACROSS BANDS, ON PURPOSE. Cohort calibration is "
            "neutral in the total — an IPF guarantees that the weighted average of the "
            "per-cohort weights reproduces the aggregate mix. Looking for this layer's effect "
            "in a total finds nothing; it's entirely in the comparison BETWEEN bands of the "
            "same row.",
            "COMPARE SHARE, NEVER COUNT. The four bands have different sizes in the base "
            "(35_49 is the largest, LT35 the smallest), so 'lines per band' measures the "
            "cohort's size, not its propensity. `share_within_band` already has the cohort "
            "itself in the denominator; that's what isolates the two things.",
            "THE PROPENSITY IS A BENCHMARK, NOT AN OBSERVATION OF THIS STORE. The indices "
            "come from Spanish household consumption measured by MAPA, and the `% Poblacion` "
            "there is the population that LIVES IN HOUSEHOLDS with a head of household in "
            "that band — not the population of that age. That's why the number enters as a "
            "relative index, and never as an absolute share.",
            "NO_FOOD and SIN_BENCHMARK appear with a ratio close to 1 by CONSTRUCTION: the "
            "report doesn't measure drugstore or cleaning items, their index is neutral and "
            "each block's share is held constant across cohorts. Reading this as 'every age "
            "buys shampoo the same' would turn an absence of measurement into a measurement.",
        ),
        sql=f"""
            with por_faixa as (
                select
                    buyer_age_band,
                    demand_group,
                    sum(lines_placed)                               as linhas
                from {DB}.MART.MART_DEMAND_COHORT
                where {FILTRO_DATA} and {FILTRO_WH}
                group by 1, 2
            ),
            total as (
                select buyer_age_band, sum(linhas) as linhas_faixa
                from por_faixa group by 1
            )
            select
                p.demand_group                                      as grupo,
                max(case when p.buyer_age_band = 'LT35'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_lt35,
                max(case when p.buyer_age_band = '35_49'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_35_49,
                max(case when p.buyer_age_band = '50_64'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_50_64,
                max(case when p.buyer_age_band = 'GE65'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_ge65,
                sum(p.linhas)                                       as linhas_total
            from por_faixa p
            join total t on t.buyer_age_band = p.buyer_age_band
            group by 1
            having sum(p.linhas) >= 100
            order by div0(
                max(case when p.buyer_age_band = 'GE65'
                         then p.linhas / t.linhas_faixa end),
                max(case when p.buyer_age_band = 'LT35'
                         then p.linhas / t.linhas_faixa end)
            ) desc
        """,
    ),

    Indicador(
        chave="pedidos_por_regiao",
        titulo="Orders by warehouse, and the regional intensity that separates them",
        grupo="C. Basket and category",
        pergunta="Why does bcn1 place more orders than mad1, if the customer bases are the same size?",
        grao="(order_date, wh) aggregated by warehouse",
        tipo="synthetic tilted by observed per-capita consumption (MAPA, section 3)",
        marts=("MART_DEMAND_COHORT",),
        armadilhas=(
            "THE DIFFERENCE IS DELIBERATE AND OBSERVED. Until the previous phase the four "
            "warehouses had the same count by construction. The report measures per-capita "
            "consumption by autonomous community — Catalonia 620.82 kg-L per person per year "
            "against 505.86 for Madrid — and that ratio now weighs HOW MANY customers order.",
            "INTENSITY BECOMES FREQUENCY, AND THAT IS A DECLARED CHOICE. The report gives kg "
            "per year and does NOT publish household purchase frequency; splitting the "
            "intensity between frequency and basket size would mean inventing the split. "
            "Reading these numbers as 'Catalans buy more often' reads the assumption, not a "
            "measurement.",
            "THE WINDOW TOTAL DOESN'T CHANGE because of this tilt: the index is renormalized "
            "over the four communities served. What it moves is the SPLIT between warehouses, "
            "never the sum.",
        ),
        sql=f"""
            select
                wh                                                  as armazem,
                count(distinct order_date)                          as dias,
                sum(lines_placed)                                   as linhas,
                sum(units_placed)                                   as unidades,
                round(sum(revenue_fulfilled), 2)                    as receita,
                max(currency)                                       as moeda
            from {DB}.MART.MART_DEMAND_COHORT
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1
            order by linhas desc
        """,
    ),

    # ================================================= D. ASSORTMENT AND PRICE
    Indicador(
        chave="sortimento_armazem",
        titulo="Assortment by warehouse",
        grupo="D. Assortment and price",
        pergunta="How much catalog does each warehouse have, and how much of it is exclusive to it?",
        grao="(snapshot_date, wh, category_id) aggregated by warehouse",
        tipo="observed",
        marts=("MART_ASSORTMENT_DAILY",),
        armadilhas=(
            "ASSORTMENT IS THE PRESENCE OF THE LINE in the price fact, not a table of its "
            "own. A row states 'this product was in this warehouse's catalog on this day'. "
            "Be careful reading absence: it can be a product out of the catalog OR a day not "
            "observed — the days 2026-08-17 through 08-23 don't exist and cannot be "
            "recovered, because the API only serves today's price.",
            "`products_in_all_warehouses` is the honest denominator for any comparison "
            "between warehouses: comparing average price across different catalogs measures "
            "the CATALOG difference, not a price difference.",
            "THIS MART'S WINDOW IS LARGER than the orders' window (catalog since 2026-08-15, "
            "orders since 08-24). Crossing the two without aligning the date compares "
            "different periods.",
        ),
        sql=f"""
            select
                wh                                                  as armazem,
                count(distinct snapshot_date)                       as dias_observados,
                round(avg(produtos_no_dia), 0)                      as produtos_media_dia,
                max(produtos_no_dia)                                as produtos_maximo_dia,
                round(avg(exclusivos_no_dia), 0)                    as exclusivos_media_dia,
                round(avg(preco_medio_dia), 4)                      as preco_medio,
                sum(novidades)                                      as novidades_periodo
            from (
                select snapshot_date, wh,
                       sum(products)                as produtos_no_dia,
                       sum(products_exclusive_here) as exclusivos_no_dia,
                       avg(avg_unit_price)          as preco_medio_dia,
                       sum(new_arrivals)            as novidades
                from {DB}.MART.MART_ASSORTMENT_DAILY
                where {FILTRO_DATA_SNAP} and {FILTRO_WH}
                group by 1, 2
            )
            group by 1
            order by 1
        """,
    ),

    Indicador(
        chave="variacao_preco",
        titulo="Largest price changes",
        grupo="D. Assortment and price",
        pergunta="Which products changed price, and between which observed days?",
        grao="(snapshot_date, wh, source_product_id)",
        tipo="observed",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "`days_since_previous_snapshot` IS MANDATORY READING. There are measured gaps of "
            "up to 8 days in the catalog; comparing an 8-day change with a 1-day one without "
            "this column treats the two as the same fact. It travels with the query on "
            "purpose.",
            "`identity_ambiguous` flags a new id whose name already existed in the previous "
            "partition. The source doesn't say whether it's the same item re-keyed or one "
            "item withdrawn and another launched — the dimension carries the flag so the "
            "choice is visible instead of inherited.",
            "This mart has 112 thousand rows and is the only one in the panel that needs "
            "filtering in SQL rather than in memory.",
        ),
        sql=f"""
            select
                snapshot_date, wh, display_name as produto, category_name as categoria,
                previous_unit_price as preco_anterior, unit_price as preco,
                price_delta as variacao, price_delta_pct as variacao_pct,
                days_since_previous_snapshot as dias_desde_o_anterior,
                change_type as tipo_de_mudanca,
                identity_ambiguous as identidade_ambigua
            from {DB}.MART.MART_PRICE_EVOLUTION
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
              and price_delta is not null and price_delta <> 0
            order by abs(price_delta) desc
            limit 200
        """,
    ),

    Indicador(
        chave="movimento_catalogo",
        titulo="Catalog movement",
        grupo="D. Assortment and price",
        pergunta="How many products entered, left, changed price, or stayed stable?",
        grao="(snapshot_date, wh, source_product_id) aggregated by type",
        tipo="observed",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "'Left' means ABSENT FROM THE NEXT OBSERVED SNAPSHOT, not discontinued. With a "
            "7-day gap in the series, the distinction matters: the product may have come back "
            "on a day nobody looked.",
        ),
        sql=f"""
            select change_type as tipo_de_mudanca, count(*) as produtos,
                   count(distinct source_product_id) as produtos_distintos,
                   count(distinct snapshot_date) as dias
            from {DB}.MART.MART_PRICE_EVOLUTION
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1 order by produtos desc
        """,
    ),

    # ================================================= E. SUPPLY x DEMAND
    Indicador(
        chave="oferta_demanda",
        titulo="Supply x demand, at the same grain",
        grupo="E. Supply x demand",
        pergunta="Of the catalog available in each category, how much was actually ordered?",
        grao="(order_date, wh, category_id) — the grain COMMON to both marts",
        tipo="mixed: observed supply, synthetic demand",
        marts=("MART_ASSORTMENT_DAILY", "MART_BASKET_DAILY"),
        armadilhas=(
            "THIS IS THE ONLY CROSS THE TWO MARTS ALLOW WITHOUT REAGGREGATION, and that's why "
            "both have grain (date, wh, category_id). The join is by equality on the three "
            "columns; any other level requires aggregating first and the result becomes "
            "dependent on the order of operations.",
            "DEMAND IS SYNTHETIC, AT TWO LEVELS THAT ARE NOT EQUAL. The demand GROUP (the "
            "high-level category) is drawn with WEIGHT CALIBRATED against MAPA 2025 since "
            "Phase 4 — it's not uniform, and this calibration is what this panel validates in "
            "`docs/demand-evidence/`. Within the group, the PRODUCT is still drawn UNIFORMLY, "
            "because no source in this repository measures turnover per SKU. Declared "
            "consequence: WITHIN a group, the mix MIRRORS THE SIZE OF THE ASSORTMENT; BETWEEN "
            "groups, the mix follows MAPA's weight. Reading 'demand coverage' as "
            "individual-product customer preference reads the assumption back — but at the "
            "aggregate category level, a real part of the signal comes from the benchmark.",
            "An `inner join` hides the category with supply and no demand, which is exactly "
            "the interesting case. The `left join` starting from supply preserves the zero.",
        ),
        sql=f"""
            select
                o.category_name                                     as categoria,
                sum(o.products)                                     as produtos_ofertados,
                sum(coalesce(d.distinct_products_ordered, 0))        as produtos_pedidos,
                round(sum(coalesce(d.distinct_products_ordered, 0))
                      / nullif(sum(o.products), 0), 4)              as cobertura_demanda,
                sum(coalesce(d.lines_placed, 0))                    as linhas_pedidas,
                sum(coalesce(d.revenue_fulfilled, 0))               as receita_apurada
            from {DB}.MART.MART_ASSORTMENT_DAILY o
            left join {DB}.MART.MART_BASKET_DAILY d
                   on  d.order_date  = o.snapshot_date
                  and  d.wh          = o.wh
                  and  d.category_id = o.category_id
            where o.snapshot_date between %(inicio)s and %(fim)s
              and array_contains(o.wh::variant, split(%(armazens)s, ','))
            group by 1
            order by produtos_ofertados desc
        """,
    ),

    # ================================================= F. BASE AND COVERAGE
    Indicador(
        chave="base_clientes",
        titulo="Customer base",
        grupo="F. Base and coverage",
        pergunta="How is the customer base distributed by warehouse, age band and sex?",
        grao="customer_id (current version)",
        tipo="SYNTHETIC (the person) over observed (the address)",
        marts=("MART_CUSTOMER_BASE",),
        datado=False,
        armadilhas=(
            "THE PERSON IS INVENTED; WHERE THEY LIVE IS NOT. Municipality, street, postal "
            "code and house-number range always come from a real Callejero row. Distribution "
            "by age and sex is a generator assumption, not demographics — there is nothing to "
            "conclude from it about the Spanish market.",
            "`age_at_ingestion` is the age the generator drew, not an age calculated against "
            "today. Calculating it against the current date would make the indicator change "
            "on its own on every birthday, with no new observation.",
            "NO DATE AXIS: this mart holds the CURRENT version of each customer "
            "(`where is_current`). The panel's period filters don't apply to it.",
        ),
        sql=f"""
            select wh as armazem, age_band as faixa_etaria, sex_label as sexo,
                   count(*) as clientes,
                   round(avg(age_at_ingestion), 1) as idade_media,
                   count(distinct municipality_code) as municipios,
                   count(distinct postal_code) as ceps
            from {DB}.MART.MART_CUSTOMER_BASE
            group by 1, 2, 3
            order by 1, 2, 3
        """,
    ),

    Indicador(
        chave="cobertura_municipal",
        titulo="Municipal coverage",
        grupo="F. Base and coverage",
        pergunta="Which municipalities in the service area have customers, and which don't?",
        grao="(wh, province_code, municipality_code) — all 370 in the AUF",
        tipo="mixed: synthetic numerator, observed denominator",
        marts=("MART_MARKET_COVERAGE",),
        datado=False,
        armadilhas=(
            "`customers_per_10k_inhabitants` HAS A SYNTHETIC NUMERATOR AND AN OBSERVED "
            "DENOMINATOR. It serves to compare SIMULATION DENSITY between municipalities — "
            "never as a market-penetration estimate. It's the easiest indicator in the whole "
            "panel to quote out of context.",
            "The mart starts from the 370 municipalities of the AUF, not from the customers, "
            "and that's what makes it worthwhile: starting from customers would show 100% "
            "coverage by construction, always. `has_no_customers = true` is a legitimate and "
            "informative result.",
        ),
        sql=f"""
            select wh as armazem, province_name as provincia,
                   count(*) as municipios_na_auf,
                   count_if(has_no_customers) as municipios_sem_cliente,
                   sum(customers) as clientes,
                   sum(municipality_population) as populacao_auf,
                   round(sum(customers) / nullif(sum(municipality_population), 0) * 10000, 2)
                       as clientes_por_10k
            from {DB}.MART.MART_MARKET_COVERAGE
            group by 1, 2
            order by 1
        """,
    ),

    # ======================================================= G. STOCK AND REPLENISHMENT
    #
    # THIS ENTIRE GROUP WAS "OUT OF SCOPE" until Phase 7, with the declared trigger
    # "a stock balance or movement source". The trigger was NOT fulfilled — there still
    # is no stock source. What changed is that the balance became CALCULATED, by
    # a Spark job, from consumption observed in orders plus a declared policy.
    #
    # That's why `tipo` says "derived: observed consumption, synthetic policy" in all
    # four, and not "observed". Reading stockout here as a field measurement would be the
    # same error as reading simulation density as market penetration.

    Indicador(
        chave="cobertura_estoque",
        titulo="Stock coverage by category",
        grupo="G. Stock and replenishment",
        pergunta="How many days of demand does each category's stock still cover?",
        grao="(stock_date, wh, category_id) — one row per level-2 category, per day",
        tipo="derived: observed consumption, synthetic policy",
        marts=("MART_STOCK_HEALTH",),
        armadilhas=(
            "BALANCE DOESN'T ADD UP ACROSS DAYS. `closing_units` from Monday plus Tuesday's "
            "is not the week's stock — it's the same stock counted twice. Summing across "
            "PRODUCTS within the day is correct; summing along a time axis never is. It's the "
            "first thing a BI tool will try.",
            "THE TWO COVERAGE MEASURES ANSWER DIFFERENT QUESTIONS and the panel publishes "
            "both on purpose. `days_of_cover` is the ratio of sums: how many days the "
            "CATEGORY's stock covers its demand. `days_of_cover_typical_product` is the "
            "average of the ratios: how many days the TYPICAL product covers. The second is "
            "always larger, because a low-turnover product has enormous coverage and "
            "dominates the average.",
            "COVERAGE IS A RATIO, SO IT ISN'T AVERAGED again. Taking `avg(days_of_cover)` "
            "over categories produces the average of an average and doesn't correspond to "
            "any real stock.",
            "THE GROUPING KEY IS `category_id`, NEVER `category_name`. The catalog has real "
            "homonyms — Pizzas and Marisco are each TWO distinct category_id — and grouping "
            "by name silently merges the two categories into a single row, in all four "
            "indicators of this group. `category_id` travels with every query because of "
            "this.",
        ),
        sql=f"""
            select stock_date as dia, wh as armazem, category_id,
                   category_name as categoria,
                   sum(closing_units) as unidades_em_estoque,
                   -- Leio a coluna PRONTA do mart, nao recalculo. Recalcular aqui com
                   -- units_demanded (demanda do DIA) em vez de mean_daily_demand (o
                   -- denominador que o mart usa) já divergiu do mart em ate 7x num dia
                   -- de pico. group by ja inclui category_id, entao o grupo e uma
                   -- linha so do mart e any_value() so repete o valor dela.
                   any_value(days_of_cover) as dias_de_cobertura,
                   round(avg(days_of_cover_typical_product), 2)
                       as cobertura_do_produto_tipico,
                   sum(product_days) as pares_produto_dia
            from {DB}.MART.MART_STOCK_HEALTH
            where {FILTRO_DATA_STOCK} and {FILTRO_WH}
            group by 1, 2, 3, 4
            order by dias_de_cobertura asc nulls last
        """,
    ),

    Indicador(
        chave="ruptura_estoque",
        titulo="Stockout: units and series affected",
        grupo="G. Stock and replenishment",
        pergunta="How much of what demand asked for did the shelf not have, and in how many products?",
        grao="(stock_date, wh, category_id)",
        tipo="derived: observed consumption, synthetic policy",
        marts=("MART_STOCK_HEALTH",),
        armadilhas=(
            "THE TWO UNITS DON'T SUBSTITUTE FOR EACH OTHER. `units_short` says HOW MUCH was "
            "missing; `series_with_shortfall` says in how many (product, day) something was "
            "missing. A popular product short 500 units and 500 products short 1 each are "
            "different operational problems with the same `units_short`. Publish both.",
            "`fill_rate` IS NULL WHEN THERE WAS NO DEMAND, not 1. A category with no orders "
            "that day didn't have 100% fulfillment — it had no order. A BI that converts that "
            "null into 1 inflates the fulfillment average with days when nothing happened.",
            "THIS STOCKOUT IS INDEPENDENT OF THE ORDER'S `unavailable` LINES. The generator "
            "removes lines at a FIXED drawn rate, without looking at balance; this ledger "
            "computes shortage from the balance. One doesn't cause the other, and crossing "
            "them as if they did would produce an invented correlation. Trigger to unify "
            "them: a second-pass generator that rereads the previous day's balance.",
        ),
        sql=f"""
            select stock_date as dia, wh as armazem, category_id,
                   category_name as categoria,
                   sum(units_demanded) as unidades_pedidas,
                   sum(units_fulfilled) as unidades_atendidas,
                   sum(units_short) as unidades_em_falta,
                   sum(series_with_shortfall) as produtos_com_falta,
                   sum(product_days) as produtos_no_dia,
                   round(sum(units_fulfilled) / nullif(sum(units_demanded), 0), 4)
                       as taxa_de_atendimento
            from {DB}.MART.MART_STOCK_HEALTH
            where {FILTRO_DATA_STOCK} and {FILTRO_WH}
            group by 1, 2, 3, 4
            having sum(units_demanded) > 0
            order by unidades_em_falta desc
        """,
    ),

    Indicador(
        chave="reposicao_estoque",
        titulo="Replenishment: orders triggered",
        grupo="G. Stock and replenishment",
        pergunta="How many purchase orders did the policy trigger, and for how many units?",
        grao="(stock_date, wh, category_id)",
        tipo="derived: observed consumption, synthetic policy",
        marts=("MART_STOCK_HEALTH",),
        armadilhas=(
            "AN ORDER ISSUED IS NOT AN ORDER ARRIVED. It arrives `supplier_lead_days` days "
            "later, and an order issued near the end of the window NEVER shows up as arrived "
            "— an order in transit at the end of the period is a real property of any ledger. "
            "Comparing orders with arrivals in the same period is the most likely wrong "
            "reading of this indicator.",
            "THE POLICY IS SYNTHETIC AND IT IS DECLARED. `reorder_point_days`, "
            "`reorder_target_days` and `supplier_lead_days` come from a seed, not from "
            "negotiation with any supplier. The number of orders is a direct consequence of "
            "them.",
            "ONE OPEN ORDER AT A TIME, by classic min-max policy. Without this guard a "
            "product in stockout would issue one order per day while the first was still on "
            "its way, and the cascading arrival would produce a stock spike no real operation "
            "would ever have.",
        ),
        sql=f"""
            select stock_date as dia, wh as armazem, category_id,
                   category_name as categoria,
                   sum(replenishment_orders) as ordens_emitidas,
                   sum(reorder_units) as unidades_pedidas_ao_fornecedor,
                   sum(product_days) as produtos_no_dia,
                   round(sum(replenishment_orders) / nullif(sum(product_days), 0), 4)
                       as fracao_de_produtos_repondo
            from {DB}.MART.MART_STOCK_HEALTH
            where {FILTRO_DATA_STOCK} and {FILTRO_WH}
            group by 1, 2, 3, 4
            having sum(replenishment_orders) > 0
            order by ordens_emitidas desc
        """,
    ),

    Indicador(
        chave="giro_estoque",
        titulo="Daily turnover by category",
        grupo="G. Stock and replenishment",
        pergunta="How many times per day does each category's stock turn over?",
        grao="(stock_date, wh, category_id)",
        tipo="derived: observed consumption, synthetic policy",
        marts=("MART_STOCK_HEALTH",),
        armadilhas=(
            "THIS IS NOT ANNUALIZED TURNOVER, so don't multiply by 365. The window has few "
            "days, and annualizing would project a seasonal behavior nobody observed — the "
            "generator itself has no day-of-week effect, because `daily_order_rate` is fixed.",
            "THE DENOMINATOR IS THE DAY'S AVERAGE BALANCE (opening + closing) / 2, not the "
            "closing balance. Using the closing balance makes turnover explode to infinity on "
            "the day the shelf hits zero — which is exactly the most interesting day.",
            "HIGH TURNOVER IS NOT GOOD ON ITS OWN. It rises both when demand grows and when "
            "stock shrinks; read it alongside coverage and stockout, or else a nearly empty "
            "shelf looks like efficiency.",
        ),
        sql=f"""
            select stock_date as dia, wh as armazem, category_id,
                   category_name as categoria,
                   round(avg(turnover_daily), 4) as giro_diario,
                   sum(units_fulfilled) as unidades_vendidas,
                   sum(opening_units) as saldo_abertura,
                   sum(closing_units) as saldo_fechamento,
                   sum(units_short) as unidades_em_falta
            from {DB}.MART.MART_STOCK_HEALTH
            where {FILTRO_DATA_STOCK} and {FILTRO_WH}
            group by 1, 2, 3, 4
            order by giro_diario desc nulls last
        """,
    ),
)

# ------------------------------------------------------------------------------------
# Freshness: not a business indicator, it's what makes the update VISIBLE.
# ------------------------------------------------------------------------------------
# Without this, a new load comes in and the panel changes the numbers without saying the
# base changed. With this, each mart's count and window appear on screen and the
# before/after comparison of a load is direct.
FRESCOR = f"""
    select 'MART_ORDER_FUNNEL' as mart, count(*) as linhas,
           min(order_date)::varchar as inicio, max(order_date)::varchar as fim
      from {DB}.MART.MART_ORDER_FUNNEL
    union all select 'MART_FULFILLMENT_SLA', count(*), min(order_date)::varchar, max(order_date)::varchar
      from {DB}.MART.MART_FULFILLMENT_SLA
    union all select 'MART_BASKET_DAILY', count(*), min(order_date)::varchar, max(order_date)::varchar
      from {DB}.MART.MART_BASKET_DAILY
    union all select 'MART_ASSORTMENT_DAILY', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {DB}.MART.MART_ASSORTMENT_DAILY
    union all select 'MART_PRICE_EVOLUTION', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {DB}.MART.MART_PRICE_EVOLUTION
    union all select 'MART_CUSTOMER_BASE', count(*), null, null
      from {DB}.MART.MART_CUSTOMER_BASE
    union all select 'MART_MARKET_COVERAGE', count(*), null, null
      from {DB}.MART.MART_MARKET_COVERAGE
    union all select 'MART_DEMAND_COHORT', count(*), min(order_date)::varchar, max(order_date)::varchar
      from {DB}.MART.MART_DEMAND_COHORT
    union all select 'MART_STOCK_HEALTH', count(*), min(stock_date)::varchar, max(stock_date)::varchar
      from {DB}.MART.MART_STOCK_HEALTH
    order by mart
"""

# Date axis limits, so the selector doesn't offer an empty period.
JANELA = f"""
    select min(inicio)::varchar as inicio, max(fim)::varchar as fim from (
        select min(order_date) as inicio, max(order_date) as fim from {DB}.MART.MART_ORDER_FUNNEL
        union all
        select min(snapshot_date), max(snapshot_date) from {DB}.MART.MART_ASSORTMENT_DAILY
        union all
        select min(stock_date), max(stock_date) from {DB}.MART.MART_STOCK_HEALTH
    )
"""

ARMAZENS = f"select distinct wh from {DB}.MART.MART_ORDER_FUNNEL order by wh"


# ------------------------------------------------------------------------------------
# What CANNOT be shown, and why. Goes into the CONTRACT and the interface.
# ------------------------------------------------------------------------------------
# A list of declared absences is worth more than an invented indicator. Each item states
# the TRIGGER that would unlock it, so the conversation is about what's missing and not
# about what could be approximated.
FORA_DE_ALCANCE: tuple[tuple[str, str, str], ...] = (
    ("Margin, profit, COGS",
     "No source in this repository has cost. The Mercadona API exposes sale price, "
     "never acquisition cost.",
     "A per-product cost source. Without it, any margin is invented."),

    ("OBSERVED stock, and the link between stockout and the unavailable line",
     "Group G has published balance, stockout, turnover and coverage since Phase 7 — but "
     "they are CALCULATED, never observed: a Spark job derives the balance from consumption "
     "measured in orders plus a policy declared in a seed. There still is no stock source in "
     "this repository, and that is written in the Mercadona source's CONTRACT.\n\n"
     "The concrete consequence, which matters when reading group G: the ledger's stockout is "
     "INDEPENDENT of the order's removed lines. The generator removes a line at a FIXED drawn "
     "rate, with reason `unavailable` and not `out_of_stock` precisely because there was no "
     "balance when it was written. One doesn't cause the other, and crossing the two would "
     "produce an invented correlation.",
     "For real balance: a stock or movement source. To link the two stockouts without a new "
     "source: a SECOND-PASS generator, that rereads the previous day's balance before deciding "
     "removal. That would invert the current dependency (today orders generate stock) and "
     "create a cycle between the two domains — it isn't cheap, and that's why it's declared "
     "here instead of approximated."),

    ("Repeat purchase, LTV, cohort, revenue per customer, RFM",
     "NO MART JOINS CUSTOMER WITH ORDER. `MART_CUSTOMER_BASE` has customer without order; "
     "`MART_ORDER_FUNNEL` and `MART_BASKET_DAILY` have aggregated order without customer. The "
     "link exists in `FACT_ORDER.customer_sk`, in GOLD — which `RETAIL_READER` doesn't reach, "
     "by design.",
     "A new mart with customer grain and order measures (candidate: MART_CUSTOMER_ORDERS). "
     "It's the most actionable gap on this list, and doesn't require a new source — just "
     "modeling."),

    ("Route, travel time, distance, delivery optimization",
     "Hard block, already on record: the Callejero has no coordinates or adjacency. What "
     "gets modeled is a delivery WINDOW — a commercial promise on a fixed grid — never a "
     "route.",
     "Geocoding. Deliberately refused: it would invent position."),

    ("Market penetration, share, potential by municipality",
     "The customers are SYNTHETIC. `customers_per_10k_inhabitants` has a synthetic numerator "
     "over an observed INE denominator: it measures SIMULATION density, and quoted out of "
     "context it looks like market share.",
     "A real customer base. Outside the project's declared scope."),

    ("Trend, seasonality, weekly or monthly comparison, YoY",
     "The orders window has 9 days (2026-08-24 to 09-01) and the catalog one has 8 observed "
     "days in the same interval, with a 7-day gap that CANNOT be recovered — the API only "
     "serves today's price. Nine points already support a daily series (Commercial tab), but "
     "not a weekly cycle: `daily_order_rate` is fixed by construction, so there's no "
     "day-of-week effect to measure, and a trend over 9 days with no full month is "
     "statistically weak.",
     "Time. The series grows on its own every day the DAG runs."),

    ("True percentile of the period",
     "The mart holds p50/p90 by (day, warehouse). Percentile doesn't sum or average; the "
     "average of percentiles is an approximation and the panel labels it as such.",
     "Order grain for the BI consumer — today only in `FACT_ORDER`, out of "
     "`RETAIL_READER`'s reach."),
)
