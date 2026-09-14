"""The price-watch panel's indicators: the query AND the explanation, in the SAME place.

WHY A SEPARATE FILE FROM `indicators.py`. The user asked for an EXCLUSIVE panel — one
screen whose only job is price oscillation, not a fourteenth tab bolted onto the
operations dashboard. It reuses `connection.py` as-is (the session/role/query plumbing has
no opinion about which indicators run over it) and imports `Indicador`, `MART` and the two
bound filters it shares verbatim with `indicators.py` — not a second implementation of the
same constant, just the one that already exists.

WHAT THIS PANEL DOES NOT INVENT. There is no "offer"/"promotion" flag anywhere in the
warehouse. `fact_price_change.sql`'s own header records that the SOURCE's flag
(`price_decreased`) is false on 100% of rows even when 152 prices moved between two
partitions — it was proven useless, not merely unused. Every "offer" shown here is a
DERIVED proxy: a `change_type = 'preco_alterado'` row ranked by `purchasable_price_delta_pct`.
That is a real, observed price drop — it is not a verified promotional campaign, and the
gotcha on `maiores_quedas` says so.

EVERY QUERY HERE READS `purchasable_*` COLUMNS, NEVER THE RAW `unit_price`/`price_delta`.
In ~10 product x warehouse combinations sold by weight without a declared `unit_size`, the
Mercadona API returns `unit_price = reference_price * 99` — the weight-selector's ceiling,
not a price anyone pays (measured max 3,663.00 EUR for 150 g of prawns). That raw ceiling
used to leak straight into this panel's own category price-range table before this fix —
see `mart_assortment_daily.sql` and `silver_price_change.sql` for where it's corrected.

NO NEW dbt MODEL. `mart_price_evolution` already carries the pair (this snapshot, the
previous one) with the delta and the day-gap between them; `mart_assortment_daily` already
carries the daily category aggregate. Both marts already answer every question this panel
asks — adding a third mart for a screen that only reads what two marts already computed
would be the premature abstraction this project keeps refusing elsewhere.

PARAMETERS. `%(inicio)s`/`%(fim)s` bind `snapshot_date` (see `FILTRO_DATA_SNAP`, imported
from `indicators.py` — the exact same string, not a copy that can drift), `%(armazens)s`
binds the warehouse multiselect (`FILTRO_WH`, same source), and `%(termo)s` binds the
product search box via `FILTRO_PRODUTO` below — `ilike` with the wildcard living inside the
bound VALUE (`f"%{termo}%"`, built in `price_app.py`), never inside the SQL text.
"""

from __future__ import annotations

from indicators import MART, FILTRO_DATA_SNAP, FILTRO_WH, Indicador

# Bound, like FILTRO_DATA_SNAP/FILTRO_WH above: the `%` wildcards travel inside the
# PARAMETER value the caller supplies, never spliced into this string.
FILTRO_PRODUTO = "display_name ilike %(termo)s"


INDICADORES: tuple[Indicador, ...] = (

    # ================================================================= A. Oscillations
    Indicador(
        chave="resumo_precos",
        titulo="Price-watch summary",
        grupo="A. Oscillations",
        pergunta="How many prices moved in the selected window, and by how much on average?",
        grao="(snapshot_date, wh, source_product_id) aggregated for the period total",
        tipo="observed (diffed from consecutive existing snapshots, never calendar days)",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "`count(*) filter (where change_type = 'preco_alterado')` counts SNAPSHOT PAIRS, "
            "not distinct products: the same product changing price twice in the window "
            "counts twice, correctly — a product that oscillates twice DID move twice.",
            "`avg_swing_pct` averages `abs(purchasable_price_delta_pct)` only over rows that "
            "actually changed (`preco_alterado`); folding in `estavel` rows would water every "
            "number down toward zero and hide exactly the movement this panel exists to show.",
        ),
        sql=f"""
            select
                count(*) filter (where change_type = 'preco_alterado')     as mudancas,
                count(*) filter (where change_type = 'preco_alterado'
                                  and purchasable_price_delta < 0)         as quedas,
                count(*) filter (where change_type = 'preco_alterado'
                                  and purchasable_price_delta > 0)         as altas,
                count(*) filter (where change_type = 'entrou')            as entradas,
                count(distinct source_product_id)
                    filter (where change_type = 'preco_alterado')          as produtos_afetados,
                round(avg(abs(purchasable_price_delta_pct))
                      filter (where change_type = 'preco_alterado'), 2)    as oscilacao_media_pct,
                min(purchasable_price_delta_pct)
                    filter (where change_type = 'preco_alterado')          as maior_queda_pct,
                max(purchasable_price_delta_pct)
                    filter (where change_type = 'preco_alterado')          as maior_alta_pct
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
        """,
    ),

    Indicador(
        chave="maiores_quedas",
        titulo="Biggest observed drops (the closest thing to an \"offer\" this data has)",
        grupo="A. Oscillations",
        pergunta="Which products dropped the most, in percentage terms, in the selected window?",
        grao="(snapshot_date, wh, source_product_id) — one row per price transition",
        tipo="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "THIS IS A DROP, NOT A VERIFIED PROMOTION. `fact_price_change.sql` found the "
            "source's own `price_decreased` flag false on 100% of rows even when 152 prices "
            "moved — there is no promotional flag anywhere upstream. Reading this list as "
            "\"confirmed offers\" attributes a marketing intent the data never asserted.",
            "`days_since_previous_snapshot` matters more here than anywhere else: a -20% move "
            "over 8 days (the real 08-16->08-24 gap) is a different event than the same -20% "
            "in 1 day. Sort by percentage alone and the two look identical.",
            "PRICES SHOWN ARE `purchasable_unit_price`, NOT the source's raw `unit_price`. In "
            "~10 product x warehouse combinations sold by weight without a declared size, the "
            "API's raw price is a `reference_price * 99` selector ceiling (measured up to "
            "3,663.00 EUR for 150 g of prawns) — reading that as a price anyone paid, or a "
            "drop from it, would be measuring the wrong number.",
        ),
        sql=f"""
            select
                snapshot_date, wh, display_name, category_name,
                previous_purchasable_unit_price, purchasable_unit_price,
                purchasable_price_delta, purchasable_price_delta_pct,
                days_since_previous_snapshot, identity_review_needed
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
              and change_type = 'preco_alterado' and purchasable_price_delta < 0
            order by purchasable_price_delta_pct asc
            limit 200
        """,
    ),

    Indicador(
        chave="maiores_altas",
        titulo="Biggest observed increases",
        grupo="A. Oscillations",
        pergunta="Which products got more expensive, in percentage terms, in the selected window?",
        grao="(snapshot_date, wh, source_product_id) — one row per price transition",
        tipo="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "Same caveat as the drops list, mirrored: a rise spread over an 8-day gap is not "
            "the same event as one that happened in a single day. Read `days_since_previous_snapshot` "
            "before comparing two rows' percentages.",
        ),
        sql=f"""
            select
                snapshot_date, wh, display_name, category_name,
                previous_purchasable_unit_price, purchasable_unit_price,
                purchasable_price_delta, purchasable_price_delta_pct,
                days_since_previous_snapshot, identity_review_needed
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
              and change_type = 'preco_alterado' and purchasable_price_delta > 0
            order by purchasable_price_delta_pct desc
            limit 200
        """,
    ),

    Indicador(
        chave="movimentacao_diaria",
        titulo="Daily change volume",
        grupo="A. Oscillations",
        pergunta="Is price movement steady across the window, or concentrated on a few days?",
        grao="(snapshot_date, wh) aggregated; one row per day",
        tipo="observed",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "A day with zero snapshot for a warehouse doesn't produce a zero row here — it "
            "produces NO row, because the mart's grain is the snapshot that exists. A gap day "
            "(08-30 is documented as absent for all four warehouses) reads as \"nothing "
            "happened\", not as \"nothing was observed\" — the distinction FACT_INGESTION_RUN "
            "exists to carry, one layer below this mart.",
        ),
        sql=f"""
            select
                snapshot_date, wh,
                count(*) filter (where change_type = 'preco_alterado'
                                  and purchasable_price_delta < 0)          as quedas,
                count(*) filter (where change_type = 'preco_alterado'
                                  and purchasable_price_delta > 0)          as altas,
                count(*) filter (where change_type = 'entrou')             as entradas
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1, 2
            order by 1, 2
        """,
    ),

    # ================================================================= B. By category
    Indicador(
        chave="volatilidade_por_categoria",
        titulo="Volatility by category",
        grupo="B. By category",
        pergunta="Which categories move the most, relative to how many products they carry?",
        grao="(category) aggregated over the period and selected warehouses",
        tipo="observed, derived ratio",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "`taxa_de_mudanca` divides changed transitions by TOTAL transitions observed in "
            "that category (changed + stable + new), not by product count — a category "
            "sampled on more days naturally accumulates more transitions. It answers \"how "
            "often does a look at this category show something different\", not \"what "
            "fraction of its catalog changed\".",
            "`oscilacao_media_pct` is built on `purchasable_price_delta_pct`, and so is "
            "`change_type` itself — never on the source's raw `unit_price`, which is a "
            "selector ceiling (not a real price) for a small number of weight-sold products.",
        ),
        sql=f"""
            select
                category_name, parent_category_name,
                count(*)                                                     as transicoes,
                count(*) filter (where change_type = 'preco_alterado')        as mudancas,
                round(
                    (count(*) filter (where change_type = 'preco_alterado'))::numeric
                    / nullif(count(*), 0), 4
                )                                                             as taxa_de_mudanca,
                round(avg(abs(purchasable_price_delta_pct))
                      filter (where change_type = 'preco_alterado'), 2)       as oscilacao_media_pct
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1, 2
            having count(*) filter (where change_type = 'preco_alterado') > 0
            order by oscilacao_media_pct desc nulls last
        """,
    ),

    Indicador(
        chave="preco_por_categoria",
        titulo="Price levels by category",
        grupo="B. By category",
        pergunta="What does a typical product in each category cost, and how wide is the range?",
        grao="(snapshot_date, wh, category) — the mart's own grain, aggregated over the period",
        tipo="observed",
        marts=("MART_ASSORTMENT_DAILY",),
        armadilhas=(
            "`preco_medio` here averages the mart's own per-day `avg_unit_price` across the "
            "whole period — an average of averages. It answers \"roughly where does this "
            "category sit\", not \"what is the exact mean price\"; for that, go back to "
            "`fact_price_snapshot` and average every row directly.",
            "`mart_assortment_daily`'s `unit_price` columns are already `purchasable_unit_price` "
            "under the hood, not the source's raw ceiling for weight-sold items — see "
            "`mart_assortment_daily.sql`. Before that fix, this exact table showed \"Marisco y "
            "pescado\" with a max above 3,600 EUR; it's a real, plausible range now.",
        ),
        sql=f"""
            select
                category_name, parent_category_name,
                round(min(min_unit_price), 4)                as preco_minimo,
                round(max(max_unit_price), 4)                as preco_maximo,
                round(avg(avg_unit_price), 4)                as preco_medio,
                round(avg(median_unit_price)::numeric, 4)    as mediana_media
            from {MART}.mart_assortment_daily
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1, 2
            order by preco_medio desc nulls last
        """,
    ),

    # ================================================================= C. Product lookup
    Indicador(
        chave="produtos_buscaveis",
        titulo="Product search",
        grupo="C. Product lookup",
        pergunta="Which products match the typed name?",
        grao="distinct product name",
        tipo="observed",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "Matches by NAME, not by `source_product_id`: the same name can legitimately "
            "cover more than one id (`identity_review_needed`/`identity_ambiguous` flag "
            "exactly this). Picking a name in the search box may pull more than one distinct "
            "product's history into the chart below — that's disclosed, not hidden.",
        ),
        datado=False,
        sql=f"""
            select distinct display_name
            from {MART}.mart_price_evolution
            where {FILTRO_PRODUTO}
            order by display_name
            limit 50
        """,
    ),

    Indicador(
        chave="historico_preco_produto",
        titulo="Product price history",
        grupo="C. Product lookup",
        pergunta="How has this product's price moved over time, warehouse by warehouse?",
        grao="(snapshot_date, wh, source_product_id) — every observed snapshot of the match",
        tipo="observed",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "A gap in the line chart (08-16 to 08-24) is the catalog not being observed that "
            "day, not the price standing still for 8 days — `days_since_previous_snapshot` on "
            "the row right after the gap is what tells the two apart.",
            "The line plots `purchasable_unit_price`, not the source's raw `unit_price` — for "
            "the ~10 weight-sold combinations without a declared size, the raw value is a "
            "selector ceiling, not anything the chart should ever show as \"the price\".",
        ),
        sql=f"""
            select
                snapshot_date, wh, display_name, category_name,
                purchasable_unit_price, previous_purchasable_unit_price,
                purchasable_price_delta_pct, change_type,
                days_since_previous_snapshot
            from {MART}.mart_price_evolution
            where {FILTRO_DATA_SNAP} and {FILTRO_WH} and {FILTRO_PRODUTO}
            order by display_name, wh, snapshot_date
        """,
    ),

    # ================================================================= D. Catalog movement
    Indicador(
        chave="entradas_e_pacotes",
        titulo="New arrivals and pack changes",
        grupo="D. Catalog movement",
        pergunta="How much of the catalog is genuinely new or repackaged, per day and warehouse?",
        grao="(snapshot_date, wh) aggregated across categories",
        tipo="observed",
        marts=("MART_ASSORTMENT_DAILY",),
        armadilhas=(
            "`is_new_arrival` and `is_pack` are source flags carried straight through from "
            "`fact_price_snapshot`, scoped out of this panel's own verification on purpose: "
            "unlike the source's `price_decreased` flag (measured false on 100% of rows and "
            "replaced by a derived column), nothing in this project's data has yet shown "
            "`is_new_arrival` to be unreliable. Declared as unverified, not assumed correct — "
            "the same distinction the rest of this panel draws between observed and proven.",
        ),
        sql=f"""
            select
                snapshot_date, wh,
                sum(products)                as produtos,
                sum(new_arrivals)             as novidades,
                sum(packs)                    as pacotes,
                sum(products_exclusive_here)  as exclusivos_do_armazem
            from {MART}.mart_assortment_daily
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1, 2
            order by 1, 2
        """,
    ),
)


# ------------------------------------------------------------------------------------
# Freshness and filter bounds — not indicators, what makes the panel's own state legible.
# ------------------------------------------------------------------------------------
FRESCOR = f"""
    select 'MART_PRICE_EVOLUTION' as mart, count(*) as linhas,
           min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
      from {MART}.mart_price_evolution
    union all select 'MART_ASSORTMENT_DAILY', count(*),
           min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {MART}.mart_assortment_daily
    order by mart
"""

JANELA = f"""
    select min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
    from {MART}.mart_price_evolution
"""

ARMAZENS = f"select distinct wh from {MART}.mart_price_evolution order by wh"
