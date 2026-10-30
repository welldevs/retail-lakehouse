"""The price-watch panel's indicators: the query AND the explanation, in the SAME place.

WHY A SEPARATE FILE FROM `operations.py`. The user asked for an EXCLUSIVE panel — one
screen whose only job is price oscillation, not a fourteenth tab bolted onto the
operations dashboard. It reuses `connection.py` as-is (the session/role/query plumbing has
no opinion about which indicators run over it) and imports `Indicator`, `MART` and the two
bound filters it shares verbatim with `operations.py` (all from `model.py`) — not a second implementation of the
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

PARAMETERS. `%(start)s`/`%(end)s` bind `snapshot_date` (see `SNAPSHOT_DATE_FILTER`, imported
from `model.py` — the exact same string, not a copy that can drift), `%(warehouses)s`
binds the warehouse multiselect (`WAREHOUSE_FILTER`, same source), and `%(term)s` binds the
product search box via `PRODUCT_FILTER` below — `ilike` with the wildcard living inside the
bound VALUE (`f"%{termo}%"`, built in `streamlit/prices_app.py`), never inside the SQL text.
"""

from __future__ import annotations

from .model import MART, SNAPSHOT_DATE_FILTER, WAREHOUSE_FILTER, Indicator

# Bound, like SNAPSHOT_DATE_FILTER/WAREHOUSE_FILTER above: the `%` wildcards travel inside the
# PARAMETER value the caller supplies, never spliced into this string.
PRODUCT_FILTER = "display_name ilike %(term)s"


INDICATORS: tuple[Indicator, ...] = (

    # ================================================================= A. Oscillations
    Indicator(
        key="resumo_precos",
        title="Price-watch summary",
        group="A. Oscillations",
        question="How many prices moved in the selected window, and by how much on average?",
        grain="(snapshot_date, wh, source_product_id) aggregated for the period total",
        kind="observed (diffed from consecutive existing snapshots, never calendar days)",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
        """,
    ),

    Indicator(
        key="maiores_quedas",
        title="Biggest observed drops (the closest thing to an \"offer\" this data has)",
        group="A. Oscillations",
        question="Which products dropped the most, in percentage terms, in the selected window?",
        grain="(snapshot_date, wh, source_product_id) — one row per price transition",
        kind="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
              and change_type = 'preco_alterado' and purchasable_price_delta < 0
            order by purchasable_price_delta_pct asc
            limit 200
        """,
    ),

    Indicator(
        key="maiores_altas",
        title="Biggest observed increases",
        group="A. Oscillations",
        question="Which products got more expensive, in percentage terms, in the selected window?",
        grain="(snapshot_date, wh, source_product_id) — one row per price transition",
        kind="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
              and change_type = 'preco_alterado' and purchasable_price_delta > 0
            order by purchasable_price_delta_pct desc
            limit 200
        """,
    ),

    Indicator(
        key="movimentacao_diaria",
        title="Daily change volume",
        group="A. Oscillations",
        question="Is price movement steady across the window, or concentrated on a few days?",
        grain="(snapshot_date, wh) aggregated; one row per day",
        kind="observed",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
            group by 1, 2
            order by 1, 2
        """,
    ),

    # ================================================================= B. By category
    Indicator(
        key="volatilidade_por_categoria",
        title="Volatility by category",
        group="B. By category",
        question="Which categories move the most, relative to how many products they carry?",
        grain="(category) aggregated over the period and selected warehouses",
        kind="observed, derived ratio",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "`taxa_de_mudanca` divides changed transitions by TOTAL transitions observed in "
            "that category (changed + stable + new), not by product count — a category "
            "sampled on more days naturally accumulates more transitions. It answers \"how "
            "often does a look at this category show something different\", not \"what "
            "fraction of its catalog changed\".",
            "`oscilacao_media_pct` is built on `purchasable_price_delta_pct`, and so is "
            "`change_type` itself — never on the source's raw `unit_price`, which is a "
            "selector ceiling (not a real price) for a small number of weight-sold products.",
            "A category needs at least 10 price changes in the period to be ranked: ordered "
            "by average swing, `Carne` (Congelados) topped the list on 2026-10-30 with ONE "
            "change in 43 transitions. One observation is an anecdote, not volatility.",
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
            group by 1, 2
            having count(*) filter (where change_type = 'preco_alterado') >= 10
            order by oscilacao_media_pct desc nulls last
        """,
    ),

    Indicator(
        key="preco_por_categoria",
        title="Price levels by category",
        group="B. By category",
        question="What does a typical product in each category cost, and how wide is the range?",
        grain="(snapshot_date, wh, category) — the mart's own grain, aggregated over the period",
        kind="observed",
        marts=("MART_ASSORTMENT_DAILY",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
            group by 1, 2
            order by preco_medio desc nulls last
        """,
    ),

    # ================================================================= C. Product lookup
    Indicator(
        key="produtos_buscaveis",
        title="Product search",
        group="C. Product lookup",
        question="Which products match the typed name?",
        grain="distinct product name",
        kind="observed",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "Matches by NAME, not by `source_product_id`: the same name can legitimately "
            "cover more than one id (`identity_review_needed`/`identity_ambiguous` flag "
            "exactly this). Picking a name in the search box may pull more than one distinct "
            "product's history into the chart below — that's disclosed, not hidden.",
        ),
        dated=False,
        sql=f"""
            select distinct display_name
            from {MART}.mart_price_evolution
            where {PRODUCT_FILTER}
            order by display_name
            limit 50
        """,
    ),

    Indicator(
        key="historico_preco_produto",
        title="Product price history",
        group="C. Product lookup",
        question="How has this product's price moved over time, warehouse by warehouse?",
        grain="(snapshot_date, wh, source_product_id) — every observed snapshot of the match",
        kind="observed",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
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
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER} and {PRODUCT_FILTER}
            order by display_name, wh, snapshot_date
        """,
    ),

    # ================================================================= D. Catalog movement
    Indicator(
        key="entradas_e_pacotes",
        title="New arrivals and pack changes",
        group="D. Catalog movement",
        question="How much of the catalog is genuinely new or repackaged, per day and warehouse?",
        grain="(snapshot_date, wh) aggregated across categories",
        kind="observed",
        marts=("MART_ASSORTMENT_DAILY",),
        caveats=(
            "`is_new_arrival` and `is_pack` are source flags carried straight through from "
            "`fact_price_snapshot`, scoped out of this panel's own verification on purpose: "
            "unlike the source's `price_decreased` flag (measured false on 100% of rows and "
            "replaced by a derived column), nothing in this project's data has yet shown "
            "`is_new_arrival` to be unreliable. Declared as unverified, not assumed correct — "
            "the same distinction the rest of this panel draws between observed and proven.",
            "`is_new_arrival` is the source's \"new\" BADGE, which stays on for weeks: about "
            "100 flagged products per warehouse per day, against 207 `entrou` rows over the "
            "whole 2026-08-24..09-01 window. Two different questions — never label the badge "
            "\"entered the catalog\".",
            "`products_exclusive_here` is null on a day only one warehouse was collected "
            "(mad1 alone on 2026-08-15 and 08-16): with nothing to compare against, every "
            "product would count as exclusive.",
        ),
        sql=f"""
            select
                snapshot_date, wh,
                sum(products)                as produtos,
                sum(new_arrivals)             as novidades,
                sum(packs)                    as pacotes,
                -- On a day only ONE warehouse was collected, every product is "exclusive"
                -- to it (mad1 on 08-15 and 08-16: 4,329 of 4,329). Null, not a number.
                -- Counted over ALL warehouses, not the filtered ones: exclusivity is
                -- against the whole network, and a one-warehouse filter must not blank it.
                case when (select count(distinct o.wh) from {MART}.mart_assortment_daily o
                           where o.snapshot_date = a.snapshot_date) > 1
                     then sum(products_exclusive_here) end as exclusivos_do_armazem
            from {MART}.mart_assortment_daily a
            where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
            group by 1, 2
            order by 1, 2
        """,
    ),
)


# ------------------------------------------------------------------------------------
# Freshness and filter bounds — not indicators, what makes the panel's own state legible.
# ------------------------------------------------------------------------------------
FRESHNESS = f"""
    select 'MART_PRICE_EVOLUTION' as mart, count(*) as linhas,
           min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
      from {MART}.mart_price_evolution
    union all select 'MART_ASSORTMENT_DAILY', count(*),
           min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {MART}.mart_assortment_daily
    order by mart
"""

WINDOW = f"""
    select min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
    from {MART}.mart_price_evolution
"""

WAREHOUSES = f"select distinct wh from {MART}.mart_price_evolution order by wh"
