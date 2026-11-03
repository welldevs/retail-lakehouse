"""The price-evolution panel's indicators: how far Mercadona's prices travelled, not how often.

WHY A THIRD CATALOG AND NOT MORE TABS ON PRICE WATCH. `prices.py` answers questions about
EVENTS — this drop, that rise, how many changes on a day. None of them answers the question a
reader brings to an accumulating price history: "since August, have prices gone up, by how
much, where?". Counting events cannot answer it: a product that drops 20% for a week and comes
back is two events and zero evolution. The user asked for a separate screen with this focus
(CR-019); this module is its catalog, `streamlit/evolution_app.py` its consumer.

THE METHOD IS THE STANDARD ONE FOR PRICES WITHOUT WEIGHTS. A chained Jevons index (ILO/IMF
Consumer Price Index Manual, elementary aggregates): at each collection, the geometric mean of
`price now / price at the previous collection` over the products observed BOTH times, chained
by multiplication. In SQL the geometric mean is `exp(avg(ln(ratio)))` and the chain a running
`sum` of the logs, so the whole index is one window function. Three properties come with it:

  - entries and exits never move the index: a product only counts when it has a price on both
    sides of a link;
  - a gap of 14 days (2026-10-14 -> 10-28) is ONE link, not 14 flat days nor a step invented
    by interpolation — the chart puts the points at their real dates;
  - an offer that goes and comes back cancels out of the chain exactly (ln a + ln 1/a = 0).

WHAT IT IS NOT. Not a CPI: there are no consumption weights. The only weights in this warehouse
would come from the SIMULATED orders, and mixing them in would turn an observed index into a
synthetic one. Every product counts the same — a 0.60 EUR yogurt and a 9 EUR ham.

NO NEW dbt MODEL. `mart_price_evolution` already carries, per observation, the previous price
and the days since it (`days_since_previous_snapshot`), which is exactly a link. Every query
reads `purchasable_*`, never the raw `unit_price` (see `prices.py` for the x99 selector ceiling).

THE WINDOW RULE. A link counts only when BOTH of its ends are inside the selected period:
`snapshot_date - days_since_previous_snapshot >= %(start)s`. Without it, the first day of any
window would carry the movement of the days BEFORE it, and two windows starting on different
days would disagree about the same week.
"""

from __future__ import annotations

from .model import MART, SNAPSHOT_DATE_FILTER, WAREHOUSE_FILTER, Indicator

# A link: a price observed now AND at the previous collection of the same warehouse, both
# inside the window. Bound parameters only — the dates travel as values. A product's first
# collection (and an entry) has no previous price: it is in the data, not in a link.
INSIDE_WINDOW = ("previous_purchasable_unit_price > 0"
                 " and snapshot_date - days_since_previous_snapshot >= %(start)s::date")
LINK = "ln(purchasable_unit_price / previous_purchasable_unit_price)"

# One row per product and warehouse that was on sale at the FIRST and at the LAST collection of
# that warehouse inside the window — the products whose start and end prices can be compared.
# Shared by the three "over the period" indicators so they cannot disagree about the population.
_PRESENT_THROUGHOUT = f"""
            with obs as (
                select wh, source_product_id, display_name, parent_category_name,
                       snapshot_date, purchasable_unit_price, change_type,
                       min(snapshot_date) over (partition by wh) as first_of_wh,
                       max(snapshot_date) over (partition by wh) as last_of_wh
                from {MART}.mart_price_evolution
                where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
            ),
            per_wh as (
                select wh, source_product_id,
                       max(display_name)                                            as display_name,
                       max(parent_category_name)                                    as departamento,
                       min(snapshot_date) = max(first_of_wh)
                           and max(snapshot_date) = max(last_of_wh)                 as throughout,
                       (array_agg(purchasable_unit_price order by snapshot_date))[1]      as preco_inicial,
                       (array_agg(purchasable_unit_price order by snapshot_date desc))[1] as preco_final,
                       count(*) filter (where change_type = 'preco_alterado'
                                          and snapshot_date > first_of_wh)          as mudancas
                from obs
                group by wh, source_product_id
            ),
            per_product as (
                select source_product_id,
                       max(display_name)                                            as display_name,
                       max(departamento)                                            as departamento,
                       count(*)                                                     as armazens,
                       round(avg(preco_inicial), 2)                                 as preco_inicial,
                       round(avg(preco_final), 2)                                   as preco_final,
                       round(((exp(avg(ln(preco_final / preco_inicial))) - 1) * 100)::numeric, 2)
                                                                                    as variacao_pct,
                       sum(mudancas)                                                as mudancas
                from per_wh
                where throughout
                group by source_product_id
            )"""


INDICATORS: tuple[Indicator, ...] = (

    # ================================================================= A. Index
    Indicator(
        key="indice_de_precos",
        title="Catalog price index (chained Jevons, base 100 on the first day)",
        group="A. Index",
        question="Taking the same products on consecutive collections, how much have Mercadona's "
                 "prices moved since the start of the period, per warehouse and overall?",
        grain="(snapshot_date, wh) — one link per warehouse per collection; plus one row per "
              "collection for the selected warehouses together (\"All selected\")",
        kind="observed, derived index",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "NOT A CONSUMER PRICE INDEX. Every product weighs the same (a Jevons elementary "
            "index): there are no consumption weights in the observed data, and borrowing them "
            "from the simulated orders would make this number synthetic. It answers \"how did "
            "the catalog's prices move\", not \"how much more does a household spend\".",
            "A link counts only when both of its collections are inside the period "
            "(`snapshot_date - days_since_previous_snapshot >= start`). Drop that condition and "
            "the first day of the window absorbs the movement of the days before it.",
            "Each warehouse starts at 100 on its OWN first collection in the window: on "
            "2026-08-15 and 08-16 only mad1 was collected, so the other three stand at 100 in "
            "the overall line until 08-24. A warehouse that misses a day (bcn1 on 2026-10-28) is "
            "not interpolated: its next link spans both days.",
            "\"All selected\" is the geometric mean of the warehouse lines, so it always lies "
            "between them. It is NOT one link over the products of every warehouse pooled: that "
            "ended above all four lines on 2026-10-30 (100.45 against 100.27-100.44), because a "
            "warehouse's long link after a missed day was averaged with the others' 1-day links.",
            "Preparation options of one fresh product (ids 81200.1, .2, .3 — 83 ids of 31 "
            "products, 1.6% of the catalog) count as separate products, so a repriced fish "
            "weighs up to three times. Small at this scale, and observed identity is kept: "
            "siblings were priced differently in 563 rows.",
            "A point is a collection, not a calendar day. Read the x axis as dates: a 14-day "
            "gap (2026-10-14 to 10-28) is one link, and the movement of those 14 days lands on "
            "10-28.",
        ),
        sql=f"""
            with links as (
                select snapshot_date, wh,
                       count(*) filter (where {INSIDE_WINDOW})                      as produtos_comparados,
                       max(days_since_previous_snapshot)
                           filter (where {INSIDE_WINDOW})                           as dias_desde_anterior,
                       coalesce(avg({LINK}) filter (where {INSIDE_WINDOW}), 0)      as log_link
                from {MART}.mart_price_evolution
                where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
                  and purchasable_unit_price > 0
                group by snapshot_date, wh
            ),
            -- The overall line is the geometric mean of the warehouse lines: each warehouse's
            -- log-link divided by the number of warehouses, a missing collection counting as
            -- no movement until the next one. Pooling the products of every warehouse into one
            -- link instead ended ABOVE all four lines (100.45 against 100.27-100.44 on
            -- 2026-10-30): bcn1's 15-day link of 10-29 was averaged with three 1-day links.
            overall as (
                select snapshot_date, 'All selected' as wh,
                       sum(produtos_comparados)                                     as produtos_comparados,
                       max(dias_desde_anterior)                                     as dias_desde_anterior,
                       sum(log_link) / (select count(distinct wh) from links)       as log_link
                from links
                group by snapshot_date
            ),
            both_levels as (
                select * from links
                union all
                select * from overall
            )
            select snapshot_date, wh, produtos_comparados, dias_desde_anterior,
                   round((100 * exp(sum(log_link) over (partition by wh order by snapshot_date)))::numeric, 2)
                       as indice
            from both_levels
            order by wh, snapshot_date
        """,
    ),

    Indicator(
        key="indice_por_departamento",
        title="Price index by department",
        group="A. Index",
        question="Which departments drove the movement of the catalog's prices over the period?",
        grain="(snapshot_date, parent_category_name) — selected warehouses combined as in the "
              "catalog index",
        kind="observed, derived index",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "Same method, window rule and warehouse combination as the catalog index, per department "
            "(`parent_category_name`, the 26 top-level groups of Mercadona's own tree). A "
            "department with few products moves in steps: one price change in a 40-product "
            "department is a 2.5% share of it — read `produtos_comparados` before ranking.",
            "The department is the product's category ON THAT DAY. A product Mercadona moves "
            "to another department carries its links with it from then on; the index does not "
            "rewrite its past.",
        ),
        sql=f"""
            with links as (
                select snapshot_date, wh, parent_category_name                      as departamento,
                       count(*) filter (where {INSIDE_WINDOW})                      as produtos_comparados,
                       coalesce(avg({LINK}) filter (where {INSIDE_WINDOW}), 0)      as log_link
                from {MART}.mart_price_evolution
                where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
                  and purchasable_unit_price > 0 and parent_category_name is not null
                group by snapshot_date, wh, parent_category_name
            ),
            -- Warehouses combined as in the catalog index: the geometric mean of their lines.
            per_department as (
                select snapshot_date, departamento,
                       sum(produtos_comparados)                                     as produtos_comparados,
                       sum(log_link) / (select count(distinct wh) from links)       as log_link
                from links
                group by snapshot_date, departamento
            )
            select snapshot_date, departamento, produtos_comparados,
                   round((100 * exp(sum(log_link) over (partition by departamento
                                                        order by snapshot_date)))::numeric, 2)
                       as indice
            from per_department
            order by departamento, snapshot_date
        """,
    ),

    # ================================================================= B. Over the period
    Indicator(
        key="balanco_do_periodo",
        title="Products up, down and unchanged over the period",
        group="B. Over the period",
        question="Comparing each product's first and last price in the period, how many ended "
                 "up dearer, how many cheaper, and by how much?",
        grain="source_product_id — products on sale at the first and last collection of every "
              "selected warehouse that carries them",
        kind="observed",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "START AGAINST END, not the sum of the moves. A product that dropped 20% for a week "
            "and came back counts as unchanged here, while Price Watch shows it as a drop and an "
            "increase. Both are right; they answer different questions.",
            "Only products present at BOTH ends of the period in a warehouse are compared there: "
            "a product that entered in October has no August price, and one that left has no "
            "October price. Entries and exits are the Catalog tab of Price Watch, not evolution.",
            "Across warehouses, a product's change is the geometric mean of its per-warehouse "
            "changes, so it is 0% only when it is 0% everywhere. Prices shown are the plain "
            "average across the warehouses that carried it throughout.",
        ),
        sql=_PRESENT_THROUGHOUT + """
            select count(*)                                                  as produtos,
                   count(*) filter (where variacao_pct > 0)                 as subiram,
                   count(*) filter (where variacao_pct < 0)                 as cairam,
                   count(*) filter (where variacao_pct = 0)                 as estaveis,
                   round(percentile_cont(0.5) within group (order by variacao_pct)
                         filter (where variacao_pct > 0)::numeric, 2)       as mediana_alta_pct,
                   round(percentile_cont(0.5) within group (order by variacao_pct)
                         filter (where variacao_pct < 0)::numeric, 2)       as mediana_queda_pct
            from per_product
        """,
    ),

    Indicator(
        key="maiores_altas_no_periodo",
        title="Products that rose the most over the period",
        group="B. Over the period",
        question="Which products are dearest now compared with the start of the period?",
        grain="source_product_id — present at both ends of the period",
        kind="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "Ranked by the change from the first to the last collection, so a temporary rise "
            "that was undone does not appear — unlike `maiores_altas` in Price Watch, which "
            "ranks single moves between two consecutive collections.",
            "Ranked by product id, not by name: two sizes of one juice share a name and are two "
            "rows. Preparation options of one fresh product (ids 81200.1, .2, .3 — 83 ids of 31 "
            "products) usually share the price and rise together. `mudancas` counts the price "
            "changes in between; a large change with 1 change is a single repricing.",
        ),
        sql=_PRESENT_THROUGHOUT + """
            select display_name, departamento, armazens, preco_inicial, preco_final,
                   variacao_pct, mudancas
            from per_product
            where variacao_pct > 0
            order by variacao_pct desc, display_name
            limit 100
        """,
    ),

    Indicator(
        key="maiores_quedas_no_periodo",
        title="Products that fell the most over the period",
        group="B. Over the period",
        question="Which products are cheapest now compared with the start of the period?",
        grain="source_product_id — present at both ends of the period",
        kind="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "A lasting decrease, not an offer: a drop that was reversed before the end of the "
            "period nets to zero and is absent. Like every drop in this warehouse, it is an "
            "observed price, never a confirmed promotion — there is no promotional flag upstream.",
            "Two rows can show the same product name: the ranking is by product id, and a name "
            "is not an identity (`identity_review_needed` in the mart).",
        ),
        sql=_PRESENT_THROUGHOUT + """
            select display_name, departamento, armazens, preco_inicial, preco_final,
                   variacao_pct, mudancas
            from per_product
            where variacao_pct < 0
            order by variacao_pct asc, display_name
            limit 100
        """,
    ),

    # ================================================================= C. Between warehouses
    Indicator(
        key="dispersao_regional",
        title="Price differences between warehouses, per collection",
        group="C. Between warehouses",
        question="Does the same product cost the same in every warehouse, and is the gap "
                 "growing or shrinking over time?",
        grain="snapshot_date — products observed in at least two selected warehouses that day",
        kind="observed",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "Only products collected in at least two of the selected warehouses ON THE SAME DAY "
            "are compared. On days one warehouse was not collected (mad1 alone on 2026-08-15 and "
            "08-16, bcn1 missing on 10-28) the comparison covers fewer warehouses, and fewer "
            "warehouses means fewer differences: the share fell from 4.2% to 3.2% on 2026-10-28 "
            "and was back at 4.2% the next day. `armazens_coletados` is in the result for that.",
            "`diferenca_media_pct` is (dearest / cheapest - 1) averaged over the products whose "
            "prices DIFFER; products priced the same everywhere are the other part of the share, "
            "not zeros diluting the average.",
            "Select one warehouse and there is nothing to compare: the query returns no row.",
        ),
        sql=f"""
            with per_product as (
                select snapshot_date, source_product_id,
                       min(purchasable_unit_price) as cheapest,
                       max(purchasable_unit_price) as dearest
                from {MART}.mart_price_evolution
                where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
                  and purchasable_unit_price > 0
                group by snapshot_date, source_product_id
                having count(distinct wh) >= 2
            )
            select snapshot_date,
                   count(*)                                                       as produtos_comparados,
                   (select count(distinct o.wh) from {MART}.mart_price_evolution o
                     where o.snapshot_date = per_product.snapshot_date
                       and o.{WAREHOUSE_FILTER})                                  as armazens_coletados,
                   round((count(*) filter (where dearest > cheapest))::numeric
                         / nullif(count(*), 0), 4)                                as share_com_diferenca,
                   round(avg((dearest / cheapest - 1) * 100)
                         filter (where dearest > cheapest), 2)                    as diferenca_media_pct
            from per_product
            group by snapshot_date
            order by snapshot_date
        """,
    ),

    Indicator(
        key="maiores_diferencas_regionais",
        title="Largest price differences between warehouses, on the last collection",
        group="C. Between warehouses",
        question="On the latest collection of the period, which products cost most differently "
                 "from one warehouse to another?",
        grain="source_product_id on the last day of the period with at least two warehouses",
        kind="observed, ranked",
        marts=("MART_PRICE_EVOLUTION",),
        caveats=(
            "The LAST day of the period with two or more selected warehouses collected — not "
            "necessarily the period's end date. The day is in the result, so the reader sees it.",
            "Same name is not same product: a row is one product id priced in several "
            "warehouses on the same day, which is the only comparison that is like for like.",
            "Fresh fish and some fruit are sold in PREPARATION OPTIONS of one product, with a "
            "suffixed id (81200.1, .2, .3: one trout, three cuts) — 83 ids of 31 products. "
            "They usually share the price, so one repriced fish fills three rows. They are not "
            "merged: siblings were priced differently in 563 rows.",
        ),
        sql=f"""
            with day as (
                select max(snapshot_date) as d
                from (select snapshot_date
                      from {MART}.mart_price_evolution
                      where {SNAPSHOT_DATE_FILTER} and {WAREHOUSE_FILTER}
                      group by snapshot_date
                      having count(distinct wh) >= 2) collected
            )
            select max(e.snapshot_date)                                            as snapshot_date,
                   max(e.display_name)                                             as display_name,
                   max(e.parent_category_name)                                     as departamento,
                   count(*)                                                        as armazens,
                   min(e.purchasable_unit_price)                                   as mais_barato,
                   (array_agg(e.wh order by e.purchasable_unit_price, e.wh))[1]    as onde_mais_barato,
                   max(e.purchasable_unit_price)                                   as mais_caro,
                   (array_agg(e.wh order by e.purchasable_unit_price desc, e.wh))[1] as onde_mais_caro,
                   round((max(e.purchasable_unit_price) / min(e.purchasable_unit_price) - 1) * 100, 2)
                                                                                   as diferenca_pct
            from {MART}.mart_price_evolution e
            join day on e.snapshot_date = day.d
            where {WAREHOUSE_FILTER} and e.purchasable_unit_price > 0
            group by e.source_product_id
            having count(*) >= 2 and max(e.purchasable_unit_price) > min(e.purchasable_unit_price)
            order by diferenca_pct desc, display_name
            limit 100
        """,
    ),
)


# ------------------------------------------------------------------------------------
# Freshness and filter bounds — not indicators, what makes the panel's own state legible.
# ------------------------------------------------------------------------------------
FRESHNESS = f"""
    select 'MART_PRICE_EVOLUTION' as mart, count(*) as linhas,
           count(distinct snapshot_date) as coletas,
           min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
      from {MART}.mart_price_evolution
"""

WINDOW = f"""
    select min(snapshot_date)::varchar as inicio, max(snapshot_date)::varchar as fim
    from {MART}.mart_price_evolution
"""

WAREHOUSES = f"select distinct wh from {MART}.mart_price_evolution order by wh"
