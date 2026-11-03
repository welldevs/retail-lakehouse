"""Price-evolution dashboard — how far Mercadona's catalog prices travelled, not how often.

A SEPARATE screen from Price Watch (CR-019). Price Watch counts events: this drop, that rise,
changes per day. This one answers the question an accumulating price history raises — "since
August, have prices gone up, by how much, where?" — with a chained index, start-against-end
changes and the gap between warehouses. An offer that goes and comes back moves Price Watch
twice and this screen not at all; both are right.

Reads MART live (`mart_price_evolution`) through the catalog in
`retail_platform/indicators/evolution.py`; the Product tab reuses the search and history of
the prices catalog rather than a second copy of them. No SQL here.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# `connection` puts platform/src on the path, so it must come before the catalog import.
from connection import DashboardError, identity, open_session, run  # noqa: E402
from display import change_bars, date_lines, fmt_int, full_height, table  # noqa: E402
from retail_platform.indicators import evolution as EV  # noqa: E402
from retail_platform.indicators import prices as PI  # noqa: E402

TTL = int(os.environ.get("RETAIL_EVOLUTION_DASHBOARD_TTL", "60"))
OVERALL = "All selected"

st.set_page_config(page_title="Retail — Price Evolution", page_icon="📈", layout="wide")


# ------------------------------------------------------------------------------------
# Reading
# ------------------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _session():
    return open_session()


@st.cache_data(ttl=TTL, show_spinner=False)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    return run(_session(), sql, params)


def numeric(df: pd.DataFrame, columns) -> pd.DataFrame:
    """Decimal columns arrive as object (Decimal); Altair needs float."""
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def fmt_change(index_value) -> str:
    """An index as the change it means: 100.34 -> +0.34%."""
    if index_value is None or pd.isna(index_value):
        return "—"
    return f"{float(index_value) - 100:+.2f}%"


IND = {i.key: i for i in EV.INDICATORS}
PRICES = {i.key: i for i in PI.INDICATORS}

# ------------------------------------------------------------------------------------
# Connection
# ------------------------------------------------------------------------------------
try:
    ident = identity(_session())
except DashboardError as exc:
    st.error(str(exc))
    st.stop()

# ------------------------------------------------------------------------------------
# Sidebar: filters
# ------------------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Filters")

    if st.button("Refresh data", type="primary", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    window = query(EV.WINDOW)
    start_min = pd.to_datetime(window.iloc[0, 0]).date()
    end_max = pd.to_datetime(window.iloc[0, 1]).date()

    period = st.date_input(
        "Period", value=(start_min, end_max),
        min_value=start_min, max_value=end_max, format="YYYY-MM-DD",
    )
    if isinstance(period, tuple) and len(period) == 2:
        start, end = period
    else:
        start, end = start_min, end_max

    warehouse_list = query(EV.WAREHOUSES).iloc[:, 0].tolist()
    warehouses = st.multiselect("Warehouses", warehouse_list, default=warehouse_list)
    if not warehouses:
        st.warning("Select at least one warehouse.")
        st.stop()

    P = {"start": str(start), "end": str(end), "warehouses": ",".join(warehouses)}

    st.divider()
    search_term = st.text_input("Product search (contains)", value="", placeholder="e.g. aceite")

    st.divider()
    freshness = query(EV.FRESHNESS)
    latest_date = pd.to_datetime(freshness["FIM"], errors="coerce").max()
    st.caption(
        f"Catalog through {latest_date:%Y-%m-%d}" if pd.notna(latest_date) else "No data yet"
    )
    st.caption(f"Read at {ident['lido_em']}")
    with st.expander("What the mart holds"):
        # The table's real name, as Postgres spells it — not the catalog's upper-case label.
        table(freshness.assign(MART=freshness["MART"].str.lower()).rename(columns={"MART": "Mart", "LINHAS": "Rows",
                                        "COLETAS": "Collections", "INICIO": "From",
                                        "FIM": "Through"}),
              hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------
# Header and KPIs
# ------------------------------------------------------------------------------------
st.title("Price Evolution")
st.caption(
    f"{start} to {end} · {len(warehouses)} warehouse(s) · Mercadona catalog prices, observed "
    f"— an index of the catalog, not a consumer price index"
)

index = numeric(query(IND["indice_de_precos"].sql, P),
                ["PRODUTOS_COMPARADOS", "DIAS_DESDE_ANTERIOR", "INDICE"])
if index.empty:
    st.warning("No price collections in the selected period. Widen the range.")
    st.stop()
index["SNAPSHOT_DATE"] = pd.to_datetime(index["SNAPSHOT_DATE"])
overall = index[index["WH"] == OVERALL].sort_values("SNAPSHOT_DATE")
by_wh = index[index["WH"] != OVERALL]
last_by_wh = by_wh.sort_values("SNAPSHOT_DATE").groupby("WH").tail(1)

balance = numeric(query(IND["balanco_do_periodo"].sql, P),
                  ["PRODUTOS", "SUBIRAM", "CAIRAM", "ESTAVEIS",
                   "MEDIANA_ALTA_PCT", "MEDIANA_QUEDA_PCT"]).iloc[0]

collections = overall["SNAPSHOT_DATE"]
gaps = collections.diff().dt.days
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Catalog prices since the start", fmt_change(overall["INDICE"].iloc[-1]),
          help="Chained Jevons index of the selected warehouses, base 100 on the first day.")
c2.metric("Products dearer", fmt_int(balance["SUBIRAM"]),
          help="Last price above the first one in the period.")
c3.metric("Products cheaper", fmt_int(balance["CAIRAM"]))
c4.metric("Products unchanged", fmt_int(balance["ESTAVEIS"]))
c5.metric("Collections", f"{len(collections)} in {(end - start).days + 1} days",
          help="Days the catalog was actually collected in the period.")

st.caption(
    f"Compared: {fmt_int(balance['PRODUTOS'])} products on sale at the start and at the end of "
    f"the period. Typical rise {balance['MEDIANA_ALTA_PCT']:+.2f}%, typical fall "
    f"{balance['MEDIANA_QUEDA_PCT']:+.2f}% (medians). Longest gap between collections: "
    f"{int(gaps.max()) if gaps.notna().any() else 0} days."
)

# ------------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------------
tabs = st.tabs(["Index", "Departments", "Over the period", "Between warehouses", "Product"])

# ---------------------------------------------------------------- Index
with tabs[0]:
    st.subheader("Catalog price index")
    st.caption(
        "The same products, collection after collection: the geometric mean of each price "
        "against its previous collection, chained. Entries and exits do not move it, and an "
        "offer that comes back cancels out. Every product weighs the same."
    )
    # One warehouse selected: "All selected" is that warehouse again, drawn twice.
    lines = index if len(warehouses) > 1 else by_wh
    date_lines(lines.rename(columns={"WH": "Warehouse", "INDICE": "Index"}),
               "SNAPSHOT_DATE", "Index", "Index (first day = 100)", color="Warehouse",
               domain=[OVERALL] + warehouse_list, emphasis=OVERALL, reference=100)
    st.caption(
        "Each point is a collection; a long segment is a gap with no collection, and the "
        "movement of the whole gap lands on its last point. Each warehouse starts at 100 on its "
        "own first collection (only mad1 was collected on 2026-08-15 and 08-16)."
    )
    summary = last_by_wh.sort_values("WH")[["WH", "INDICE"]].assign(
        Change=lambda d: d["INDICE"] - 100).rename(
        columns={"WH": "Warehouse", "INDICE": "Index", "Change": "Change %"})
    if len(warehouses) > 1:
        summary = pd.concat([summary, pd.DataFrame(
            {"Warehouse": [OVERALL], "Index": [overall["INDICE"].iloc[-1]],
             "Change %": [overall["INDICE"].iloc[-1] - 100]})])
    table(summary, hide_index=True, width="stretch")
    with st.expander("Every collection behind the overall line"):
        table(overall.drop(columns=["WH"]).rename(columns={
            "SNAPSHOT_DATE": "Collection", "PRODUTOS_COMPARADOS": "Price pairs compared",
            "DIAS_DESDE_ANTERIOR": "Days since previous collection", "INDICE": "Index",
        }), hide_index=True, width="stretch")

# ---------------------------------------------------------------- Departments
with tabs[1]:
    st.subheader("Which departments moved the index")
    dept = numeric(query(IND["indice_por_departamento"].sql, P),
                   ["PRODUTOS_COMPARADOS", "INDICE"])
    dept["SNAPSHOT_DATE"] = pd.to_datetime(dept["SNAPSHOT_DATE"])
    final = dept.sort_values("SNAPSHOT_DATE").groupby("DEPARTAMENTO").tail(1).assign(
        CHANGE=lambda d: d["INDICE"] - 100).sort_values("CHANGE", ascending=False)
    change_bars(final.rename(columns={"DEPARTAMENTO": "Department", "CHANGE": "Change %"}),
                "Department", "Change %", "Change since the start of the period (%)")
    st.caption("Mercadona's own top-level departments, warehouses combined as in the index.")

    names = final["DEPARTAMENTO"].tolist()
    # The two biggest risers and the two biggest fallers: four lines stay readable, 26 do not.
    default = names[:2] + names[-2:] if len(names) >= 4 else names
    chosen = st.multiselect("Departments to draw over time", names, default=default,
                            max_selections=6)
    if chosen:
        date_lines(dept[dept["DEPARTAMENTO"].isin(chosen)].rename(
                       columns={"DEPARTAMENTO": "Department", "INDICE": "Index"}),
                   "SNAPSHOT_DATE", "Index", "Index (first day = 100)", color="Department",
                   domain=sorted(chosen), reference=100)
    peak = dept.groupby("DEPARTAMENTO")["PRODUTOS_COMPARADOS"].max()
    table(final.assign(PRODUCTS=final["DEPARTAMENTO"].map(peak))[
              ["DEPARTAMENTO", "INDICE", "CHANGE", "PRODUCTS"]].rename(columns={
              "DEPARTAMENTO": "Department", "INDICE": "Index", "CHANGE": "Change %",
              "PRODUCTS": "Price pairs per collection (max)"}),
          hide_index=True, width="stretch", height=full_height(len(final)))

# ---------------------------------------------------------------- Over the period
PERIOD_COLUMNS = {
    "DISPLAY_NAME": "Product", "DEPARTAMENTO": "Department", "ARMAZENS": "Warehouses",
    "PRECO_INICIAL": "First price", "PRECO_FINAL": "Last price",
    "VARIACAO_PCT": "Change %", "MUDANCAS": "Price changes in between",
}
PERIOD_NUMERIC = ["ARMAZENS", "PRECO_INICIAL", "PRECO_FINAL", "VARIACAO_PCT", "MUDANCAS"]
PERIOD_CONFIG = {"Product": st.column_config.TextColumn(width="large"),
                 "Department": st.column_config.TextColumn(width="medium")}
with tabs[2]:
    st.caption(
        "First price against last price, for products on sale at both ends of the period. A "
        "drop that was undone before the end is not here — that is Price Watch's job. Prices "
        "are averaged over the warehouses that carried the product throughout."
    )
    # One above the other: side by side, the tables would lose their right-hand columns.
    st.subheader("Rose the most")
    rose = numeric(query(IND["maiores_altas_no_periodo"].sql, P), PERIOD_NUMERIC).head(20)
    table(rose.rename(columns=PERIOD_COLUMNS), hide_index=True, width="stretch",
          column_config=PERIOD_CONFIG, height=full_height(len(rose)))
    st.subheader("Fell the most")
    fell = numeric(query(IND["maiores_quedas_no_periodo"].sql, P), PERIOD_NUMERIC).head(20)
    table(fell.rename(columns=PERIOD_COLUMNS), hide_index=True, width="stretch",
          column_config=PERIOD_CONFIG, height=full_height(len(fell)))
    st.caption("Observed prices, not confirmed promotions: there is no promotional flag "
               "anywhere upstream. Two rows with the same name are two product ids (two sizes "
               "of orange juice). Fresh fish and some fruit come in preparation options of one "
               "product — Mercadona ids 81200.1, .2, .3 — which usually share the price and "
               "therefore appear together.")

# ---------------------------------------------------------------- Between warehouses
with tabs[3]:
    st.subheader("Does the same product cost the same everywhere?")
    if len(warehouses) < 2:
        st.info("Select at least two warehouses to compare prices between them.")
    else:
        spread = numeric(query(IND["dispersao_regional"].sql, P),
                         ["PRODUTOS_COMPARADOS", "ARMAZENS_COLETADOS", "SHARE_COM_DIFERENCA",
                          "DIFERENCA_MEDIA_PCT"])
        spread["SNAPSHOT_DATE"] = pd.to_datetime(spread["SNAPSHOT_DATE"])
        # Two measures, two charts: one axis each, never a dual axis. The question goes in the
        # heading; an axis title that long was cut off.
        st.markdown("**Share of products priced differently between warehouses**")
        date_lines(spread.rename(columns={"SHARE_COM_DIFERENCA": "Share"}),
                   "SNAPSHOT_DATE", "Share", "Share of products", value_format=".1%")
        st.markdown("**Average gap between the dearest and the cheapest warehouse**")
        date_lines(spread.rename(columns={"DIFERENCA_MEDIA_PCT": "Gap %"}),
                   "SNAPSHOT_DATE", "Gap %", "Gap (%)")
        short = spread[spread["ARMAZENS_COLETADOS"] < len(warehouses)]
        st.caption(
            "Among products collected in two or more selected warehouses on the same day; the "
            "gap is averaged only over products whose prices differ."
            + (" Fewer warehouses were collected on "
               + ", ".join(f"{d:%Y-%m-%d} ({int(n)})" for d, n in
                           zip(short["SNAPSHOT_DATE"], short["ARMAZENS_COLETADOS"]))
               + " — fewer warehouses to compare, so fewer differences: a dip there is "
                 "collection, not prices converging." if not short.empty else ""))
        gaps_last = numeric(query(IND["maiores_diferencas_regionais"].sql, P),
                            ["ARMAZENS", "MAIS_BARATO", "MAIS_CARO", "DIFERENCA_PCT"])
        day = (pd.to_datetime(gaps_last["SNAPSHOT_DATE"]).max() if not gaps_last.empty else None)
        st.subheader(f"Largest gaps on {day:%Y-%m-%d}" if day is not None else "Largest gaps")
        gaps_last = gaps_last.head(20)
        table(gaps_last.drop(columns=["SNAPSHOT_DATE"]).rename(columns={
            "DISPLAY_NAME": "Product", "DEPARTAMENTO": "Department", "ARMAZENS": "Warehouses",
            "MAIS_BARATO": "Cheapest", "ONDE_MAIS_BARATO": "Cheapest in",
            "MAIS_CARO": "Dearest", "ONDE_MAIS_CARO": "Dearest in", "DIFERENCA_PCT": "Gap %",
        }), hide_index=True, width="stretch", height=full_height(len(gaps_last)),
            column_config={"Product": st.column_config.TextColumn(width="large")})
        st.caption("The three trout rows are one fish in three preparation options (ids "
                   "81200.1, .2, .3), priced per piece and repriced almost daily.")

# ---------------------------------------------------------------- Product
with tabs[4]:
    st.subheader("One product, warehouse by warehouse")
    if not search_term.strip():
        st.info("Type a product name in the sidebar search box to see how its price evolved.")
    else:
        PT = {**P, "term": f"%{search_term.strip()}%"}
        matches = query(PRICES["produtos_buscaveis"].sql, PT).iloc[:, 0].tolist()
        if not matches:
            st.warning(f"No product matches \"{search_term}\".")
        else:
            # ONE product per chart: several matches on one line drew a sawtooth of different
            # products, not one price moving (CR-018).
            # An exact name first: searching "Melón piel de sapo" opened on "Medio melón...".
            exact = [m.lower() for m in matches].index(search_term.strip().lower()) \
                if search_term.strip().lower() in [m.lower() for m in matches] else 0
            chosen = st.selectbox(
                f"Product ({len(matches)} matching name(s)"
                f"{', showing up to 50' if len(matches) == 50 else ''})", matches, index=exact)
            history = numeric(query(PRICES["historico_preco_produto"].sql, PT),
                              ["PURCHASABLE_UNIT_PRICE"])
            history = history[history["DISPLAY_NAME"] == chosen]
            history["SNAPSHOT_DATE"] = pd.to_datetime(history["SNAPSHOT_DATE"])
            date_lines(history.rename(columns={"WH": "Warehouse",
                                               "PURCHASABLE_UNIT_PRICE": "Price"}),
                       "SNAPSHOT_DATE", "Price", "Price (EUR)", color="Warehouse",
                       domain=warehouse_list)
            first = history.sort_values("SNAPSHOT_DATE").groupby("WH").agg(
                FIRST=("PURCHASABLE_UNIT_PRICE", "first"), LAST=("PURCHASABLE_UNIT_PRICE", "last"),
                FROM=("SNAPSHOT_DATE", "min"), TO=("SNAPSHOT_DATE", "max"),
                COLLECTIONS=("SNAPSHOT_DATE", "count")).reset_index()
            first["CHANGE"] = (first["LAST"] / first["FIRST"] - 1) * 100
            table(first.rename(columns={
                "WH": "Warehouse", "FIRST": "First price", "LAST": "Last price",
                "CHANGE": "Change %", "FROM": "From", "TO": "To", "COLLECTIONS": "Collections",
            })[["Warehouse", "From", "To", "Collections", "First price", "Last price", "Change %"]],
                hide_index=True, width="stretch")
            st.caption("A name can cover more than one product id; the line shows every id with "
                       "the chosen name.")
