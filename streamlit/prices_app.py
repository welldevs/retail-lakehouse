"""Price-watch dashboard — exclusive to Mercadona catalog price oscillation.

Reads MART live (`mart_price_evolution`, `mart_assortment_daily`) and shows nothing else:
no funnel, no basket, no stock. A SEPARATE screen from the operations panel, not a tab
bolted onto it; every query lives in the shared catalog
(`retail_platform/indicators/prices.py`), where the question and the SQL travel together.

WHAT AN "OFFER" MEANS HERE: a `change_type = 'preco_alterado'` row with `price_delta < 0`.
There is no promotional flag in the warehouse — `fact_price_change.sql` proved the
source's own flag false on every row — so every drop shown is a REAL, OBSERVED price
decrease, never a confirmed marketing campaign. The panel says so; see the prices catalog.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# `connection` puts platform/src on the path, so it must come before the catalog import.
from connection import DashboardError, identity, open_session, run  # noqa: E402
from display import CHANGE_TYPE, fmt_int, table  # noqa: E402
from retail_platform.indicators import prices as PI  # noqa: E402

TTL = int(os.environ.get("RETAIL_PRICE_DASHBOARD_TTL", "60"))

st.set_page_config(page_title="Retail — Price Watch", page_icon="💶", layout="wide")


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


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.2f}%"


IND = {i.key: i for i in PI.INDICATORS}

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

    window = query(PI.WINDOW)
    start_min = pd.to_datetime(window.iloc[0, 0]).date()
    end_max = pd.to_datetime(window.iloc[0, 1]).date()

    period = st.date_input(
        "Snapshot period", value=(start_min, end_max),
        min_value=start_min, max_value=end_max, format="YYYY-MM-DD",
    )
    if isinstance(period, tuple) and len(period) == 2:
        start, end = period
    else:
        start, end = start_min, end_max

    warehouse_list = query(PI.WAREHOUSES).iloc[:, 0].tolist()
    warehouses = st.multiselect("Warehouses", warehouse_list, default=warehouse_list)
    if not warehouses:
        st.warning("Select at least one warehouse.")
        st.stop()

    P = {"start": str(start), "end": str(end), "warehouses": ",".join(warehouses)}

    st.divider()
    search_term = st.text_input("Product search (contains)", value="", placeholder="e.g. coca cola")

    st.divider()
    freshness = query(PI.FRESHNESS)
    latest_date = pd.to_datetime(freshness["FIM"], errors="coerce").max()
    st.caption(
        f"Catalog through {latest_date:%Y-%m-%d}" if pd.notna(latest_date)
        else "No data yet"
    )
    st.caption(f"Read at {ident['lido_em']}")
    with st.expander("What each mart shows"):
        table(freshness.rename(columns={"MART": "Mart", "INICIO": "From", "FIM": "Through",
                                        "LINHAS": "Rows"}),
              hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------
# Header and KPIs
# ------------------------------------------------------------------------------------
st.title("Price Watch")
st.caption(
    f"{start} to {end} · {len(warehouses)} warehouse(s) · Mercadona catalog snapshots only"
)

summary = query(IND["resumo_precos"].sql, P)
if summary.empty or pd.isna(summary.iloc[0]["MUDANCAS"]):
    st.warning("No price snapshots in the selected period. Widen the range.")
    st.stop()
r = summary.iloc[0]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Price changes", fmt_int(r["MUDANCAS"]))
c2.metric("Drops", fmt_int(r["QUEDAS"]))
c3.metric("Increases", fmt_int(r["ALTAS"]))
c4.metric("New listings", fmt_int(r["ENTRADAS"]))
c5.metric("Avg swing", fmt_pct(r["OSCILACAO_MEDIA_PCT"]))
c6.metric("Biggest drop", fmt_pct(r["MAIOR_QUEDA_PCT"]))

st.caption(
    f"{fmt_int(r['PRODUTOS_AFETADOS'])} distinct products had at least one price change in "
    f"this window. \"Biggest drop\"/\"biggest rise\" are OBSERVED moves, not confirmed "
    f"promotions — see the Drops tab for why."
)

# ------------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------------
tabs = st.tabs([
    "Drops (offers?)", "Increases", "Daily movement", "Categories", "Product", "Catalog",
])

# ---------------------------------------------------------------- Drops
with tabs[0]:
    st.subheader("Biggest observed drops")
    st.caption(
        "Ranked by percentage change. No source in this warehouse flags a genuine "
        "promotion — this is a REAL price decrease between two observed snapshots, not a "
        "confirmed marketing offer."
    )
    quedas = numeric(
        query(IND["maiores_quedas"].sql, P),
        ["PREVIOUS_PURCHASABLE_UNIT_PRICE", "PURCHASABLE_UNIT_PRICE",
         "PURCHASABLE_PRICE_DELTA", "PURCHASABLE_PRICE_DELTA_PCT",
         "DAYS_SINCE_PREVIOUS_SNAPSHOT"],
    )
    table(
        quedas.drop(columns=["PURCHASABLE_PRICE_DELTA"]).rename(columns={
            "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "DISPLAY_NAME": "Product",
            "CATEGORY_NAME": "Category",
            "PREVIOUS_PURCHASABLE_UNIT_PRICE": "Previous price",
            "PURCHASABLE_UNIT_PRICE": "Price",
            "PURCHASABLE_PRICE_DELTA_PCT": "Change %",
            "DAYS_SINCE_PREVIOUS_SNAPSHOT": "Days apart",
            "IDENTITY_REVIEW_NEEDED": "Review identity",
        }),
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Increases
# Both ranking tables drop the absolute change (Previous price and Price are on screen) and
# use short headers: with them, the identity-review flag scrolled out of the frame.
with tabs[1]:
    st.subheader("Biggest observed increases")
    altas = numeric(
        query(IND["maiores_altas"].sql, P),
        ["PREVIOUS_PURCHASABLE_UNIT_PRICE", "PURCHASABLE_UNIT_PRICE",
         "PURCHASABLE_PRICE_DELTA", "PURCHASABLE_PRICE_DELTA_PCT",
         "DAYS_SINCE_PREVIOUS_SNAPSHOT"],
    )
    table(
        altas.drop(columns=["PURCHASABLE_PRICE_DELTA"]).rename(columns={
            "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "DISPLAY_NAME": "Product",
            "CATEGORY_NAME": "Category",
            "PREVIOUS_PURCHASABLE_UNIT_PRICE": "Previous price",
            "PURCHASABLE_UNIT_PRICE": "Price",
            "PURCHASABLE_PRICE_DELTA_PCT": "Change %",
            "DAYS_SINCE_PREVIOUS_SNAPSHOT": "Days apart",
            "IDENTITY_REVIEW_NEEDED": "Review identity",
        }),
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Daily movement
with tabs[2]:
    st.subheader("Change volume by day")
    daily = numeric(
        query(IND["movimentacao_diaria"].sql, P), ["QUEDAS", "ALTAS", "ENTRADAS"],
    )
    daily["SNAPSHOT_DATE"] = pd.to_datetime(daily["SNAPSHOT_DATE"])
    daily_wide = daily.rename(columns={
        "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "QUEDAS": "Drops",
        "ALTAS": "Increases", "ENTRADAS": "New listings",
    })
    # Drops and increases summed over warehouses, stacked: unstacked, the increases bar
    # covered the drops bar on the same day.
    totals = daily_wide.groupby("Date", as_index=False)[["Drops", "Increases"]].sum()
    st.bar_chart(totals, x="Date", y=["Drops", "Increases"], stack=True,
                 x_label="Snapshot date", y_label="Price changes (all selected warehouses)")
    st.caption("Days without a bar are days the catalog was not collected. On a warehouse's "
               "first snapshot every count is 0: there is no earlier price to compare with.")
    table(daily_wide, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Categories
with tabs[3]:
    # One above the other: side by side, both tables lost their right-hand columns.
    left = right = st.container()
    with left:
        st.subheader("Most volatile categories")
        st.caption("Categories with at least 10 price changes in the period, by average swing.")
        vol = numeric(
            query(IND["volatilidade_por_categoria"].sql, P),
            ["TRANSICOES", "MUDANCAS", "TAXA_DE_MUDANCA", "OSCILACAO_MEDIA_PCT"],
        )
        table(
            vol.head(30).rename(columns={
                "CATEGORY_NAME": "Category", "PARENT_CATEGORY_NAME": "Parent",
                "TRANSICOES": "Transitions", "MUDANCAS": "Changes",
                "TAXA_DE_MUDANCA": "Change rate", "OSCILACAO_MEDIA_PCT": "Avg swing %",
            }),
            hide_index=True, width="stretch",
            column_config={"Change rate": st.column_config.ProgressColumn(
                format="percent", min_value=0.0, max_value=1.0)},
        )
    with right:
        st.subheader("Price levels by category")
        niveis = numeric(
            query(IND["preco_por_categoria"].sql, P),
            ["PRECO_MINIMO", "PRECO_MAXIMO", "PRECO_MEDIO", "MEDIANA_MEDIA"],
        )
        table(
            niveis.head(30).rename(columns={
                "CATEGORY_NAME": "Category", "PARENT_CATEGORY_NAME": "Parent",
                "PRECO_MINIMO": "Min price", "PRECO_MAXIMO": "Max price",
                "PRECO_MEDIO": "Avg price", "MEDIANA_MEDIA": "Avg of daily median",
            }),
            hide_index=True, width="stretch",
        )

# ---------------------------------------------------------------- Product
with tabs[4]:
    st.subheader("Product price history")
    if not search_term.strip():
        st.info("Type a product name in the sidebar search box to see its price history.")
    else:
        PT = {**P, "term": f"%{search_term.strip()}%"}
        matches = query(IND["produtos_buscaveis"].sql, PT).iloc[:, 0].tolist()
        if not matches:
            st.warning(f"No product matches \"{search_term}\".")
        else:
            st.caption(
                f"{len(matches)} matching name(s){' (showing up to 50)' if len(matches) == 50 else ''}."
            )
            historico = numeric(
                query(IND["historico_preco_produto"].sql, PT),
                ["PURCHASABLE_UNIT_PRICE", "PREVIOUS_PURCHASABLE_UNIT_PRICE",
                 "PURCHASABLE_PRICE_DELTA_PCT", "DAYS_SINCE_PREVIOUS_SNAPSHOT"],
            )
            historico["SNAPSHOT_DATE"] = pd.to_datetime(historico["SNAPSHOT_DATE"])
            historico = historico.rename(columns={
                "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "DISPLAY_NAME": "Product",
                "CATEGORY_NAME": "Category", "PURCHASABLE_UNIT_PRICE": "Price",
                "PREVIOUS_PURCHASABLE_UNIT_PRICE": "Previous price",
                "PURCHASABLE_PRICE_DELTA_PCT": "Change %",
                "CHANGE_TYPE": "Change type",
                "DAYS_SINCE_PREVIOUS_SNAPSHOT": "Days since previous",
            }).replace({"Change type": CHANGE_TYPE})
            # ONE product per chart. With every match on the same line per warehouse, a
            # search for "aguacate" drew a 1.60-3.50 sawtooth that was four products, not one
            # price moving (CR-018).
            chosen = st.selectbox("Product", matches, index=0)
            historico = historico[historico["Product"] == chosen]
            st.line_chart(historico, x="Date", y="Price", color="Warehouse")
            table(historico, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Catalog
with tabs[5]:
    st.subheader("New arrivals and pack changes")
    catalogo = numeric(
        query(IND["entradas_e_pacotes"].sql, P),
        ["PRODUTOS", "NOVIDADES", "PACOTES", "EXCLUSIVOS_DO_ARMAZEM"],
    )
    catalogo["SNAPSHOT_DATE"] = pd.to_datetime(catalogo["SNAPSHOT_DATE"])
    # "New" is Mercadona's own badge (`is_new_arrival`), not a product that entered the
    # catalog that day: the badge stays on for weeks, hence ~100 a day against a few
    # dozen real entries. One line per warehouse — overlapping bars showed only vlc1.
    catalogo = catalogo.rename(columns={
        "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "PRODUTOS": "Products",
        "NOVIDADES": "Flagged new by Mercadona", "PACOTES": "Packs",
        "EXCLUSIVOS_DO_ARMAZEM": "Exclusive to this warehouse",
    })
    st.line_chart(catalogo, x="Date", y="Flagged new by Mercadona", color="Warehouse")
    st.caption("Mercadona's own \"new\" badge, which stays on for weeks — not the products "
               "that entered the catalog that day. Exclusivity is blank on a day only one "
               "warehouse was collected: there is nothing to compare it with.")
    table(catalogo, hide_index=True, width="stretch")
