"""Price-watch dashboard — exclusive to Mercadona catalog price oscillation.

Reads MART live (`mart_price_evolution`, `mart_assortment_daily`) and shows nothing else:
no funnel, no basket, no stock. A SEPARATE screen from `app.py`'s operations dashboard,
not a fourteenth tab bolted onto it — the user asked for an exclusive panel, and every
query here already lives in `price_indicators.py`, which is where the question and the SQL
travel together, same discipline as `indicators.py`.

WHAT AN "OFFER" MEANS HERE: a `change_type = 'preco_alterado'` row with `price_delta < 0`.
There is no promotional flag in the warehouse — `fact_price_change.sql` proved the
source's own flag false on every row — so every drop shown is a REAL, OBSERVED price
decrease, never a confirmed marketing campaign. The panel says so; see price_indicators.py.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import price_indicators as PI  # noqa: E402
from connection import DashboardError, identity, open_session, run  # noqa: E402

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


def fmt_int(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{int(value):,}".replace(",", ".")


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.2f}%"


IND = {i.chave: i for i in PI.INDICADORES}

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

    window = query(PI.JANELA)
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

    warehouse_list = query(PI.ARMAZENS).iloc[:, 0].tolist()
    warehouses = st.multiselect("Warehouses", warehouse_list, default=warehouse_list)
    if not warehouses:
        st.warning("Select at least one warehouse.")
        st.stop()

    P = {"inicio": str(start), "fim": str(end), "armazens": ",".join(warehouses)}

    st.divider()
    termo = st.text_input("Product search (contains)", value="", placeholder="e.g. coca cola")

    st.divider()
    freshness = query(PI.FRESCOR)
    latest_date = pd.to_datetime(freshness["FIM"], errors="coerce").max()
    st.caption(
        f"Catalog through {latest_date:%Y-%m-%d}" if pd.notna(latest_date)
        else "No data yet"
    )
    st.caption(f"Read at {ident['lido_em']}")
    with st.expander("What each mart shows"):
        st.dataframe(freshness, hide_index=True, width="stretch")

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
    st.dataframe(
        quedas.rename(columns={
            "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "DISPLAY_NAME": "Product",
            "CATEGORY_NAME": "Category",
            "PREVIOUS_PURCHASABLE_UNIT_PRICE": "Previous price",
            "PURCHASABLE_UNIT_PRICE": "Price", "PURCHASABLE_PRICE_DELTA": "Change",
            "PURCHASABLE_PRICE_DELTA_PCT": "Change %",
            "DAYS_SINCE_PREVIOUS_SNAPSHOT": "Days since previous",
            "IDENTITY_REVIEW_NEEDED": "Identity review needed",
        }),
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Increases
with tabs[1]:
    st.subheader("Biggest observed increases")
    altas = numeric(
        query(IND["maiores_altas"].sql, P),
        ["PREVIOUS_PURCHASABLE_UNIT_PRICE", "PURCHASABLE_UNIT_PRICE",
         "PURCHASABLE_PRICE_DELTA", "PURCHASABLE_PRICE_DELTA_PCT",
         "DAYS_SINCE_PREVIOUS_SNAPSHOT"],
    )
    st.dataframe(
        altas.rename(columns={
            "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "DISPLAY_NAME": "Product",
            "CATEGORY_NAME": "Category",
            "PREVIOUS_PURCHASABLE_UNIT_PRICE": "Previous price",
            "PURCHASABLE_UNIT_PRICE": "Price", "PURCHASABLE_PRICE_DELTA": "Change",
            "PURCHASABLE_PRICE_DELTA_PCT": "Change %",
            "DAYS_SINCE_PREVIOUS_SNAPSHOT": "Days since previous",
            "IDENTITY_REVIEW_NEEDED": "Identity review needed",
        }),
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Daily movement
with tabs[2]:
    st.subheader("Change volume by day")
    diario = numeric(
        query(IND["movimentacao_diaria"].sql, P), ["QUEDAS", "ALTAS", "ENTRADAS"],
    )
    diario["SNAPSHOT_DATE"] = pd.to_datetime(diario["SNAPSHOT_DATE"])
    diario_wide = diario.rename(columns={
        "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "QUEDAS": "Drops",
        "ALTAS": "Increases", "ENTRADAS": "New listings",
    })
    st.bar_chart(diario_wide, x="Date", y=["Drops", "Increases"], color="Warehouse", stack=False)
    st.dataframe(diario_wide, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Categories
with tabs[3]:
    left, right = st.columns(2)
    with left:
        st.subheader("Most volatile categories")
        vol = numeric(
            query(IND["volatilidade_por_categoria"].sql, P),
            ["TRANSICOES", "MUDANCAS", "TAXA_DE_MUDANCA", "OSCILACAO_MEDIA_PCT"],
        )
        st.dataframe(
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
        st.dataframe(
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
    if not termo.strip():
        st.info("Type a product name in the sidebar search box to see its price history.")
    else:
        PT = {**P, "termo": f"%{termo.strip()}%"}
        matches = query(IND["produtos_buscaveis"].sql, PT).iloc[:, 0].tolist()
        if not matches:
            st.warning(f"No product matches \"{termo}\".")
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
            })
            st.line_chart(historico, x="Date", y="Price", color="Warehouse")
            st.dataframe(historico, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Catalog
with tabs[5]:
    st.subheader("New arrivals and pack changes")
    catalogo = numeric(
        query(IND["entradas_e_pacotes"].sql, P),
        ["PRODUTOS", "NOVIDADES", "PACOTES", "EXCLUSIVOS_DO_ARMAZEM"],
    )
    catalogo["SNAPSHOT_DATE"] = pd.to_datetime(catalogo["SNAPSHOT_DATE"])
    catalogo = catalogo.rename(columns={
        "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "PRODUTOS": "Products",
        "NOVIDADES": "New arrivals", "PACOTES": "Packs",
        "EXCLUSIVOS_DO_ARMAZEM": "Exclusive to this warehouse",
    })
    st.bar_chart(catalogo, x="Date", y="New arrivals", color="Warehouse", stack=False)
    st.dataframe(catalogo, hide_index=True, width="stretch")
