"""Operations dashboard over the warehouse's MART.

Reads MART live and shows KPI, table, and chart — what whoever runs the business looks at
day to day. Holds no state, writes nothing, exposes no SQL and no layer metadata.

This dashboard's technical counterpart (the question each indicator answers, the source
grain, what's observed vs. synthetic, and the gotchas of rebuilding it in another tool)
still exists — in the shared catalog (`retail_platform/indicators/operations.py`), versioned
alongside the same SQL, and published in `docs/indicators/operations.md`. It just doesn't
show up on this screen.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# `connection` puts platform/src on the path, so it must come before the catalog import.
from connection import DashboardError, identity, open_session, run  # noqa: E402
from display import CHANGE_TYPE, daily_bars, fmt_currency, fmt_int, table  # noqa: E402
from retail_platform.indicators import operations as I  # noqa: E402

TTL = int(os.environ.get("RETAIL_DASHBOARD_TTL", "60"))

st.set_page_config(page_title="Retail — Operations Dashboard", page_icon="📦", layout="wide")


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

    window = query(I.WINDOW)
    start_min = pd.to_datetime(window.iloc[0, 0]).date()
    end_max = pd.to_datetime(window.iloc[0, 1]).date()
    # The DEFAULT is the orders window, not the union of every mart: the catalog reaches
    # further than the orders, and a header saying "08-15 to 10-30" over orders that exist
    # for nine days misleads the first reading of every KPI.
    orders_window = query(I.ORDERS_WINDOW)
    default_start = pd.to_datetime(orders_window.iloc[0, 0]).date()
    default_end = pd.to_datetime(orders_window.iloc[0, 1]).date()

    period = st.date_input(
        "Period", value=(default_start, default_end),
        min_value=start_min, max_value=end_max, format="YYYY-MM-DD",
    )
    if isinstance(period, tuple) and len(period) == 2:
        start, end = period
    else:
        start, end = start_min, end_max

    warehouse_list = query(I.WAREHOUSES).iloc[:, 0].tolist()
    warehouses = st.multiselect("Warehouses", warehouse_list, default=warehouse_list)
    if not warehouses:
        st.warning("Select at least one warehouse.")
        st.stop()

    P = {"start": str(start), "end": str(end), "warehouses": ",".join(warehouses)}

    st.divider()
    freshness = query(I.FRESHNESS)
    latest_date = pd.to_datetime(freshness["FIM"], errors="coerce").max()
    st.caption(
        f"Data through {latest_date:%Y-%m-%d}" if pd.notna(latest_date)
        else "No data yet"
    )
    st.caption(f"Read at {ident['lido_em']}")

# ------------------------------------------------------------------------------------
# Header and KPIs
# ------------------------------------------------------------------------------------
st.title("Operations Dashboard")
st.caption(f"{start} to {end} · {len(warehouses)} warehouse(s)")

ind = {i.key: i for i in I.INDICATORS}
summary = query(ind["resumo_comercial"].sql, P)
if summary.empty or pd.isna(summary.iloc[0]["PEDIDOS_COLOCADOS"]):
    st.warning("No orders in the selected period. Widen the range.")
    st.stop()
r = summary.iloc[0]
currency = r["MOEDA"] or ""

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Orders placed", fmt_int(r["PEDIDOS_COLOCADOS"]))
c2.metric("Delivered", fmt_int(r["PEDIDOS_ENTREGUES"]),
          f"{float(r['TAXA_ENTREGA'])*100:.2f}% of placed")
c3.metric(f"Fulfilled revenue ({currency})", fmt_currency(r["RECEITA_APURADA"]))
c4.metric(f"Average ticket ({currency})", fmt_currency(r["TICKET_MEDIO"]),
          help="Fulfilled revenue divided by orders that were actually picked.")
c5.metric("Unfulfilled",
          fmt_currency(float(r["VALOR_COLOCADO"]) - float(r["RECEITA_APURADA"])),
          help="Placed value that never became revenue — cancellation, declined payment, "
               "or a basket reduced during picking.")

# ------------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------------
tabs = st.tabs([
    "Commercial", "Operations", "Basket", "Assortment and price",
    "Supply and demand", "Customers", "Inventory",
])

# ---------------------------------------------------------------- Commercial
with tabs[0]:
    left, right = st.columns([3, 2])

    with left:
        st.subheader("Funnel, by milestone reached")
        funnel = numeric(query(ind["funil"].sql, P), ["PEDIDOS", "TAXA"])
        table(
            funnel[["ETAPA", "PEDIDOS", "TAXA"]].rename(
                columns={"ETAPA": "Stage", "PEDIDOS": "Orders",
                         "TAXA": "Share of placed"}),
            hide_index=True, width="stretch",
            column_config={"Share of placed": st.column_config.ProgressColumn(
                format="percent", min_value=0.0, max_value=1.0)},
        )

    with right:
        st.subheader("Leakage points")
        leak = numeric(query(ind["vazamento"].sql, P), ["PEDIDOS", "SOBRE_COLOCADOS"])
        table(
            leak.rename(columns={"MOTIVO": "Reason", "PEDIDOS": "Orders",
                                "SOBRE_COLOCADOS": "Share of placed"}),
            hide_index=True, width="stretch",
        )

    st.divider()
    st.subheader("Value loss, by cause")
    loss = numeric(query(ind["decomposicao_perda"].sql, P),
                     ["VALOR", "PEDIDOS_AFETADOS", "POR_PEDIDO"])
    table(
        loss[["CAUSA", "VALOR", "PEDIDOS_AFETADOS", "POR_PEDIDO"]].rename(columns={
            "CAUSA": "Cause", "VALOR": f"Amount ({currency})",
            "PEDIDOS_AFETADOS": "Orders", "POR_PEDIDO": f"Per order ({currency})"}),
        hide_index=True, width="stretch",
    )

    st.divider()
    st.subheader("Daily series")
    series = numeric(query(ind["serie_diaria"].sql, P),
                     ["ORDERS_PLACED", "ORDERS_DELIVERED", "NET_AMOUNT_PICKED",
                      "AMOUNT_DELTA", "TICKET_MEDIO", "DELIVERY_RATE"])
    series["ORDER_DATE"] = pd.to_datetime(series["ORDER_DATE"])
    series = series.rename(columns={
        "ORDER_DATE": "Date", "WH": "Warehouse",
        "ORDERS_PLACED": "Orders placed", "ORDERS_DELIVERED": "Orders delivered",
        "NET_AMOUNT_PICKED": "Fulfilled revenue", "AMOUNT_DELTA": "Amount variance",
        "TICKET_MEDIO": "Average ticket", "DELIVERY_RATE": "Delivery rate",
    })
    # Keyed by the day the order was PLACED — no MART has a delivery-date grain. A chart of
    # "delivered" over that axis showed a bar on Sunday, the day the operation never
    # delivers (those are Sunday ORDERS, delivered Monday to Wednesday), and was read as
    # Sunday deliveries even after relabelling. So the axis only carries what happened on
    # that day. Stacked, because orders and revenue add up across warehouses.
    e, d = st.columns(2)
    e.caption(f"Fulfilled revenue, by order date ({currency})")
    with e:
        daily_bars(series, "Date", "Fulfilled revenue", "Warehouse", "Order date",
                   value_format=",.2f")
    d.caption("Orders placed, by order date")
    with d:
        daily_bars(series, "Date", "Orders placed", "Warehouse", "Order date")
    table(series.rename(columns={"Date": "Order date",
                                 "Orders delivered": "Of which delivered (on a later day)"}),
          hide_index=True, width="stretch")
    st.caption("Every date here is the day the order was placed. Delivery is on a later "
               "day, Monday to Saturday — an order placed on Sunday is delivered from Monday.")

# ---------------------------------------------------------------- Operations
with tabs[1]:
    st.subheader("Picking SLA")
    sla = numeric(query(ind["sla_separacao"].sql, P),
                   ["LIMIAR_DECLARADO_MIN", "MAXIMO_OBSERVADO_MIN", "VIOLACOES",
                    "PEDIDOS_COM_SEPARACAO"])
    s = sla.iloc[0]
    a, b, cc, dd = st.columns(4)
    a.metric("Threshold", f"{s['LIMIAR_DECLARADO_MIN']:.0f} min")
    b.metric("Maximum observed", f"{s['MAXIMO_OBSERVADO_MIN']:.0f} min")
    cc.metric("Breaches", fmt_int(s["VIOLACOES"]))
    dd.metric("Orders with picking", fmt_int(s["PEDIDOS_COM_SEPARACAO"]))

    st.divider()
    # Stacked, not side by side: in a 3:2 split the stage table lost its Orders column.
    e = d = st.container()
    with e:
        st.subheader("Time per stage")
        pct = numeric(query(ind["percentis_etapa"].sql, P),
                       ["MEDIA_P50", "MEDIA_P90", "PEDIDOS"])
        pct["MEDIA_P50_H"] = pct["MEDIA_P50"] / 60
        table(
            pct[["ETAPA", "MEDIA_P50", "MEDIA_P90", "MEDIA_P50_H", "PEDIDOS"]].rename(columns={
                "ETAPA": "Stage", "MEDIA_P50": "average p50 (min)",
                "MEDIA_P90": "average p90 (min)", "MEDIA_P50_H": "average p50 (h)",
                "PEDIDOS": "Orders"}),
            hide_index=True, width="stretch",
        )
        st.caption("Delivery is the next operating day at the earliest, so most of the cycle "
                   "is the wait for the chosen slot, not handling time.")

    with d:
        st.subheader("Delivery window")
        win = numeric(query(ind["janela_entrega"].sql, P), ["ENTREGAS"])
        total = win["ENTREGAS"].sum()
        win["Share of deliveries"] = win["ENTREGAS"] / total if total else 0
        table(
            win[["RESULTADO", "ENTREGAS", "Share of deliveries"]].rename(
                columns={"RESULTADO": "Result", "ENTREGAS": "Deliveries"}),
            hide_index=True, width="stretch",
            column_config={"Share of deliveries": st.column_config.ProgressColumn(
                format="percent", min_value=0.0, max_value=1.0)},
        )

# ---------------------------------------------------------------- Basket
with tabs[2]:
    st.subheader("Revenue by category")
    cat = numeric(query(ind["receita_categoria"].sql, P),
                   ["RECEITA_APURADA", "VALOR_PEDIDO", "VALOR_PERDIDO",
                    "LINHAS_PEDIDAS", "UNIDADES_ENTREGUES"])
    cat = cat.drop(columns=["MOEDA"], errors="ignore").rename(columns={
        "CATEGORIA_NIVEL1": "Department", "CATEGORIA": "Category", "RECEITA_APURADA": "Fulfilled revenue",
        "VALOR_PEDIDO": "Order value", "VALOR_PERDIDO": "Lost value",
        "LINHAS_PEDIDAS": "Lines placed", "UNIDADES_ENTREGUES": "Units delivered",
        "DIAS": "Days",
    })
    top = cat.head(20)
    st.caption(f"Top 20 by fulfilled revenue ({currency}) — of {len(cat)} categories")
    st.bar_chart(top, x="Category", y="Fulfilled revenue", horizontal=True,
                 sort="-Fulfilled revenue")
    table(cat, hide_index=True, width="stretch")

    st.divider()
    st.subheader("Substitution and line removal")
    sub = numeric(query(ind["substituicao_categoria"].sql, P),
                   ["LINHAS_PEDIDAS", "LINHAS_SUBSTITUIDAS", "LINHAS_REMOVIDAS",
                    "LINHAS_NUNCA_SEPARADAS", "TAXA_SUBSTITUICAO", "TAXA_REMOCAO"])
    table(
        sub.rename(columns={
            "CATEGORIA": "Category", "LINHAS_PEDIDAS": "Lines placed",
            "LINHAS_SUBSTITUIDAS": "Lines substituted",
            "LINHAS_REMOVIDAS": "Lines removed",
            "LINHAS_NUNCA_SEPARADAS": "Lines never picked",
            "TAXA_SUBSTITUICAO": "Substitution rate", "TAXA_REMOCAO": "Removal rate",
        }),
        hide_index=True, width="stretch",
    )

    st.divider()
    st.subheader("Consumption profile by age band")
    profile = numeric(query(ind["perfil_por_faixa"].sql, P),
                      ["PCT_LT35", "PCT_35_49", "PCT_50_64", "PCT_GE65", "LINHAS_TOTAL"])
    profile = profile.rename(columns={
        "GRUPO": "Group", "PCT_LT35": "% <35", "PCT_35_49": "% 35-49",
        "PCT_50_64": "% 50-64", "PCT_GE65": "% ≥65", "LINHAS_TOTAL": "Total lines",
    })
    if not profile.empty:
        extremes = pd.concat([profile.head(8), profile.tail(8)])
        st.bar_chart(extremes, x="Group", y=["% <35", "% ≥65"], horizontal=True,
                     x_label="Demand group", y_label="Share of the age band's lines (%)")
    table(profile, hide_index=True, width="stretch")

    st.divider()
    st.subheader("Orders by warehouse")
    region = numeric(query(ind["pedidos_por_regiao"].sql, P),
                      ["DIAS", "LINHAS", "UNIDADES", "RECEITA"])
    table(
        region.drop(columns=["MOEDA"], errors="ignore").rename(columns={
            "ARMAZEM": "Warehouse", "DIAS": "Days", "LINHAS": "Lines",
            "UNIDADES": "Units", "RECEITA": "Revenue",
        }),
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Assortment
with tabs[3]:
    st.subheader("Assortment by warehouse")
    assortment = numeric(query(ind["sortimento_armazem"].sql, P),
                    ["DIAS_OBSERVADOS", "PRODUTOS_MEDIA_DIA", "PRODUTOS_MAXIMO_DIA",
                     "EXCLUSIVOS_MEDIA_DIA", "PRECO_MEDIO", "NOVIDADES_PERIODO"])
    table(
        assortment.rename(columns={
            "ARMAZEM": "Warehouse", "DIAS_OBSERVADOS": "Days observed",
            "PRODUTOS_MEDIA_DIA": "Products, avg/day",
            "PRODUTOS_MAXIMO_DIA": "Products, max/day",
            "EXCLUSIVOS_MEDIA_DIA": "Exclusive, avg/day",
            "PRECO_MEDIO": "Average price",
            "NOVIDADES_PERIODO": "Flagged new by Mercadona",
        }),
        hide_index=True, width="stretch",
    )

    st.divider()
    e = d = st.container()
    with e:
        st.subheader("Catalog movement")
        movement = numeric(query(ind["movimento_catalogo"].sql, P),
                       ["PRODUTOS", "PRODUTOS_DISTINTOS", "DIAS"])
        movement["TIPO_DE_MUDANCA"] = movement["TIPO_DE_MUDANCA"].map(CHANGE_TYPE).fillna(
            "First observation (no earlier snapshot)")
        table(
            movement.rename(columns={
                "TIPO_DE_MUDANCA": "Change type", "PRODUTOS": "Products",
                "PRODUTOS_DISTINTOS": "Distinct products", "DIAS": "Days",
            }),
            hide_index=True, width="stretch",
        )
    with d:
        st.subheader("Largest price changes")
        var = numeric(query(ind["variacao_preco"].sql, P),
                       ["PRECO_ANTERIOR", "PRECO", "VARIACAO", "VARIACAO_PCT",
                        "DIAS_DESDE_O_ANTERIOR"])
        table(
            # Every row here is a price change, and the absolute change is the two prices
            # side by side: both columns pushed the identity flag out of the frame.
            var.head(50).drop(columns=["TIPO_DE_MUDANCA", "VARIACAO"], errors="ignore")
            .rename(columns={
                "SNAPSHOT_DATE": "Date", "WH": "Warehouse", "PRODUTO": "Product",
                "CATEGORIA": "Category", "PRECO_ANTERIOR": "Before",
                "PRECO": "After", "VARIACAO_PCT": "Change %",
                "DIAS_DESDE_O_ANTERIOR": "Days apart",
                "IDENTIDADE_AMBIGUA": "Review identity",
            }),
            hide_index=True, width="stretch",
            # Fixed widths: a long product name ends in "…" inside its cell instead of
            # pushing the last columns out of the frame.
            column_config={"Product": st.column_config.TextColumn(width="large"),
                           "Category": st.column_config.TextColumn(width="medium")},
        )

# ---------------------------------------------------------------- Supply and demand
with tabs[4]:
    st.subheader("Supply x demand")
    sd = numeric(query(ind["oferta_demanda"].sql, P),
                  ["PRODUTOS_OFERTADOS", "PRODUTOS_PEDIDOS", "COBERTURA_DEMANDA",
                   "LINHAS_PEDIDAS", "RECEITA_APURADA"])
    sd = sd.rename(columns={
        "CATEGORIA": "Category", "PRODUTOS_OFERTADOS": "Products offered",
        "PRODUTOS_PEDIDOS": "Products ordered",
        "COBERTURA_DEMANDA": "Share of offered products ordered",
        "LINHAS_PEDIDAS": "Lines placed", "RECEITA_APURADA": "Fulfilled revenue",
    })
    st.scatter_chart(sd.head(60), x="Products offered", y="Fulfilled revenue",
                     size="Lines placed")
    table(
        sd,
        hide_index=True, width="stretch",
    )

# ---------------------------------------------------------------- Customers
with tabs[5]:
    st.caption("Current version of the base — the period filter doesn't apply on this tab.")
    e = d = st.container()
    with e:
        st.subheader("Customer base")
        base = numeric(query(ind["base_clientes"].sql), ["CLIENTES", "IDADE_MEDIA",
                                                            "MUNICIPIOS", "CEPS"])
        base["SEXO"] = base["SEXO"].replace({"Hombres": "Men", "Mujeres": "Women"})
        base = base.rename(columns={
            "ARMAZEM": "Warehouse", "FAIXA_ETARIA": "Age band", "SEXO": "Sex",
            "CLIENTES": "Customers", "IDADE_MEDIA": "Average age",
            "MUNICIPIOS": "Municipalities", "CEPS": "Postal codes",
        })
        st.bar_chart(base, x="Age band", y="Customers", color="Warehouse", stack=True)
        table(base, hide_index=True, width="stretch")
    with d:
        st.subheader("Municipal coverage")
        cov = numeric(query(ind["cobertura_municipal"].sql),
                       ["MUNICIPIOS_NA_AUF", "MUNICIPIOS_SEM_CLIENTE", "CLIENTES",
                        "POPULACAO_AUF", "CLIENTES_POR_10K"])
        table(
            cov.rename(columns={
                "ARMAZEM": "Warehouse", "PROVINCIA": "Province",
                "MUNICIPIOS_NA_AUF": "Municipalities in service area",
                "MUNICIPIOS_SEM_CLIENTE": "Municipalities without customers",
                "CLIENTES": "Customers",
                "POPULACAO_AUF": "Service area population",
                "CLIENTES_POR_10K": "Customers per 10,000 pop.",
            }),
            hide_index=True, width="stretch",
        )

# ---------------------------------------------------------------- Inventory
with tabs[6]:
    coverage = numeric(query(ind["cobertura_estoque"].sql, P),
                         ["UNIDADES_EM_ESTOQUE", "DIAS_DE_COBERTURA",
                          "COBERTURA_DO_PRODUTO_TIPICO", "PARES_PRODUTO_DIA"])
    stockout = numeric(query(ind["ruptura_estoque"].sql, P),
                       ["UNIDADES_PEDIDAS", "UNIDADES_ATENDIDAS", "UNIDADES_EM_FALTA",
                        "PRODUTOS_COM_FALTA", "PRODUTOS_NO_DIA", "TAXA_DE_ATENDIMENTO"])
    replenishment = numeric(query(ind["reposicao_estoque"].sql, P),
                         ["ORDENS_EMITIDAS", "UNIDADES_PEDIDAS_AO_FORNECEDOR",
                          "PRODUTOS_NO_DIA", "FRACAO_DE_PRODUTOS_REPONDO"])
    turnover = numeric(query(ind["giro_estoque"].sql, P),
                    ["GIRO_DIARIO", "UNIDADES_VENDIDAS", "SALDO_ABERTURA",
                     "SALDO_FECHAMENTO", "UNIDADES_EM_FALTA"])

    if stockout.empty:
        st.warning("No inventory data in the selected period. Widen the range.")
    else:
        demand_total = stockout["UNIDADES_PEDIDAS"].sum()
        fulfilled_total = stockout["UNIDADES_ATENDIDAS"].sum()
        short_total = stockout["UNIDADES_EM_FALTA"].sum()
        orders_total = replenishment["ORDENS_EMITIDAS"].sum() if not replenishment.empty else 0

        g1, g2, g3, g4 = st.columns(4)
        g1.metric("Units ordered", fmt_int(demand_total))
        g2.metric("Units short", fmt_int(short_total))
        g3.metric("Fulfillment rate",
                  f"{(fulfilled_total / demand_total * 100) if demand_total else 0:.2f}%")
        g4.metric("Replenishment orders issued", fmt_int(orders_total))

        st.divider()
        st.subheader("Stock coverage by category")
        worst = coverage.sort_values("DIAS_DE_COBERTURA", na_position="last").head(20)
        table(
            worst[["DIA", "ARMAZEM", "CATEGORIA", "UNIDADES_EM_ESTOQUE",
                   "DIAS_DE_COBERTURA", "COBERTURA_DO_PRODUTO_TIPICO",
                   "PARES_PRODUTO_DIA"]].rename(columns={
                "DIA": "Day", "ARMAZEM": "Warehouse", "CATEGORIA": "Category",
                "UNIDADES_EM_ESTOQUE": "Units in stock",
                "DIAS_DE_COBERTURA": "Days of coverage (category)",
                "COBERTURA_DO_PRODUTO_TIPICO": "Typical product coverage",
                "PARES_PRODUTO_DIA": "Product-day pairs",
            }),
            hide_index=True, width="stretch",
        )
        st.caption("20 lowest coverage figures in the period.")

        st.divider()
        st.subheader("Stockouts: units and categories affected")
        table(
            stockout.sort_values("UNIDADES_EM_FALTA", ascending=False).head(20)
                .drop(columns=["CATEGORY_ID"]).rename(
                columns={"DIA": "Day", "ARMAZEM": "Warehouse", "CATEGORIA": "Category",
                         "UNIDADES_PEDIDAS": "Ordered", "UNIDADES_ATENDIDAS": "Fulfilled",
                         "UNIDADES_EM_FALTA": "Short",
                         "PRODUTOS_COM_FALTA": "Products short",
                         "PRODUTOS_NO_DIA": "Products stocked",
                         "TAXA_DE_ATENDIMENTO": "Fulfillment rate"}),
            hide_index=True, width="stretch",
        )

        st.divider()
        e = d = st.container()
        with e:
            st.subheader("Replenishment: orders issued")
            table(
                replenishment.sort_values("ORDENS_EMITIDAS", ascending=False).head(20)
                    .drop(columns=["CATEGORY_ID"]).rename(
                    columns={"DIA": "Day", "ARMAZEM": "Warehouse", "CATEGORIA": "Category",
                             "ORDENS_EMITIDAS": "Orders",
                             "UNIDADES_PEDIDAS_AO_FORNECEDOR": "Units to supplier",
                             "PRODUTOS_NO_DIA": "Products stocked",
                             "FRACAO_DE_PRODUTOS_REPONDO": "Share reordering"}),
                hide_index=True, width="stretch",
            )
        with d:
            st.subheader("Daily turnover by category")
            table(
                turnover.sort_values("GIRO_DIARIO", ascending=False, na_position="last").head(20)
                    .drop(columns=["CATEGORY_ID"])
                    .rename(columns={"DIA": "Day", "ARMAZEM": "Warehouse",
                                      "CATEGORIA": "Category",
                                      "GIRO_DIARIO": "Daily turnover",
                                      "UNIDADES_VENDIDAS": "Units sold",
                                      "SALDO_ABERTURA": "Opening balance",
                                      "SALDO_FECHAMENTO": "Closing balance",
                                      "UNIDADES_EM_FALTA": "Units short"}),
                hide_index=True, width="stretch",
            )
