"""One number format for both panels — KPIs and tables (CR-017 review).

The operations panel showed 211.367 in a KPI, 211367 in a table and 0.0294 next to 94.58%:
three notations on one screen, plus the word None in an empty cell. English notation is the
project's since CR-014. Shared here so the two panels cannot drift apart again.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

# Silver's `change_type` codes, as a reader should see them. A null is the first snapshot of
# a series: there is nothing earlier to compare it with.
CHANGE_TYPE = {"estavel": "Unchanged", "preco_alterado": "Price changed",
               "entrou": "Entered the catalog", "saiu": "Left the catalog"}


def fmt_int(value) -> str:
    """English notation (7,285,229). A missing value is a dash, never "nan"."""
    if value is None or pd.isna(value):
        return "—"
    return f"{int(value):,}"


def fmt_currency(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.2f}"


def table(df: pd.DataFrame, column_config: dict | None = None, **kwargs) -> None:
    """`st.dataframe` with ONE number format for the whole panel.

    Without it the KPIs said 211,367, the tables 211367 and the shares 0.0294 next to 94.58%:
    three notations on one screen, and an empty cell rendered as the word None. Display-only:
    the Styler formats the text and sorting still uses the underlying numbers. Columns with
    their own `column_config` (progress bars) are left to it; identifiers are not grouped.
    """
    configured = set(column_config or {})
    shown = df.copy()
    formats = {}
    for column in shown.columns:
        if column in configured:
            continue
        # A day stored as a timestamp printed "2026-08-24 00:00:00" in every row.
        if pd.api.types.is_datetime64_any_dtype(shown[column]):
            values = shown[column].dropna()
            if len(values) and (values == values.dt.normalize()).all():
                shown[column] = shown[column].dt.strftime("%Y-%m-%d")
            continue
        if shown[column].dtype == object:
            converted = pd.to_numeric(shown[column], errors="coerce")
            if converted.notna().sum() and converted.notna().sum() == shown[column].notna().sum():
                shown[column] = converted
        if not pd.api.types.is_numeric_dtype(shown[column]):
            # A null text cell (the first snapshot has no change type) printed as None.
            shown[column] = shown[column].where(shown[column].notna(), "—")
            continue
        name = str(column).lower()
        if "id" in name.split() or name.endswith("_id") or name in ("year",):
            formats[column] = "{:.0f}"
            continue
        if name.startswith("share") or name.endswith("rate") or "rate " in name:
            formats[column] = "{:.2%}"
        elif pd.api.types.is_integer_dtype(shown[column]) or (
            shown[column].dropna() % 1 == 0
        ).all():
            formats[column] = "{:,.0f}"
        else:
            formats[column] = "{:,.2f}"
    # Streamlit draws a null cell as the word None whatever the Styler says, so a numeric
    # column WITH nulls (a TOTAL row's "per order") becomes formatted text with a dash. Only
    # those columns lose numeric sorting.
    for column, pattern in list(formats.items()):
        if shown[column].isna().any():
            shown[column] = shown[column].map(lambda v, p=pattern: "—" if pd.isna(v) else p.format(v))
            del formats[column]
    st.dataframe(shown.style.format(formats), column_config=column_config, **kwargs)


def daily_bars(df: pd.DataFrame, date: str, value: str, color: str, axis_title: str,
               value_format: str = ",.0f") -> None:
    """Stacked bars, one per day, with ONE date label format and grouped numbers.

    `st.bar_chart` lets Vega pick the labels: "Mon 24 … Aug 30, Mon 31, September" — the
    Sunday and the first of the month looked like a different kind of day — and the tooltip
    printed 8848 next to KPIs that say 8,848. Every day of the window has a bar, so the axis
    is a category per day ("Sun 30"), in date order.
    """
    shown = df.copy()
    shown["_day"] = pd.to_datetime(shown[date])
    # Short on the axis so all nine days fit; the full date is in the tooltip.
    shown["_label"] = shown["_day"].dt.strftime("%a %d")
    shown["_full"] = shown["_day"].dt.strftime("%a %d %b %Y")
    order = shown.sort_values("_day")["_label"].drop_duplicates().tolist()
    chart = alt.Chart(shown).mark_bar().encode(
        x=alt.X("_label:N", sort=order, title=axis_title, axis=alt.Axis(labelAngle=0)),
        y=alt.Y(f"{value}:Q", title=value, axis=alt.Axis(format=",.0f")),
        color=alt.Color(f"{color}:N", title=color),
        order=alt.Order(f"{color}:N"),
        tooltip=[alt.Tooltip("_full:N", title=axis_title),
                 alt.Tooltip(f"{color}:N", title=color),
                 alt.Tooltip(f"{value}:Q", title=value, format=value_format)],
    )
    st.altair_chart(chart, width="stretch")
