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

# Categorical hues in a FIXED order (validated for colour-blind separation, CR-019), and a
# neutral ink for an overall line, so it never looks like one more series. Streamlit's own
# default paired blue with light blue and red with pink: All/bcn1 and mad1/svq1 looked alike.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
OVERALL_INK = "#3b3b3b"


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


def full_height(rows: int) -> int:
    """A dataframe height that shows every row: a table that scrolls inside itself hides rows
    from the reader and from a screenshot alike (CR-019: 10 of 26 departments visible)."""
    return (rows + 1) * 35 + 3


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



def date_lines(df: pd.DataFrame, date: str, value: str, y_title: str,
               color: str | None = None, domain: list[str] | None = None,
               emphasis: str | None = None, reference: float | None = None,
               value_format: str = ",.2f") -> None:
    """Lines over REAL dates, a point on every collection, optionally a reference rule.

    The x axis is temporal on purpose (unlike `daily_bars`): collections are irregular, and a
    14-day gap must look like 14 days, not like the next bar. The points are the collections;
    the segment between two points is one link, never an observed price. Colors follow
    `domain` — the full list of series — so unticking a warehouse does not repaint the others.
    `reference=100` marks the base of an index.
    """
    shown = df.copy()
    shown["_day"] = pd.to_datetime(shown[date])
    shown["_full"] = shown["_day"].dt.strftime("%a %d %b %Y")
    tooltip = [alt.Tooltip("_full:N", title="Collection")]
    encoding = {
        "x": alt.X("_day:T", title="Collection date",
                   axis=alt.Axis(format="%d %b", labelAngle=0, tickCount="week")),
        "y": alt.Y(f"{value}:Q", title=y_title, scale=alt.Scale(zero=False),
                   axis=alt.Axis(format=value_format)),
    }
    if color:
        domain = domain or sorted(shown[color].dropna().unique().tolist())
        series = [d for d in domain if d != emphasis]
        colors = dict(zip(series, SERIES))
        if emphasis:
            colors[emphasis] = OVERALL_INK
        encoding["color"] = alt.Color(
            f"{color}:N", title=color,
            scale=alt.Scale(domain=domain, range=[colors.get(d, "#999999") for d in domain]),
            legend=alt.Legend(orient="top", labelLimit=0))
        tooltip.append(alt.Tooltip(f"{color}:N", title=color))
    tooltip.append(alt.Tooltip(f"{value}:Q", title=y_title, format=value_format))
    base = alt.Chart(shown).encode(tooltip=tooltip, **encoding)
    width = (alt.condition(alt.datum[color] == emphasis, alt.value(3), alt.value(1.5))
             if color and emphasis else alt.value(2))
    points = base.mark_point(filled=True, size=40)
    if not color:
        points = points.encode(color=alt.value(SERIES[0]))
    lines = base.mark_line().encode(strokeWidth=width)
    if not color:
        lines = lines.encode(color=alt.value(SERIES[0]))
    chart = lines + points
    if reference is not None:
        rule = alt.Chart(pd.DataFrame({"y": [reference]})).mark_rule(
            strokeDash=[4, 4], color="#888888", opacity=0.6).encode(y="y:Q")
        chart = rule + chart
    st.altair_chart(chart, width="stretch")


def change_bars(df: pd.DataFrame, label: str, value: str, x_title: str) -> None:
    """Horizontal bars of a signed change, sorted, rises and falls in two hues from zero.

    Polarity is the job, so it is two hues and a zero line — never a rainbow over the
    categories. The value is also in the tooltip, formatted like the tables.
    """
    shown = df.copy()
    shown["Direction"] = shown[value].map(lambda v: "Up" if v > 0 else ("Down" if v < 0 else "Flat"))
    chart = alt.Chart(shown).mark_bar(cornerRadiusEnd=3).encode(
        x=alt.X(f"{value}:Q", title=x_title, axis=alt.Axis(format="+,.1f")),
        y=alt.Y(f"{label}:N", title=None, sort=alt.EncodingSortField(value, order="descending"),
                axis=alt.Axis(labelLimit=320, labelOverlap=False)),
        color=alt.Color("Direction:N", scale=alt.Scale(domain=["Up", "Down", "Flat"],
                                                      range=["#c0504d", "#2f7ebc", "#999999"]),
                        title="Direction",
                        legend=alt.Legend(orient="top")),
        tooltip=[alt.Tooltip(f"{label}:N"), alt.Tooltip(f"{value}:Q", title=x_title, format="+,.2f")],
    ).properties(height=24 * len(shown))
    st.altair_chart(chart, width="stretch")


def dated_bars(df: pd.DataFrame, date: str, value: str, color: str, y_title: str) -> None:
    """Stacked bars on a TEMPORAL axis, for additive counts over irregular collections.

    `daily_bars` gives every day a category, right for nine consecutive order days; over a
    price history with gaps it would close them up. Here a missing collection is an empty
    stretch of axis, and the labels are one format ("05 Oct") — `st.bar_chart` printed
    "Sat 15 … Aug 23 … October … Oct 25" on the same axis (CR-019).
    """
    shown = df.copy()
    shown["_day"] = pd.to_datetime(shown[date])
    shown["_full"] = shown["_day"].dt.strftime("%a %d %b %Y")
    domain = sorted(shown[color].dropna().unique().tolist())
    chart = alt.Chart(shown).mark_bar(width={"band": 0.8}).encode(
        x=alt.X("yearmonthdate(_day):T", title="Collection date",
                axis=alt.Axis(format="%d %b", labelAngle=0, tickCount="week")),
        y=alt.Y(f"{value}:Q", title=y_title, axis=alt.Axis(format=",.0f")),
        color=alt.Color(f"{color}:N", title=color,
                        scale=alt.Scale(domain=domain, range=SERIES[:len(domain)]),
                        legend=alt.Legend(orient="top")),
        order=alt.Order(f"{color}:N"),
        tooltip=[alt.Tooltip("_full:N", title="Collection"), alt.Tooltip(f"{color}:N"),
                 alt.Tooltip(f"{value}:Q", title=y_title, format=",.0f")],
    )
    st.altair_chart(chart, width="stretch")


def paired_bars(df: pd.DataFrame, label: str, values: list[str], x_title: str) -> None:
    """Horizontal bars side by side, one per measure, for measures that must NOT be added.

    `st.bar_chart(horizontal=True)` stacks by default: the share of the under-35s' lines
    and the share of the over-65s' were drawn end to end, a bar whose length meant nothing.
    """
    shown = df[[label] + values].melt(id_vars=label, var_name="Measure", value_name="Value")
    chart = alt.Chart(shown).mark_bar().encode(
        x=alt.X("Value:Q", title=x_title, axis=alt.Axis(format=",.1f")),
        y=alt.Y(f"{label}:N", title=None, sort=None,
                axis=alt.Axis(labelLimit=320, labelOverlap=False)),
        yOffset=alt.YOffset("Measure:N", sort=values),
        color=alt.Color("Measure:N", title=None, sort=values,
                        scale=alt.Scale(domain=values, range=SERIES[:len(values)]),
                        legend=alt.Legend(orient="top")),
        tooltip=[alt.Tooltip(f"{label}:N"), alt.Tooltip("Measure:N"),
                 alt.Tooltip("Value:Q", title=x_title, format=",.2f")],
    ).properties(height=28 * df[label].nunique())
    st.altair_chart(chart, width="stretch")
