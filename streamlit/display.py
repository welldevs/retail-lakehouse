"""One number format for both panels — KPIs and tables (CR-017 review).

The operations panel showed 211.367 in a KPI, 211367 in a table and 0.0294 next to 94.58%:
three notations on one screen, plus the word None in an empty cell. English notation is the
project's since CR-014. Shared here so the two panels cannot drift apart again.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


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
        if shown[column].dtype == object:
            converted = pd.to_numeric(shown[column], errors="coerce")
            if converted.notna().sum() and converted.notna().sum() == shown[column].notna().sum():
                shown[column] = converted
        if not pd.api.types.is_numeric_dtype(shown[column]):
            continue
        name = str(column).lower()
        if "id" in name.split() or name.endswith("_id") or name in ("year",):
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
