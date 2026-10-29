"""The indicator catalog: every business question the warehouse answers, as data.

One catalog, several consumers. Each `Indicator` carries the question, the grain, whether
the data is observed or synthetic, the exact SQL over MART, and the caveats. The Streamlit
panels render it; `docs/indicators/` is generated from it; the LLM layer will retrieve
from it. Nothing here touches a database — a consumer brings the connection.

    operations — orders, fulfillment, basket, assortment, stock, customer base
    prices     — Mercadona catalog price movement
"""
