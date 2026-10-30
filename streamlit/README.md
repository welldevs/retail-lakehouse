# Dashboards (Streamlit over the MART)

Two panels, one shared indicator catalog. The panels are **consumers**: every query, its
question, grain, data kind and caveats live in
[`retail_platform/indicators/`](../platform/src/retail_platform/indicators/), and the
reviewable pages are generated into [`docs/indicators/`](../docs/indicators/).

```bash
make dashboard-venv        # once: streamlit/pandas/altair (an extra, outside the Airflow image)
make dashboard             # operations panel
make price-dashboard       # price-watch panel
make indicators-contract   # regenerates docs/indicators/ from the catalog (no connection)
make dashboard-check       # renders both panels against the live warehouse (AppTest)
```

| File | Role |
|---|---|
| `operations_app.py` | Orders, fulfillment, basket, assortment, stock, customer base |
| `prices_app.py` | Mercadona catalog price movement — drops, rises, volatility, per-product history |
| `connection.py` | Session as `retail_reader`, shared by both panels |
| `display.py` | One number format for both panels — English notation in KPIs and tables, shares as %, a dash for null |
| `smoke.py` | End-to-end render check with `AppTest`, for one or both panels |

## Decisions

**Wears `retail_reader`, and proves it.** The BI role reads MART and nothing else;
`make dashboard-check` runs a live probe confirming the refusal on `GOLD` and `STAGE`.

**Reads live, with the clock visible.** 60 s cache TTL and a button that clears it; the
sidebar shows the read time and each mart's row count and window, so a fresh load appears as
a *base change*, not as an unexplained number.

**Money columns are `purchasable_*`.** For weight-sold products without a `unit_size`, the
source's `unit_price` is the selector's ceiling (×99), not a price anyone pays.

**"Offer" means an observed price drop.** There is no promotional flag in the warehouse —
the source's `price_decreased` is false on every row. A drop is a real, observed decrease,
never a confirmed campaign.

**No mart joins customer with order.** The link exists in GOLD (`FACT_ORDER.customer_sk`),
out of `retail_reader`'s reach by design. Closing it needs a customer-grain mart, not a new
source — see "What the catalog does NOT answer" in `docs/indicators/operations.md`.
