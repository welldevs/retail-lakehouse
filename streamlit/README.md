# Dashboards (Streamlit over the MART)

Three panels, one shared indicator catalog. The panels are **consumers**: every query, its
question, grain, data kind and caveats live in
[`retail_platform/indicators/`](../platform/src/retail_platform/indicators/), and the
reviewable pages are generated into [`docs/indicators/`](../docs/indicators/).

```bash
make dashboard-venv        # once: streamlit/pandas/altair (an extra, outside the Airflow image)
make dashboard             # operations panel
make price-dashboard       # price-watch panel
make price-evolution       # price-evolution panel
make indicators-contract   # regenerates docs/indicators/ from the catalog (no connection)
make dashboard-check       # renders the three panels against the live warehouse (AppTest)
```

| File | Role |
|---|---|
| `operations_app.py` | Orders, fulfillment, basket, assortment, stock, customer base |
| `prices_app.py` | Mercadona catalog price movement — drops, rises, volatility, per-product history |
| `evolution_app.py` | Mercadona catalog price evolution — a chained index, start against end, the gap between warehouses |
| `connection.py` | Session as `retail_reader`, shared by every panel |
| `display.py` | One number format and chart style for every panel — English notation, shares as %, a dash for null, dates on a single label format, fixed series colors |
| `smoke.py` | End-to-end render check with `AppTest`, per panel; fails on raw column names and on codes shown as values |

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

**Evolution is not the sum of events.** Price Watch counts moves; Price Evolution measures how
far prices travelled. An offer that goes and comes back is two events there and zero here.
The index is a chained Jevons (geometric mean of matched price ratios between consecutive
collections), with no consumption weights: borrowing them from the simulated orders would
make an observed index synthetic.

**No mart joins customer with order.** The link exists in GOLD (`FACT_ORDER.customer_sk`),
out of `retail_reader`'s reach by design. Closing it needs a customer-grain mart, not a new
source — see "What the catalog does NOT answer" in `docs/indicators/operations.md`.
