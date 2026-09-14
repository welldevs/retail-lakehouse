# Two dashboards over the MART

Two SEPARATE screens, same session (`connection.py`, role `retail_reader`), each with its
own indicator catalog — because the questions each one answers are different.

## Operations — a verification bench for the indicators **before** rebuilding them in Power BI

```bash
make dashboard            # http://localhost:8501
make dashboard-contract   # regenerates CONTRACT.md from indicators.py
```

First time: `make dashboard-venv` installs `streamlit`, `pandas`, `pyarrow` and `altair`
into the platform's venv. They live in `[project.optional-dependencies]` of
`platform/pyproject.toml`, **outside** `dependencies` — `infra/Dockerfile.airflow` installs
exactly that list, and ~150 MB of UI has no business in an image that renders no dashboard
at all.

## What's here

| File | Role |
|---|---|
| `indicators.py` | **The single source.** SQL and explanation for each indicator, in the same place |
| `CONTRACT.md` | **Generated** from `indicators.py`. The verification document |
| `contract.py` | The generator. Connects to nothing — the CONTRACT is reviewable with no credential |
| `connection.py` | `retail_reader` session on the local Postgres, via `set role` — **shared by both dashboards** |
| `app.py` | The operations dashboard's interface |

## Three decisions this dashboard makes

**Wears `retail_reader`, and proves it on screen.** The BI role reads MART and nothing
else. The dashboard runs a live probe confirming the refusal on `GOLD` and `STAGE` — a
dashboard that claims to respect a boundary without demonstrating it is asking for trust.
It's the first time this role is worn by a real consumer; the load and dbt already wore the
other two.

**Reads live, with the clock visible.** The cache has a 60 s TTL and a button that clears
it. The sidebar shows the read time and the row count and window of **each** mart, so a
fresh load appears as a *base change*, not as a different number with no explanation. Run
`make warehouse-refresh` with the dashboard open and click **Reread the destination now**.

**The pitfalls don't live in a footnote.** Each indicator carries its own, sourced from the
same module that carries the SQL — so the warning can't age relative to the query.

## What the dashboard does not show

It's in the *Out of reach* tab and in `CONTRACT.md`, with each item's trigger: margin,
stock, repeat purchase/LTV/cohort, route, market penetration, trend.

The most actionable gap: **no mart joins customer with order.** The link exists in
`FACT_ORDER.customer_sk`, in GOLD, out of `retail_reader`'s reach by design. Closing it
requires no new source — it requires a new customer-grain mart with order measures.

## Price-watch — an EXCLUSIVE panel for Mercadona catalog price oscillation

```bash
make price-dashboard            # http://localhost:8502
make price-dashboard-contract   # regenerates PRICE_CONTRACT.md from price_indicators.py
make price-dashboard-check      # runs the real dashboard (AppTest), needs a live account
```

| File | Role |
|---|---|
| `price_indicators.py` | Its own catalog of 9 indicators — imports `Indicador`, `MART`, `FILTRO_DATA_SNAP` and `FILTRO_WH` from `indicators.py` instead of duplicating them |
| `PRICE_CONTRACT.md` | Generated from `price_indicators.py` |
| `price_contract.py` | The generator — separate from `contract.py` because the two catalogs already diverge in shape |
| `price_app.py` | The interface |
| `price_smoke.py` | Smoke test (`AppTest`) — sibling of `smoke.py`, against `price_app.py` |

**There is no "offer"/"promotion" flag anywhere in the warehouse.**
`fact_price_change.sql` measured that the source's own flag (`price_decreased`) is false on
100% of rows even when 152 prices changed between two partitions — proven useless, not
merely unused. Every "offer" this panel shows is a `change_type = 'preco_alterado'` row with
`purchasable_price_delta < 0`: a REAL, OBSERVED price drop, never a confirmed promotional
campaign. The "Drops" tab says so on screen.

**No new mart.** `mart_price_evolution` already carries the pair (this snapshot, the
previous one) with the delta and the day gap between them; `mart_assortment_daily` already
carries the daily aggregate by category. The two already answer every question this panel
asks — a third mart for a screen that only reads what two marts already compute would be
the premature abstraction this project refuses elsewhere.

**Its own test, not a trimmed-down `test_dashboard_indicators.py`.**
`platform/tests/test_price_dashboard_indicators.py` checks the same properties (grain/type/
pitfall declared, every parameter bound, the dashboard reads only MART), but swaps the
bidirectional "every mart on disk is used" check for
`test_o_painel_usa_exatamente_os_dois_marts_declarados` — this panel is exclusive by
design, and proving "only these two got in" is the right property, not its opposite.
