# Backlog — what didn't make it in, and what would let it in

This file exists for one simple rule: **a new idea goes here, not into the code.** After
Phase 7 the project is frozen, and "it would be nice to have X" stops being a reason to open
a new phase.

Each item carries the **trigger** — the concrete condition that would unlock it. Absence
without a trigger is an excuse; with a trigger it's a decision. Same discipline as the
not-adopted-technologies table in [ARCHITECTURE.md](ARCHITECTURE.md) and the *Out of scope*
list in the dashboard.

**Nothing here is hidden debt.** What is debt is declared as debt in ARCHITECTURE, with a
date. What's here is scope that was never promised.

---

## Needs a new source

These don't depend on effort, but on data that doesn't exist in this repository.
Approximating them would produce a plausible, invented number — the one class of error no
test catches.

| Item | Why not | Trigger |
|---|---|---|
| **Route, distance, travel time, delivery optimization** | The INE Callejero has no coordinates or adjacency. `minutes_to_delivered_*` is a declared assumption, and the seed itself says it is **not** route time. | A source with geometry: CartoCiudad/IGN, or OSM. |
| **Margin, profit, COGS** | The Mercadona API exposes **selling** price, never acquisition cost. No other source has cost. | A per-product cost source. |
| **Seasonality, YoY, monthly comparison** | The observed catalog covers days, not months. A trend over 9 days measures noise. | A months-long catalog window — which requires running the extraction over months, not a modeling decision. |
| **Observed inventory** | No source in this repository measures actual stock. Phase 7's ledger is **calculated** from observed consumption plus a declared policy, and the `synthetic` label travels with every row. | A stock-balance or stock-movement source. |

---

## Doesn't need a new source — needs work

These are actionable with what already exists. They're out because the project closed, not
because they're impossible.

### RFM, LTV, repeat purchase, customer cohort

**Why not.** No mart joins customer with order. `MART_CUSTOMER_BASE` has customer without
order; `MART_ORDER_FUNNEL` and `MART_BASKET_DAILY` have aggregated order without customer.
The link exists in `FACT_ORDER.customer_sk`, in GOLD — which `RETAIL_READER` can't reach, by
design.

**Trigger.** A new mart with customer grain and order measures (candidate:
`MART_CUSTOMER_ORDERS`). **This is the most actionable gap on this list** and needs no new
source.

**Declared trade-off.** It exposes a trap: a customer × order mart invites reading LTV off a
**synthetic** base as if it were market behavior. The label would need to travel in every
column, as it does in `MART_MARKET_COVERAGE`.

### Non-food mix: cleaning vs personal care vs pets

**Why not.** `NO_FOOD` is ONE demand group, sized by the declared `food_line_share`, and the
product inside it is chosen by popularity weight (CR-017). That cut perfume's revenue to a
third, but cheap own-brand cosmetics still lead the non-food ranking and toilet paper and
detergent sit outside the top 10 — the catalog simply has far more cosmetics SKUs. MAPA does
not cover non-food, so the calibration that fixed the food mix has no anchor here.

**Trigger.** The INE household budget survey measures it: EPF table **75006** (5-digit
COICOP 2018) gives household spend on `05.6.1.1` cleaning products, `13.1.1.3` personal care,
`09.3.2.2` pet articles and `06.1` pharmacy. Splitting `NO_FOOD` into those blocks, with a
value-share target per block, is the same mechanism as the MAPA groups.

**Declared trade-off.** `13.1.1.3` lumps perfume, shampoo and make-up together: the EPF would
fix cleaning against personal care, never perfume against shampoo. Saying more than that would
be inventing the split.

### Second-pass order generator

**Why.** The stockout the ledger calculates is **independent** of the order's removed lines.
The generator removes a line at a fixed sampled rate, with reason `unavailable` — a name
chosen precisely because there was no stock balance when it was written. One doesn't cause
the other, and crossing them as if they did would produce an invented correlation.

**Trigger.** A generator that re-reads the previous day's balance before deciding on removal.

**Trade-off, and it's what holds the item back.** This **inverts the project's dependency
direction**: today an order generates stock movement; there would be a cycle between the two
domains. It's not cheap and it's not innocent — hence declared here instead of approximated.

### Dimension structures that were never needed

`BRIDGE_PRODUCT_CATEGORY` (a product appears in more than one category; today Gold uses
`primary_category_id`), `DIM_CENSUS_SECTION`, and `DIM_ADDRESS`. **Trigger:** a business
question that requires the grain they offer. Building them before that would be modeling
against a hypothesis.

---

## Operations and infrastructure

| Item | Why not | Trigger |
|---|---|---|
| **Debezium / Kafka Connect** | The outbox already delivers the event in the same transaction as the state change, and `wal_level=logical` is already on in `oltp-postgres`, precisely so no restart is needed when this gets plugged in. | Needing to capture changes from tables **outside** the outbox design. |
| **Observability: cross-service trace correlation and real-time alerting (OpenTelemetry, Grafana, Prometheus)** | CR-004 measured the six required signals (`make observability-prove-signals`) and closed the ones with a real gap using what already existed — no collector, no new service. What's left is correlating Kafka/Postgres/dbt/Airflow in one trace, and paging someone in real time; neither has a proven consumer. A dashboard existing just to generate a screenshot is still the opposite of the point. | A concrete incident that needs correlating those services in one trace, or an operator role that needs to be paged. Only then `application → OTel → Collector → backend`. |
| **`FACT_INGESTION_RUN` coverage for `ine_population_api`/`ine_callejero`** | CR-004 measured the gap: no `wh` axis, no daily reload (population is yearly, Callejero is a manual download), and no consumer today asks "was this observed?" for these two sources the way price series already does for the catalog. Building two more manifest models and widening the STAGE union has a real cost with no measured need. | A reconciliation test or a mart that needs to distinguish "population not observed" from "population is zero" for these two sources. |
| **`dbt build`'s own `run_results.json`** | Latency, status and rows-affected per model already exist, generated free on every `dbt build`, in `target/` (gitignored, ephemeral). Nothing reads it today — found while proving CR-004's signals, not asked for. | A real, measured need to debug `dbt build` latency or a specific model's failure pattern across runs — not "it's already there, might as well use it." |
| **A real Spark cluster** | The job runs `local[*]`: driver and executor in the same JVM. There's no shuffle across nodes, and a fake cluster would prove neither scale nor interoperability — it would prove that compose can bring up containers. | A volume that doesn't fit on one node. `make spark-evidence` publishes the measurement saying it hasn't arrived yet. |

---

## How an item leaves this list

Through the same path as any change after the freeze: a **change request**, using the
template that closes [DECISIONS.md](DECISIONS.md) — need, evidence, impact on contracts,
tests that might break, and the recorded decision.

What does **not** count as justification, and the list is literal:

> "it's used in the industry" · "it looks more professional" · "it's a best practice" ·
> "companies use it" · "might be useful someday" · "looks good on a résumé"
