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
| **Day-of-week cadence** | `daily_order_rate` is fixed by construction, so the daily order count per warehouse is constant. There's no Monday effect and no weekend effect, and inventing one would mean picking a curve nobody measured. | A source that measures weekly purchase cadence in Spanish grocery retail. |

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

### The projection rebuild is O(n²)

**Measured on 2026-09-01.** `orders-rebuild-projection` rebuilt 206,523 orders in **414
commits and ~55 minutes**. Each commit is a pyiceberg `upsert` against the whole table, so
the per-batch cost grows with what's already been written. In Phase 3, with 6,400 orders,
this took seconds and was invisible.

**This is the one place in the project where volume actually hurt** — and the irony is worth
noting: Spark's justification says the volume trigger never fired, and it fired here, on the
Python path.

**The trigger, which is also the fix.** When `--reset` is used, the table starts **empty**
and there's no concurrent writer: there's nothing to `upsert` against. A single-batch
`append` path in that case trades 414 commits for a handful, and the cost goes back to
linear. It wasn't done because the regeneration is one-off and happens before the freeze —
but **on a fresh machine it's a 55-minute tax**, and that's reason enough for the item to sit
here instead of disappearing.

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
| **CI** | No remote exists. `make test` runs offline in 15 s and is the same thing a CI would run. | The repository getting a remote. |
| **Debezium / Kafka Connect** | The outbox already delivers the event in the same transaction as the state change, and `wal_level=logical` is already on in `oltp-postgres`, precisely so no restart is needed when this gets plugged in. | Needing to capture changes from tables **outside** the outbox design. |
| **Observability: cross-service trace correlation and real-time alerting (OpenTelemetry, Grafana, Prometheus)** | CR-005 measured the six required signals (`make observability-prove-signals`) and closed the ones with a real gap using what already existed — no collector, no new service. What's left is correlating Kafka/Postgres/dbt/Airflow in one trace, and paging someone in real time; neither has a proven consumer. A dashboard existing just to generate a screenshot is still the opposite of the point. | A concrete incident that needs correlating those services in one trace, or an operator role that needs to be paged. Only then `application → OTel → Collector → backend`. |
| **`FACT_INGESTION_RUN` coverage for `ine_population_api`/`ine_callejero`** | CR-005 measured the gap: no `wh` axis, no daily reload (population is yearly, Callejero is a manual download), and no consumer today asks "was this observed?" for these two sources the way price series already does for the catalog. Building two more manifest models and widening the STAGE union has a real cost with no measured need. | A reconciliation test or a mart that needs to distinguish "population not observed" from "population is zero" for these two sources. |
| **`dbt build`'s own `run_results.json`** | Latency, status and rows-affected per model already exist, generated free on every `dbt build`, in `target/` (gitignored, ephemeral). Nothing reads it today — found while proving CR-005's signals, not asked for. | A real, measured need to debug `dbt build` latency or a specific model's failure pattern across runs — not "it's already there, might as well use it." |
| **A real Spark cluster** | The job runs `local[*]`: driver and executor in the same JVM. There's no shuffle across nodes, and a fake cluster would prove neither scale nor interoperability — it would prove that compose can bring up containers. | A volume that doesn't fit on one node. `make spark-evidence` publishes the measurement saying it hasn't arrived yet. |

---

## How an item leaves this list

Through the same path as any change after the freeze: a **change request**, using the
template that closes [DECISIONS.md](DECISIONS.md) — need, evidence, impact on contracts,
tests that might break, and the recorded decision.

What does **not** count as justification, and the list is literal:

> "it's used in the industry" · "it looks more professional" · "it's a best practice" ·
> "companies use it" · "might be useful someday" · "looks good on a résumé"
