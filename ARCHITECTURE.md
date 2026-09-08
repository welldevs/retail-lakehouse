# Architecture — the current state

What the platform **is today**: the layers, the engine, the technologies adopted with the
date and the trigger for each one that's still absent, the measured constraints that shaped
the design, and the declared technical debt.

**This document holds state. The history lives in [DECISIONS.md](DECISIONS.md)** — what was
decided, against what evidence, and what was lost. The separation is verified by test, and
exists because the two were growing together: the same text appeared in both places and aged
at different rates. Three documentation revisions were spent on this.

**Where a number can live.** Structural — grain, invariant, ratio by construction — can stay
here. A property **of this capture** (counts, measured percentages) only in a generated page,
or explicitly dated. RAW is not reproducible; on a new machine the numbers change, and
[`docs/FREEZE.md`](docs/FREEZE.md) is what names the capture that produced the ones published.

**Wall-clock time is the extreme case of this rule**, and it has already produced divergence
between two documents in this repository: the same command measured twice gives two numbers.
That's why the prose here carries the **magnitude** — "seconds", "~1,5 s" — and the decimal
lives only in the generated page that measured it. See
[DECISIONS.md § "What the measurement publishes against Spark"](DECISIONS.md).

Last revised: **2026-09-02** — documentation closeout for Phase 7. The technical state is
that of 2026-09-01; what changed afterward was only documentation, evidence, and the
structure of the debt.

## Real scale

**Measured on 2026-08-24**, over the three catalog partitions that existed then. It's the
scale that justified the engine, and it's still the right question — volume never grew
enough to change the answer:

| Metric | Value |
|---|---|
| Volume per partition | ~8,3 MB, 152 files |
| Rows per partition | ~4.600 (4.329 unique products) |
| Growth | ~3 GB/year if run every day |
| Duration of one extraction | 227 s (152 requests at 1,5 s) |
| Full Silver (3 partitions, 4 models, 36 tests) | ~6 s |

**Where it stands today, measured on 2026-09-01**, in the capture sealed by
[`docs/FREEZE.md`](docs/FREEZE.md) (`capture_id cec10cb5…`):

| Metric | Value |
|---|---|
| Customers (`silver_customer`) | 286.826, across 4 AUFs |
| Orders, 9-day window | 206.523 · 3.892.062 rows · 1.433.723 events |
| Stock ledger | 173.970 rows · 17.397 series · 10 days |
| Sealed RAW | 81 partitions · 2.221.069 records |
| `make silver` | 25 models, 16 seeds, 391 nodes — **53 s** |
| `make warehouse` | 24 models, 199 nodes — **131 s** |
| Python suites, offline | 1.095 |

**No distributed technology is justified by this volume, and that's a result, not an
assumption.** Orders grew 32× since Phase 3 measured them (6.400 → 206.523), and the full
Silver `dbt build` still runs in under a minute. The basket self-join — the natural
candidate for "too big for one node" — doubled to 37,9 M pairs and still runs in **~1,5 s**
(the current decimal is in [`docs/spark-evidence/`](docs/spark-evidence/README.md)).

**Where volume HURT, and it's a single place.** Rebuilding the Iceberg projection cost **414
commits and ~55 min** for the 206.523 orders: each batch does an `upsert` against the entire
table, so the cost grows with what's already been written. In Phase 3, with 6.400 orders,
this took seconds and was invisible. The irony is worth writing down: Spark's justification
says the volume trigger didn't fire, and it fired here — in the Python path, not in the
analysis. Correction declared in [BACKLOG.md](BACKLOG.md).

What follows is not a refusal — it's the condition under which each technology becomes
worth it.

## Adopted layers

```
L0  Source        API -> canonical partition on disk, manifest, validate
                  5 FROZEN packages (dependencies = []) in sources/
                  4 deliver a SNAPSHOT; simulated_orders delivers an EVENT LOG
L1  RAW           byte-identical partition in object storage, sha256 checked post-PUT
                  s3://retail-raw/mercadona_catalog_api/ingestion_date=…/wh=…/
    ┌─────────────── operational plane, on demand (`make stream-up`) ────────────────┐
    │ OLTP    Postgres `oltp`: orders, order_line, outbox                            │
    │         `orders-apply` — state + outbox in the SAME transaction, per event     │
    │         the outbox reconstitutes the log byte for byte (manifest sha256)       │
    │ Broker  Kafka  retail.orders.events.v1  (4 partitions, key = order_id)         │
    │         `orders-publish` — at-least-once, and the window is demonstrated       │
    │ Read    Postgres `projection` OR Iceberg `projection.live_order_state`         │
    │ model   `orders-project` — dedup by (order_id, sequence_no)                    │
    │         `orders-rebuild-projection` — the SECOND writer of the table           │
    │         monotonic merge: whatever doesn't advance `last_sequence_no` drops out │
    └────────────────────────────────────────────────────────────────────────────────┘
L2  Silver        typed parquet + 1 temporal model             · DuckDB
                  s3://retail-lakehouse/silver/…                7.098.881 rows · 205 MB
─────────────────── physical boundary: COPY INTO, never ref() ───────────────────
L3  Stage         1:1 mirror of a CUT of the Silver             · Snowflake
                  RETAIL.STAGE.STG_*                              3.327.809 rows (46,9%)
L4  Gold          conformed DIM_* / FACT_*, SCD2                · dbt-snowflake
L5  Mart          MART_*, grain declared per table                 · dbt-snowflake
```

RAW is the point of no return: everything downstream is reconstructible from it without
touching the API again. This matters more here than in the general case, because **this
source doesn't allow rereading the past** — see "Backfill" below.

The L2→L3 boundary is **physical, not logical**, and it's the same mechanism as the
Source↔platform boundary one level up: no Snowflake model can `ref()` a Silver model,
because different adapters don't cross within a single dbt run. The link is `COPY INTO`
plus `source()`. Snowflake is, in practice, **the fourth consumer that doesn't reach the
Lakehouse** — the other three are the FROZEN Sources.

## Engine: DuckDB + dbt-duckdb

Adopted. In-process, reads MinIO directly via `httpfs`, writes parquet, runs the 4 models in
~6 s. The hive layout the Source already produces (`ingestion_date=…/wh=…`) is consumed
with `hive_partitioning=1`, so `ingestion_date` and `wh` become columns **with no manual
parsing** — contract obligation 4.3 comes for free.

The dbt SQL is the portable asset: the same models run on `dbt-spark` and `dbt-snowflake`
without rewriting. That's what makes the swaps below configuration, not a new project.

## What didn't get in, and when it gets in

| Technology | What it buys | Why not now | Trigger | Where the swap happens |
|---|---|---|---|---|
| **Iceberg** | Snapshot isolation between concurrent writers, time travel, interop between engines | — | — | **Adopted on 2026-08-28** (Phase 3, Milestone 6). The trigger that fired was the literal one — *"a second engine needing to write the same table"*: `live_order_state` is written by the streaming consumer and by the batch rebuild, with DuckDB reading while both write. **It fired due to CONCURRENCY, not volume** — at this volume an atomic `os.replace` parquet would do. Preceded by `make spike-iceberg`, a closed experiment that measured catalog, upsert, conflict, isolation and reading by DuckDB before the projection existed. The old trigger (`dim_product` SCD2 via `MERGE`) still hasn't fired: the SCD2 is derived from the full history, not accumulated. |
| **Kafka** | Event transport, replay, decoupling point | — | — | **Adopted on 2026-08-28** (Phase 3, Milestone 5). The trigger that fired was the literal one — *"CDC of an OLTP"*: the event is born in the same transaction that changes the order (Milestone 4) and a stateful consumer keeps a read model below the batch. What got proven was **transport semantics**: at-least-once demonstrated by reproducing the duplication window, idempotent consumption with no ever-growing set, a gap refused, replay with no effect, 16 sha256 reproduced. **Not** proven, and it's written down: that anyone needs the latency, and that the broker is the origin — the canonical log is still born on disk. |
| **Spark** | An engine outside Python writing the catalog, and a form of computation SQL doesn't express | — | — | **Adopted on 2026-09-01** (Phase 7). **The declared trigger did NOT fire, and that's measured**: the basket self-join — 37,9 M pairs, the natural candidate for "a partition DuckDB can't hold" — runs in **~1,5 s and ~2,4 GB** on one node. The final window doubled that number relative to the intermediate one (18,3 M) and the time stayed in seconds, which makes the claim stronger, not weaker. It got in for two other reasons. **First, interop:** Iceberg was justified by *interop between engines* since Phase 3, and that half was claimed and never demonstrated — both writers were Python. **Second, the shape:** the stock ledger is a running sum whose *inputs are generated by decisions made from the state itself* (low balance → order → arrival in N days → changes the next balance); a window function reads the partition but doesn't write back into it, and that was measured — the SQL running sum diverges on 14 of 30 days from the test case and reaches a balance of −70. Preceded by `make spike-spark-iceberg`, with **both outcomes declared beforehand**: if Spark couldn't read the pyiceberg catalog, it wouldn't get in **and** the interop clause would come out of this table. `make spark-evidence` publishes the same job on both engines, **including when plain Python wins**. **Not** proven, and it's written down: scale. It runs `local[*]`, with no shuffle across nodes. |
| **Snowflake** | Governed SQL, RBAC, BI connectivity | — | — | **Adopted on 2026-08-27** (Phase 2). Receives a cut by scope, not the entire Silver — the measured ratio went from 3,85% to 46,9% between Phase 2 and Phase 6 with no rule changing, because it's a function of which sources fit the scope. The old friction — "can't reach a local MinIO" — was resolved without real S3 or a storage integration: an **internal stage** (`PUT file://`) reverses the direction, and it's the local process pushing the bytes, which sees both sides. |
| **Airflow** | Retry, exit codes, pools, SLA, execution history | — | **Adopted.** Heavy for a 4-minute daily job, and taken on with that awareness: the value is in the operational contract (the 1-slot pool and exit-code handling have no cron equivalent). | — |

## Four measured constraints that shaped the design

These aren't preferences. They're observed behavior, and each one is enforced in code.

### 1. Backfill is impossible

The API serves only **today's** price. A DAG run dated 2026-08-17 would write today's
prices under the 08-17 key — silently wrong data, and internally consistent, which means
no downstream test would catch it.

There's a real 8-day gap between `2026-08-16` and `2026-08-24`: the days 08-17 through
08-23 **don't exist and can't be recovered**. That's the practical reason for the
scheduling — every day without it is price history that doesn't come back.

Enforced by three layers, verified: Airflow refuses a future `execution_date`;
`catchup=False` + `start_date` prevent runs before the start; and `guard_date` refuses a
manual trigger for any date ≠ today. While `start_date` == today the third is redundant —
it becomes the only protection the following day.

### 2. The throttle is per process, not across processes

The Source client limits to `1/delay` req/s, measured from the start of the previous
request — within the **process**. Two concurrent `extract` runs double the real rate
against the host.

Measured: 152 requests in 9 s produced ~20% `403`s and intermittent blocking for minutes;
sequential at 1,5 s gave 0 failures across repeated runs.

With more than one warehouse, the Airflow pool with **1 slot** is the only thing preserving
the safe rate. It's not a configuration decoration.

The DAG covers **four** warehouses (`WAREHOUSES = ["mad1", "bcn1", "vlc1", "svq1"]`), so
the pool is under real pressure: that's 4 × 152 ≈ 608 requests, ~15 min sequential. It
existed before it was needed, on purpose. Known ceiling: the 7 warehouses that serve a
catalog would take ≈ 27 min, which still fits comfortably in a daily window.

### 2.1. Which warehouses, and why these

The source serves **7 warehouses** — `mad1`, `mad2`, `mad3`, `bcn1`, `vlc1`, `svq1`, `alc1`
— plus `vlc2` and `pmi1`, which the server recognizes but which have no catalog. Measured
on `2026-08-24`.

The choice of the four is by **assortment divergence**, not market size, because assortment
and price behave at different levels:

- **Assortment is a property of the city.** The three Madrid warehouses have **identical**
  product sets among themselves (0 exclusives); `bcn1` differs from `mad1` by 12,0% (551
  products).
- **Price varies within the same city.** Measured on perishable categories, `mad3` diverges
  from `mad1` by 18,4% of prices, against 26,4% for Madrid↔Barcelona **in the same
  categories** — that is, 70% of the magnitude between cities happens within a single one.
  These two numbers are comparable to each other since they're from the same cut; they
  aren't comparable to the ones in the matrix below, which is for the whole catalog.

So a second warehouse in the same city pays 152 requests to add just one axis. Between
cities, both vary.

Pairwise matrix, **calculated over the whole catalog from Silver** on `2026-08-24`
(reproducible with a query against `silver_product_price`):

| pair | assortment | price |
|---|---|---|
| bcn1 / svq1 | 13,8% | 3,3% |
| svq1 / vlc1 | 13,6% | 2,8% |
| mad1 / svq1 | 13,0% | 2,3% |
| mad1 / vlc1 | 12,4% | 2,3% |
| bcn1 / mad1 | 12,0% | 3,1% |
| bcn1 / vlc1 | 10,0% | 3,1% |

`svq1` appears in the **three most divergent pairs**, and its smallest divergence from the
rest of the set is 13,0% — the largest minimum available. That's what justifies choosing it.

**`alc1` was discarded** for duplicating `vlc1` — same autonomous community, 170 km. Honest
caveat about this number: `alc1` **was not extracted**, so it isn't in this matrix. The
comparison comes from a probe over 2 categories, where `vlc1/alc1` gave 34,4% against
52,3% for `vlc1/svq1`. That probe had **selection bias** — the categories were chosen for
concentrating exclusives between `mad1` and `bcn1`, which inflated that specific pair (it
gave 78,6% there against 12,0% here). The bias doesn't reach the `alc1` vs `svq1` pair,
measured on the same categories without being a selection criterion, so the **ordering**
between the two holds; the magnitudes from that probe don't. `alc1` remains the obvious
candidate if a fifth one gets in, and measuring it properly would require extracting it.

### 2.2. Two facts about the source that change how this is operated

**An invalid `wh` doesn't fail.** The source doesn't refuse an unknown code: it returns
`200`, falling back to `vlc1`, its default. Measured — `zzz9`, `mad9` and the request *with
no `wh` at all* return identical content. The operational consequence is that a typo in
`WAREHOUSES` produces no error at all: it produces a partition labeled `wh=<typo>`
**containing Valencia's data**, internally consistent and therefore invisible to every
downstream test. A recognized code with no catalog is distinguished by
`/categories/?wh=X` answering `content-length: 52` (empty tree) instead of the full tree.

**The category tree is byte-identical across the 7 warehouses.** It's national structure,
not regional. Each warehouse spends 1 request on an already-known tree, and
`silver_category` carries 151 redundant rows per warehouse. Harmless at the current volume,
and recorded here as known waste rather than discovered later.

### 3. Re-running `extract` on a complete partition returns exit 2

A complete partition is immutable (contract guarantee 5). A naive retry by the orchestrator
would mark a day that succeeded as a failure. That's why idempotency comes from the
`_SUCCESS` marker, and `retries=0` on `extract`.

The DAG's gate looks at the **destination**, not the local disk: it reuses
`verify-landing` as a short-circuit. Exit 0 → nothing to do; exit 1 → landed but divergent,
proceeds and `land` fixes it; exit 2 → doesn't even exist locally, proceeds to extract.
Checking the local `_SUCCESS` there would be a mistake — it would skip `land` for a
partition extracted by hand and never landed.

### 4. The Source writes with mode 600, and that dictates the container's UID

`canonical.py` does an atomic write with `tempfile.mkstemp()`, which creates the file with
mode **0600**, and `os.replace` preserves that mode. Measured: **153 of the 155 files** in
a partition are `-rw-------` (only `_run.log`, written with a plain `open()`, is 664).

The consequence is operational and has no middle ground: **any consumer needs to run with
the UID of the files' owner.** There's no group fallback. That's why the Airflow container
runs as `${AIRFLOW_UID}` and not as the image's default `airflow` (50000).

Two known traps along this path, both found in execution:

- **Compose doesn't read the root `.env` on its own.** The project dir is `infra/`, so
  `${AIRFLOW_UID}` fell back to the default 50000 and the container couldn't read the
  partition. The `Makefile` passes `--env-file .env` explicitly.
- **Overriding `entrypoint` on an Airflow service breaks the user.** The image's
  `/entrypoint` is what creates the `/etc/passwd` entry for the chosen UID; without it
  Airflow dies at `getpass.getuser()`. Use `command`, never `entrypoint`.

Changing the mode in the Source would resolve this more directly, but it's FROZEN — so it's
the UID that adjusts instead.

## Verification instead of trust

A pattern inherited from the Source's `validate.py`, which recalculates instead of
accepting recorded values. Repeated at every boundary:

| Boundary | What gets reprocessed |
|---|---|
| disk → RAW | local file sha256 checked **before** the PUT; `ChecksumSHA256` validated on the server |
| RAW | `verify-landing` downloads every object and recalculates sha256, size and inventory |
| RAW → Silver | a dbt test reconciles the derived count against the manifest's `totals` |
| Silver | 36 tests (7 singular + 29 schema), including uniqueness of the composite grain and redundant lineage |

Every check has been proven capable of **failing**: tampering with one byte at the
destination fails `verify-landing` (exit 1); removing a catalog object fails the
reconciliation test. A check that has never failed is not a check.

## Source ↔ platform boundary

The Source is FROZEN and keeps `dependencies = []`, enforced by AST in
`tests/test_dependencies.py`. The platform has its own `pyproject.toml` and venv.

The boundary isn't the folder. It's enforced by:

1. **The platform never imports `mercadona_catalog_source`.** It consumes the physical
   contract (canonical JSON + `_manifest.json`), like an external consumer.
   `platform/…/manifest.py` implements section 4's contract obligations as code, not as a
   comment.
2. **`make source-test` runs on the system Python, with no venv.** If it passes, the
   Source still has no third-party dependency. It's the boundary verified, not claimed.
3. **Zero dependencies pays off in practice:** the Source runs in the Airflow worker's own
   interpreter via `PYTHONPATH`, without conflicting with its pinned dependencies.

## Verification panel (Streamlit over the MART)

A **verification** bench for the indicators before rebuilding them in Power BI, not a BI
deliverable. It's 22 indicators in 7 groups. Reads **only** `RETAIL.MART`, with
`RETAIL_READER` and `use secondary roles none` — the session is refused on GOLD and STAGE,
and the panel runs a live probe that demonstrates the refusal instead of asserting it.

The single source is [`streamlit/indicators.py`](streamlit/indicators.py): the SQL and the
explanation live together, and [`CONTRACT.md`](streamlit/CONTRACT.md) is **generated** from
it. If the explanation lived in a hand-written markdown, the two would diverge at the first
SQL tweak — and verification would keep passing, because nobody reads a SQL and a text side
by side looking for disagreement.

Each indicator carries **traps**: the cases where the obvious measure produces a plausible,
wrong number. It's the only class of error no test catches, and it's what makes the panel
useful for whoever rebuilds the model in another tool.

The *Out of reach* list is part of the deliverable: every absence carries the **trigger**
that would unlock it. An absence with no trigger is an excuse; with a trigger it's a
decision.

`make dashboard-check` runs the real app via `AppTest` and requires zero exceptions against
the live account: it's the only way for the 25 queries to be exercised the way Streamlit
runs them, with parameters bound, instead of checked as text.

## Technical debt

Reviewed on **2026-09-01**, after Phase 7, and **restructured on 2026-09-02** so every item
declares status and next step instead of just a reason. **Eight open items**, all
deliberate. The rest of the table is history: it stays because what got closed and *how*
it got closed is the part worth learning from.

**The count went from five to eight, and that's a result, not a regression.** Three of the
new items were *discovered* by Phase 7 — two of them by measuring what it built itself. A
debt list that only shrinks is a sign nobody's looking.

Each item carries **problem, impact, status, and next step**, and nothing else. Status is
one of four: **mitigated** (the damage is contained, the cause isn't), **accepted** (won't
be fixed, and the reason is written down), **open** (identified work remains), or **out of
scope**.

| # | Problem | Impact | Status | Next step |
|---|---|---|---|---|
| 1 | `models/warehouse/` has no offline test | Two real defects only showed up on the first run against the live account; a DuckDB mirror would have caught them | **Mitigated** by `make warehouse-evidence` — dated evidence, not a second engine | None. The answer is **not** a mirror: see the section below |
| 2 | The Snowflake account is a trial | It expires, and with it the entire right half of the pipeline | **Accepted** — it's open by nature | None. The destination is swappable via `.env.snowflake`, and that's verified |
| 3 | There's no CI | The offline suite depends on someone running `make test` | **Open**, blocked by having no remote — writing a workflow that never ran would be claiming a check nobody saw | The repository gaining a remote; the workflow covers `make test` + `make silver`, never the Snowflake half |
| 4 | `CustomKeyInConfigDeprecation` warning on `dbt build` | Log noise | **Accepted** — cosmetic and external: `dbt-duckdb` config, with no supported form published | Track `dbt-duckdb` |
| 5 | No mart joins customer with order | No repurchase, RFM, LTV or cohort. The link exists in `FACT_ORDER.customer_sk`, in GOLD, out of reach of the BI role | **Open** — it's the most actionable functional gap, and the only one that closes by writing SQL | A customer-grain mart (`MART_CUSTOMER_ORDERS`), with the `synthetic` label traveling in every column. See [BACKLOG.md](BACKLOG.md) |
| 6 | The projection rebuild is **O(n²)** | 414 commits and ~55 min for 206.523 orders: each batch does an `upsert` against the entire table. On a new machine it's a 55-minute tax | **Open.** It's the **only place in the project where volume actually hurt** — and the irony is worth noting: Spark's justification says the volume trigger didn't fire, and it fired here, in the Python path | A single-batch `append` path when `--reset` is used: the table starts empty and there's no concurrent writer, so there's nothing to `upsert` against. See [BACKLOG.md](BACKLOG.md) |
| 7 | The ledger stockout is independent of the order's `unavailable` rows | One doesn't cause the other, and crossing them would produce an invented correlation | **Accepted**, and declared in the data: the generator drops a row at a fixed sampled rate, without looking at balance | A second-pass generator that rereads the balance. It **would invert the project's dependency** — today the order generates the stock —, and that's what's holding the item back |
| 8 | Silver's parquet survives the gate excluding the model | It actually happened: `MART_STOCK_HEALTH` described 5 days while the other marts described 9, **with not a single test failing** — each domain closed on its own | **Mitigated per domain, class still open.** `assert_stock_ledger_covers_the_order_window` compares the two domains' windows in the warehouse. The class is general: **any** excluded model leaves stale parquet behind | A generic check — for every model the gate excludes, compare the parquet's age against the current build's. Not done because it would require the gate to publish what it excluded, and the phase closed |

| Item | Status |
|---|---|
| Test coverage | **Closed.** 19 → 416 tests on the platform, with an in-memory S3 double |
| Extraction path in container | **Closed.** `bcn1` extracted, validated, landed and transformed inside the container |
| Redundant environments | **Removed.** 265 MB (broken `venv/` and `orchestration/.venv`) |
| Development credentials | **Hardened.** Loopback ports, random keys, compose refuses to start without them |
| `data/` divergence | **Contained.** The `data/` pattern in `.gitignore` matches at any level |
| Homonym fanout in `silver_ine_population_by_municipality` | **Closed** on 2026-08-27, the same day it was found |
| Silver model and simulated OLTP DAG | **Closed** in Phase 2 (`silver_customer`, `silver_oltp_manifest`, `simulated_oltp_customers.py`) |
| `currency` var declared for Gold and never used | **Closed.** `FACT_PRICE_SNAPSHOT` carries the column: the assumption travels with the number |
| Snowflake roles created and verified, but never worn | **Closed.** The load runs as `RETAIL_LOADER` and dbt as `RETAIL_TRANSFORMER`; four defects surfaced when they were put on |
| Account identity hardcoded in `profiles.yml` | **Closed.** `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER` with no default; the account is swappable via `.env.snowflake` |
| Warehouse with `auto_suspend` of 300 s | **Closed.** `bootstrap` sets X-Small and 60 s, as the Phase 2 plan called for and never applied |
| `warehouse_load` had never run in a container | **Closed.** Image missing Phase 2's dependencies and no credential; the dependency list stopped being duplicated |
| **`models/warehouse/` tree with no offline test** | **Open**, and it's the consequence of a choice. Mitigated by `make warehouse-evidence` |
| **Snowflake account is a trial** | **Open by nature**, and the destination is swappable — verified, not claimed |
| dbt `CustomKeyInConfigDeprecation` warning | **Open, cosmetic.** `dbt-duckdb` config, with no supported form yet |
| **CI** | **Open, and blocked by having no remote.** Would cover the offline half (`make test` + `make silver`), never the Snowflake half |
| Silver model and simulated orders DAG | **Closed** in Phase 3 (4 models, 8 singular tests, `simulated_orders_events.py`) |
| **Streaming half with no offline test** | **Partially closed** in Milestones 4, 5 and 6: `fake_pg.py` covers the transaction boundary, `fake_kafka.py` the order between write and offset commit, and `fake_iceberg.py` the monotonic merge and the retry loop — offline, in `make test`. What no double covers remains open: that `rollback` actually undoes, that the broker preserves order per key, and that Iceberg refuses to commit a stale snapshot. That's `make orders-prove-atomicity`, `make orders-prove-stream` and `make orders-prove-projection` |
| **Orders Silver claimed a separation the log doesn't declare** | **Closed** in Milestone 4, the day it was found: 5.508 rows from 298 orders. Found by two independent folds disagreeing, not by a test |
| Gold and orders marts | **Closed** in Milestone 7: 4 STAGE, 4 FACT, 3 MART and 5 tests, each proven capable of failing via `make warehouse-prove-tests` |
| **`TIMESTAMP` crossed the boundary 56 million years into the future** | **Closed** in Milestone 7, the day it was found. `use_logical_type = true` on `COPY INTO`; DuckDB annotates the unit only on the modern `LogicalType` and Snowflake was falling back to the legacy `ConvertedType`. **166 dbt nodes built green on top of the defect** — a human reading a mart is who caught it. Now guarded by `assert_order_milestones_are_plausible_against_the_order_date` |
| Event count turning into `FLOAT` in parquet | **Closed** in Milestone 7. `sum()` returns `HUGEINT`, parquet has no `INT128`, the write downgrades to `DOUBLE`. Found because the DDL is derived from the cut itself |
| **Generator assumption living in two places** | **Avoided** in Milestone 7 instead of closed: `STG_ORDER_PREMISE`/`FACT_ORDER_PREMISE` carry the entire seed to the warehouse, so `MART_FULFILLMENT_SLA` measures against the same number that generated the durations. A dbt var would have created the copy |
| **The Silver `dbt build` gate lived in six files** | **Closed on 2026-08-31**, the day the DAG failed. See below |
| `RETAIL_READER` role created, verified and with no consumer | **Closed on 2026-08-31.** The Streamlit panel is the first to wear it, and proves the refusal on GOLD/STAGE right on the screen |
| **No mart joins customer with order** | **Open, and it's the model's most actionable gap.** Without it there's no repurchase, LTV, cohort or revenue per customer. The link exists in `FACT_ORDER.customer_sk`, in GOLD, out of reach of the BI role. It requires no new source — it requires a customer-grain mart |
| **Streaming half with no record of real execution** | **Closed** in Milestone 8. `make stream-evidence` writes `docs/stream-evidence/README.md` from the three live planes — no number by hand, and a missing section shows up as a declared absence, never as a zero |
| Every customer bought the same expected basket | **Closed** in Phase 5: the mix became conditional on the cohort (age × community), calibrated by IPF so the aggregate wouldn't move |
| **Newborn with a primary-holder registration** | **Closed** in Phase 6, and it fixed the domain Phase 5 got wrong. Phase 5 blocked the minor at the *order* (`min_buyer_age`) and left the registration untouched; `min_customer_age` now lives in `customer_premises_seed.csv`, and 3.602 minors became 0 |
| **Customer base with no density** | **Closed** in Phase 6. It was 5.000 per warehouse for AUFs that differ by 4,6× in population — nothing failed, because density doesn't show up in any total. Today it's `municipal population × the province's adult share × 2,2%`, and the total is a consequence, not a quota |
| Snowflake screenshot list, 1 of 6 captured | **Closed on 2026-09-01.** Five of the six items were already covered by the generated evidence; the sixth became the **Roles in execution** section of `make warehouse-evidence`, read from `query_history`. `PRINTS.md` was removed: it was a to-do list living in the repository |
| Environment variables read by the code and declared nowhere | **Closed on 2026-09-01.** Nineteen of them — from `AWS_ACCESS_KEY_ID` to `RETAIL_DASHBOARD_TTL`. All have a default in the code, so nothing broke: they simply didn't exist for anyone who cloned the repository. They're in `.env.example` as commented-out overrides, and `TodaVariavelDeAmbienteEDeclarada` scans the code for `os.environ`/`getenv` and fails if a new one appears undeclared |
| `streamlit/CONTRACT.md` eternally "modified" in git | **Closed on 2026-09-01.** The header carried the generation date, so the derived file changed on every run and the sync test had to **exempt that line** — a blind spot inside the very test that exists so there'd be no blind spot. It now carries the sha256 of `indicators.py`: the comparison became byte for byte |
| Four versioned seeds with no executable provenance | **Closed on 2026-09-01.** The `scripts/derive_*.py` existed, with a good docstring, and **no Makefile target** — the origin of four CSVs was only discoverable by opening a file the README never said how to run. They became `make seed-province-map`, `seed-service-area`, `seed-municipality-codes` and `seed-ambiguous-series`; all four reproduced the versioned CSV byte for byte |
| Documentation checked only by reading | **Closed on 2026-09-01.** `test_documentacao.py` scans what can be verified by machine: every path in the README's tree exists, every relative link resolves, no log/artifact is versioned, every Makefile target appears in `make help`, every script has a target, every Makefile variable is used. Six injections seen red |
| **Interop between engines: claimed for four phases, never demonstrated** | **Closed in Phase 7.** Iceberg was justified by interop since Phase 3 and both writers were Python, using the same library. `make spike-spark-iceberg` measured 18 questions against the real stack, with **both outcomes declared beforehand**: if it failed, Spark wouldn't get in **and** the clause would come out of this table. Today the catalog has three writers, and `written_by` makes that queryable |
| **Order assumptions contradicting one another** | **Closed in Phase 7**, after three phases "logged instead of fixed". 84% of deliveries arrived before the window even opened; the SLA threshold was set to 90 against a possible ceiling of 80. What was missing was the distinction between *tweaking until the output looks nice* and *making two assumptions coherent* — the first is refused, the second is a model correction. Guarded by `assert_order_premises_are_internally_coherent`, which checks the **derivation** and never the result |
| **Stock, stockout, turnover and coverage out of reach** | **Closed in Phase 7, with a caveat that travels in the data.** The declared trigger was "a source of balance or movement" and it was **not** met: the ledger is *calculated* from observed consumption plus a declared policy. `stock_label = 'synthetic'` is on every row of the mart |
| **"Don't touch RAW after closing" was discipline, not verification** | **Closed in Phase 7.** Three documentation revisions happened because a regeneration changed numbers already written and nothing flagged it. `make freeze` seals the capture and `make freeze-check` fails if it changes. The seal covers the **data**, not the execution: `run_id` and timestamps are left out, otherwise a byte-identical re-land would break the seal |
| **References by section name and anchor were never checked** | **Closed on 2026-09-01.** `LinksRelativosTest` checks that the FILE exists, and a broken anchor points to a file that exists — so it passed, and the reader landed at the top of the document. Found while moving 17 sections to `DECISIONS.md`: one reference was left orphaned. Two new tests cover the `§ "…"` label and the anchor, at every heading level and in CONTRACT's explicit HTML anchors |
| **README and ARCHITECTURE explaining the same thing twice** | **Closed on 2026-09-01.** 1.435 + 2.475 lines, with the same subject in two places aging at different rates. The narrative went to `DECISIONS.md`, the future scope to `BACKLOG.md`, and a tested line-count ceiling keeps both from growing back without it being a decision |
| **Silver's parquet survives the gate excluding the model** | **Open, mitigated.** When `silver_gate` drops `silver_stock_ledger` (or `silver_live_order_state`) from the build, the parquet from the last successful build **stays** in object storage — and the export to Snowflake reads it without knowing it's stale. It actually happened: `MART_STOCK_HEALTH` described a 5-day window while every other mart described 9, with not a single test failing. Mitigated by `assert_stock_ledger_covers_the_order_window`, which compares the two domains' windows in the warehouse — which is where they finally meet. Not closed because the mitigation is per domain, and the class is general: any excluded model leaves stale parquet behind |

### Wearing the roles: what only shows up once you stop running as admin

The three roles had existed since Phase 2, with the right grants and the isolation matrix
verified by `check_isolation`. And **no execution ever went through them** — the load and
dbt ran as `ACCOUNTADMIN`. It's the difference between verified governance and adopted
governance, and it cost four defects, all invisible while the admin ran everything:

| Symptom | Cause | Why it didn't show up before |
|---|---|---|
| `No active warehouse selected` on load | The roles had no `usage` on the **warehouse** | An admin sees every warehouse. Data doesn't move without compute, and the error points at the session, not the grant |
| `schema missing: GOLD, MART` | `require_schemas` checked all three schemas | It was **isolation working**: `information_schema` returns only what the role sees, and the loader can't see GOLD. A check broader than the need turns a control into a failure |
| 8 models with `must have OWNERSHIP granted on TABLE` | `grant all` grants the **applicable** privileges, and ownership isn't one of them | `create or replace table` requires ownership. On a fresh account it's harmless — whoever creates is born the owner; it only shows up on an account where the admin created it first |
| `information_schema` empty for the admin | The custom roles weren't hung off `SYSADMIN` | It only surfaced **after** ownership left the admin and secondary roles were switched off. It throws no error: it just erases the objects from the administrator's view |

The fourth is the most instructive of the four, because it's the only one that **doesn't
fail** — role inheritance flows up (`SYSADMIN` starts seeing `MART`) and never down
(`RETAIL_READER` still has no `GOLD`), so the fix doesn't loosen anything, and its absence
would have passed as "everything's fine" until someone needed to administer the account.

All four became tests in `test_snowflake_load.py` (16 → 29), by the same criterion as the
rest of the file: none fails obviously if it regresses.

**What still isn't real isolation:** there's a single user, holding all three roles. Real
isolation would be one service user per role, with no `ACCOUNTADMIN` — but that's a
decision for whoever administers the account, not for the repository. What the repository
guarantees is that `default_secondary_roles = ()` is applied: without it, modern Snowflake
accounts activate **all** of a user's roles besides the primary, and wearing the role would
be decorative. Measured on this account before the fix: `current_secondary_roles()`
returned `ORGADMIN, RETAIL_READER, RETAIL_TRANSFORMER, RETAIL_LOADER`.

### `warehouse_load` compiled and didn't run

The DAG was written in Phase 2 and verified like the other four: it imports, builds the
graph, `airflow dags list` sees it. **Compiling is not running** — and only the path
through the host (`make warehouse-refresh`) had actually been exercised. On its first real
run it stopped at `load_stage` with `exit 2`:

```
ERRO: snowflake-connector-python nao esta instalado neste venv.
```

Two independent causes, and the second would only show up once the first was fixed:

1. **`infra/Dockerfile.airflow` duplicated the dependency list** from
   `platform/pyproject.toml`. Phase 2 added `dbt-snowflake` and
   `snowflake-connector-python` to the pyproject; the image stayed with Phase 1's list. The
   duplication did what duplication does, and the cost was paid at runtime, days later,
   with the data stuck halfway through.
2. **There was no credential in the container.** `~/.snowflake` wasn't mounted, so neither
   the connector nor dbt would have any way to authenticate.

The fix for the first isn't "add two packages": it's **reading the `pyproject.toml`** at
build time, which eliminates the entire class of problem, and importing both adapters plus
the connector as the last step of `RUN` — if a dependency goes missing, the **build**
breaks, not the DAG.

The fix for the second mounts `~/.snowflake` **at the same path** inside and outside
(`${HOME}/.snowflake:${HOME}/.snowflake:ro`), with `SNOWFLAKE_HOME` pointing there. That's
what makes `config.toml`'s `private_key_file` and `.env.snowflake`'s
`SNOWFLAKE_PRIVATE_KEY_PATH` resolve identically on both sides — no path needs rewriting
anywhere. Read-only: the orchestrator reads the key and never rewrites it, and since the
container runs with `AIRFLOW_UID` (the host's uid), no permission on the mode-600 file
needs loosening.

The account identity comes in via `env_file` with `required: false`, and **not** via
`environment:` — in the map block, a missing variable becomes present-and-empty in the
container, and `env_var('SNOWFLAKE_ACCOUNT')` with no default would return empty instead of
aborting. It's the same trap already documented in the Makefile, one level up. With
`required: false`, anyone without a Snowflake account still brings up `make up` and the
four source DAGs normally.

Measured after the fix: the DAG completes all four tasks, and `query_history` shows the
role separation in the query **type** — `RETAIL_LOADER` with `PUT_FILES`/`COPY`,
`RETAIL_TRANSFORMER` with `CREATE_TABLE_AS_SELECT`, `RETAIL_READER` with only `SELECT`.

### The destination is swappable — verified, not claimed

The account is a trial, and the response to that isn't avoiding depending on it: it's
making sure swapping it is cheap. Two things contradicted that and were fixed.

`profiles.yml` carried `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_USER` **hardcoded as defaults**.
Anyone cloning the repository without configuring anything wouldn't get "configure the
account" — they'd get a connection attempt against someone else's account, failing with
`Object does not exist` far from the actual cause. For MinIO the default makes sense
(`minioadmin` is a local convention that `.env.example` repeats); for a Snowflake account
there's no default that serves anyone else. With no default, dbt aborts saying `Env var
required but not provided: 'SNOWFLAKE_ACCOUNT'`.

Swapping accounts today is: edit [.env.snowflake.example](.env.snowflake.example) copied to
`.env.snowflake`, the corresponding block of `~/.snowflake/config.toml`, and run
`make warehouse-bootstrap`. **No model, no SQL and no test changes** — it's the physical
L2→L3 boundary paying for itself.

Verified in both directions: `dbt parse --target dev` passes with every Snowflake variable
missing, and `--target snowflake` fails naming the one that's missing.

### `models/warehouse/` with no offline test — and why the answer isn't a DuckDB mirror

A DuckDB mirror of the 24 models would have **passed** on the two errors that broke the
first real run: `FILTER (WHERE ...)` and `WINDOW ... AS`, which DuckDB accepts and
Snowflake doesn't. A test that doesn't reproduce the failure mode isn't a test — it's a
second implementation to maintain, and it would give false confidence exactly where there
is none.

The mitigation is different: [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
records the result of the **real** run — ownership object by object, volume, isolation
matrix and a sample of each mart — with date and account identity. It converts "code with
no test" into "code that ran, with the proof attached and dated". It's regenerable:
pointing at another account and running again produces that account's evidence.

What's still true: the cut (`snowflake_export.py`) and the transport (`snowflake_load.py`)
are covered offline, and that's where the silent errors live — an aggregate summed together
with the detail, a forgotten scope, a column matched by position. The warehouse SQL fails
loud when it fails.

### Homonym fanout in the population Silver

`silver_ine_population_by_municipality.sql` joined the RAW table with the code seed using
`inner join ... on e.municipality_name = c.municipality_name` — **by name alone**. The
model's own comment justified this as safe because "the seed already comes scoped to
08/28/41/46, where no collision was measured". The measurement confirms the seed really has
no internal collision (0 across the 4 provinces), but **the conclusion didn't follow**: the
RAW side is **national** (~8,200 municipalities) and brings in homonyms from other
provinces with an identical `Nombre`. The docstring of
`scripts/derive_municipality_codes.py` had already anticipated exactly this ("a Spain-wide
join by name alone would be ambiguous for 3 of these 18 names"); what went unnoticed is
that the model does exactly that Spain-wide join, because the extraction filters nothing.
Three municipalities got two rows:

| Municipality in the AUF | Correct series | Intruding series |
|---|---|---|
| Arroyomolinos (28/015) | `DPOP12967` = 38.075 | `DPOP4729` = 816 (Cáceres, 10023) |
| Molar, El (28/086) | `DPOP13174` = 9.999 | `DPOP19423` = 295 (Tarragona, 43085) |
| Torrent (46/244) | `DPOP21778` = 90.928 | `DPOP7960` = 182 (Girona, 17197) |

Effect: `mad1` returned 130 rows for 128 municipalities and `vlc1`, 64 for 63.

**Why the grain test didn't catch it.** The tested grain is
`(ingestion_date, series_code, year, fk_periodo)`, and the two series have different
`series_code` — the grain stayed unique. The test that was missing now exists:
`assert_ine_population_by_municipality_has_one_series_per_municipality` asserts one row per
`(municipality, sex, year)`, which is the real invariant.

**Fix: by the source's own official code, not by heuristic.** The `DATOS_TABLA/29005`
payload only has `COD`, `Nombre`, `FK_Escala`, `FK_Unidad` and `Data` — no province, no
`MetaData` (verified). But `GET /ES/VALORES_SERIE/{COD}` returns, for each series, the
**official INE municipality code** (province+municipality), the same schema as the
Callejero and the `warehouse_*` seeds.
[scripts/derive_ambiguous_series.py](scripts/derive_ambiguous_series.py) identifies offline
which names are ambiguous in RAW *and* exist in the 4-province seed (today 3 names, 18
series), queries those series, and writes `ine_ambiguous_series_seed.csv`. The model now
filters: a series with an ambiguous name only gets in if its official code matches the
seed's; a non-ambiguous name is still resolved by name.

Measured after the fix: 128/133/46/63 municipalities per warehouse, one series each. The
`customers.json` files for the four partitions came out **byte for byte identical** to the
earlier ones — the export's provisional defense ("keep the larger value") had been getting
it right, but by coincidence of size, not by knowing which series was which.
`export-oltp-reference` stopped deduplicating: it now just **rechecks** the invariant and
**refuses** if it breaks, instead of picking a value on its own.

### Test coverage

**No count per module, on purpose.** The table that used to live here carried an integer
per module, copied by hand, and it rotted: it said 131 tests on the platform when there
were 389, listed `verify.py` and `query.py` as if they had their own file (they don't —
they're exercised from inside `test_landing_roundtrip.py` and `test_config.py`), and
claimed 29 and 16 for `snowflake_load.py` in two paragraphs of the **same section**. A
number kept by hand in two places is a contradiction waiting for its date; the list below
says what each suite proves, which is the part that doesn't change with every new test. The
total comes from `make test`, measured, not written.

| File | What the suite proves |
|---|---|
| `test_manifest.py` | Consumer contract obligations, and the refusals |
| `test_land.py` · `test_landing_roundtrip.py` | Upload, idempotency, self-correction, aborting before `_SUCCESS`, and the reread that rechecks (`verify.py`) |
| `test_config.py` | Credential precedence, `.env` not overriding the environment, endpoint conversion (`query.py`), and every read variable being declared |
| `test_prune_local.py` | Only deletes the local copy after two independent checks |
| `test_oltp_reference.py` | The export's queries against real DuckDB fixtures, the population-based allocation and the adult share measured before the cut |
| `test_orders_reference.py` · `test_demand_profile.py` · `test_demand_check.py` | The price calendar, the IPF, the MAPA extraction's two checksums, and the reality check |
| `test_orders_oltp.py` · `test_orders_stream.py` · `test_orders_projection.py` | The transaction boundary, the order between write and offset commit, the monotonic merge |
| `test_snowflake_export.py` | The cut: `'Total'` aggregate, AUF scope, dedup, DDL derived from the cut itself |
| `test_snowflake_load.py` | Qualified stage, `OVERWRITE`, matching by name, rechecking, and the 4 role defects |
| `test_snowflake_evidence.py` · `test_stream_evidence.py` | Totals summed and not written, broken isolation highlighted, and a failure not turning into an empty value |
| `test_silver_gate.py` | The `dbt build` gate deciding in a single place |
| `test_dashboard_indicators.py` | `CONTRACT.md` byte for byte equal to what the generator produces, and every parameter bound |
| `test_cli.py` | The command-line defaults, including `--count` **not** having one |

Closed with an in-memory S3 client double (`platform/tests/fake_s3.py`), in the spirit of
the HTTP double the Source already uses. The double **validates the declared
`ChecksumSHA256`**, like the real server does: an error in the hex→base64 conversion would
fail in test, not in production.

Confirmed non-empty by mutation: turning off the sha256 comparison in `verify.py` makes
`test_catches_a_tampered_object` fail.

**What's still not covered**, and the list grew with Phase 2:

- **The Silver `dbt` path** — the data tests require object storage to be up.
- **The DAGs** — no test imports the Airflow module. All six compile via `DagBag` in the
  container, which catches import errors but not behavior.
- **The `models/warehouse/` tree** — the 24 models and their tests only run **against
  Snowflake**. There's no offline equivalent, and it's not an oversight: a DuckDB mirror
  would be a second materialization of the same truth, and it was precisely the difference
  between the two engines (`FILTER`, `WINDOW`) that broke them on the first real run — a
  DuckDB mirror would have passed and hidden exactly those errors. **The consequence is
  real and stays on record: with no Snowflake account, `make warehouse` doesn't run and
  that half of the project isn't verifiable.** What softens it is that the cut feeding it
  (`snowflake_export.py`) and the transport (`snowflake_load.py`) are covered offline, and
  that's where the silent errors live — the Gold SQL fails loud when it fails.

All three were verified manually, in a real run.

### Hardening the local stack

- **Ports on `127.0.0.1`** by default (`BIND_ADDR`). They used to listen on `0.0.0.0` with
  `minioadmin/minioadmin` and `admin/admin` — any machine on the network could reach the
  MinIO console and the Airflow UI.
- **Random `AIRFLOW_SECRET_KEY` and `AIRFLOW_FERNET_KEY`**, generated by `make secrets` into
  `.env` (mode 600, outside version control). Compose uses `:?` interpolation and **refuses
  to start without them**, so there's no path where an example value becomes the real key
  by oversight.
- **The orchestrator's `DUCKDB_PATH` lives inside the container**, not in the mounted
  repository. DuckDB is single-writer: with the file shared, a DBeaver window left open on
  the host would take down the DAG's `silver` task.

### No CI

No `.github/workflows`, and **no remote configured** (`git remote -v` is empty). The
boundary depends on someone running `make test` — and the `source-test` target exists
precisely to be a job that installs nothing. Two jobs (Source with no dependency, platform
with a venv) would make the boundary verified on every push instead of by discipline.

It's not written because **a workflow that has never run is the opposite of what this
repository does with testing**: it would be a file claiming a check nobody ever saw happen,
neither green nor red. The trigger is literal — the day there's a remote, both jobs get in
and the first run is the proof.

### The Snowflake account is a trial — **open, by nature**

A 14-day trial starting 2026-08-27. When it expires, `make warehouse` stops running and
with it the 24 models and the Gold/Mart tests. **The lakehouse isn't affected**:
`make silver` and the Python suites keep running offline, with no credential and no cost —
that's exactly why the L2→L3 boundary is physical. An earlier trial already expired during
this phase and the symptom was `390913`, with the login authenticating and no warehouse
available.

### dbt deprecation warning — **open, cosmetic**

`dbt build` emits 16 occurrences of `CustomKeyInConfigDeprecation` because of
`+format: parquet` in `dbt_project.yml`. It's `dbt-duckdb` config, not dbt-core's, and
moving it to `config.meta` as the warning suggests can break the `external`
materialization. Left as is until `dbt-duckdb` publishes the supported form; the warning is
noise, not a symptom.

## Out of scope

**Gold — GOT IN during Phase 2**, and what was recorded here as "to be named once it gets
in" has been fulfilled: `DIM_PRODUCT`'s SCD2 is keyed on `source_product_id` and therefore
models the lifecycle **of the source's key**, not of the commercial item. The
`name_seen_before` cases (7 in the current base) produce "one product died, another was
born" — the dimension carries `identity_ambiguous` so that's queryable instead of inherited
unknowingly. The name `fct_price_daily` was **discarded**: it became
`FACT_PRICE_SNAPSHOT`, because "daily" would promise a continuity the source doesn't have.

**Sales fact — GOT IN during Phase 3, and it's still synthetic.** The source exposes no
sale, order, or stock in any known endpoint; that hasn't changed and won't change. What
changed is that the transactional fact now exists, **and it's stated instead of
implicit**: the order is synthetic, and the customer, product, and price it carries are
observed. The assumptions table (`order_premises_seed.csv`) is the closed list of what was
invented, labeled `synthetic` row by row, and the export refuses any other label.

Gold **received them** at the end of Phase 3: `FACT_ORDER` (accumulating snapshot),
`FACT_ORDER_ITEM`, `FACT_ORDER_EVENT` and `FACT_ORDER_PREMISE`, plus three marts. That's
what finally makes the SCD2 pay for itself — until then the two versioned dimensions had no
fact pointing at a version.

**Currency.** The source doesn't declare it. Silver doesn't invent it. The dbt project's
`currency` var exists as a **consumer assumption**, and it's Gold that materializes it:
`FACT_PRICE_SNAPSHOT` carries a `currency` column with the var's value, so the assumption
travels with the number instead of living only in a config file. Switching currency means
editing the var, not hunting down `EUR` scattered across models.
