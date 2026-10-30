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
That's why the prose here carries the **magnitude** — "seconds", "~1.5 s" — and the decimal
lives only in the generated page that measured it. See
[DECISIONS.md § "What the measurement publishes against Spark"](DECISIONS.md).

Last revised: **2026-10-30** — price window widened and capture re-sealed (CR-010, CR-016),
renames, the indicator catalog move and English across the code (CR-012 to CR-014), pinned
MinIO images (CR-015), the daily schedule actually running (CR-016), and customers, orders
and stock regenerated to look like a real online grocer, anchored on INE and MAPA (CR-017),
and every panel tab read to the bottom (CR-018).

## Real scale

**Measured on 2026-08-24**, over the three catalog partitions that existed then. It's the
scale that justified the engine, and it's still the right question — volume never grew
enough to change the answer:

| Metric | Value |
|---|---|
| Volume per partition | ~8.3 MB, 152 files |
| Rows per partition | ~4,600 (4,329 unique products) |
| Growth | ~3 GB/year if run every day |
| Duration of one extraction | 227 s (152 requests at 1.5 s) |
| Full Silver (3 partitions, 4 models, 36 tests) | ~6 s |

**Where it stands today, measured on 2026-10-29** and re-sealed on 2026-10-30 with one more
collection day, in the capture named by [`docs/FREEZE.md`](docs/FREEZE.md) (`capture_id
a781ea46…`; CR-010 and CR-016 widened the price window, CR-017 regenerated customers and
orders on the same windows):

| Metric | Value |
|---|---|
| Customers (`silver_customer`) | 286,826, across 4 AUFs (shaped by INE TIC-H since CR-017) |
| Orders, 9-day window | 211,367 · 4,251,892 rows · 1,464,306 events (2026-10-30) |
| Stock ledger | 191,356 rows · 17,396 series · 11 days (2026-10-30) |
| Sealed RAW | 167 partitions · 2,646,782 records (2026-10-30) |
| `make silver` | 25 models, 17 seeds, 393 nodes — **~90 s** |
| `make warehouse` | 24 models, 205 nodes — **90 s** |
| Python suites, offline | 1,175 (2026-10-30) |

**No distributed technology is justified by this volume, and that's a result, not an
assumption.** Orders grew 33× since Phase 3 measured them (6,400 → 211,367), and the full
Silver `dbt build` still runs in about a minute and a half on one laptop. The basket self-join — the natural
candidate for "too big for one node" — grew to 41.9 M pairs and still runs in **~2 s**
(the current decimal is in [`docs/spark-evidence/`](docs/spark-evidence/README.md)).

**Where volume HURT, and it's a single place.** Rebuilding the Iceberg projection cost **414
commits and ~55 min** for 206,523 orders (2026-09-01): each batch did an `upsert` against the
entire table, so the cost grew with what was already written — and the streaming sink on an
empty table reached only 38,300 of 211,367 orders in 80 minutes (2026-10-30). The irony is
worth writing down: Spark's justification says the volume trigger didn't fire, and it fired
here — in the Python path, not in the analysis. **Closed in CR-017:** after `--reset` the
table is empty and the rebuild is alone on it, so the state goes in ONE append — **1 commit,
~22 s** for 211,367 orders.

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
                  s3://retail-lakehouse/silver/…               11,658,778 rows (2026-10-30)
─────────────────── physical boundary: COPY INTO, never ref() ───────────────────
L3  Stage         1:1 mirror of a CUT of the Silver             · Postgres (Snowflake opt-in)
                  STAGE.STG_*                                     7,732,735 rows (66.3%)
L4  Gold          conformed DIM_* / FACT_*, SCD2                · dbt-postgres / dbt-snowflake
L5  Mart          MART_*, grain declared per table                 · dbt-postgres / dbt-snowflake
```

RAW is the point of no return: everything downstream is reconstructible from it without
touching the API again. This matters more here than in the general case, because **this
source doesn't allow rereading the past** — see "Backfill" below.

The L2→L3 boundary is **physical, not logical**, and it's the same mechanism as the
Source↔platform boundary one level up: no warehouse model can `ref()` a Silver model,
because different adapters don't cross within a single dbt run. The link is a load into
`STAGE` plus `source()`. What that buys is told once, in
[§ "Warehouse destination"](#warehouse-destination-postgres-by-default-snowflake-one-target-away).

## Engine: DuckDB + dbt-duckdb

Adopted. In-process, reads MinIO directly via `httpfs`, writes parquet; it started with 4
models in ~6 s and carries the whole Silver today (timings in the table above). The hive layout the Source already produces (`ingestion_date=…/wh=…`) is consumed
with `hive_partitioning=1`, so `ingestion_date` and `wh` become columns **with no manual
parsing** — contract obligation 4.3 comes for free.

The dbt SQL is the portable asset. The Silver tree is DuckDB-specific where it reads the
RAW (`read_json` over `s3://`); the warehouse tree runs unchanged on `dbt-postgres` and
`dbt-snowflake`, with the dialect differences translated in one macro file
(`macros/warehouse_compat.sql`) and one `on-run-start` hook. That's what makes the swaps below configuration, not a new
project.

## What didn't get in, and when it gets in

| Technology | What it buys | Why not now | Trigger | Where the swap happens |
|---|---|---|---|---|
| **Iceberg** | Snapshot isolation between concurrent writers, time travel, interop between engines | — | — | **Adopted on 2026-08-28** (Phase 3, Milestone 6). The trigger that fired was the literal one — *"a second engine needing to write the same table"*: `live_order_state` is written by the streaming consumer and by the batch rebuild, with DuckDB reading while both write. **It fired due to CONCURRENCY, not volume** — at this volume an atomic `os.replace` parquet would do. Preceded by a closed experiment (removed in CR-012, kept in git history) that measured catalog, upsert, conflict, isolation and reading by DuckDB before the projection existed; `make orders-prove-projection` proves the same properties on the live module today. The old trigger (`dim_product` SCD2 via `MERGE`) still hasn't fired: the SCD2 is derived from the full history, not accumulated. |
| **Kafka** | Event transport, replay, decoupling point | — | — | **Adopted on 2026-08-28** (Phase 3, Milestone 5). The trigger that fired was the literal one — *"CDC of an OLTP"*: the event is born in the same transaction that changes the order (Milestone 4) and a stateful consumer keeps a read model below the batch. What got proven was **transport semantics**: at-least-once demonstrated by reproducing the duplication window, idempotent consumption with no ever-growing set, a gap refused, replay with no effect, 16 sha256 reproduced. **Not** proven, and it's written down: that anyone needs the latency, and that the broker is the origin — the canonical log is still born on disk. |
| **Spark** | An engine outside Python writing the catalog, and a form of computation SQL doesn't express | — | — | **Adopted on 2026-09-01** (Phase 7). **The declared trigger did NOT fire, and that's measured**: the basket self-join — 41.9 M pairs since CR-017 (37.9 M before), the natural candidate for "a partition DuckDB can't hold" — runs in **~2 s and ~2.5 GB** on one node. The final window doubled that number relative to the intermediate one (18.3 M) and the time stayed in seconds, which makes the claim stronger, not weaker. It got in for two other reasons. **First, interop:** Iceberg was justified by *interop between engines* since Phase 3, and that half was claimed and never demonstrated — both writers were Python. **Second, the shape:** the stock ledger is a running sum whose *inputs are generated by decisions made from the state itself* (low balance → order → arrival in N days → changes the next balance); a window function reads the partition but doesn't write back into it, and that was measured — the SQL running sum diverges on 14 of 30 days from the test case and reaches a balance of −70. Preceded by `make check-spark-iceberg`, with **both outcomes declared beforehand**: if Spark couldn't read the pyiceberg catalog, it wouldn't get in **and** the interop clause would come out of this table. `make spark-evidence` publishes the same job on both engines, **including when plain Python wins**. **Not** proven, and it's written down: scale. It runs `local[*]`, with no shuffle across nodes. |
| **Snowflake** | Governed SQL, RBAC, BI connectivity | — | — | **Adopted on 2026-08-27** (Phase 2); **opt-in since 2026-09-14**, when Postgres became the default (see § "Warehouse destination" below). Receives a cut by scope, not the entire Silver — the measured ratio went from 3.85% in Phase 2 to 66.3% on 2026-10-30 with no rule changing, because it's a function of which sources fit the scope. The old friction — "can't reach a local MinIO" — was resolved without real S3 or a storage integration: an **internal stage** (`PUT file://`) reverses the direction, and it's the local process pushing the bytes, which sees both sides. |
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
it becomes the only protection the following day. The date it checks is the day the run
**executes** (the later of logical date and interval end), not `ds`: a scheduled cron run
is dated with the start of its interval, and checking `ds` refused every scheduled run
until CR-016.

### 2. The throttle is per process, not across processes

The Source client limits to `1/delay` req/s, measured from the start of the previous
request — within the **process**. Two concurrent `extract` runs double the real rate
against the host.

Measured: 152 requests in 9 s produced ~20% `403`s and intermittent blocking for minutes;
sequential at 1.5 s gave 0 failures across repeated runs.

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
  product sets among themselves (0 exclusives); `bcn1` differs from `mad1` by 12.0% (551
  products).
- **Price varies within the same city.** Measured on perishable categories, `mad3` diverges
  from `mad1` by 18.4% of prices, against 26.4% for Madrid↔Barcelona **in the same
  categories** — that is, 70% of the magnitude between cities happens within a single one.
  These two numbers are comparable to each other since they're from the same cut; they
  aren't comparable to the ones in the matrix below, which is for the whole catalog.

So a second warehouse in the same city pays 152 requests to add just one axis. Between
cities, both vary.

Pairwise matrix, **calculated over the whole catalog from Silver** on `2026-08-24`
(reproducible with a query against `silver_product_price`):

| pair | assortment | price |
|---|---|---|
| bcn1 / svq1 | 13.8% | 3.3% |
| svq1 / vlc1 | 13.6% | 2.8% |
| mad1 / svq1 | 13.0% | 2.3% |
| mad1 / vlc1 | 12.4% | 2.3% |
| bcn1 / mad1 | 12.0% | 3.1% |
| bcn1 / vlc1 | 10.0% | 3.1% |

`svq1` appears in the **three most divergent pairs**, and its smallest divergence from the
rest of the set is 13.0% — the largest minimum available. That's what justifies choosing it.

**`alc1` was discarded** for duplicating `vlc1` — same autonomous community, 170 km. Honest
caveat about this number: `alc1` **was not extracted**, so it isn't in this matrix. The
comparison comes from a probe over 2 categories, where `vlc1/alc1` gave 34.4% against
52.3% for `vlc1/svq1`. That probe had **selection bias** — the categories were chosen for
concentrating exclusives between `mad1` and `bcn1`, which inflated that specific pair (it
gave 78.6% there against 12.0% here). The bias doesn't reach the `alc1` vs `svq1` pair,
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
| Silver | schema and singular dbt tests, including uniqueness of the composite grain and redundant lineage |

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

## Warehouse destination: Postgres by default, Snowflake one target away

**This is the one place the swap story is told**; README and DECISIONS link here.

The physical L2→L3 boundary exists so the destination is swappable by configuration alone,
and that claim was exercised for real. CR-006 stood up a Postgres target next to the
Snowflake trial; when the trial expired on 2026-09-14 (`load_stage` failing auth with
`390913`), CR-007 made Postgres the default — same 24 models, same data tests, only the
loader (`postgres_load.py` instead of `snowflake_load.py`) and the `--target` changed. On
2026-10-29 a new Snowflake account took the opt-in path over, and its first bootstrap
surfaced an escaping defect. See [DECISIONS.md § "CR-007"](DECISIONS.md) and
[§ "CR-008"](DECISIONS.md).

| | Postgres (default) | Snowflake (opt-in) |
|---|---|---|
| Runs | `warehouse-postgres` compose container, profile opt-in | a real account, `.env.snowflake` + RSA key outside the repo |
| Targets | `make warehouse-postgres-up`, `make warehouse-refresh` | `make warehouse-snowflake-bootstrap`, `make warehouse-snowflake-refresh` |
| Transport | `COPY` from the cut's parquet into `STAGE` | `PUT file://` to an internal stage, then `COPY INTO` |
| Evidence | `make warehouse` (the dbt tests) | [`docs/warehouse-evidence/`](docs/warehouse-evidence/README.md), dated, via `make warehouse-snowflake-evidence` |

The cut itself (`warehouse_cut.py`, written to `data/warehouse-cut`) is engine-neutral: it
reads the Silver and writes local parquet without talking to either warehouse.

**Swapping accounts** is editing `.env.snowflake` (copied from
[.env.snowflake.example](.env.snowflake.example)) and the matching block of
`~/.snowflake/config.toml`, then running the bootstrap. `profiles.yml` carries **no
default** for `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER` on purpose: an earlier version hardcoded
them, and anyone cloning got a connection attempt against someone else's account instead of
`Env var required but not provided`. Verified in both directions: `dbt parse --target dev`
passes with every Snowflake variable missing, and `--target snowflake` fails naming the one
that's missing. The Postgres target does carry defaults (`localhost:5434/retail`), because
it only exists as this project's own container.

**Roles, worn by the pipeline.** Both bootstraps create the same three roles; the load runs
as `retail_loader`, dbt as `retail_transformer` (`role:` in `profiles.yml`), the panels as
`retail_reader` — never as the database superuser.

| Role | Can | Cannot |
|---|---|---|
| `retail_loader` | write `STAGE` | read `GOLD` or `MART` |
| `retail_transformer` | read `STAGE`, write `GOLD` and `MART` | — |
| `retail_reader` | read `MART` | read `STAGE` or `GOLD` |

On Snowflake, the bootstrap **proves this matrix** role by role (with secondary roles off)
before returning success, and the evidence page records it. On Postgres the bootstrap
applies the grants and default privileges, covered offline by `test_postgres_load.py`;
live, `make dashboard-check` proves the reader's row of the matrix (reads MART, refused on
GOLD and STAGE) — the loader and transformer rows have no live check on that path. Stopping running as an administrator account
exposed real defects — missing grants, ownership confused with privilege, an account that
silently stops seeing objects it no longer owns — told in
[DECISIONS.md § "Phase 2"](DECISIONS.md).

### Why the warehouse models have no offline test — and the answer isn't a DuckDB mirror

The 24 warehouse models and their tests only run **against a live engine**. A DuckDB mirror
would have **passed** on the two errors that broke the first real Snowflake run —
`FILTER (WHERE ...)` and `WINDOW ... AS`, which DuckDB accepts and Snowflake didn't. A test
that doesn't reproduce the failure mode isn't a test; it's a second implementation that
gives false confidence exactly where there is none.

What covers the gap instead: the cut (`warehouse_cut.py`) and both transports
(`postgres_load.py`, `snowflake_load.py`) are tested offline, and that's where the silent
errors live — an aggregate summed with the detail, a forgotten scope, a column matched by
position. The warehouse SQL fails loud when it fails. And a non-expiring second engine
(Postgres) means the tree is no longer one trial account away from not running at all.

## Indicator catalog and the two panels

The panels in `streamlit/` (`operations_app.py`, `prices_app.py`) are **consumers**. The
asset is the catalog in `platform/src/retail_platform/indicators/`: `operations.py` holds 22
indicators in 7 groups plus `OUT_OF_SCOPE`, `prices.py` holds 9, both built on the
`Indicator` dataclass and bound filters in `model.py`. Both panels read **only** `"MART"`,
as `retail_reader`.

The SQL and the explanation live together, and the reviewable pages
[`docs/indicators/operations.md`](docs/indicators/operations.md) and
[`docs/indicators/prices.md`](docs/indicators/prices.md) are **generated** from them by
`contract.py` (`make indicators-contract`, no connection). If the explanation lived in a
hand-written markdown, the two would diverge at the first SQL tweak — and verification would
keep passing, because nobody reads a SQL and a text side by side looking for disagreement.
An offline test fails if the pages drift from the generator.

Each indicator carries **caveats**: the cases where the obvious measure produces a
plausible, wrong number. It's the only class of error no test catches, and it's what makes
the catalog useful for whoever rebuilds the model in another tool — or for the LLM/RAG layer
meant to consume it next, which is why the catalog lives in the platform package and not in
`streamlit/`. Parameters are always bound, never concatenated, so nothing can be injected
through a selector or a prompt.

The *Out of scope* list is part of the deliverable: every absence carries the **trigger**
that would unlock it. An absence with no trigger is an excuse; with a trigger it's a
decision.

`make dashboard-check` renders both panels via `AppTest` and requires zero exceptions
against the live warehouse: it's the only way for the 25 queries of the operations panel to
be exercised the way Streamlit runs them, with parameters bound, instead of checked as text.

**The panels are not where pipeline observability lives, and can't be.** `FACT_INGESTION_RUN`
(duration, throughput, failure count per run — `DECISIONS.md § "CR-004"`) sits in GOLD, out
of `retail_reader`'s reach on purpose. That evidence is read instead from
`docs/warehouse-evidence/README.md` (`make warehouse-snowflake-evidence`, dated snapshot
against the live account) or `make observability-prove-signals` (a per-signal verdict,
offline). Building a second panel just to relocate a query this role can't run would be a
workaround for an RBAC boundary the project keeps on purpose.

## Technical debt

Reviewed on **2026-09-01**, after Phase 7, and **restructured on 2026-09-02** so every item
declares status and next step instead of just a reason, then kept current as items closed
(CI on 2026-09-08, the Snowflake trial on 2026-09-14, the dbt warning and the projection rebuild on 2026-10-30) — a debt list that only shrinks by
deletion is a sign nobody's looking, so a closed row stays, with its status updated, rather
than disappearing.

Each item carries **problem, impact, status, and next step**, and nothing else. Status is
one of four: **mitigated** (the damage is contained, the cause isn't), **accepted** (won't
be fixed, and the reason is written down), **open** (identified work remains), or **closed**.

| # | Problem | Impact | Status | Next step |
|---|---|---|---|---|
| 1 | `models/warehouse/` has no offline test | Two real defects only showed up on the first run against the live account; a DuckDB mirror would **not** have caught them | **Mitigated** — offline tests on the cut and both transports, dated evidence, and a second live engine | None. The answer is **not** a mirror: see § "Warehouse destination" |
| 2 | The Snowflake account was a trial | It expired on 2026-09-14 | **Closed (CR-007)** | None — told in § "Warehouse destination" |
| 3 | `CustomKeyInConfigDeprecation` warning on `dbt build` | Log noise | **Closed (CR-016)** — a full `make silver` on dbt-duckdb 1.11 emits zero deprecation warnings, with `+format: parquet` unchanged | None |
| 4 | No mart joins customer with order | No repurchase, RFM, LTV or cohort. The link exists in `FACT_ORDER.customer_sk`, in GOLD, out of reach of the BI role | **Open** — it's the most actionable functional gap, and the only one that closes by writing SQL | A customer-grain mart (`MART_CUSTOMER_ORDERS`), with the `synthetic` label traveling in every column. See [BACKLOG.md](BACKLOG.md) |
| 5 | The projection rebuild was **O(n²)** | 414 commits and ~55 min for 206,523 orders: each batch did an `upsert` against the entire table | **Closed (CR-017)** — with `--reset` the table is empty and the rebuild is alone on it, so the state goes in one `append`: 1 commit, ~22 s for 211,367 orders. The batched merge stays for a rebuild WITHOUT reset, where a second writer may exist | None |
| 6 | The ledger stockout is independent of the order's `unavailable` rows | One doesn't cause the other, and crossing them would produce an invented correlation | **Accepted**, and declared in the data: the generator drops a row at a fixed sampled rate, without looking at balance | A second-pass generator that rereads the balance. It **would invert the project's dependency** — today the order generates the stock —, and that's what's holding the item back |
| 7 | Silver's parquet survives the gate excluding the model | It actually happened: `MART_STOCK_HEALTH` described 5 days while the other marts described 9, **with not a single test failing** — each domain closed on its own | **Mitigated per domain, class still open.** `assert_stock_ledger_covers_the_order_window` compares the two domains' windows in the warehouse. The class is general: **any** excluded model leaves stale parquet behind | A generic check — for every model the gate excludes, compare the parquet's age against the current build's. Not done because it would require the gate to publish what it excluded, and the phase closed |
| 8 | The seal guards a closed window, and the daily DAG now grows it every day | Since CR-016 fixed the schedule, every 06:00 run lands 4 partitions outside the seal: `make freeze-check` goes red daily by design, and an alarm that is always red trains reviewers to ignore it | **Open** — found once the schedule actually ran | Decide between re-sealing per published revision (today's practice, CR-010/CR-016) and a check that separates "window grew" from "sealed content changed" |

**Where the rest of this history lives.** Closed items with their full story (the RBAC
defects, `warehouse_load`'s first real run, the timestamp 56 million years in the future,
the population homonym fanout) are told once, in [DECISIONS.md](DECISIONS.md).

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
| `test_customers_reference.py` | The export's queries against real DuckDB fixtures, the population-based allocation and the adult share measured before the cut |
| `test_orders_reference.py` · `test_demand_profile.py` · `test_demand_check.py` | The price calendar, the IPF, the MAPA extraction's two checksums, and the reality check |
| `test_orders_oltp.py` · `test_orders_stream.py` · `test_orders_projection.py` | The transaction boundary, the order between write and offset commit, the monotonic merge |
| `test_warehouse_cut.py` | The engine-neutral cut: `'Total'` aggregate, AUF scope, dedup, DDL derived from the cut itself |
| `test_snowflake_load.py` | Qualified stage, `OVERWRITE`, matching by name, rechecking, and the 4 role defects |
| `test_postgres_load.py` | Quoted uppercase schemas, type mapping that fails instead of guessing, count recheck, and the bootstrap's grants and default privileges |
| `test_snowflake_evidence.py` · `test_stream_evidence.py` | Totals summed and not written, broken isolation highlighted, and a failure not turning into an empty value |
| `test_silver_gate.py` | The `dbt build` gate deciding in a single place |
| `test_indicators_operations.py` · `test_indicators_prices.py` | `docs/indicators/` byte for byte equal to what the generator produces, every parameter bound, every caveat and out-of-scope trigger declared |
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
- **The `models/warehouse/` tree** — only runs against a live engine, by choice; see
  § "Warehouse destination".

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

### CI — closed 2026-09-08, red 2026-09-14 to 2026-10-29

`.github/workflows/ci.yml`: `source-test` (no dependency installed, proves the five Sources
stay that way) and `platform` (`make venv` → `make platform-test` → `make up` → `make
silver`), never a warehouse engine. See `DECISIONS.md § "CR-005"` for why it wasn't written
before there was a remote to run it against, and what the first real run found.

For six weeks `platform` failed at `make up`: the official MinIO images stopped being
publicly pullable, and only a runner without an image cache could see it. Images are now
pinned community builds — `DECISIONS.md § "CR-015"`.

### dbt deprecation warning — **closed 2026-10-30**

`dbt build` used to emit `CustomKeyInConfigDeprecation` for `+format: parquet` in
`dbt_project.yml`, a `dbt-duckdb` config with no supported alternative. On dbt-duckdb 1.11
a full `make silver` emits none, with the config unchanged — closed by the adapter, not by
us (`DECISIONS.md § "CR-016"`).

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
