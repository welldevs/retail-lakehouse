# Retail Lakehouse — Mercadona + INE

Data platform built on the **Mercadona Catalog Source** (retail catalog), the **INE
Population Source** (population by province and by municipality), and the **INE Callejero
Source** (official geography — census sections, streets, population clusters): it preserves
the RAW snapshot in object storage and produces typed Silver in parquet, orchestrated by
Airflow. On top of that base, the **Simulated OLTP Source** generates geographically
coherent synthetic customers — the customer is invented, the address where they live is
not — and the **Simulated Orders Source** generates orders as an **event log**: the order
is invented; who buys, what is bought, and how much it costs is not.

Each Source is a **frozen, independent** component — the Mercadona one at
[sources/mercadona-catalog-source/](sources/mercadona-catalog-source/), the population one
at [sources/ine-population-source/](sources/ine-population-source/), the Callejero one at
[sources/ine-callejero-source/](sources/ine-callejero-source/), the simulated OLTP one at
[sources/simulated-oltp-source/](sources/simulated-oltp-source/), the orders one at
[sources/simulated-orders-source/](sources/simulated-orders-source/) — each with its own
physical contract (`CONTRACT.md`) and **zero runtime dependencies**. This platform consumes
them through the physical contract — never importing any of their code. See
[DECISIONS.md § "Second source: INE population"](DECISIONS.md) for why they are sibling
packages, not a shared abstraction.

The architecture decisions are in [ARCHITECTURE.md](ARCHITECTURE.md), with the adoption
date of Snowflake, Kafka, Iceberg, and **Spark**. The history of each one — decision,
reason, evidence, trade-off — is in [DECISIONS.md](DECISIONS.md), what does not go in is in
[BACKLOG.md](BACKLOG.md), and the constraints governing any agent that continues the work
are in [AI_ENGINEERING_CONSTRAINTS.md](AI_ENGINEERING_CONSTRAINTS.md). Spark's case is the
most instructive of the four: the trigger declared for it — *"a partition DuckDB can't
hold"* — **never fired, and that's measured** (37,9 M basket pairs in ~1,5 s and ~2,4 GB on
a single node, over the final window). It got in for two other reasons: it's the first
writer to the Iceberg catalog outside Python, and the shape of the job it carries — a
running sum fed back by its own state — isn't expressible in SQL.
[`make spark-evidence`](docs/spark-evidence/README.md) publishes the same job on both
engines, **including when pure Python wins**.

## Layers

```
L0  Source     API -> canonical partition, manifest with sha256, validate --strict
               5 FROZEN packages; 4 deliver a snapshot, simulated_orders delivers a LOG
L1  RAW        byte-identical partition in MinIO/S3, checksum verified post-PUT
    ┌──── operational plan, on demand (make stream-up) ─────────────────────┐
    │ OLTP   Postgres: orders, order_line, outbox                           │
    │        state + event IN THE SAME transaction, one transaction/event   │
    │ Broker Kafka retail.orders.events.v1 — 4 partitions, key = order_id   │
    │ Read   Postgres OR Iceberg: live_order_state                          │
    │ model  dedup by (order_id, sequence_no); TWO writers to Iceberg,      │
    │        with monotonic merge — whatever doesn't advance seq is dropped │
    └───────────────────────────────────────────────────────────────────────┘
L2  Silver     typed parquet + order fold + price variation · DuckDB · 4,1 M rows
────────────── physical boundary: COPY INTO, never ref() ──────────────
L3  Stage      1:1 mirror of a CUT of the Silver                 · Snowflake · 407 k rows
L4  Gold       conformed DIM_* / FACT_*, SCD2 history
L5  Mart       MART_*, with the grain declared in each table
```

Snowflake does **not** receive the whole Silver: 3.327.809 of 7.098.881 rows cross the
boundary (**46,9%** on 2026-09-01). The reason **is not a property of the pipeline** — it's
a function of how much of each source falls within scope, which is why it moves on its
own: it was 3,85% in Phase 2 and 9,95% in Phase 3, with nobody loosening the cut. The INE
population is national and delivers 1,8%; orders and customers are born inside the four
AUFs and deliver ~100%, and Phase 6 multiplied both by 14× and 14×. Without Orders, the cut
is 19,2%. See [DECISIONS.md § "Phase 2"](DECISIONS.md).

## Requirements

Python 3.12, Docker with Compose v2. `make venv` creates the platform environment.

## Usage

```bash
cp .env.example .env && make secrets   # random keys; the compose refuses to start without them
make venv                              # creates platform/.venv and installs the platform
make up                                # MinIO + buckets. Does NOT bring up Kafka, Spark, or Airflow
make daily                             # extract -> validate -> land -> verify -> silver
make test                              # every suite, no network
make help                              # every target, one per line
```

The full sequence, from zero to the dashboard, is in **[From zero to the dashboard](#from-zero-to-the-dashboard)**.

`make up` brings up **only** the data plane. Airflow costs ~2 GB of RAM and isn't needed to
iterate on a dbt model; OLTP and Kafka live under the `stream` profile, and Spark under the
`spark` profile. None of the three comes up on its own, and **nothing in the standard path
depends on them** — `silver_gate.py` removes from the build whatever depends on a table
that doesn't exist, and there's a test proving `make silver` stays green on a tree where
Spark never ran.

`make daily`, `make ine-refresh`, and `make callejero-refresh` are **idempotent**: a
partition that's already complete isn't re-extracted (it's immutable, same `_SUCCESS`
guard on all three), and objects already landed with the expected checksum are skipped, not
re-sent.

Individual targets accept `DATE=` and `WH=` (Mercadona), `DATE=` and `TABLES=` (INE
population), or `DATE=`, `CALLEJERO_IN=`, and `CALLEJERO_PROVINCES=` (INE Callejero).

**Disk space.** `data/` is extraction scratch space and nothing is ever deleted on its
own — an INE partition takes between 264 and 384 MB. After `land` + `verify-landing`, the
object storage is the source of truth:

```bash
make data-usage                                          # quanto cada source ocupa
make prune-local PARTITION=data/ine/ingestion_date=2026-08-25
```

`prune-local` runs **two** checks before removing anything — the local copy against its own
manifest, and the destination against that same manifest — and refuses if either fails.
There's no `--force`, and it's never chained into a `*-refresh`: deleting data is a decision
made by whoever operates it, not a pipeline side effect.

## Structure

```
.
├── ARCHITECTURE.md                     # ADR: what didn't get in, and each one's trigger
├── DECISIONS.md                        # the history: decision -> reason -> evidence -> trade-off
├── BACKLOG.md                          # what does NOT get in, and each item's trigger
├── AI_ENGINEERING_CONSTRAINTS.md       # the engineering constraints for AI/agents
├── Makefile                            # the platform's entry point
├── sources/
│   ├── mercadona-catalog-source/       # Catalog Source, FROZEN, dependencies = []
│   ├── ine-population-source/          # Population Source, FROZEN, dependencies = []
│   ├── ine-callejero-source/           # Callejero Source, FROZEN, dependencies = [] — no API
│   ├── simulated-oltp-source/          # Synthetic customers, FROZEN, dependencies = [] — derived from Silver
│   └── simulated-orders-source/        # Orders as an EVENT LOG, FROZEN — derived from Silver
├── .env.example                        # copy to .env; development-only credentials
├── .env.snowflake.example              # copy to .env.snowflake; account identity, no secret
├── docs/README.md                      # the MAPA benchmark: URL, sha256, how to re-extract
├── docs/demand-evidence/               # BEFORE | MAPA | TARGET | AFTER — generated, plus the frozen BEFOREs
├── docs/warehouse-evidence/            # the real run on Snowflake, dated — generated, not hand-written
├── docs/stream-evidence/               # live OLTP, broker, and projection, dated — generated, not hand-written
├── platform/
│   ├── pyproject.toml                  # boto3, duckdb, dbt-core, dbt-duckdb, dbt-snowflake
│   ├── src/retail_platform/
│   │   ├── config.py                   # endpoint and credentials, from the environment
│   │   ├── manifest.py                 # the consumer contract, in code — generic per source
│   │   ├── land.py                     # partition -> object storage, verified
│   │   ├── verify.py                   # independent re-read and re-check
│   │   ├── query.py                    # configured connection + DuckDB secret
│   │   ├── oltp_reference.py           # Silver -> 3 flat JSON files for the simulated OLTP source
│   │   ├── orders_reference.py         # Silver -> 5 flat JSON files for the orders source
│   │   ├── demand_profile.py           # mapping + MAPA benchmark -> weights per cohort (IPF)
│   │   ├── demand_check.py             # reality check BEFORE | MAPA | TARGET | AFTER + cohort
│   │   ├── orders_oltp.py              # the orders OLTP: state + outbox IN THE SAME transaction
│   │   ├── orders_stream.py            # producer, consumer, and read model; delivery semantics
│   │   ├── orders_projection.py        # the Iceberg projection: two writers, monotonic merge
│   │   ├── snowflake_export.py         # the CUT: Silver -> parquet (10%), no business rule
│   │   ├── snowflake_load.py           # transport: DDL, PUT to an internal stage, COPY INTO, roles
│   │   ├── snowflake_evidence.py       # observes the destination and writes dated evidence
│   │   ├── stream_evidence.py          # observes all THREE live planes; absence is declared
│   │   ├── silver_gate.py              # what to exclude from the Silver dbt build — a SINGLE place
│   │   └── cli.py                      # land / verify-landing / query / prune-local / has-data
│   │                                   # / export-oltp-reference / export-orders-reference
│   │                                   # / orders-oltp-ddl / orders-oltp-init
│   │                                   # / orders-apply / orders-outbox
│   │                                   # / export-snowflake / snowflake-ddl
│   │                                   # / snowflake-bootstrap / load-snowflake / snowflake-evidence
│   │                                   # / stream-evidence / silver-build
│   ├── dbt/seeds/                      # 16 seeds, all with a provenance column
│   │   ├── warehouse_province_map_seed.csv  # wh -> province/municipality (headquarters), INE codes
│   │   ├── warehouse_service_area_seed.csv  # wh -> N municipalities in the same AUF (INE)
│   │   ├── order_premises_seed.csv          # order generator assumptions, ALL `synthetic`
│   │   ├── customer_premises_seed.csv       # who EXISTS: minimum age, denominator, allocation
│   │   ├── ine_municipality_codes_seed.csv  # name (Tempus3) -> municipality code, 08/28/41/46
│   │   ├── ine_ambiguous_series_seed.csv    # series -> official code, for homonymous names in Spain
│   │   ├── ine_ccaa_map_seed.csv            # province -> autonomous community; no default target
│   │   ├── demand_profile_seed.csv          # the demand model configuration, versioned
│   │   ├── demand_category_mapping_seed.csv # Mercadona category -> MAPA group (444 triples)
│   │   ├── demand_seasonality_seed.csv      # what the report does NOT publish per category, declared
│   │   ├── mapa_2025_benchmark_seed.csv     # 64 rows from the report, each citing the section
│   │   ├── mapa_2025_region_seed.csv        # per-capita consumption by autonomous community
│   │   ├── demand_cohort_age_seed.csv       # volume x population by age bracket, `benchmark`
│   │   └── demand_cohort_region_seed.csv    # same, by community; the PDF page in each row
│   ├── dbt/macros/                     # generate_schema_name: absolute GOLD/MART, no prefix
│   ├── dbt/models/silver/              # target dev (duckdb) — 25 models
│   │   ├── warehouse_province_map.sql   # pass-through from the seed to object storage
│   │   ├── warehouse_service_area.sql   # same, for the service area
│   │   ├── order_premises.sql           # same, for the assumptions — carries through to the warehouse
│   │   ├── customer_premises.sql        # same, for the REGISTRATION assumptions (a different domain)
│   │   ├── mercadona/                   # 4 models, grain per wh
│   │   ├── ine_population/              # population series by province AND by municipality
│   │   ├── ine_callejero/               # sections, clusters, streets — official geography
│   │   ├── simulated_oltp/              # synthetic customers + manifest with lineage
│   │   └── simulated_orders/            # the log fold: event, order, line, manifest
│   ├── dbt/models/warehouse/           # target snowflake — 24 models, linked via source()
│   │   ├── sources.yml                  # the 15 STAGE tables: the boundary, declared
│   │   ├── gold/                        # 6 DIM + 8 FACT, SCD2 derived from history
│   │   └── mart/                        # 9 marts, grain in each one's header
│   ├── dbt/tests/                      # Silver singular tests
│   ├── dbt/tests/warehouse/            # same for Gold/Mart (separate: cross ref() doesn't compile)
│   └── tests/                          # no network (in-memory doubles of S3 and Postgres)
│       ├── fake_s3.py                   # validates ChecksumSHA256 the way the server would
│       ├── fake_pg.py                   # RECORDS the transaction boundary: what only the engine
│       │                                # proves (which rollback undoes) is left out, on purpose
│       ├── fake_kafka.py                # a SHARED log: the order between writing and
│       │                                # committing the offset is the delivery semantics
│       └── fake_iceberg.py              # conflict on demand: exercises the monotonic merge
├── orchestration/airflow/dags/         # 6 DAGs: 5 sources + warehouse_load
│   ├── mercadona_catalog_daily.py      # daily cron
│   ├── ine_population_on_demand.py     # no cron — manual trigger
│   ├── ine_callejero_on_demand.py      # no cron — manual trigger, no API
│   ├── simulated_oltp_customers.py     # no cron — the base changes when someone decides
│   ├── simulated_orders_events.py      # no cron — streaming is NOT a DAG task
│   └── warehouse_load.py               # the only one crossing the boundary between two engines
├── streamlit/                          # operations dashboard over the MART (RETAIL_READER)
│   ├── indicators.py                   # the SINGLE SOURCE: SQL and explanation together
│   ├── CONTRACT.md                     # GENERATED from indicators.py — the verification doc
│   ├── contract.py                     # the generator; imports nothing that connects
│   ├── connection.py                   # RETAIL_READER session + `use secondary roles none`
│   ├── smoke.py                        # runs the real app and requires zero exceptions
│   └── app.py                          # the interface, 7 groups — KPI, table, chart, no SQL
├── scripts/                            # each one has a Makefile target; none runs on its own
│   ├── derive_warehouse_province_map.py   # derives the province/municipality seed from the Callejero
│   ├── derive_warehouse_service_area.py   # derives each warehouse's AUF from the INE's AUF_mun.xlsx
│   ├── derive_municipality_codes.py       # derives name (Tempus3) -> official municipality code
│   ├── derive_ambiguous_series.py         # resolves national homonyms via the VALORES_SERIE code
│   ├── gen-secrets.py                     # generates the Airflow keys in .env (mode 600)
│   ├── prove_oltp_atomicity.py            # injects a DATABASE failure and proves both sides fall together
│   ├── prove_stream_semantics.py          # reproduces the duplication window and proves the replay
│   ├── spike_iceberg_duckdb.py            # the CLOSED experiment, run before Milestone 6
│   ├── prove_iceberg_projection.py        # concurrency, monotonic merge, snapshot isolation
│   ├── spike_spark_iceberg.py             # the Phase 7 GATE: can Spark read the pyiceberg catalog?
│   └── prove_warehouse_orders_tests.py    # injects the defect each test claims to catch, into real data
├── jobs/spark/                         # the only code that runs outside the platform's venv
│   ├── session.py                      # the session with the Iceberg catalog; the spike imports from here
│   └── stock_ledger.py                 # balance, stockout, and replenishment — the shape SQL can't express
├── infra/
│   ├── docker-compose.yml              # MinIO + mc + Postgres + scheduler + webserver
│   │                                   # + oltp-postgres and kafka (profile `stream`), and
│   │                                   # spark (profile `spark`); the Iceberg catalog
│   │                                   # lives in oltp-postgres itself
│   ├── Dockerfile.airflow              # the orchestrator image: two runtimes
│   └── Dockerfile.spark                # the Spark image: Iceberg + JDBC + S3FileIO, pinned versions
└── data/                               # extraction scratch, outside version control
    ├── mercadona/                       # ingestion_date=…/wh=…/
    ├── ine/                             # ingestion_date=…/
    ├── callejero/                       # ingestion_date=…/
    ├── oltp-reference/                  # ingestion_date=…/ (ephemeral input, not a partition)
    └── oltp/                            # ingestion_date=…/wh=…/
```

## L1 — RAW in object storage

```
s3://retail-raw/mercadona_catalog_api/ingestion_date=YYYY-MM-DD/wh=<wh>/
    categories/categories.json
    catalog/category_id=<id>.json      (151 objects)
    _manifest.json  _run.log
    _SUCCESS                            <- last object written
```

The partition goes up **byte-for-byte identical**: it's the canonicalization done by the
Source that makes the manifest's sha256 verifiable end to end, and a `.tar` would destroy
both the per-object check and direct reads by DuckDB.

- `_SUCCESS` written last — an interrupted upload never looks complete.
- sha256 of the local file checked **before** the PUT: the platform doesn't propagate
  corruption, and doesn't assume `validate` ran.
- `ChecksumSHA256` declared on the PUT: the server refuses the object if the bytes diverge.
- Versioning turned on for the RAW bucket.

## L2 — Silver

| Model | Grain | Rows/partition |
|---|---|---|
| `raw_manifest` | (ingestion_date, warehouse) | 1 |
| `silver_category` | (…, category_id) | 151 |
| `silver_product_price` | (…, category_id, subgroup_id, source_product_id) | ~4.600 |
| `silver_price_change` | (…, source_product_id) vs. previous partition | ~4.330 |

`raw_manifest` exists so that **"did the Silver lose a row?" is a test**, not a loose
script: the derived count is reconciled against the `totals` declared by the Source.

`silver_price_change` is the only model that crosses dates. It exists because
`price_decreased` is **false in 100% of rows** across the three partitions, while 152
prices changed between 08-16 and 08-24 — the field the source offers to signal variation
signals nothing. It compares each partition against the **previous one that exists**
(`lag` over the partition sequence, not date arithmetic), which keeps it correct across the
8-day gap.

### Grain: why it isn't `source_product_id`

A product appears in more than one category and more than one subgroup — ~270 repeated
rows per partition. That's source semantics, preserved on purpose. A `unique` test on the
isolated id would fail by design, not by defect, so uniqueness is tested on the composite
key.

`source_product_id` is the **source's key**, not a business identity. The
`name_seen_before` column flags new ids whose `display_name` already existed in the
previous partition (6 measured cases) as a review queue, instead of silently treating them
as a new product.

## The five Sources

Each is a **frozen** Python package, with `dependencies = []`, its own physical contract
(`CONTRACT.md`), and its own README. The platform consumes them through the contract on
disk — never importing any of their code. **The reasoning behind each decision is in
[DECISIONS.md](DECISIONS.md);** here is what each one delivers and how to run it.

| Source | What it delivers | Real or synthetic | How to run it |
|---|---|---|---|
| [`mercadona-catalog-source`](sources/mercadona-catalog-source/) | Catalog and price per warehouse and day, from Mercadona's public API | **observed** | `make extract validate land verify-landing WH=mad1` |
| [`ine-population-source`](sources/ine-population-source/) | Population by province (with age and sex) and by municipality (sex only), from the INE's Tempus3 API | **observed** | `make ine-refresh` |
| [`ine-callejero-source`](sources/ine-callejero-source/) | Census sections, population units, streets and segments — with postal code and numbering range | **observed** (manual, twice-yearly download) | `make callejero-refresh` |
| [`simulated-oltp-source`](sources/simulated-oltp-source/) | Customer base, anchored to the real population of the municipality and the real address of the street segment | **synthetic over observed geography** | `make oltp-export-reference && make oltp-refresh-all` |
| [`simulated-orders-source`](sources/simulated-orders-source/) | Order **event log** — not a state snapshot | **synthetic over observed catalog and customers** | `make orders-export-reference && make orders-refresh-all` |

**The real/synthetic distinction is not a footnote.** It travels with the data:
`label = 'synthetic'` on every assumption, `stock_label` on every row of the stock mart,
and the dashboard's *Out of scope* list states what **cannot** be asked. A platform that
mixes the two without a label invites reading simulation density as market penetration.

**The four derived seeds** have a Makefile target and reproduce byte for byte — a
versioned artifact with no command that generates it is indistinguishable from a
hand-typed number:

```bash
make seed-province-map         # wh -> province/municipality, cross-checked against the Callejero
make seed-service-area         # wh -> AUF municipalities (AUF_XLSX=temp/AUF_mun.xlsx)
make seed-municipality-codes   # name (Tempus3) -> official code    [network: INE API]
make seed-ambiguous-series     # series -> code, for homonyms       [network: INE API]
```

## From zero to the dashboard

The standard path does **not** require Kafka, Iceberg, or Spark. The two optional planes
come up on demand, and whatever depends on them leaves the build on its own —
`silver_gate.py` decides this in a single place, and there's a test proving `make silver`
stays green without either of them.

```bash
# 1. data plane
make up                       # MinIO + Postgres + Airflow. Does NOT bring up Kafka or Spark
make daily                    # extract -> validate -> land -> verify -> silver
make ine-refresh callejero-refresh
make oltp-export-reference && make oltp-refresh-all
make orders-export-reference && make orders-refresh-all
make silver && make test

# 2. demand calibration, against MAPA 2025
make demand-check-mapping     # 444 catalog triples, one rule each, zero default
make demand-reality-check     # BEFORE | MAPA | TARGET | AFTER + propensity by cohort

# 3. stream plane (optional) — OLTP, outbox, Kafka, Iceberg projection
make stream-up
make orders-apply-all         # log -> OLTP + outbox, in the SAME transaction
make orders-publish           # outbox -> topic, at-least-once by design
make orders-project           # topic -> live_order_state, idempotent
make orders-rebuild-projection PROJECTION_RESET=1   # the SECOND writer, in batch
make orders-reconcile         # three independent folds; exits 1 if they diverge
make orders-prove-atomicity orders-prove-stream orders-prove-projection

# 4. stock plane (optional) — the Spark job
make spike-spark-iceberg      # the GATE: does Spark read pyiceberg's catalog?
make stock-ledger             # observed consumption -> balance, stockout, and reorder
make spark-evidence           # both engines, and both durations

# 5. warehouse and dashboard
make warehouse-refresh        # export -> load -> dbt on Snowflake
make warehouse-prove-tests    # injects the defect each test claims to catch and demands red
make dashboard                # http://localhost:8501

# 6. close
make freeze                   # seals the RAW capture
make freeze-check             # and confirms it hasn't changed
```


## Analytical warehouse (Snowflake)

```bash
cp .env.snowflake.example .env.snowflake   # account, user, role — no secret in it
make warehouse-bootstrap                   # once per account, requires ACCOUNTADMIN
make warehouse-refresh                     # export -> load -> dbt
make warehouse-evidence                    # records what landed at the destination, dated
```

**Three separate verbs**, for the same reason `land` and `verify-landing` are separate:
`warehouse-export` reads the Silver and writes local parquet without talking to Snowflake;
`warehouse-load` runs `PUT` to an internal stage plus `COPY INTO` and re-checks count
against count; `warehouse` runs `dbt build --target snowflake`. A cut failure is a data
failure and isn't retryable; a load failure is network and is.

**Internal stage, not external.** A managed Snowflake can't see a MinIO on `localhost`;
`PUT file://` reverses the direction and does away with real S3 and a storage integration.

**Credential**: an RSA key pair, never a password — it's the recommended method for
programmatic access and the only one that works for a DAG. The private key lives in
`~/.snowflake/keys/` with mode 600, outside the repository; `config.toml` holds only the
path.

**Governance used, not just verified.** `warehouse-bootstrap` creates three roles, applies
the grants, **and proves the isolation matrix** before returning success — with
`use secondary roles none`, without which the check would pass by mistake. And the
pipeline **wears** the roles: the load runs as `RETAIL_LOADER` and dbt as
`RETAIL_TRANSFORMER`, never as `ACCOUNTADMIN`.

| Role | Can | Cannot |
|---|---|---|
| `RETAIL_LOADER` | write `STAGE` (owns the 13 tables) | read `GOLD` or `MART` |
| `RETAIL_TRANSFORMER` | read `STAGE`, write `GOLD` and `MART` (owns the 21) | — |
| `RETAIL_READER` | read `MART` | read `STAGE` or `GOLD` |

Stopping running as administrator exposed four defects no test would have caught before —
missing `usage` on the warehouse, ownership confused with privilege, and one where the
administrator simply **stops seeing** the objects with no error at all. They're described
in [ARCHITECTURE.md](ARCHITECTURE.md), and became a test.

**Switching Snowflake accounts** means editing `.env.snowflake` and the matching block of
`~/.snowflake/config.toml`, then running `make warehouse-bootstrap`. No model, no SQL, and
no test changes: the L2→L3 boundary is physical. The Lakehouse half doesn't depend on
this — `make silver` and the 1.068 checks in `make test` run with no Snowflake variable
defined.

**Dated evidence.** The Snowflake half isn't reproducible offline the way the Lakehouse
is, and the account used here is a trial. [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
records ownership, volume, the isolation matrix, **execution roles as seen by the verb**,
and a sample of each mart, with the date and account identity — so the models keep having
proof after it expires. The roles table is what separates verified governance from adopted
governance: it shows that `RETAIL_READER` only ever ran `SELECT`, and that whoever wrote
GOLD was `RETAIL_TRANSFORMER` — never the administrator. The console capture at
[docs/warehouse-evidence/screens/query-history.png](docs/warehouse-evidence/screens/query-history.png)
is the same separation seen through the vendor's own interface, which is the one thing here
the repository can't produce on its own.

```bash
make warehouse-ddl   # prints the STAGE DDL without connecting to anything (derived from the cut)
```

## Strategic dashboard (Streamlit over the MART)

**Operations dashboard**, built for whoever looks at the business — not whoever audits the
pipeline: KPI, table, chart, no SQL on screen and no methodology note interrupting each
chart. 22 indicators in 7 groups, reading only the `MART`.

```bash
make dashboard-venv       # once: streamlit/pandas/altair (extra, outside the Airflow image)
make dashboard            # http://localhost:8501
make dashboard-contract   # regenerates streamlit/CONTRACT.md, without connecting to anything
make dashboard-check      # runs the real dashboard and demands zero exceptions (needs an account)
```

**Wears `RETAIL_READER` for real** — it's the first consumer to wear the BI role, and the
proof of that (the live probe confirming the refusal on `GOLD`/`STAGE` with
`use secondary roles none`) lives in the test, not on the screen: `test_dashboard_indicators.py`
and `connection.py::probe_isolation`. A manager doesn't need to see RBAC proven live; the
engineering behind it didn't stop existing just because it left the interface.

**Reads live.** A 60 s cache and a refresh button. The sidebar shows only the most recent
date covered by the marts — enough to know whether what's on screen is today's.

**The technical counterpart didn't disappear — it moved.** The question each indicator
answers, the source grain, what's observed vs. synthetic, and the pitfalls of rebuilding it
in another tool still live in `indicators.py`, versioned together with the same SQL,
published in [`streamlit/CONTRACT.md`](streamlit/CONTRACT.md), and covered by an offline
test that fails if the two fall out of sync. They just no longer render in the production
dashboard. Three examples of what the CONTRACT records:

| Pitfall | The error it avoids |
|---|---|
| Value loss has **two** causes | `SUM(gross) − SUM(net)` mixes a basket that shrank at picking with an order that died before that — causes from different areas |
| Average ticket has **two** denominators | revenue/picked ≠ revenue/placed — the second measures something that doesn't exist |
| `orders_touching_category` **isn't additive** | summing a day's categories gives far more than that day's orders |

What the dashboard does **not** show — margin, observed stock, repeat purchase/LTV/cohort,
route, market penetration, trend — remains declared, with each absence's trigger, in
`indicators.py::FORA_DE_ALCANCE` and in the CONTRACT. The most actionable gap: **no mart
joins customer with order** — the link exists in `FACT_ORDER.customer_sk`, in GOLD, out of
`RETAIL_READER`'s reach by design.

## The Silver `dbt build` gate

Not all 25 Silver models can always be built, and the two reasons are legitimate: a source
that hasn't landed anything yet makes `read_json` **fail** (not return zero rows), and
`silver_live_order_state` can only be read when the Iceberg catalog responds.

`make silver` and the five DAGs call **the same verb**, and it states what it decided:

```
mercadona_catalog_api....... landed
ine_population_api.......... landed
ine_callejero............... landed
simulated_oltp.............. landed
simulated_orders............ landed
iceberg projection.......... CATALOG UNAVAILABLE (excluded)
arguments ................... --exclude silver_live_order_state assert_live_projection_matches_batch_fold
```

**This already lived in six different files**, and the cost showed up: the Mercadona DAG
had no gate at all — it always has data, so nobody missed it — and it started failing every
day the moment Milestone 6 created a model that has nothing to do with its source. `make
silver` passed, `make test` passed, and only the real run failed, a day later.

`plan()` is **pure**, so the whole decision is testable without MinIO and without a
catalog. And a source check requires that no DAG assemble its own `dbt build`, because **a
single gate is only worth it while it stays the only one**. See [ARCHITECTURE.md](ARCHITECTURE.md).

## Querying the Silver

The real data is the **parquet in object storage**. The `platform/dbt/retail.duckdb` file
holds only **views** pointing at it — one per materialized model, no data of its own — sits
outside version control, and `make clean-duckdb` deletes it with no loss.

That's why opening the file with any DuckDB client **fails** with `NoSuchBucket`: the new
session knows neither the endpoint nor the credential, and DuckDB tries the real AWS. Two
ways out:

```bash
make query                                    # summary by partition
make query SQL="select * from silver_price_change where name_seen_before"

make duckdb-secret                            # writes the secret once...
duckdb platform/dbt/retail.duckdb             # ...and from then on any client works
```

`make duckdb-secret` writes a DuckDB secret to `~/.duckdb/stored_secrets` from `.env`.
After that, any client — CLI, DBeaver, notebook — opens the file and queries the views
without configuring anything.

### `where is_latest_ingestion` in the reference models

The INE models (population and Callejero) **stack every `ingestion_date`** — the history is
deliberate. The consequence is that re-extracting the *same* release duplicates equivalent
rows, and it already has: `silver_ine_population_series` ended up with **1.547.496 rows on
each of two dates**. Anyone querying without filtering would count double, with no visible
error.

The seven reference models expose `is_latest_ingestion` precisely so that reading the
current state doesn't depend on the consumer remembering a `max(ingestion_date)`:

```sql
-- current state (what's almost always wanted)
select count(*) from silver_ine_population_series where is_latest_ingestion;

-- full history — now an explicit choice, not an accident
select ingestion_date, count(*) from silver_ine_population_series group by 1;
```

A dbt test guarantees the flag marks **exactly one** date per model. The Mercadona models
**don't** have the column: there, the several `ingestion_date`s are the product (price
history), not a side effect.

> **DuckDB is single-writer.** A client with the file open read-write (DBeaver does this by
> default) **blocks `make silver`**. The target detects this and fails with an actionable
> message instead of a traceback. Three ways out:
>
> ```bash
> # 1. close the connection in the client, or open it in read-only mode
> # 2. write the state somewhere else:
> make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb
> ```
>
> **`make query` is not affected:** it builds the views in memory directly over the parquet
> and doesn't open the file. A *writer* blocks readers too, so depending on the file to
> query would mean depending on nobody having forgotten an open window.
>
> The DAG isn't affected either: the orchestrator's `DUCKDB_PATH` lives inside the
> container, not in the mounted repository.
>
> This doesn't put any data at risk: the file only holds views — the data is the parquet in
> object storage — and `make clean-duckdb` recreates it.

## Verification

```bash
make test          # 1,095 tests, no network: 145 Mercadona + 136 INE population + 95 Callejero
                   #                        + 140 simulated OLTP + 163 orders + 416 platform
make silver        # dbt build on DuckDB: 25 models + 16 seeds + the data tests
make warehouse     # dbt build on Snowflake: 24 models + the data tests
```

`make test` and `make silver` read no Snowflake variable — that's what keeps the Lakehouse
half reproducible when the account doesn't exist.

The two dbt trees are **mutually exclusive per target** (`+enabled` guard in
`dbt_project.yml`): `--target dev` sees only the Silver, `--target snowflake` only the
warehouse. Confirmed with `dbt list`: zero overlap.

Known numbers, used as acceptance criteria:

| partition | rows | unique products | objects in RAW |
|---|---|---|---|
| `mad1` 08-15 | 4.600 | 4.329 | 155 |
| `mad1` 08-16 | 4.599 | 4.328 | 155 |
| `mad1` 08-24 | 4.581 | 4.311 | 155 |
| `bcn1` 08-24 | 4.587 | 4.320 | 155 |
| `vlc1` 08-24 | 4.599 | 4.328 | 155 |
| `svq1` 08-24 | 4.558 | 4.287 | 155 |

All with the same `schema_fingerprint` (`37a3d95d…`): the shape of the response doesn't
vary by warehouse or by date. `silver_price_change` has rows **only** for `mad1` — the
warehouses with a single partition come in with zero, because there's no previous one to
compare against.

| Window | Prices changed | Ids out | Ids in | Name already existed |
|---|---|---|---|---|
| 08-15 → 08-16 | 17 | 2 | 1 | 1 |
| 08-16 → 08-24 | 152 | 33 | 16 | 5 |

Every check was proven **able to fail**: tampering with one byte at the destination fails
`verify-landing` with exit 1; removing a catalog object fails the reconciliation test.

## Operational notes

**There's no backfill.** The API serves only today's price. Extracting for a past date
would write today's prices under that date's key — silently wrong data. The days 08-17
through 08-23 are lost irrecoverably. See [ARCHITECTURE.md](ARCHITECTURE.md).

**The container runs with your UID, not Airflow's.** The Source writes files with mode
`600` (a consequence of `tempfile.mkstemp()` for atomic writes), so a container running as
the image's default `airflow` user (50000) **cannot read the partition**. `AIRFLOW_UID` in
`.env` fixes this — generate it with `id -u`. There's no group fallback.

**Four warehouses.** The DAG covers `mad1`, `bcn1`, `vlc1`, and `svq1` — four cities, ~608
requests/day, ~15 min. `wh` changes assortment **and** price: between `mad1` and `bcn1`, of
4.040 shared products, **124 (3,1%) have a different price**, by up to ±24%; and 551
products (12,0%) exist in only one of the two. The source serves 7 warehouses — the choice
of these four is based on pairwise-measured assortment divergence, and `alc1` was left out
for duplicating `vlc1`. Full criteria and matrix in [ARCHITECTURE.md](ARCHITECTURE.md).

**An invalid `wh` doesn't fail — it falls back to `vlc1`.** The source returns `200` for
any unknown code. A typo in `WAREHOUSES` produces a partition labeled with the wrong code
containing Valencia's data, internally consistent and invisible to the tests. Double-check
the code before adding a warehouse.

**One extraction per day, per warehouse.** The partition is immutable and the host's
`robots.txt` declares `Disallow: /api`. The `mercadona_api` pool with 1 slot serializes
requests because the Source's throttle is per-process — two concurrent `extract` runs
double the real rate.

**Airflow.** `make airflow` brings up the full stack — Postgres for the metadata, a
scheduler with **LocalExecutor**, and a webserver on `:8080` (`admin`/`admin`).

```bash
make airflow           # builds the image and brings up the stack
make airflow-trigger   # unpauses and triggers today's DAG run
make airflow-logs      # follows the scheduler
make airflow-down      # tears down only Airflow, keeping MinIO up
```

`LocalExecutor` is a deliberate choice, not a convenience: it's the only executor in which
the `mercadona_api` pool's 1 slot means anything. With `SequentialExecutor` the
serialization would happen by accident, not by mechanism — and the mechanism is what
protects the source's throttle.

### Two runtimes in the image, on purpose

Airflow and `dbt-core` pin incompatible versions of `jinja2`, `click`, and `pydantic`.
Instead of fighting that, the image has two:

| Runtime | What runs | Why it fits there |
|---|---|---|
| `/usr/local/bin/python` | Airflow + **the Source** | The Source has `dependencies = []`: there's no third-party package to conflict with |
| `/opt/platform-venv` | `boto3`, `duckdb`, `dbt-duckdb` | Isolated from Airflow's pins |

**No code goes into the image.** The repository is mounted at `/opt/retail-lakehouse` and
reached via `PYTHONPATH`, so editing a dbt model or a platform module doesn't require a
rebuild — only the dependencies live in the image.

Inside the compose network the MinIO endpoint is `minio:9000`, not `localhost:9000`.
Compose overrides `S3_ENDPOINT` for the Airflow services, and that works because
`config.load_dotenv()` **does not** override a variable already present in the
environment — `.env` is a development default, not an authority.

## What changes on a new machine

**This project's RAW is not reproducible, and promising it were would be false.** The
Mercadona API is live, the Callejero is a manual, twice-yearly download, and the MAPA URL
points to "latest data." Running the extraction tomorrow produces a different capture — and
that's not a defect, it's the nature of public sources.

What **is** guaranteed: everything downstream is deterministic **given the same RAW**. The
same manifests produce the same Silver, the same warehouse, and the same marts.

That's where the rule for where each number can live comes from, and it applies to whoever
edits the documentation:

| Nature of the number | Where it can live |
|---|---|
| Structural — grain, invariant, ratio by construction | README, ARCHITECTURE |
| Property **of this capture** — measured counts, percentages | generated page, or explicitly dated |

On a new machine the operator runs `make freeze`, which seals **their capture** in
[`docs/FREEZE.md`](docs/FREEZE.md) with a `capture_id`. From then on `make freeze-check`
fails if any sealed partition changes — and the test starts guarding their capture, not the
one that produced the numbers published here.

**Three things require a download or a credential and don't come up on their own:** the
Callejero files (`temp/`), the Snowflake account (`.env.snowflake`, RSA key outside the
repository), and the MAPA report PDF. Without them the standard path still runs — what
disappears is the analytical layer and the calibration, and each absence is declared where
it would appear.

## The fourteen closing questions

This is an **index**, not a new explanation: each answer fits on one line and points to
where the evidence lives. It exists because the project closed and a reader has the right
to check, without reading 3.000 lines, whether the documentation backs up what it claims.

| | Question | Short answer | Where the evidence lives |
|---|---|---|---|
| 1 | What does the project do? | Ingests 5 sources, preserves the RAW, produces typed Silver, serves a dimensional model, and keeps a stream plane that converges to the same state as the batch | [Layers](#layers) |
| 2 | Which data is **real**? | Mercadona's catalog and price; INE population; INE geography (sections, streets, clusters); the MAPA 2025 report, used as a **benchmark** | [The five Sources](#the-five-sources) · [`docs/README.md`](docs/README.md) |
| 3 | Which data is **synthetic**? | Customers and orders — invented over real attributes (the address exists, the person doesn't); and the **stock ledger**, computed from observed consumption plus a policy in a seed. No source in this repository measures stock | seeds `*_premises_seed.csv`, all with a provenance column · `stock_label = 'synthetic'` in `MART_STOCK_HEALTH` |
| 4 | Why does each technology exist? | Each one has an adoption date, the trigger that fired it, and what **wasn't** proven | [ARCHITECTURE.md § "What didn't get in, and when it gets in"](ARCHITECTURE.md) |
| 5 | Why does Spark exist while **being slower**? | Three reasons, and performance is not one of them: cumulative state whose output depends on the prior state; Iceberg interop demonstrated by a spike; and the need to validate more than one engine writing the same catalog. At this volume pure Python is ~3× faster, and the number is published | [`docs/spark-evidence/`](docs/spark-evidence/README.md) · [DECISIONS.md § "Phase 7"](DECISIONS.md) |
| 6 | What is **Kafka's** role? | **Transport**, never the canonical source. `publish → broker ack → mark the outbox`, at-least-once, with the duplication window reproduced in a test | [`docs/stream-evidence/`](docs/stream-evidence/README.md) |
| 7 | What is **Iceberg's** role? | Atomic commit with optimistic concurrency between writers, and **interop between engines** — asserted in Phase 3, demonstrated in Phase 7 with three writers on the catalog (`platform`, `rebuild`, `spark`) | `make spike-iceberg` · `make spike-spark-iceberg` |
| 8 | What is the **system of record** at each stage? | RAW in object storage is the point of no return; the OLTP is the origin of the event (state + outbox in the same transaction); Kafka is transport; the projection and the Silver are **derived** and disposable | [L1 — RAW](#l1--raw-in-object-storage) · [ARCHITECTURE.md](ARCHITECTURE.md) |
| 9 | How does it handle **duplication**? | Consumer-side dedup by `(order_id, sequence_no)`: `seq <= last` is discarded. Duplication in the window between publishing and marking the outbox is **accepted and declared** — end-to-end exactly-once is not promised | `make orders-prove-stream` · `fake_kafka.py` |
| 10 | How does it handle **gaps**? | `seq == last + 1` applies; `seq > last + 1` is a gap and **stops** instead of applying out of order | `make orders-prove-stream` |
| 11 | How does it handle **concurrency**? | Optimistic conflict on Iceberg with the full cycle: detect → reload → reapply → retry, and an old `seq` **never** overwrites a newer `seq`. Demonstrated even **across different engines** | `make orders-prove-projection` · `make spike-spark-iceberg` |
| 12 | How does it know the **folds agree**? | `make orders-reconcile` closes across all three paths — batch, Iceberg projection, and Postgres sink — over 206.523 orders. It was disagreement between folds that found **three** real defects in this project | [Verification](#verification) |
| 13 | How does it know the **RAW hasn't changed**? | `make freeze` seals `(path, sha256, bytes, records)` for 81 partitions into a `capture_id`; `make freeze-check` re-reads the RAW and exits 1 on any difference | [`docs/FREEZE.md`](docs/FREEZE.md) |
| 14 | What **limitations** remain? | Eight debt items, each with problem, impact, status, and next step; plus the scope that requires a new source | [ARCHITECTURE.md § "Technical debt"](ARCHITECTURE.md) · [BACKLOG.md](BACKLOG.md) |

### What is **not** demonstrated in this project

Written in these words on purpose. The absence of proof is information, and erasing it
would be the only way to make this list look nice:

- **That Spark scales.** It runs `local[*]` — driver and executor on the same JVM, no
  shuffle between nodes. **Not demonstrated in this project.**
- **That anyone needs Kafka's latency, or that the broker is the origin.** The canonical
  log still originates on disk. **Not demonstrated in this project.**
- **End-to-end exactly-once.** It isn't promised, and the window where duplication happens
  is reproduced in a test rather than hidden.
- **That the RAW is reproducible.** It isn't, by the nature of the sources. What's
  guaranteed is `same frozen RAW → reproducible downstream`.
- **That the operation is production-grade.** There's no real traffic, SLO, on-call, or
  incident. What exists is behavior verified in tests and dated execution evidence.
- **That purchasing behavior is realistic.** Customer and order are synthetic, calibrated
  against a **benchmark** that is never treated as ground truth.
