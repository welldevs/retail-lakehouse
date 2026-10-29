# Operations — running and inspecting the platform

The [README](../README.md) carries the pitch, the layers and one quickstart. This page holds
what you need once the pipeline runs: what lands where, how the Silver build decides what to
build, how to query it, how the containers behave, and what changes on a new machine. The
design reasoning stays in [ARCHITECTURE.md](../ARCHITECTURE.md); the history in
[DECISIONS.md](../DECISIONS.md).

## Everyday targets

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
make data-usage                                          # how much each source occupies
make prune-local PARTITION=data/ine/ingestion_date=2026-08-25
```

`prune-local` runs **two** checks before removing anything — the local copy against its own
manifest, and the destination against that same manifest — and refuses if either fails.
There's no `--force`, and it's never chained into a `*-refresh`: deleting data is a decision
made by whoever operates it, not a pipeline side effect.

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

## L2 — Silver (the Mercadona core)

| Model | Grain | Rows/partition |
|---|---|---|
| `raw_manifest` | (ingestion_date, warehouse) | 1 |
| `silver_category` | (…, category_id) | 151 |
| `silver_product_price` | (…, category_id, subgroup_id, source_product_id) | ~4,600 |
| `silver_price_change` | (…, source_product_id) vs. previous partition | ~4,330 |

`raw_manifest` exists so that **"did the Silver lose a row?" is a test**, not a loose
script: the derived count is reconciled against the `totals` declared by the Source.

`silver_price_change` is the only model that crosses dates. It exists because
`price_decreased` is **false in 100% of rows**, while prices do change between partitions
(152 between 08-16 and 08-24) — the field the source offers to signal variation signals
nothing. It compares each partition against the **previous one that exists** (`lag` over
the partition sequence, not date arithmetic), which keeps it correct across the 8-day gap.

### Grain: why it isn't `source_product_id`

A product appears in more than one category and more than one subgroup — ~270 repeated
rows per partition. That's source semantics, preserved on purpose. A `unique` test on the
isolated id would fail by design, not by defect, so uniqueness is tested on the composite
key.

`source_product_id` is the **source's key**, not a business identity. The
`name_seen_before` column flags new ids whose `display_name` already existed in the
previous partition as a review queue, instead of silently treating them as a new product.

## The Silver `dbt build` gate

Not all 25 Silver models can always be built, and the two reasons are legitimate: a source
that hasn't landed anything yet makes `read_json` **fail** (not return zero rows), and
`silver_live_order_state` can only be read when the Iceberg catalog responds.

`make silver` and the five source DAGs call **the same verb** (`silver_gate.py`), and it
prints what it decided — which sources landed, which Iceberg tables are reachable, and the
resulting `--exclude` arguments.

**This used to live in six different files**, and the cost showed up: the Mercadona DAG
had no gate at all — it always has data, so nobody missed it — and it started failing every
day the moment Milestone 6 created a model that has nothing to do with its source. `make
silver` passed, `make test` passed, and only the real run failed, a day later.

`plan()` is **pure**, so the whole decision is testable without MinIO and without a
catalog. And a source check requires that no DAG assemble its own `dbt build`, because **a
single gate is only worth it while it stays the only one**.

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
rows, and it already has: `silver_ine_population_series` once ended up with the same
1,547,496 rows on each of two dates. Anyone querying without filtering would count double,
with no visible error.

The reference models expose `is_latest_ingestion` precisely so that reading the current
state doesn't depend on the consumer remembering a `max(ingestion_date)`:

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
> message instead of a traceback. Close the connection (or open it read-only), or write the
> state somewhere else: `make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb`.
>
> **`make query` is not affected:** it builds the views in memory directly over the parquet
> and doesn't open the file. The DAG isn't affected either: the orchestrator's
> `DUCKDB_PATH` lives inside the container, not in the mounted repository. No data is at
> risk — the file only holds views, and `make clean-duckdb` recreates it.

## Acceptance numbers for the first partitions

Measured on the August 2026 partitions and kept as acceptance criteria for the extraction:

| partition | rows | unique products | objects in RAW |
|---|---|---|---|
| `mad1` 08-15 | 4,600 | 4,329 | 155 |
| `mad1` 08-16 | 4,599 | 4,328 | 155 |
| `mad1` 08-24 | 4,581 | 4,311 | 155 |
| `bcn1` 08-24 | 4,587 | 4,320 | 155 |
| `vlc1` 08-24 | 4,599 | 4,328 | 155 |
| `svq1` 08-24 | 4,558 | 4,287 | 155 |

All with the same `schema_fingerprint` (`37a3d95d…`): the shape of the response doesn't
vary by warehouse or by date.

| Window | Prices changed | Ids out | Ids in | Name already existed |
|---|---|---|---|---|
| 08-15 → 08-16 | 17 | 2 | 1 | 1 |
| 08-16 → 08-24 | 152 | 33 | 16 | 5 |

## Operational notes

**There's no backfill.** The API serves only today's price. Extracting for a past date
would write today's prices under that date's key — silently wrong data. The days 08-17
through 08-23 are lost irrecoverably. See
[ARCHITECTURE.md § "Four measured constraints"](../ARCHITECTURE.md).

**The container runs with your UID, not Airflow's.** The Source writes files with mode
`600` (a consequence of `tempfile.mkstemp()` for atomic writes), so a container running as
the image's default `airflow` user (50000) **cannot read the partition**. `AIRFLOW_UID` in
`.env` fixes this — generate it with `id -u`. There's no group fallback.

**Four warehouses.** The DAG covers `mad1`, `bcn1`, `vlc1`, and `svq1` — ~608
requests/day, ~15 min. `wh` changes assortment **and** price. The source serves 7
warehouses; the choice of these four, and the matrix behind it, is in
[ARCHITECTURE.md](../ARCHITECTURE.md).

**An invalid `wh` doesn't fail — it falls back to `vlc1`.** The source returns `200` for
any unknown code. A typo in `WAREHOUSES` produces a partition labeled with the wrong code
containing Valencia's data, internally consistent and invisible to the tests. Double-check
the code before adding a warehouse.

**One extraction per day, per warehouse.** The partition is immutable and the host's
`robots.txt` declares `Disallow: /api`. The `mercadona_api` pool with 1 slot serializes
requests because the Source's throttle is per-process — two concurrent `extract` runs
double the real rate.

**Airflow.** `make airflow` brings up the full stack — Postgres for the metadata
(`airflow-postgres`), a scheduler with **LocalExecutor**, and a webserver on `:8080`
(`admin`/`admin`).

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
[`FREEZE.md`](FREEZE.md) with a `capture_id`. From then on `make freeze-check` fails if any
sealed partition changes — and the test starts guarding their capture, not the one that
produced the numbers published here.

**Three things require a download or a credential and don't come up on their own:** the
Callejero files (`temp/`), the MAPA report PDF, and — only for the opt-in Snowflake path —
a Snowflake account (`.env.snowflake`, RSA key outside the repository). Without them the
standard path still runs on Postgres — what disappears is the Callejero refresh, the
calibration, and the Snowflake evidence, and each absence is declared where it would appear.
