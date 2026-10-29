# Mercadona Catalog API Source

Catalog data Source. Extracts **product, category and price** from Mercadona's public API
(`https://tienda.mercadona.es/api`) and produces partitioned, reproducible and verifiable
snapshots.

This repository is a **Source**, not a transformation platform. The full contract with the
consumer is in [CONTRACT.md](CONTRACT.md).

- **Does:** connects to the API · discovers the category tree · extracts products and prices ·
  controls the request rate · retries with backoff · resumes incomplete partitions ·
  produces partitioned snapshots · records metadata and provenance · validates
  integrity, coverage, schema and quality.
- **Does not:** deduplication · dimensional modeling · normalization · Silver/Gold ·
  enrichment · history · metrics · Spark · dbt · Airflow · Kafka · Snowflake · S3.

## Requirements

Python 3.12+ and nothing else. Standard library only. `dependencies = []` in
[pyproject.toml](pyproject.toml) — there is nothing to install.

## Location

This Source lives in `sources/mercadona-catalog-source/` of the monorepo and stays independent
(its own `pyproject.toml`, `dependencies = []`, its own suite — see
[ARCHITECTURE.md](../../ARCHITECTURE.md)). Day to day, use the **root Makefile**
(`make -C ../.. extract validate`), which points `--out` at the shared `data/`; running
`make extract` **from here** writes a local `data/mercadona/` instead, useful only for
isolated testing.

## Usage

```bash
make test                  # 145 tests, no network and no dependencies
make extract               # today's snapshot (UTC), ~4 min
make validate              # validates today's partition in --strict mode
make check                 # test + validate
```

Without `make`, with `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m mercadona_catalog_source extract  --out data/mercadona
PYTHONPATH=src python3 -m mercadona_catalog_source validate \
    "data/mercadona/ingestion_date=$(date -u +%F)/wh=mad1" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Once the package is installed, the `mercadona-catalog-source` console script is available
with the same subcommands, without needing `PYTHONPATH`:

```bash
make install               # creates venv/, installs the build tools and the package (-e .)
./venv/bin/mercadona-catalog-source --version
./venv/bin/mercadona-catalog-source validate "data/mercadona/ingestion_date=$(date -u +%F)/wh=mad1" --strict
```

### `extract`

| Flag | Default | Effect |
|---|---|---|
| `--out` | `data/mercadona` | snapshot root directory |
| `--wh` | `mad1` | warehouse. Validated as the token `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; whether the code exists is only known from the API response — and that response does not tell: **any** code, including a typo (`zzz9`, `mad9`, or no `--wh` at all), returns `200` falling back to the content of `vlc1` (measured, see [ARCHITECTURE.md § "Two facts about the source that change how this is operated"](../../ARCHITECTURE.md)). The 7 warehouses that actually serve a catalog (measured on `2026-08-24`): `mad1`, `mad2`, `mad3`, `bcn1`, `vlc1`, `svq1`, `alc1` — plus `vlc2`/`pmi1`, recognized by the server but with an empty category tree (`200`, not `404`) |
| `--lang` | `es` | API language. **Fixed per partition**: reopening with another language is refused with code 2, unless `--overwrite`, which rewrites the whole partition |
| `--date` | today (UTC) | partition date. Validated as `YYYY-MM-DD` |
| `--delay` | `1.5` | minimum interval between requests, in seconds |
| `--timeout` | `30.0` | timeout of each request |
| `--max-retries` | `5` | extra attempts per request |
| `--limit` | — | processes only the first N level-2 categories. **`N ≥ 1`**; `0` and negatives are refused with code 2. Marks the partition as `complete: false` |
| `--overwrite` | off | rewrites the partition, even if it is complete |

### `validate`

| Argument | Effect |
|---|---|
| `partition` | partition path (positional, required) |
| `--strict` | besides integrity, fails if any record has a missing in-scope field |

## Layout

```
.
├── CONTRACT.md                      # contract with the consumer
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadata; dependencies = []
├── requirements.txt                 # runtime: empty by construction, verified by a test
├── requirements-build.txt           # setuptools / wheel / packaging, frozen
├── src/mercadona_catalog_source/
│   ├── __init__.py                  # source name, manifest version
│   ├── __main__.py                  # python -m mercadona_catalog_source
│   ├── cli.py                       # subcommands and exit codes
│   ├── canonical.py                 # canonical form + atomic write
│   ├── http_client.py               # sequential client, throttle and backoff
│   ├── schema.py                    # defensive navigation and fingerprint
│   ├── partition.py                 # identity, immutability, _SUCCESS
│   ├── extract.py                   # the two extraction stages
│   └── validate.py                  # independent validation
├── tests/                           # 145 tests, stdlib unittest, no network
└── venv/                            # build environment, created by `make venv`; ignored
```

The snapshots **do not live here**. The partition the platform consumes lives in
`<root>/data/mercadona/ingestion_date=…/wh=…/`, two levels up, with this shape:

```
data/mercadona/ingestion_date=2026-08-24/
└── wh=mad1/
    ├── categories/categories.json
    ├── catalog/category_id=<id>.json     (151 files)
    ├── _manifest.json
    ├── _SUCCESS
    └── _run.log
```

The `.gitignore` sits at the monorepo root and covers `venv/`, `data/` and the atomic
write's temporary files.

## Output layout

The files contain the API payload **intact in content** — no field is added, removed,
renamed or type-converted (prices stay strings, e.g. `"17.75"`) — but **re-serialized
canonically**: `json.dumps(ensure_ascii=False, sort_keys=True, indent=2)` plus a trailing
newline.

So they **are not the original bytes of the HTTP response**. Not preserved: original key
order, formatting, Unicode escapes and the lexical spelling of floats; duplicate keys, if
any, collapse to the last occurrence. Nothing from the HTTP layer (headers, `ETag`,
`Content-Encoding`) is kept.

The **recorded SHA-256 is that of the canonical file produced by the Source**, not that of
the HTTP response. Canonicalization is what makes the digest stable across runs with the
same content — raw bytes would produce different digests for identical data every time the
server reordered keys. The write is atomic (temporary file + `fsync` + `os.replace`): an
interrupted process never leaves a partial file in the final place.

**[`sample-catalog-response.json`](sample-catalog-response.json)** is a real capture,
byte for byte as the Source writes it: `category_id=112` (level 2 `Aceite, vinagre y sal`),
captured at `wh=mad1`, `ingestion_date=2026-09-04`, sha256
`b5bfb47c7dfea67081dfe1ad3b2dde3c5fc775b2fecf80b39475fa1aed5919f0`. It exists so that prices
are not copied by hand into this README: Mercadona prices change, and a number typed here
would rot at the first real update while the text kept claiming otherwise — open the file
to see the price in force on the capture date.

Structurally, what holds regardless of any update: inside it, subgroup
`Aceite de oliva` → product `Aceite de oliva 0,4º Hacendado` appears **twice**, once as
id `4241` (5 L) and once as id `4240` (1 L) — same `display_name`, different packages. The
key is `id`, not the name. None of the files contains `currency`, `EUR` or `€`.

### `_manifest.json`

Records `manifest_version`, `run_id`, run window, duration, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint` and the file list with `sha256`,
`bytes` and `records`. Categories that failed go into `failures[]`; events handled during
the extraction go into `anomalies[]`; the summary of previous runs over the same partition
goes into `history[]`.

`files[].path` is relative to the **snapshot root** (`--out`), not to the partition: it
starts at `ingestion_date=…/wh=…/`. To resolve it from the partition path, go up two
levels.

## Recorded run

Environment: Python 3.12.7, Linux. Three partitions on disk, all `wh=mad1`, all validated.

| Metric | `2026-08-15` | `2026-08-16` | `2026-08-24` |
|---|---|---|---|
| `run_id` | `20260815T143445Z_mad1` | `20260816T214655Z_mad1` | `20260824T140006Z_mad1` |
| HTTP requests | 152 (1 tree + 151 categories) | 152 | 152 |
| Retries / Failures | 0 / 0 | 0 / 0 | 0 / 0 |
| Duration | 227.3 s | 227.0 s | 227.8 s |
| Level-1 / level-2 categories | 26 / 151 | 26 / 151 | 26 / 151 |
| Catalog files | 151 | 151 | 151 |
| Product rows | 4,600 | 4,599 | 4,581 |
| Unique products | 4,329 | 4,328 | 4,311 |
| Volume | 8,291,070 bytes | 8,289,262 bytes | 8,261,218 bytes |
| `schema_fingerprint` | `37a3d95d…` | `37a3d95d…` | `37a3d95d…` (same in all three) |
| `complete` | `true`, with `_SUCCESS` | `true`, with `_SUCCESS` | `true`, with `_SUCCESS` |

All in `manifest_version: 2`, all with 152 declared files. The `2026-08-15` partition was
written before `anomalies[]` was introduced and does not have the field — read it as an
empty list, per [CONTRACT.md § 1](CONTRACT.md).

**There is an 8-day gap** between `2026-08-16` and `2026-08-24`. The days `08-17` to `08-23`
**do not exist and cannot be recovered**: the API only serves today's price, and the Source
has no way to reconstruct the past. That is why the extraction needs to be scheduled, not
triggered by hand.

**Validation** (`--strict`, all): 152/152 files checked by checksum, 0 orphans,
0 anomalies, coverage 151/151 categories, same schema, totals re-checked, 0 products
without a name, 0 without a price, 0 without a category — 100.00% completeness. Output `OK`,
code 0.

**Suite**: 145 tests, `OK`, no network access.

**Immutability**: re-running `extract` on a complete partition returns code 2 with
`partition is already complete and is immutable`.

### Variation observed between partitions

Temporal change at the source, measured over the snapshots — not inferred.

**1-day window**, `2026-08-15` → `2026-08-16`:

- **17 prices changed**, almost all fresh produce: `Coliflor` 3.50 → 3.63, `Patata`
  0.42 → 0.44, `Mango` 1.50 → 1.46, `Piña` 3.72 → 3.66, `Melocotón amarillo` 0.57 → 0.60.
- **2 products left**, **1 came in**: `Chicles gragea original frutas sin azúcar Hacendado`
  switched from `id` 65135 (1.00) to 24537 (1.95) — the same `display_name`, in the same
  category (95) and the same subgroup (389), with another key and another price. Neither the
  name nor the `id` works as business identity: the source does not say whether it is the
  same item re-keyed or one item withdrawn and another launched.
- `schema_fingerprint` identical: the response shape did not change between the two days.

**8-day window**, `2026-08-16` → `2026-08-24` — the same source, measured over a longer
interval, changes much more:

- **152 prices changed** among the 4,295 ids present in both snapshots.
- **33 ids left**, **16 came in**. Of those 16, **5 have a `display_name` that already
  existed** on `08-16` — that is, the source reissued items under another key, without
  signaling. See [CONTRACT.md § 4.5](CONTRACT.md): `source_product_id` is not business
  identity.
- `schema_fingerprint` identical here too.

**`price_decreased` is false in 100% of rows across the three partitions**, despite the 152
changed prices. The field the source offers to signal variation signals nothing: the only
way to detect a price change is a **snapshot diff**. A fact about the source, recorded
here; the Source does not interpret it.

The Source **does not interpret** this: it delivers the snapshots. Any reading of history,
SCD or price variation belongs to the next layer.

### Row repetition

Figures from the `2026-08-15` partition. The 4,600 rows contain 4,329 unique products, and
the **271 surplus rows** are:

- **261** — the same product in **two different level-2 categories** (4,068 products in
  1 category, 261 in 2; none in 3 or more).
- **10** — the same product in **two subgroups of the same file**: 8 cola soft drinks
  in `Cola sin cafeína` / `Cola zero` (category 158), 1 in `Cepillo de dientes` /
  `Pasta de dientes` (186) and 1 in `Pollo` / `Pavo y otras aves` (38).

Source semantics, deliberately preserved. Resolution belongs to the downstream layer.

### Observed price fields

Present in 100% of records across the three partitions: `unit_price`, `reference_price`,
`reference_format`, `bulk_price`, `previous_unit_price`, `price_decreased`,
`tax_percentage`.

Measured fill rate:

| | `2026-08-15` | `2026-08-16` | `2026-08-24` |
|---|---|---|---|
| `unit_price` string, non-null | 100% | 100% | 100% |
| `tax_percentage` non-null | 100% | 100% | 100% |
| `previous_unit_price` non-null | 205 (4.5%) | 211 (4.6%) | 194 (4.2%) |
| `price_decreased` true | **0** | **0** | **0** |

Two factual observations the consumer needs to know, both measured and neither
interpreted here:

1. **`previous_unit_price` comes with leading spaces** — `"       18.75"` — in **100% of
   non-nulls** (205/205, 211/211, 194/194). Trimming before converting is the consumer's
   obligation; different engines tolerate the padding in different ways.
2. **`iva` is null in 100% of rows** while `tax_percentage` is filled. The useful field is
   the second one.

## Operational behavior

**Throttle.** Sequential requests, limited to `1/--delay` req/s, measured from the start of
the previous request. With the 1.5 s default, the recorded run took 227.3 s for 152
requests, with 0 blocks.

**Retry.** Status `403, 408, 429, 500, 502, 503, 504` and any non-HTTP error (timeout,
connection reset, invalid JSON body) are retried with exponential backoff
`5s → 10s → 20s → 40s → 60s` (60 s cap), up to `--max-retries`. Other statuses fail
immediately — which is the case for the `404`/`410` of level-1 ids.

**Resume.** A file already present in the partition is not re-downloaded, unless
`--overwrite`. It is automatically re-downloaded from the source when it is unreadable,
truncated, has an unexpected shape, is **not in canonical form** or **differs from the
previous manifest** — in the last three cases the event goes into `anomalies[]`. A
partition is immutable: content altered on disk is not valid input and is not promoted to
the new truth by a re-run.

**Identity before the fan-out.** A preliminary manifest (`complete: false`) is written right
after the category tree. Without it, an interrupted run would leave the partition without a
manifest — and the immutability and fixed-language locks, which depend on it, would be
switched off on resume.

**Inventory.** At the end, the extraction sweeps `catalog/` and declares in the manifest the
valid files that run did not visit (the `--limit` case, or a category that left the
source's tree). That way the manifest describes the partition, not just what the run
touched.

**Exit codes.** See [CONTRACT.md § 7](CONTRACT.md). They belong to the process that runs;
the downstream consumer decides by `_SUCCESS` and `_manifest.json`.

## Source restrictions

Verified against the API, not assumed:

- **Only level-2 category ids** are reachable at `/api/categories/{id}/`. Level-1 ids
  respond `404`/`410`. The tree is flattened to level 2, deduplicated by id.
- **Behavior under load.** With 8 threads, 152 requests in 9 s produced ~20% `403`s, and
  the host started answering `403` intermittently for a few minutes. The same 152
  sequential requests at 1.5 s had 0 failures, across repeated runs.
  **Behavior beyond 152 requests per run was not measured**, and a WAF was not
  distinguished from an edge rate limit — only the status was observed.
- **`wh` changes assortment AND price.** Measured over the whole catalog, `mad1` vs `bcn1`
  on `2026-08-24`: 4,311 products in `mad1`, 4,320 in `bcn1`, **4,040 in common** — that is,
  271 exclusive to one and 280 to the other. Of the 4,040 in common, **124 (3.1%) have a
  different price**, with variation of up to ±24%, concentrated in perishables (`Sandía`
  4.82 → 3.86; `Solomillo de cerdo` 3.90 → 4.84) and soft drinks.

  An earlier measurement in this file, restricted to **category 112** (35 products in
  common), found no price difference and concluded that `wh` only varied the assortment.
  **That conclusion was wrong** — it was an artifact of the sample size. Recorded here
  because a consumer who treats price as independent of the warehouse is wrong on 3% of the
  rows.

  `pmi1` is recognized by the server but returns an empty category tree (`200`,
  `content-length: 52`) — not `404` (see [ARCHITECTURE.md § 2.2](../../ARCHITECTURE.md)).
- **`robots.txt` of `tienda.mercadona.es` declares `Disallow: /api`**, and the client sends
  a **Chrome browser User-Agent**, not a robot identifier
  ([http_client.py](src/mercadona_catalog_source/http_client.py)). Using this route as a
  source is the operator's decision, taken with these two facts in view. The Source
  operates at the lowest possible impact — sequential, 152 requests per run, no
  parallelism — but that does not cancel the directive. Note also that `403` is among the
  retryable statuses: a deliberate block is retried up to 5 times. An alternative allowed
  by `robots.txt` (empty `Disallow:`): the mirror `mercadonacercademi.com`, at the cost of
  ~2 h per extraction and no prices on the listing pages.
- **No cross-source key.** `ean`, `gtin`, `barcode`, `brand` and `origin` do not exist in
  this endpoint's payloads; `product.id` is the **source key**, with no guarantee of
  temporal stability — it identifies the record within the snapshot, but the source may
  reissue the same commercial item under another id without signaling
  ([CONTRACT.md § 4.5](CONTRACT.md)). The `/api/products/{id}/` endpoint would bring them,
  at the cost of +4,329 requests (~1h50 at the safe throttle) against a host with
  `Disallow: /api`, and it is outside the product/category/price scope.
- **No transactional fact.** No known API endpoint exposes sales, orders or stock.

## Validation

`validate` re-reads and re-parses every file declared in the manifest, recomputing
checksums, counts and the fingerprint instead of accepting the recorded values, and sweeps
the directory for undeclared files.

**Integrity** — always fatal: manifest missing, unreadable or without the `files` list ·
incompatible `manifest_version` · malformed manifest entry (no `path`) · entry pointing
outside the snapshot root · declared file missing · SHA-256 or size mismatch · invalid
JSON · record count mismatch, including the tree's · file on disk not in the manifest, at
any depth · `_SUCCESS` inconsistent with `complete` · category tree missing or unreadable ·
level-2 category without a file (when the partition is not partial) · `schema_fingerprint`
mismatch · `totals.product_rows`, `totals.unique_product_ids`, `totals.catalog_files` or
`totals.bytes` differing from the re-read · no product read despite catalog files ·
failure recorded in `failures[]`.

**Warnings** — reported, never fatal: incomplete coverage in a `--limit` partition; each
entry of `anomalies[]`.

**Quality** — measured and reported; fatal only with `--strict`: product without
`display_name`, without `price_instructions.unit_price` or without category lineage.

In a partial partition (`--limit`), incomplete coverage becomes a **warning**, not an error.

## Dependencies and container

**Runtime: none.** Every import in the package and in the tests is from the Python 3.12
standard library, and `[project.dependencies]` is empty. This is not a documentation
claim: `tests/test_dependencies.py` walks the AST of every module and fails if any
third-party import appears, and also checks that `requirements.txt` lists no package
and that the installed metadata declares `requires: None`.

| File | Content | When it is needed |
|---|---|---|
| [requirements.txt](requirements.txt) | empty, comments only | never — there is no runtime dependency |
| [requirements-build.txt](requirements-build.txt) | `setuptools==84.0.0`, `wheel==0.48.0`, `packaging==26.3` | only for `pip install .` / building the wheel |

`make freeze` rewrites the second file from `venv/`, excluding `pip` and the package
itself — which does not exist on PyPI and would break a `pip install -r`.

### Container

The image **does not need to install any third-party package**. Two modes, both valid:

| Mode | How | Cost |
|---|---|---|
| No install | copy `src/` and run with `PYTHONPATH=src python -m mercadona_catalog_source` | zero `pip install`; image = `python:3.12-slim` + code |
| With install | `pip install -r requirements-build.txt` and `pip install --no-build-isolation .` | brings the console script; build tooling does not need to stay in the final image |

Verified: a clean venv, with only `requirements-build.txt` installed, builds and installs
the package with `--no-build-isolation` (without fetching build dependencies from the
network) and the console script responds.

**There is no `Dockerfile` for this Source, and none is needed for now.** The monorepo has
`infra/Dockerfile.airflow`, which is the *orchestrator* image: in it the Source runs on
Airflow's own interpreter, via `PYTHONPATH`, **installing nothing** — exactly the "no
install mode" of the table above. That is the practical payoff of `dependencies = []`: zero
dependencies means it fits in any image without negotiating versions with anyone.

A dedicated image only becomes worthwhile if the Source is run outside the orchestrator; in
that case the no-install mode allows a `distroless`/`slim` final stage without `pip`.

Points the container will need to respect: `data/` must be a volume, not an image layer
(the snapshot is an artifact, not code); the process needs network egress to
`tienda.mercadona.es`; and the exit codes 0/1/2/3 of [CONTRACT.md § 7](CONTRACT.md) are
the contract with the orchestrator.

## Tests

145 tests in standard-library `unittest`, no network access — the HTTP client is replaced
by a double that answers from memory, exercising the same extraction code. Every contract
guarantee has a corresponding test: atomic write under failure and under
`KeyboardInterrupt`, canonical form determinism, refusal of path traversal, immutability,
fixed language, resume, truncated/empty/tampered file, source shape change, orphan
detection, `_SUCCESS` consistency, tampering with totals and fingerprint, partition with no
product at all, and absence of third-party dependencies.
