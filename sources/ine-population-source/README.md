# INE Population Source

Population data Source. Extracts **population series** from the INE's public Tempus3 API
(`https://servicios.ine.es/wstempus/js`) and produces partitioned, reproducible, verifiable
snapshots. Generic mechanism by `table_id` (see "Usage" below) — today it feeds a series by
**province** (`table_id=31304`) and by **municipality** (`table_id=29005`), configured
outside this Source (see `--tables` and the platform Makefile).

This repository is a **Source**, not a transformation platform. The full contract with the
consumer is in [CONTRACT.md](CONTRACT.md).

Second source of the monorepo, alongside
[`mercadona-catalog-source`](../mercadona-catalog-source/). The two are independent sibling
packages — no code is shared between them, each with its own `pyproject.toml`,
`CONTRACT.md` and test suite. See [ARCHITECTURE.md](../../ARCHITECTURE.md) for the reason
behind this choice.

- **Does:** connects to the Tempus3 API · fetches the full series of one or more
  `table_id`s · controls the request rate · retries with backoff · resumes incomplete
  partitions · produces partitioned snapshots · records metadata and provenance · validates
  integrity, coverage, schema and quality.
- **Does not:** deduplication · dimensional modeling · normalization · Silver/Gold ·
  province-code extraction · joining with another source · metrics · Spark · dbt ·
  Airflow.

## Requirements

Python 3.12+ and nothing else. Standard library only. `dependencies = []` in
[pyproject.toml](pyproject.toml) — there is nothing to install. The Tempus3 API is plain
JSON over HTTPS, with no need for a third-party client or an API key.

## Location

This Source lives in `sources/ine-population-source/` of the monorepo and stays independent
(its own `pyproject.toml`, `dependencies = []`, its own suite — see
[ARCHITECTURE.md](../../ARCHITECTURE.md)). Day to day, use the **root Makefile**
(`make -C ../.. ine-extract ine-validate`), which points `--out` at the monorepo's shared
`data/`.

## Usage

```bash
make test                  # suite, no network and no dependencies
make extract               # today's (UTC) snapshot of table 31304
make validate              # validate today's partition in --strict mode
make check                 # test + validate
```

Without `make`, with `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m ine_population_source extract --out data/ine
PYTHONPATH=src python3 -m ine_population_source validate \
    "data/ine/ingestion_date=$(date -u +%F)" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Installing the package makes the `ine-population-source` console script available with the
same subcommands, without needing `PYTHONPATH`:

```bash
make install               # creates venv/, installs build tools and the package (-e .)
./venv/bin/ine-population-source --version
```

### `extract`

| Flag | Default | Effect |
|---|---|---|
| `--out` | `data/ine` | snapshot root directory |
| `--tables` | `31304` | INE Tempus3 `table_id`(s), comma-separated. `31304` (province+age+sex) and `29005` (municipality+sex) are the confirmed ones — see [CONTRACT.md § 2](CONTRACT.md). The platform (root Makefile) passes `31304,29005` by default |
| `--date` | today (UTC) | partition date. Validated as `YYYY-MM-DD` |
| `--delay` | `0.5` | minimum interval between requests, in seconds. HTTP-client good practice, not a response to a measured block — see "Source restrictions" |
| `--timeout` | `30.0` | timeout of each request |
| `--max-retries` | `5` | extra attempts per request |
| `--overwrite` | off | rewrites the partition, even if it is complete |

### `validate`

| Argument | Effect |
|---|---|
| `partition` | partition path (positional, required) |
| `--strict` | beyond integrity, fails if any series has no point with a `Valor` |

## Layout

```
.
├── CONTRACT.md                      # contract with the consumer
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadata; dependencies = []
├── requirements.txt                 # runtime: empty by construction, verified by a test
├── requirements-build.txt           # setuptools / wheel / packaging
├── src/ine_population_source/
│   ├── __init__.py                  # source name, manifest version
│   ├── __main__.py                  # python -m ine_population_source
│   ├── cli.py                       # subcommands and exit codes
│   ├── canonical.py                 # canonical form + atomic write
│   ├── http_client.py               # sequential client, throttle and backoff
│   ├── schema.py                    # defensive navigation and fingerprint
│   ├── partition.py                 # identity, immutability, _SUCCESS — no warehouse axis
│   ├── extract.py                   # one fetch per table_id, no tree crawl
│   └── validate.py                  # independent validation
├── tests/                           # stdlib unittest, no network
└── venv/                            # build environment, created by `make venv`; ignored
```

The snapshots **do not live here**. The partition the platform consumes lives at
`<root>/data/ine/ingestion_date=…/`, one level deep (not two: no warehouse axis):

```
data/ine/ingestion_date=2026-08-25/
├── tables/table_id=31304.json
├── tables/table_id=29005.json
├── _manifest.json
├── _SUCCESS
└── _run.log
```

## Output layout

The files contain the API payload **with its content intact** — no field is added,
removed, renamed or type-converted — but **re-serialized in canonical form**:
`json.dumps(ensure_ascii=False, sort_keys=True, indent=2)` plus a trailing newline. So they
**are not the original bytes of the HTTP response**; the recorded SHA-256 is that of the
canonical file produced by the Source, not that of the HTTP response.

Shape of one series (`tables/table_id=31304.json` is a **list** of these):

```json
{
  "COD": "DPOP1",
  "Nombre": "Total. Madrid. Ambos sexos. Población. Número.",
  "FK_Unidad": 3,
  "FK_Escala": 1,
  "Data": [
    {"Fecha": 1735686000000, "FK_Periodo": 1, "Anyo": 2025, "Valor": 6779888}
  ]
}
```

`Data[]` covers many years in a single response — each snapshot is already the whole
historical series, not an increment.

### `_manifest.json`

Records `manifest_version`, `run_id`, run window, duration, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint` and the list of files with `sha256`,
`bytes` and `records` (series count). `table_id`s that failed go in `failures[]`; events
handled during extraction go in `anomalies[]`; the summary of previous runs over the same
partition goes in `history[]`.

`files[].path` is relative to the **snapshot root** (`--out`), not to the partition: it
starts at `ingestion_date=…/`. To resolve it from the partition path, go up **one** level
(not two: there is no warehouse axis between the root and the partition).

## Operational behavior

**Throttle.** Sequential requests, limited to `1/--delay` req/s. Unlike the Mercadona
Catalog Source, this **is not a response to a measured block** — there is no concurrency at
all here to measure, since a run fetches a handful of `table_id`s sequentially. It is
HTTP-client good practice, kept by default.

**Retry.** Statuses `408, 429, 500, 502, 503, 504` and any non-HTTP error (timeout,
connection reset, invalid JSON body) are retried with exponential backoff
`5s → 10s → 20s → 40s → 60s` (60 s cap), up to `--max-retries`. `403` is **not** among the
retryable statuses here — unlike Mercadona, where there is measured evidence of
intermittent blocking under bursts; without that evidence for the INE, treating `403` as
transient would be inventing a policy with no basis.

**Resume.** A file already present in the partition is not downloaded again, unless with
`--overwrite`. It is downloaded again from the source automatically when it is unreadable,
truncated, has an unexpected shape, is not in canonical form or differs from the previous
manifest — in the last three cases the event goes into `anomalies[]`.

**Identity before fan-out.** A preliminary manifest (`complete: false`) is written as soon
as the partition is created, before any `table_id` is fetched.

**Inventory.** At the end, the extraction scans `tables/` and declares in the manifest the
valid files that run did not visit (the case of a `table_id` removed from `--tables`
between runs, but whose file remains on disk).

**Exit codes.** See [CONTRACT.md § 7](CONTRACT.md).

## Source restrictions

- **Rate-limit has not been measured against this source.** Unlike Mercadona
  (intermittent 403 under concurrency, measured and documented), this Source has no local
  concurrent fan-out scenario that would have motivated measuring it yet — a run is
  sequential by nature. If that ever changes (e.g. many `table_id`s in parallel), measure
  before assuming the current throttle is enough.
- **`table_id: 31304` and `table_id: 29005` were confirmed.** I queried
  `GRUPOS_TABLA/31304` (dimensions: `Sexo`, `Edad`, `Provincias`) and `DATOS_TABLA/31304`
  and `DATOS_TABLA/29005` (the `Nombre` format of each) directly against the API before
  fixing this contract. Other INE `table_id`s can be passed via `--tables`, but their
  `Nombre` format was not verified — it may differ from what is documented in
  [CONTRACT.md § 2](CONTRACT.md).
- **Publication cadence is irregular.** The INE updates the Cifras de Población on no fixed
  schedule — historically on the order of 1-2 times a year, sometimes with months between
  one publication and the next. This shapes how the platform orchestrates this Source (no
  fixed cron; see `orchestration/`), not the behavior of this Source itself — it extracts
  whatever the API has at the moment it is called.
- **No authentication.** The Tempus3 API is public; no key or credential is sent.

## Validation

`validate` re-reads and re-parses every file declared in the manifest, recomputing
checksums, counts and fingerprint instead of accepting the recorded values, and scans the
directory for undeclared files.

**Integrity** — always fatal: manifest missing, unreadable or without the `files` list ·
incompatible `manifest_version` · malformed manifest entry (no `path`) · entry pointing
outside the snapshot root · declared file missing · SHA-256 or size mismatch · invalid
JSON · record count mismatch · file on disk not in the manifest, at any depth · `_SUCCESS`
inconsistent with `complete` · configured `table_id` without a file ·
`schema_fingerprint` mismatch · `totals.series_count`, `totals.data_point_count`,
`totals.table_files` or `totals.bytes` differing from the re-read · failure recorded in
`failures[]`.

**Warnings** — reported, never fatal: each entry of `anomalies[]`.

**Quality** — measured and reported; fatal only with `--strict`: a series without any point
with a `Valor`.

## Dependencies and container

**Runtime: none.** Every import of the package and of the tests is from the Python 3.12
standard library, and `[project.dependencies]` is empty. `tests/test_dependencies.py` walks
the AST of every module and fails if any third-party import appears.

| File | Content | When it is needed |
|---|---|---|
| [requirements.txt](requirements.txt) | empty, comments only | never — there is no runtime dependency |
| [requirements-build.txt](requirements-build.txt) | `setuptools`, `wheel`, `packaging` | only for `pip install .` / building the wheel |

### Container

The image **does not need to install any third-party package**, for the same reason as the
Mercadona Catalog Source: `dependencies = []` means the code runs with
`PYTHONPATH=src python -m ine_population_source` in Airflow's own interpreter, with no
`pip install` at all. There is no dedicated `Dockerfile`, and none is needed, by the same
reasoning documented in the Mercadona Catalog Source README.

## Tests

Suite in the standard library's `unittest`, with no network access — the HTTP client is
replaced by a double that answers from memory, exercising the same extraction code.
Covers: atomic write under failure and under `KeyboardInterrupt`, canonical-form
determinism, path-traversal rejection, immutability, resume, truncated/empty/tampered file,
source shape change, orphan detection, `_SUCCESS` consistency, tampering with totals and
fingerprint, and absence of third-party dependencies.

**Validation against the real API** happened (2026-08-26, `--tables 31304,29005`): 40,791
series, 2,253,624 data points, ~402 MB landed in MinIO — full numbers and the network
incident (retry because a large payload was corrupted in transit) in
[CONTRACT.md § 2](CONTRACT.md). `validate` ran **without `--strict`**: that same run found
6 series of `29005` without any data point, in 2 municipalities outside the scope of the
platform that consumes this Source — a decision recorded in CONTRACT.md and in the comment
of the root Makefile's `ine-validate` target. The `Makefile` in this folder (`make
validate`, `check` target) still calls `--strict`, because its default `TABLES` (`31304`
alone) has never produced an empty series in the tests done so far.
