# INE Callejero Source

Official INE geography Source. Ingests **census sections, population
units, streets, pseudo-streets and street segments with postal code** (`SECC`, `UP`,
`VIAS`, `PSEU`, `TRAM`) from the Callejero, downloaded manually from the INE website — **there is no
API** for this dataset — and produces partitioned, reproducible and
verifiable snapshots, byte-for-byte identical to the download.

This repository is a **Source**, not a transformation platform. The full
contract with the consumer is in [CONTRACT.md](CONTRACT.md).

Third source in the monorepo, alongside
[`mercadona-catalog-source`](../mercadona-catalog-source/) and
[`ine-population-source`](../ine-population-source/). An independent sibling package —
no code is shared. See [ARCHITECTURE.md](../../ARCHITECTURE.md).

- **Does:** locates the expected files per province in an input directory ·
  copies them verbatim (no reserialization) · computes checksums · resumes incomplete partitions
  · produces partitioned snapshots · records metadata and provenance · validates
  integrity, coverage and structural consistency.
- **Does not:** network requests · field parsing · deduplication · dimensional
  modeling · Silver/Gold · warehouse → province mapping · Spark · dbt · Airflow.

## Where the data comes from

The Callejero is an official product of INE (Instituto Nacional de Estadística of
Spain) — **there is no API**: it is published as a set of ASCII files
compressed in `.zip`, one per province, for manual download on the INE website. Official
page: [Cartografía secciones censales y callejero de Censo Electoral](https://www.ine.es/ss/Satellite?L=es_ES&c=Page&cid=1259952026632&pagename=ProductosYServicios%2FPYSLayout).

- **Cadence**: published twice a year. The January release has reference date Dec 31 of the
  previous year; the July release, Jun 30 of the same year. No guarantee of different content
  between two publications.
- **Content of each `.zip`**: 5 fixed-width text files, in ISO-8859-1
  (Latin-1), one per dataset — `SECC` (census sections), `UP` (population
  units/nuclei), `VIAS` (streets), `PSEU` (pseudo-streets) and `TRAM` (street segments,
  the only one with a postal code). Official name, e.g. `VIAS.P28.D260630.G260702`
  (dataset + province + date + generation).
- **Manual process, end to end**: download the `.zip` for the desired province(s)
  from the INE website → extract into a local directory (`--in`) → run `extract`, which
  **copies the files verbatim** (no download, no transformation) into the partition.
  This is the only input path for this Source — there is no network fallback.
- **Field layout** is not documented inside the `.zip`. The layout used in this
  Source (CONTRACT.md § 2) was measured directly against the real files and, for
  `TRAM`, cross-checked with the official INE/IDA-Padrón "Diseño de registro"
  ([PDF](https://idapadron.ine.es/repositorio/DisReg/disregok.PDF)), which names the
  fields of the "Tramero" in the same order observed in the bytes.

## Requirements

Python 3.12+ and nothing else. Standard library only. `dependencies = []` in
[pyproject.toml](pyproject.toml) — there is nothing to install, and there is no HTTP client because
there is no API.

## Location

This Source lives in `sources/ine-callejero-source/` of the monorepo and remains independent
(its own `pyproject.toml`, `dependencies = []`, its own suite). Day to day, use the
**root Makefile** (`make -C ../.. callejero-extract IN=../../temp`), which writes to
`<root>/data/callejero`.

## Usage

```bash
make test                        # suite, no network and no dependencies
make extract IN=../../temp       # ingest the files from IN into today's partition
make validate                    # validate today's partition in --strict mode
make check                       # test + validate
```

Without `make`, with `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m ine_callejero_source extract --in /path/to/download --out data/callejero
PYTHONPATH=src python3 -m ine_callejero_source validate \
    "data/callejero/ingestion_date=$(date -u +%F)" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Installing the package makes the `ine-callejero-source` console script available:

```bash
make install
./venv/bin/ine-callejero-source --version
```

### `extract`

**Does not touch the network.** Locates, inside `--in`, the file for each configured (province,
dataset) and copies it verbatim into the partition.

| Flag | Default | Effect |
|---|---|---|
| `--in` | *(required)* | directory where the Callejero files were downloaded manually. Scanned recursively — the folder structure of the original downloads is accepted as is |
| `--out` | `data/callejero` | snapshot root directory |
| `--provinces` | `08,28,41,46` | INE province code(s), comma-separated. The 4 warehouse provinces |
| `--date` | today (UTC) | partition date. Validated as `YYYY-MM-DD` |
| `--overwrite` | off | rewrites the partition, even if it is complete |

### `validate`

| Argument | Effect |
|---|---|
| `partition` | partition path (positional, required) |
| `--strict` | besides integrity, fails if there is a diverging line-width warning |

## Layout

```
.
├── CONTRACT.md                      # contract with the consumer
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadata; dependencies = []
├── requirements.txt                 # runtime: empty by construction, checked by a test
├── requirements-build.txt           # setuptools / wheel / packaging
├── src/ine_callejero_source/
│   ├── __init__.py                  # source name, manifest version
│   ├── __main__.py                  # python -m ine_callejero_source
│   ├── cli.py                       # subcommands and exit codes
│   ├── canonical.py                 # canonical JSON manifest + atomic verbatim copy
│   ├── schema.py                    # structural facts (line count/width), no field parsing
│   ├── partition.py                 # identity, immutability, official INE name — no warehouse axis
│   ├── intake.py                    # locate in --in, copy, no network
│   └── validate.py                  # independent validation
├── tests/                           # stdlib unittest, no network, without the real 110 MB
└── venv/                            # build environment, created by `make venv`; ignored
```

The snapshots **do not live here**. The partition the platform consumes lives in
`<root>/data/callejero/ingestion_date=…/`:

```
data/callejero/ingestion_date=2026-08-25/
├── provinces/
│   ├── province=08/{SECC,UP,VIAS,PSEU,TRAM}.P08.D<date>.G<generation>
│   ├── province=28/{SECC,UP,VIAS,PSEU,TRAM}.P28.D<date>.G<generation>
│   ├── province=41/...
│   └── province=46/...
├── _manifest.json
├── _SUCCESS
└── _run.log
```

## Output layout

**Original bytes, no reserialization.** Unlike the other two Sources (which
reserialize JSON into a canonical form), the 5 datasets are fixed-width text in
ISO-8859-1 (Latin-1) with CRLF — copied exactly as INE distributes them. The recorded SHA-256
is over the original file, not over a transformation by this Source.

The measured column layout of each dataset is in
[CONTRACT.md § 2](CONTRACT.md#2-guaranteed-content). Summary: `SECC` is just a
10-digit code (province+municipality+district+section); `VIAS`/`PSEU` have a code + the
street/pseudo-street name in 2-3 redundant forms; `UP` has 604 characters with the
municipality name in one position and the population unit (nucleus) name in another; `TRAM` links,
in a single record, section + entity/nucleus + street or pseudo-street + **postal code** +
number range — it is the only one of the 5 with a postal code. None of the 5 has an official
administrative neighborhood (that belongs to each city's council, not to INE).

### `_manifest.json`

Records `manifest_version`, `run_id`, run window, duration, `complete`,
`source`, `partition`, `config` (`in_dir`, `provinces`, `datasets`, `overwrite`),
`totals` and the file list with `sha256`, `bytes`, `line_count` and
`dominant_line_width`. (province, dataset) combinations not found in
`--in` go into `failures[]`; the summary of previous runs over the same partition
goes into `history[]`.

`files[].path` is relative to the **snapshot root** (`--out`), starting at
`ingestion_date=…/`.

## Operational behavior

**No network, no throttle, no retry.** `extract` only reads local files.

**Resume.** An already landed file in an incomplete partition is reused if it still
matches the current file in `--in`; if it diverges, it becomes a failure (`--overwrite` resolves it in
favor of what is in `--in` now).

**Identity before the fan-out.** A preliminary manifest (`complete: false`) is written
as soon as the partition is created, before any file is copied.

**Exit codes.** See [CONTRACT.md § 7](CONTRACT.md).

## Source restrictions

- **No API.** The Callejero is only distributed for manual download, on the INE website (see
  [Where the data comes from](#where-the-data-comes-from)).
- **Published twice a year**, with no guarantee of different content between publications —
  this shapes how the platform orchestrates this Source (no cron; on demand), not the
  behavior of this Source itself.
- **Layout not documented inside the `.zip` itself.** The table in CONTRACT.md § 2 was
  reconstructed by measuring the real files of the 4 warehouse provinces (and, for
  `TRAM`, cross-checked against the official INE/IDA-Padrón "Diseño de registro"); it may not
  generalize to other provinces without revalidation.
- **Postal code only in `TRAM`** (field `CPOS`); the other 4 datasets do not have it. **An official
  administrative neighborhood does not exist in any of the 5** — confirmed by inspection, not
  assumed from a lack of searching. The closest is the `UP` nucleus/pedanía, which only
  has useful granularity in València among the 4 warehouses (16 named nuclei);
  Madrid/Barcelona/Sevilla city are a single entity with no subdivision in `UP`.

## Validation

`validate` rereads every file declared in the manifest, recomputing checksum and
line statistics instead of accepting the recorded values, and scans the directory
for undeclared files.

**Integrity** — always fatal: manifest missing, unreadable or without the `files` list ·
incompatible `manifest_version` · malformed entry · entry pointing outside the
snapshot root · declared file missing · SHA-256, size or line count
mismatch · file on disk not in the manifest · `_SUCCESS` inconsistent with `complete`
· configured (province, dataset) combination without a file · `totals.files_landed` or
`totals.bytes` diverging from the reread · failure recorded in `failures[]`.

**Warnings** — reported, fatal only with `--strict`: dominant line width different
from the one recorded in the manifest (a sign of a possible INE layout change).

## Dependencies and container

**Runtime: none.** `tests/test_dependencies.py` walks the AST of every module
and fails if any third-party import appears.

| File | Content | When it is needed |
|---|---|---|
| [requirements.txt](requirements.txt) | empty, comments only | never |
| [requirements-build.txt](requirements-build.txt) | `setuptools`, `wheel`, `packaging` | only for `pip install .` / building the wheel |

### Container

The image **does not need to install any third-party package** — `dependencies = []`
means the code runs with `PYTHONPATH=src python -m ine_callejero_source` on
Airflow's own interpreter. No dedicated `Dockerfile`.

## Tests

A standard-library `unittest` suite, with no network and without the 110 MB of real files —
`tests/support.py` generates sample files with the same fixed widths measured against the
production files. Covers: atomic copy under failure, byte preservation (including
invalid UTF-8 sequences, since the real encoding is Latin-1), rejection of file
names outside the INE pattern, immutability, resume of an incomplete partition,
divergence between an already landed file and the current `--in`, inclusion of `TRAM` (5th dataset),
orphan detection, `_SUCCESS` consistency, tampered totals, and absence of
third-party dependencies.

**Validation against the real Callejero files** was done during the
development of this Source: measurement of encoding, width and column offsets for
the 5 datasets (see CONTRACT.md § 2), including the `CPOS` (postal code) field of `TRAM`
cross-validated against ~305 thousand real lines from the 4 provinces and against real geography
(province prefix always correct, València nuclei matching the known postal code
of the area). A full `make callejero-refresh` run (extract → validate --strict →
land → verify-landing → silver) against the production files of the 4 provinces — see
[DECISIONS.md](../../DECISIONS.md#third-source-ine-callejero).
