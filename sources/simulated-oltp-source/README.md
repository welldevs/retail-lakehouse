# Simulated OLTP Source

Generates **geographically coherent** synthetic customers: the customer is invented, the
place where they live is not. Each customer is born on a real street of a real
municipality, with that tramo's real postal code, in a municipality that actually belongs
to their warehouse's Functional Urban Area (FUA).

Isolated sibling package, **with no runtime dependency at all** — standard library only. See
[CONTRACT.md](CONTRACT.md) for the full contract.

## Where the data comes from

This is the first **derived** source in the repository. The other three are upstream of
external data; this one consumes the Silver they produced:

| Input | Origin | What it provides |
|---|---|---|
| `silver_callejero_tramos` + `streets`/`pseudo_addresses`/`population_units` | INE Callejero | street, postal code, numbering range, municipality |
| `silver_ine_population_by_municipality` (table 29005) | INE Tempus3 | municipality weight and sex proportion |
| `silver_ine_population_series` (table 31304) | INE Tempus3 | provincial age distribution (proxy) |
| `warehouse_service_area_seed`, `warehouse_province_map_seed` | dbt seeds | which municipalities each warehouse serves |

**The Source does not talk to the Lakehouse.** Every Source in this repo is FROZEN
(`dependencies = []`, verified by AST), and `duckdb`/`boto3` are platform-only dependencies
by design. The bridge is a **physical** contract, not an import: the platform materializes
what the Source needs into three flat JSON files, and the Source reads them with the
stdlib `json`.

```
Silver  ──[ retail-platform export-oltp-reference ]──>  data/oltp-reference/
                                                                 │
                                                        (3 flat JSON files)
                                                                 │
                                                                 v
                                        [ simulated_oltp_source extract ]  ──> customers.json
```

Same precedent `ine-callejero-source` already uses, one level up: there `extract` does not
touch the network either, it ingests files already prepared in `--in`.

## Requirements

Python 3.12+. Nothing else: `PYTHONPATH=src` is enough to run everything. The venv targets
exist for packaging, not for running.

Before the first `extract`, the Callejero and the population must already be in the
Lakehouse (`make callejero-refresh` and `make ine-refresh` at the repository root).

## Usage

From the repository root (recommended):

```bash
make oltp-export-reference          # once: materializes the Silver (covers all 4 warehouses)
make oltp-refresh WH=mad1           # extract -> validate -> land -> verify-landing
make oltp-refresh-all               # the same for mad1, bcn1, svq1 and vlc1
```

Directly, without the root Makefile:

```bash
PYTHONPATH=src python3 -m simulated_oltp_source extract \
  --reference ../../data/oltp-reference/ingestion_date=2026-08-27 \
  --out data/oltp --wh mad1 --date 2026-08-27 --seed 20260827

PYTHONPATH=src python3 -m simulated_oltp_source validate \
  data/oltp/ingestion_date=2026-08-27/wh=mad1 \
  --reference ../../data/oltp-reference/ingestion_date=2026-08-27 --strict
```

### `extract`

| Flag | Default | Role |
|---|---|---|
| `--reference` | — (required) | directory of the three reference JSON files |
| `--wh` | — (required) | `mad1`, `bcn1`, `svq1` or `vlc1` |
| `--out` | `data/oltp` | snapshot root |
| `--date` | today in UTC | partition `ingestion_date` |
| `--count` | **no default** | customers to generate. If omitted, uses the reference's `customer_allocation` target — warehouse adult population × penetration rate. A default here would silently reintroduce a customer base sized by nobody |
| `--seed` | `20260827` | seed: the same seed reproduces the same output |
| `--overwrite` | off | rewrites a complete partition |

### `validate`

`--reference` is **required** here, unlike the other three Sources. The central guarantee —
every customer lives at a real address in their warehouse's FUA — is only verifiable by
re-reading the same reference that generated the partition. `--strict` promotes
distribution warnings to failures.

## Layout

```
sources/simulated-oltp-source/
├── CONTRACT.md
├── README.md
├── Makefile
├── pyproject.toml            (dependencies = [])
├── requirements.txt          (empty, on purpose)
├── src/simulated_oltp_source/
│   ├── cli.py                arguments and exit codes
│   ├── reference_data.py     reads and distrusts the three JSON files
│   ├── customers_generator.py  pure, deterministic sampling
│   ├── extract.py            orchestrates partition + manifest + _SUCCESS
│   ├── validate.py           integrity + geospatial coherence
│   ├── partition.py          path, tokens, immutability (wh= axis)
│   ├── schema.py             fields and fingerprint
│   └── canonical.py          canonical form + atomic write
└── tests/                    140 tests, no network
```

## Output layout

`customers.json` is a JSON array of objects. Real example, generated against the Lakehouse:

```json
{
  "customer_id": "cust_vlc1_000000",
  "wh": "vlc1",
  "province_code": "46",
  "province_name": "Valencia/València",
  "municipality_code": "244",
  "municipality_name": "Torrent",
  "candidate_index": 207249,
  "street_name": "PARE MÉNDEZ",
  "postal_code": "46900",
  "numbering_type": "1",
  "house_number": 37,
  "first_name": "Eva",
  "last_name": "González Gil",
  "sex_label": "Mujeres",
  "birth_year": 1987
}
```

`candidate_index` points to the exact row of `address_candidates.json` that supplied the
address — it is the only exact audit path back to the source tramo (postal code + street
alone identify only 13.3% of tramos; see CONTRACT.md section 4).

`house_number` is `null` when the source does not support a real number — never an invented
number. The reason is readable in the companion field `numbering_type` next to it.

## Operational behavior

- **Idempotent per partition.** A complete partition is immutable; re-running without
  `--overwrite` refuses instead of rewriting.
- **Deterministic.** Same reference + same seed + same date ⇒ byte-for-byte identical
  `customers.json`. Verified even with a different `PYTHONHASHSEED` between runs.
- **Write order.** Data → manifest → `_SUCCESS`, always. A process killed midway never
  leaves a partition that announces itself complete without being so.
- **The seed lives in the manifest**, not repeated in each record, and `history[]` keeps the
  seed of each previous run.

## Source restrictions

Summary; the measured numbers and the full explanation are in
[CONTRACT.md section 6](CONTRACT.md).

- **Age is provincial**, used as a proxy: there is no age band per municipality in the
  ingested tables.
- **Different vintages** between the two demographic inputs: a real lag of 2 years and
  6 months, measured by `reference_date`.
- **Different natures**: age comes from an estimate (`fk_tipo_dato=2`), the municipal weight
  comes from official figures (cifras oficiales).
- **Sex and age are sampled independently**; the age×sex correlation present in the source
  is discarded by construction.
- The tramo choice within the municipality is **uniform, not weighted** — there is no
  population at that granularity, and calling it density would be false.
- 4.2% of tramos have no real numbering; 3 are orphans; 14 have a degenerate range.

## Validation

`validate` recomputes everything instead of trusting the manifest, and then re-reads the
reference:

```
partition ........ data/oltp/ingestion_date=2026-08-27/wh=vlc1
run_id ........... 20260827T130256Z_vlc1
source ........... simulated_oltp
warehouse ........ vlc1
seed ............. 20260827
complete ......... True
files ............ 1/1 read and checked
orphans .......... 0
coherence ........ 200/200 customers with a real address in the FUA of vlc1

OK: integrity, schema, totals and geospatial coherence validated.
```

Injecting a postal code from outside the FUA into the partition makes validation fail with
code 1 — verified end to end, not just in a unit test.

## Dependencies and container

`requirements.txt` is empty on purpose and `[project.dependencies]` is `[]`. The promise is
**verified, not asserted**: `tests/test_dependencies.py` walks the AST of every module in
the package and the tests and fails any import outside the stdlib, besides checking
`pyproject.toml`, `requirements.txt` and the installed metadata.

A container running this Source only needs Python 3.12 and the `src/` directory on
`PYTHONPATH` — plus the mounted reference directory.

## Tests

```bash
make test                 # 140 tests, no network
```

They cover, among others: same seed ⇒ byte-identical output (even under a different
`PYTHONHASHSEED`, in a subprocess); different seeds ⇒ different output; 100% of customers in
the correct FUA; no `numbering_type='0'` produces a number; `house_number` is never 0;
municipal weight respected and uniform tramo choice; a missing or inconsistent reference
fails the generation; manifest and totals match the produced file; and one negative test for
each way of corrupting the partition.
