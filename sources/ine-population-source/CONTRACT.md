# Source Contract — `ine_population_api`

Contract between this Source and any component that consumes its snapshots.
Everything here is enforced by code and covered by a test, except where marked
**[source]** — a characteristic of the INE's Tempus3 API, outside this Source's control.

Leaner than the Mercadona Catalog Source contract: no warehouse axis, no product/price
identity, no currency premise — none of that exists here. What the two share (atomic write,
canonical form, closed inventory, immutability, safe resume) is generic partition hygiene,
kept verbatim.

## 1. Snapshot identity

Each complete run produces a snapshot identified by two keys — **no** warehouse axis,
unlike Mercadona: the Tempus3 API returns every province and every year in a single
payload per `table_id`, so there is no fetch axis to promote to a directory.

| Key | Value | Where it lives |
|---|---|---|
| `source` | `ine_population_api` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | partition directory and `partition.ingestion_date` |

```
<root>/ingestion_date=YYYY-MM-DD/
├── tables/table_id=<id>.json
├── _manifest.json
├── _SUCCESS          (only exists when complete = true)
└── _run.log
```

Top-level blocks of `_manifest.json`: `manifest_version`, `run_id`, `started_at_utc`,
`finished_at_utc`, `duration_seconds`, `complete`, `source`, `partition`, `config`,
`totals`, `schema_fingerprint`, `files[]`, `failures[]`, `anomalies[]`, `history[]`.

**Compatibility within a manifest version:** a missing list is equivalent to an empty
list. Consumers must use `manifest.get("anomalies", [])`, never direct indexing.

## 2. Guaranteed content

The snapshot preserves **population time series by province**, as the API returns it —
with no interpretation.

| Field | Path |
|---|---|
| Series code | `tables/table_id=<id>.json` → `[].COD` |
| Series description (includes the province) | `tables/table_id=<id>.json` → `[].Nombre` |
| Series data points (year, value) | `tables/table_id=<id>.json` → `[].Data[]` |

**`table_id` is 31304** ("Población por fecha, sexo, edad y provincia"), with dimensions
`Sexo`, `Edad`, `Provincias` — confirmed by querying `GRUPOS_TABLA/31304` and
`DATOS_TABLA/31304` directly against the API. **The province comes as text**, not as a
numeric code: `Nombre` concatenates age, territory and sex separated by `". "` — e.g.
`"Total. Albacete. Ambos sexos. Población. Número."`.

**Measured against the real payload** (16,377 series, 1,547,496 data points,
table_id=31304):

- **The order of the first three segments is NOT fixed.** 97.2% of the series follow
  `"<Age>. <Territory>. <Sex>."`, but the whole `"85 y más años"` age bucket (156 series)
  comes as `"<Sex>. <Age>. <Territory>."`, and the 309 national-total series come as
  `"Total Nacional. <Age>. <Sex>."` (or, only for `"100 y más años"`,
  `"<Age>. Total Nacional. <Sex>."`). Extracting by fixed position (e.g.
  `split_part(..., 2)`) gets the territory wrong in ~2.8% of the series. The consumer must
  identify territory and sex by **known vocabulary** (closed list of 52 provinces + the
  value `"Total Nacional"`; 3 sex values), not by index.
- **`"Total Nacional"` is not a province** — it is the aggregate of the whole country,
  present in the same table alongside the 52 provinces (50 + Ceuta and Melilla).
- **Each series has 2 data points per year**, not 1: one per `FK_Periodo` (`27` =
  reference 30/06, `26` = reference 31/12 — the INE publishes two population estimates
  per year). `(series, Anyo, FK_Periodo)` is unique; `(series, Anyo)` alone is not.

The Silver (`silver_ine_population_series`) already implements vocabulary-based extraction
and the grain with `fk_periodo`.

**`table_id` 29005** ("Cifras oficiales del padrón por municipio") is also confirmed, by
querying `DATOS_TABLA/29005` directly against the API. Different dimensions from 31304:
only **municipality + sex**, no age. **The municipality comes as text, with no code** —
not even a province code, contrary to what one might assume by analogy with 31304.

**Measured against a large sample of the real payload** (4,509 complete series from a
larger payload that the network truncated during the investigation — see the project
history):

- **The segment order IS fixed here**, unlike 31304: 100% of the sample follows
  `"<Municipality>. <Sex>. Total habitantes. Personas."` — always 4 segments; municipality
  and sex can be extracted by position, not by vocabulary.
- **The sex vocabulary differs from 31304's**: `"Total"` / `"Hombres"` / `"Mujeres"`, not
  `"Ambos sexos"` / `"Hombres"` / `"Mujeres"`.
- **`Secreto: true` did not appear in the sample** (0 occurrences) — but the sample does not
  cover all ~8,200 municipalities of Spain, so this is not proven for the full dataset.
  There is no observed example of what the source puts in `Valor` when `Secreto: true` —
  the consumer must not assume it becomes `0` or `null`.
- **The unfiltered payload is large** (historical series since 1996, ~8,200 municipalities
  × 3 sex series × ~30 years): it was truncated in the middle of this extension's own
  investigation on that environment's network, despite `HTTP 200`. This Source's `Fetcher`
  already treats an invalid JSON body as a retryable error (`http_client.py`), so a
  truncated response is retried with backoff — but the default `--timeout` (30s) is short
  for a payload this size; whoever runs `--tables 29005` on a slow link should consider a
  larger `--timeout`.
- **There is no municipality code in the payload** — the same limitation as 31304 with
  provinces, only more serious here because a municipality name is not a safe key
  nationally (names are duplicated across different provinces). Resolving the code is the
  consumer's responsibility (see `scripts/derive_municipality_codes.py` at the project
  root, outside this Source).

The Silver (`silver_ine_population_by_municipality`) implements the positional extraction
and the code lookup.

**Measured against the real, complete payload** (production extraction, 40,791 series,
2,253,624 data points): 6 series of `table_id=29005` — 2 municipalities (`"Gatova"`,
province of Castellón, and `"Palmerola"`, province of Girona), 3 series each
(Total/Hombres/Mujeres) — have **no** data point at all (`Data: []`), not just a null
`Valor`. Neither municipality falls within this platform's scope (08/28/41/46). The
platform (root Makefile, DAG) runs `validate` **without `--strict`** because of this —
failing every extraction over 2 out-of-scope municipalities would protect nothing real;
the count of series without a value is still reported in the `validate` output either way,
it just stopped being fatal.

**That same production extraction measured the large `table_id=31304` payload as
genuinely fragile in transit.** The first attempt (network of the machine that ran this
extension, 2026-08-26) downloaded ~264 MB and failed near the end —
`JSONDecodeError: Expecting ':' delimiter` at byte 154,057,166 — after ~33 min. The
`Fetcher`'s automatic retry (`http_client.py`, which already treated an invalid JSON body
as a retryable error before this extension) redid the fetch and completed intact on the
2nd attempt (`retry 1/3`, 5s backoff). Total landed in MinIO: `table_id=31304`
(276,503,711 bytes) + `table_id=29005` (125,350,984 bytes) ≈ 402 MB in 2 table files, 5
objects in total with manifest/log/`_SUCCESS`. Recorded here because the default
`--timeout` (30 s, see README § "extract") is too short for a payload this size on that
network — whoever repeats this extraction on a slow link should consider a larger
`--timeout`, not just more `--max-retries`.

**The JSON is the physical contract.** There is no relational schema: the structure of
the API response is preserved as received.

## 3. Guarantees

1. **Atomic write.** No data file is observable in a partial state.
2. **Canonical form.** Every file is `json.dumps(ensure_ascii=False, sort_keys=True,
   indent=2)` + trailing newline.
3. **Verifiable integrity.** `_manifest.json` declares `sha256`, `bytes` and `records`
   (series count) for each file. `validate` recomputes all three from disk.
4. **Closed inventory.** Every file inside the partition is declared in the manifest.
5. **Immutability.** A partition with `complete: true` cannot be rewritten without an
   explicit `--overwrite`. A reused file whose content differs from the previous manifest
   is downloaded again from the source and recorded in `anomalies[]`.
6. **Safe resume.** An incomplete partition can be completed by re-running the same
   command with the same `--tables`.
7. **Provenance preserved.** `history[]` accumulates the summary of each previous run.
8. **Schema change detection.** `schema_fingerprint` records the sorted set of series and
   data-point keys, plus a hash. `validate` recomputes and compares.
9. **Completion marker.** `_SUCCESS` exists if and only if `complete: true`.
10. **Identity persisted before fan-out.** A preliminary manifest (`complete: false`) is
    written as soon as the partition is created, before any `table_id` is fetched — not
    after a discovery step, because there is none here.
11. **Canonical form verified on reuse.** An already present file is only reused if its
    bytes are exactly the canonical serialization of its own content.
12. **Anomalies recorded.** `anomalies[]` lists events handled during extraction.
    `validate` reports them as warnings, without failing an already corrected partition.
13. **Coverage verified.** `validate` checks that every `table_id` in
    `manifest.config.tables` has a file — coverage is config-driven, not discovered
    against an external tree as with Mercadona.
14. **Totals re-checked.** `totals.series_count`, `totals.data_point_count`,
    `totals.table_files` and `totals.bytes` are recomputed from the re-read.
15. **A recorded failure is fatal in validation.** Any entry in `failures[]` fails the
    partition.

**Deliberately without an equivalent here** (they exist in Mercadona, they make no sense
for this source): a fixed `lang` per partition — there is no language axis; product/price
identity; currency premise.

## 4. Consumer obligations

1. **Iterate `manifest.files[]`.** `files[].path` is relative to the snapshot root, not to
   the partition — it starts at `ingestion_date=…/`. Resolve it with
   `os.path.join(partition, "..", entry["path"])` — one level less than Mercadona, because
   there is no warehouse axis between the root and the partition.
2. **Treat the partition as immutable.** Do not write inside it.
3. **Each snapshot already contains the whole historical series.** `Data[]` covers many
   years per series. A new `ingestion_date` is an **updated pull**, not incremental — do
   not assume only "new rows" appear.
4. **Do not assume an update cadence.** The INE publishes the Cifras de Población
   irregularly (nominally ~1-2x/year, historically sometimes months between publications).
   A new `ingestion_date` with no `Valor` different from the previous snapshot is the
   expected case, not a sign of failure.
5. **Extract the province from `Nombre`, carefully.** There is no province code in the
   payload — only the full name, inside a string with other concatenated fields (see §2).
   There is no ready-made mapping to the INE province code here; if you need one, it lives
   in the layer that joins this source with another (outside this Source's scope).
6. **Check `schema_fingerprint` between snapshots** if you depend on the shape of the
   fields.

## 5. What the Source does not do

Deduplication · dimensional modeling · normalization · facts and dimensions · Silver
transformation · Gold/Marts · province → other geographic system mapping · metric
computation · ingestion into object storage · Spark · dbt · Airflow.

Each run is a **snapshot** of the requested table. Interpretation (extracting the province,
filtering by sex/age, joining with another source) belongs to the downstream layer.

## 6. Source scope and limits **[source]**

- The Tempus3 API (`servicios.ine.es/wstempus`) is public and requires no authentication.
- Rate-limit behavior **has not been measured** against this source — unlike Mercadona,
  where intermittent 403 under concurrency was measured and documented. This Source has no
  concurrent fan-out (a run fetches a handful of `table_id`s, sequentially), so there is no
  local scenario that would require measuring it yet.
- `table_id: 31304` (province+age+sex) and `table_id: 29005` (municipality+sex, no age)
  are the confirmed ones in this contract version — see §2 for the measured format of
  each. Other INE tables can be passed via `--tables`, but their `Nombre` shape was not
  verified.
- Data below the municipality level (e.g. census section) was not found in this API
  (`DATOS_TABLA`/Tempus3) during the investigation that led to `table_id: 29005` — what
  exists (Censo/SDC21) appears to be a structurally different API, not verified, outside
  this Source's scope for now.
- Municipality data with age (not just sex) exists at the INE, but fragmented across a
  family of dozens of `table_id`s — one per province — not mapped to this platform's 4
  provinces; not pursued in this contract version (see ARCHITECTURE.md).

## 7. Exit codes

| Code | `extract` | `validate` |
|---|---|---|
| 0 | no target `table_id` failed | passed |
| 1 | partial failure, recorded in `failures[]` | failed |
| 2 | fatal failure: unusable partition or invalid use | — |
| 3 | unhandled exception | unhandled exception |

## 8. Contract version

`manifest_version: 1`. A counter independent of Mercadona's — they are separate contracts.
`validate` rejects a manifest whose version differs from the one supported by the code,
instead of interpreting it by guesswork.
