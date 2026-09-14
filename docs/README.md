# Reference documents

## External sources used as **benchmark**, not ingested

A benchmark is not a source of this platform. It has no Source, no RAW, no
partition, and none of its numbers enters a fact. It is used **only as a distribution
target**, and the Orders Source's CONTRACT treats `benchmark` as a third nature
alongside `observed` and `synthetic`.

### Informe del Consumo Alimentario en España 2025 — MAPA

| | |
|---|---|
| file | `docs/Informe comsumo 2025_.pdf` — **not versioned** (`docs/*.pdf` in `.gitignore`) |
| source | Ministerio de Agricultura, Pesca y Alimentación (gob.es) |
| URL | https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/panel-de-consumo-alimentario/ultimos-datos |
| sha256 | `b8c8abb6230ceda48db36f9fd59f8f99f268d332d49af3a3cdd73b430c4f0856` |
| size | 30.086.039 bytes · 645 pages |
| downloaded on | 2026-08-31 |

**Every number traces back to this file, not to a hand-typed guess.** The 29 MB PDF
itself is `.gitignore`d — what's versioned is `platform/dbt/seeds/mapa_2025_benchmark_seed.csv`,
where each of the 64 rows cites the report section it came from and a `provenance`
column (`informe_table`, `informe_prose`, `informe_chart`, `derived`) says how. The
`sha256` above pins the exact edition: the URL serves "latest data" and will start
serving MAPA's 2026 report the day it's published, so a re-download that no longer
matches this hash means the seeds no longer describe the benchmark in force.

**How to extract again.** `pdftotext -layout` recovers each section's header tables
verbatim — that's where the volume/value/price shares come from. What it can't
recover is the ~45 pages whose demographic breakdown is a bar chart, not a table;
those were read by hand off the printed axis labels and checked against two
independent sums that must both close to 100,00 (one per age band across volume,
one across population) — a misread digit breaks one of them. Two source-side
discrepancies (a population row on page 206, a region label on page 84) were caught
this way and are recorded in the seed's `note` column rather than silently fixed.

## What the live sources return

Two sources are **live APIs**, not downloads — the platform re-fetches current data on
every run, so nothing about them is frozen the way the two downloads below are. What's
worth keeping is a dated sample of the response shape:

- **Mercadona catalog** — [`sources/mercadona-catalog-source/sample-catalog-response.json`](../sources/mercadona-catalog-source/sample-catalog-response.json), a real capture (`wh=mad1`, `ingestion_date=2026-09-04`, sha256 `b5bfb47c7dfea67081dfe1ad3b2dde3c5fc775b2fecf80b39475fa1aed5919f0`, matches the run's own `_manifest.json`). Prices in it are whatever they were on that date — see [sources/mercadona-catalog-source/README.md § "Forma dos arquivos"](../sources/mercadona-catalog-source/README.md) for what's structural versus what's just that day's price.
- **INE population** — no static sample kept; the series is small enough (`make ine-refresh`) that running it is cheaper than a snapshot going stale.

## External sources downloaded manually, not via API

Two sources have no API and are only published as a manual download. Both are
`.gitignore`d (large reference files, not source code) — what the repository keeps
instead is the exact URL and the derivation script that turns the download into a
versioned seed, so re-downloading and re-deriving reproduces the same seed byte for
byte.

### INE Callejero (streets, census sections, population units)

| | |
|---|---|
| what | official geography — `SECC`, `UP`, `VIAS`, `PSEU`, `TRAM`, one `.zip` per province |
| source page | [Cartografía secciones censales y callejero de Censo Electoral](https://www.ine.es/ss/Satellite?L=es_ES&c=Page&cid=1259952026632&pagename=ProductosYServicios%2FPYSLayout) (ine.es) |
| provinces used | 08 (Barcelona), 28 (Madrid), 41 (Sevilla), 46 (Valencia) |
| cadence | semiannual — the INE republishes it twice a year |
| full detail | [sources/ine-callejero-source/README.md](../sources/ine-callejero-source/README.md) |

### `AUF_mun.xlsx` — INE Functional Urban Areas

Used to derive `warehouse_service_area_seed.csv` — which municipalities neighboring
each warehouse belong to the same commuting area, not just which municipality the
warehouse sits in.

| | |
|---|---|
| file | `temp/AUF_mun.xlsx` — **not versioned** (`temp/` in `.gitignore`) |
| source | Instituto Nacional de Estadística — page "Áreas Urbanas Funcionales" |
| URL | https://www.ine.es/uaudit_imagenes/AUF_mun.xlsx |
| sha256 | `773643c416a5c1384069f7615b4b57ae4e4e5ca1f8b83e029037bbbb35d99ba5` |
| size | 49.009 bytes |
| downloaded on | 2026-08-25 |
| derived by | `scripts/derive_warehouse_service_area.py <AUF_mun.xlsx> <callejero-dir>` → `platform/dbt/seeds/warehouse_service_area_seed.csv` |

**The URL can move or the file can be revised** the same way the MAPA one can — if the
sha256 stops matching, re-run the derivation script instead of assuming the seed is
still correct.

## Generated evidence

None of the numbers on these pages are written by hand. Regenerate them instead of
editing them.

| directory | generated by | what the page proves |
|---|---|---|
| `docs/demand-evidence/` | `make demand-reality-check` | the generated basket matches the MAPA target, cohort by cohort |
| `docs/warehouse-evidence/` | `make warehouse-evidence` | ownership, volume and role isolation at the destination |
| `docs/stream-evidence/` | `make stream-evidence` | the semantics of the real engines — broker, OLTP, Iceberg |
| `docs/spark-evidence/` | `make spark-evidence` | the same job on both engines, **including when plain Python wins** |
| `docs/FREEZE.md` | `make freeze` | the capture seal: partition, `content_sha256`, `capture_id` |

**`docs/spark-evidence/` is the only one that exists to back a *negative* claim** — "Spark
was not adopted for performance". A negative without a benchmark has the same disease
as a hand-copied number, with the sign flipped, so the page publishes both times side
by side no matter which one wins.

The `before_*.json` files of `docs/demand-evidence/` are the exception: they are not
generated on every run, they are **frozen snapshots**. After `orders-refresh-all
--overwrite` the prior state no longer exists anywhere — without them, the reality
check can only say "this is how it is today", which is half the question.

| snapshot | what it freezes |
|---|---|
| `before_mapa_2025_v1.json` | the **uniform** mix, before any calibration |
| `before_mapa_2025_v2.json` | the mix **calibrated in aggregate**, before the cohort layer |
| `before_customer_v2.json` | the base of **20.000 customers equal across warehouses**, with 18,01% minors, before density existed |

The default for `--before` is the **most recent** one, the immediately prior state.
Using an old one as the default would sum the effects of several phases into a single
column — Phase 4's price-correction revenue drop would read as if it were Phase 6's.
The earlier ones stay on disk and are cited in the page's text.

Each snapshot records `null` for whatever did not yet exist when it was frozen,
rather than an empty object: `cohorts: null` in v2 (before the `buyer_age_band`
stamp), `channel: null` in the first two (before the served population entered the
measurement). Empty would be indistinguishable from "I measured and there was
nothing there".
