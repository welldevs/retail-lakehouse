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

**Why it is not versioned.** It is 29 MB of binary, and git history is permanent. What
the repository needs to preserve is **traceability**, not the file: each of the 64
lines of `platform/dbt/seeds/mapa_2025_benchmark_seed.csv` cites the report section the
number came from, and `provenance` distinguishes `informe_table` (the section's header
table) from `informe_prose` (a number cited in the text), `informe_chart` (a label
printed on a chart), and `derived` (calculated from two published numbers, with the
derivation written on the row itself). The cohort seeds cite the **PDF page**, not the
section, because it's the page you open to double-check a chart label.

**The URL points to "latest data" and will change.** When MAPA publishes the 2026
report, this link will start serving the new file. The `sha256` above is what
identifies the edition used; if it stops matching, the benchmark in force is not what
the seeds describe, and the model version (`mapa_2025_v2`) needs to change along with
it.

**The report also sizes the customer base, not just the mix.** E-commerce's share of
food volume (2,2%, section 3) is used as a penetration rate over the adult population
of the four AUFs — it is the only observed number in this repository able to size a
registry. It lives in a single line
(`demand_profile_seed.channel_reference_pct`), and `customer_premises_seed`
**points** to it instead of copying it. Two declared assumptions carry out the
transposition from volume share to people share, and neither is measured: that the
online buyer consumes like the average, and that these four warehouses model the
AUF's entire channel and not one operator within it.

**Provenance finding.** The PDF's cover page says *"Informe del consumo alimentario en
España 2024"*, while the entire body reports the year **2025** ("A cierre del año
2025…", "frente a los 26.823,4 millones del año 2024"). It's copy-paste residue from
the previous edition on the credits page. The seeds cite the **body**. The
discrepancy is recorded here and in the CONTRACT instead of being silently resolved.

**How to extract again.** `pdftotext -layout` preserves the alignment of each
section's header tables, which is where `Parte de mercado volumen (%)`, `Parte de
mercado valor (%)` and `Precio medio (€/kg)` come from. The monthly and channel
charts are **images**: only the axis labels come out in the text, which is why there
is no seasonal profile by category.

**The `Demográficos` blocks come in two formats, and the second requires reading the
page.** Seventeen sections carry a compact table that `pdftotext` recovers whole; the
rest carry bar charts. Those charts **carry a printed numeric label** — page 158 shows
`8,89 / 2,63 · 30,33 / 18,19 · 31,34 / 34,55 · 29,44 / 44,63` — so reading them is
extraction, not estimation. About 45 pages were read to cover the 39 weighable groups
across the two cohort dimensions.

**Two independent checksums verify every reading**, and are why the manual extraction
is acceptable:

1. the four **volume** age bands add up to 100,00;
2. the **population** ones add up to `8,89 + 30,33 + 31,34 + 29,44 = 100,00`, and
   those four numbers repeat in **every** section, because they are the universe and
   not a category measurement.

A misread digit breaks one of the two sums, and `demand_profile.load_cohort_age`
fails. The region seed has no sum to close — it carries 4 of the 17 communities — so
its check is the constancy of the population share, verified in
`test_share_de_populacao_e_o_mesmo_em_todo_grupo`.

**Two discrepancies from the source itself**, recorded rather than smoothed over:
page 206 publishes `30,5 / 31,7 / 29,0` for population where every other page
publishes `30,3 / 31,3 / 29,4`, and page 84 labels the Comunidad de Madrid `13,78`
where the others label it `13,86`. Both are in the `note` column of the
corresponding row, and the checksums' tolerance is wide enough to admit them and
narrow enough to catch a swapped digit.

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

**`docs/spark-evidence/` is the only one that exists to back a NEGATIVE claim** — "Spark
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
