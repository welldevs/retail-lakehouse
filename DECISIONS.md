# Decisions, and what each one cost

This file holds the **history**: what was decided, why, against what evidence, and what
was lost along the way. [ARCHITECTURE.md](ARCHITECTURE.md) holds the **state** — what holds
true today. The rule that separates the two is simple and is verified by test:

> **State in ARCHITECTURE and README. History only here.**

It exists because the two documents used to grow together, with the same text appearing in
different places and aging at different rates. Three documentation revisions were
spent on exactly this.

## How to read

The index below gives each decision in four lines — **decision, reason, evidence, trade-off** —
and points to the full narrative, which was kept **intact**, with the date and the numbers that
held true when it was written. A dated number doesn't rot: it was true on that day. What
rots is a number presented as current state, and that's why it doesn't live here.

**Not every decision has a comfortable trade-off, and the ones that don't are the ones that matter most.**
A "cost nothing" in this list is a sign that the analysis was shallow, not that the decision was
good.

---

## Decision index

### Five Sources as sibling packages, with `dependencies = []`

- **Decision.** Each source is an independent Python package, with no third-party dependency, and
  the platform consumes it through the physical contract on disk — never importing its code.
- **Reason.** A shared abstraction between sources that don't resemble each other (live API,
  manual download, synthetic data) would force inventing the common denominator before knowing
  the differences.
- **Evidence.** `make source-test` runs on the system Python, outside the venv: if any
  Source gains a dependency, it breaks. An AST test forbids the import.
- **Trade-off.** Repeated code across the five. Accepted: clear repetition costs less than
  premature abstraction, and the payoff showed up in the Airflow image — zero dependency means
  the Source runs anywhere.
- → [§ "Source ↔ platform boundary"](ARCHITECTURE.md)

### Second and third sources: INE and Callejero

- **Decision.** Real population and addresses from the INE come in as their own sources, not as
  seeds.
- **Reason.** A synthetic customer base needs real geography so it isn't a uniform draw
  over a map that doesn't exist.
- **Evidence.** The ambiguity of municipal homonyms only showed up against the real data, and
  produced `ine_ambiguous_series_seed` — derived by script, with a target in the Makefile.
- **Trade-off.** The Callejero is a **semiannual manual download**, and that breaks the
  automatic reproducibility of RAW. That's the reason `capture_id` exists.
- → [§ "Second source"](#second-source-ine-population) · [§ "Third source"](#third-source-ine-callejero)

### Snowflake gets a cut, not the whole Silver

- **Decision.** The warehouse receives the tables that answer analytical questions, not a
  second copy of the lakehouse.
- **Reason.** Copying RAW to Snowflake just because Snowflake exists is the opposite of
  architecture.
- **Evidence.** The cut ratio went from 3.85% to 46.9% between Phase 2 and Phase 6 **with no
  rule changing** — it is a function of which sources fall within scope, and therefore is not a
  property of the design.
- **Trade-off.** Questions outside the cut require going back to the lakehouse. Accepted, and
  declared on the dashboard as *Out of scope*.
- → [§ "Phase 2"](#phase-2-analytical-layer-on-snowflake)

### Orders are born as an EVENT LOG, not a snapshot

- **Decision.** The fifth source delivers an append-only log; `silver_order` is a fold.
- **Reason.** A table with `status` says where the order is, never how long it took to
  get there. The warehouse durations are the concrete answer to what the log bought.
- **Evidence.** Three independent folds — window function, transactional, and streaming —
  agree order by order. Two folds disagreeing found two defects that no test
  caught.
- **Trade-off.** The fold is expensive and reconstruction is O(n²) at the current volume — 55 min
  for 206 thousand orders. Declared in [BACKLOG.md](BACKLOG.md) with the fix.
- → [§ "Phase 3"](#phase-3-orders-as-events-fifth-source)

### Transactional outbox instead of dual write

- **Decision.** Business state and event in the **same transaction**; the publisher reads the
  table.
- **Reason.** Writing to the database and the broker in two operations loses an event on every
  failure between the two.
- **Evidence.** `prove_oltp_atomicity.py` injects a failure into the database and proves that
  both sides fall together.
- **Trade-off.** Duplication **is possible** between the broker's ACK and marking the outbox,
  and the project **does not promise exactly-once**. Dedup lives in the consumer.
- → [§ "Phase 3, second half"](#phase-3-second-half-the-oltp-and-the-transactional-outbox)

### Kafka is transport, never the canonical source

- **Decision.** The canonical log is born on disk; the broker carries it.
- **Reason.** A broker with 7-day retention is not a system of record.
- **Evidence.** Replay reproduces the 16 sha256; a sequence gap is refused; a duplicate is
  discarded.
- **Trade-off.** Nobody here needs the latency that Kafka buys, and that is written down.
- → [§ "Phase 3, third half"](#phase-3-third-half-transport-replay-and-idempotent-consumption)

### Iceberg comes in for CONCURRENCY, not volume

- **Decision.** `live_order_state` in Iceberg, with two writers and one concurrent reader.
- **Reason.** At this volume an atomic-`os.replace` parquet would do; what Iceberg
  buys is snapshot isolation between writers.
- **Evidence.** `make spike-iceberg` measured catalog, upsert, conflict, and read **before**
  the projection existed. A commit over a stale snapshot is refused, and the retry after
  reloading passes.
- **Trade-off.** Half of the justification — *interop between engines* — stayed **asserted and
  not demonstrated** for four phases, because both writers were Python. Only Phase 7 paid down
  that debt.
- → [§ "Phase 3, fourth half"](#phase-3-fourth-half-the-concurrent-projection-in-iceberg)

### Calibrate demand against MAPA 2025 — benchmark, never ground truth

- **Decision.** The basket's category mix is calibrated against the MAPA consumption report.
- **Reason.** Without it, the mix mirrored the **assortment size**: a catalog with 475 SKUs of
  personal care produced a basket nobody buys.
- **Evidence.** The calibration found a **price** defect that the previous demand was
  hiding — `unit_price` isn't always the price of a purchasable unit.
- **Trade-off.** MAPA measures **household consumption in volume**, not e-commerce cart in
  value. The conversion is a declared heuristic, and the benchmark calibrates volume; value is
  a consequence of the observed price.
- → [§ "Demand calibration"](#demand-calibration-against-mapa-2025-phase-4)

### Cohort only with an observed attribute

- **Decision.** Demand varies by age and region cohort, and only by attributes that exist
  in the data.
- **Reason.** A benchmark cut can only become a segmentation if the attribute is observed on
  both sides.
- **Evidence.** Phase 5 found 18% of the base under 18 years old — newborn account
  holders — because age had started to govern demand.
- **Trade-off.** Neutral-index groups hide defects: a segment with no measured difference
  looks calibrated and simply wasn't tested.
- → [§ "Customer consumption profile"](#customer-consumption-profile-phase-5)

### The customer base is served population, not buyers

- **Decision.** `silver_customer` sizes the base by the real population of the served areas.
- **Reason.** A base of 20 thousand customers distributed uniformly has no density to
  measure.
- **Evidence.** Phase 6 showed that the proportion was wrong **without breaking any sum** —
  an allocation defect, which only a test that redoes the split from the source catches.
- **Trade-off.** Simulation density looks like market penetration, and it's the number easiest
  to quote out of context across the whole dashboard. The label travels with every column.
- → [§ "Real density of the customer base"](#real-density-of-the-customer-base-phase-6)

---

## Phase 7 — the closing

### Spark comes in for interop and for shape; volume is a declared NON-TRIGGER

- **Decision.** A Spark job computes the stock ledger and writes to the same Iceberg catalog.
- **Reason.** Two, and performance is not one of them. **Interop:** it's the catalog's third
  writer and the first one outside Python — the property that justified Iceberg had been
  asserted since Phase 3 and never demonstrated. **Shape:** the balance is a running sum whose
  *inputs are generated by decisions made from the state itself*, and a window function doesn't
  write back into the partition it reads.
- **Evidence.** `make spike-spark-iceberg`, 18 questions, run **before** any line of this
  phase, with both outcomes declared beforehand. The volume trigger was **measured and did not
  fire**: 37.9M basket pairs in ~1.5 s and ~2.4 GB on one node. And S7b runs the same input
  through the running sum in SQL: it diverges on 14 of 30 days and reaches a balance of −70.
- **Trade-off.** One more JVM, one more image, and one more compose profile. Contained by
  construction: Spark is **optional** — `make silver` runs green on a tree where it never ran,
  and that is test, not promise. And `make spark-evidence` publishes the pure-Python time
  alongside it, which wins at this volume.

### Contradictory assumptions are a model defect, and can be corrected

- **Decision.** `slot_lead_hours_*` and `sla_minutes_picking` became **derived** from the other
  rows of the same seed.
- **Reason.** The project's rule — refuse to adjust an assumption until the output pleases —
  was correct and was missing a distinction: tweaking an assumption to improve a number is one
  thing; making two assumptions **mutually coherent** is another. An alert above the arithmetic
  ceiling and a window that opens after delivery are not undesired outcomes, they are
  contradictions.
- **Evidence.** 84% of deliveries arrived **before** the window opened (73,124 of 86,803); the
  SLA threshold was 90 against a possible ceiling of 80.
- **Trade-off.** It becomes trivially easy to keep tweaking until the KPI pleases. Contained:
  `assert_order_premises_are_internally_coherent` checks the **derivation**, never the result —
  measured, swapping the threshold from 60 to 75 fails, even though it's a plausible value.

### RAW is sealed, not reproduced

- **Decision.** `make freeze` seals the capture; `make freeze-check` verifies it.
- **Reason.** RAW is **not** reproducible — live API, manual download, moving URL — and
  promising otherwise would be false. Everything downstream is deterministic **given the same
  RAW**.
- **Evidence.** Three documentation revisions existed because a regeneration changed numbers
  already written and nothing warned about it. The discipline became verification.
- **Trade-off.** The seal covers the **data**, not the run: `run_id` and timestamps are left
  out on purpose, otherwise a byte-identical re-land would break the seal — and an alarm that
  fires with no cause trains reviewers to ignore it.

---

# The full narrative, by phase

The sections below are **as they were written**, with the date and the numbers that held
true on that day. A dated number doesn't rot — it was true when it was measured. What
rots is a number presented as current state, and that's why it doesn't live here.

## Second source: INE population

[sources/ine-population-source/](sources/ine-population-source/) was added on
2026-08-25, along with three changes to the platform that the Source ↔ platform boundary
above didn't foresee, because only one source existed when it was written.

**`platform/…/manifest.py` generalized to a second, optional axis.** Before, `Partition`
required `warehouse` and computed the snapshot root by climbing exactly two fixed levels —
`SOURCE_NAME` was a single constant in `__init__.py`, and `land.py` used it directly instead
of `partition.source_name` (which already existed in the dataclass, populated from the
manifest, but never used for that purpose — a residue of a comment that already promised this
without the code delivering). A source with no warehouse axis, like the INE, broke with
`"manifest missing partition.ingestion_date or partition.warehouse"`. Generalized to
`axis_name`/`axis_value` (`None` when there is no axis) and `SUPPORTED_MANIFEST_VERSIONS` as a
dictionary keyed by `source_name`, keeping the `warehouse` property as compatibility so as
not to touch `raw_manifest.sql` or the Mercadona tests. Land and verify started deriving
the prefix from `partition.source_name` — the change that makes real the promise of the
original comment ("so that a second source can land alongside without reorganizing
anything").

**`dbt build` compiles the whole project, so an empty source used to bring down the rest.**
Measured: DuckDB's `read_json` raises a fatal error on a glob with no matching file — the
normal state of a newly added source before its first land, or of a fresh repository clone.
Left untreated, this would break the whole Mercadona `make daily` just because of the INE
model, even for someone who never touched it. The obvious attempt — making the model's SQL
fake an empty relation with `WHERE false` — doesn't fix it: dbt-duckdb's own `external`
materialization, when handling an empty relation, writes a sentinel row (all columns `NULL`)
into a `__HIVE_DEFAULT_PARTITION__` file to preserve the parquet schema, and only filters that
row out in the *view* of that same run — the physical file persists, and the next run with
real data reads the whole `location` back, including the ghost row. The fix stayed out of
SQL: `retail-platform has-data <prefix>` (a new, generic subcommand — any source) checks
whether any object has landed, and `make silver` passes `--exclude` for the model of a source
with no data yet, instead of making the model lie about having an empty partition.

**Sibling sources, not a "multi-source" abstraction.** When a second INE dataset comes in
(considered: the Electoral Census Callejero, see the project history), the pattern is another
complete sibling package in `sources/`, not a shared layer inside the current INE package.
The two datasets are structurally distinct (a small, instant JSON API vs. a semiannual ZIP of
fixed-width ASCII files per province) — forcing a common interface now would fit both poorly,
and each package keeps `dependencies = []` independently and verifiably by AST. Extracting a
shared helper would only be worth it once two concrete cases exist to compare, not before.

## Extension: population by municipality (Phase A)

Added on 2026-08-26, on top of the already-existing `ine-population-source` (not a fourth
source) — the fetch mechanism had already been generic by `table_id` since day 1 (see the
previous section), so adding `table_id=29005` (population by **municipality**, INE) alongside
`31304` (population by **province**) was a configuration extension, confirmed by a code audit
before implementing: `http_client.py`/`extract.py`/`partition.py` don't know about any
specific `table_id`.

**Motivation, not aesthetics.** `31304` alone is inadequate for density: measured that
"Valencia/València" in that table is the **entire province** (2.6 million residents, 266
municipalities), not the city — distributing that number across streets would be fabricated
data. `29005` gives the real number per municipality, joinable against
`warehouse_service_area` (Callejero) for real density.

**Real collision between the two tables, measured before it happened in production:**
"Sevilla" is at the same time a province name (a closed list of 52 in
`silver_ine_population_series`) AND the name of that province's capital municipality. A
generic glob (`table_id=*.json`) over the two tables together would make the `31304`
classifier (by vocabulary) capture "Sevilla. Total. Total habitantes. Personas." (from
`29005`) as though it were a province row, with the wrong sex/age. Fixed by restricting each
Silver model to its own `table_id`, literal glob, no shared wildcard — no "one model reads
every population table" abstraction, because the two tables have incompatible `Nombre`
vocabulary (see the source's CONTRACT.md, § 2).

**A municipality name is not a key severable from outside this platform's scope.** `29005`
carries no code, only the spelled-out name. Measured against the full payload of
`VALORES_VARIABLE/19` (the "Municipios" variable of the same Tempus3 API): 18 of Spain's
~8,200 municipalities share a name with another municipality in a different province (e.g.
"Arroyomolinos" exists in Madrid [28015] and Cáceres [10023]) — a Spain-wide join by name
would be ambiguous for those cases. No collision happens **within** this platform's 4
provinces (verified before writing `ine_municipality_codes_seed`), so scoping the seed to
08/28/41/46 (the same reasoning as `warehouse_service_area`) resolves this structurally, not
by luck.

**Tested and discarded: reusing the already-landed Callejero for the code, instead of a new
call to `VALORES_VARIABLE/19`.** `silver_callejero_population_units` already has
`(province_code, municipality_code, municipality_name)` — fetching another source seemed
redundant. Discarded after querying both live: the spelling diverges structurally, not just
upper/lower case — the Callejero writes the whole name in UPPERCASE and moves the definite
article to a parenthetical suffix (`"AMETLLA DEL VALLÈS (L')"`), while Tempus3 (both `29005`
and `VALORES_VARIABLE/19`) uses the natural name with the article as a prefix (`"L'Ametlla
del Vallès"`). A direct join between the two spellings would fail silently.
`VALORES_VARIABLE/19` was chosen for being in the SAME endpoint family as `29005` —
confirmed 0 spelling divergences in a sample of 1,501 names.

**Age bracket by municipality was left out, deliberately.** There's a family of dozens of
INE `table_id`s with municipality+age (confirmed that at least one, `33956`, is populated —
but only for the province of Zamora, suggesting one `table_id` per province, not discovered
for this platform's 4 provinces). Chasing this now would amount to building a whole other
integration with no proven need yet. Decision: keep `31304` (the only source of age
structure, province level) and `29005` (the only source of real density, municipality level)
as **complementary**, not try to substitute one for the other — if the synthetic customer
simulation needs an age pyramid by municipality in the future, that family of tables is the
next place to investigate, not before.

**The real production extraction (2026-08-26) confirmed two problems that only showed up
running for real, not on a sample.** (1) The unfiltered `31304` payload is large enough (~264
MB in the canonical format) to get corrupted in transit before finishing — the first attempt
failed with `JSONDecodeError` at byte 154,057,166 after ~33 min; the `Fetcher`'s automatic
retry (already existed, treating an invalid JSON body as a retryable error) resolved it on
the 2nd attempt. Total landed: ~402 MB, 40,791 series, 2,253,624 data points — see the
source's CONTRACT.md for the full numbers. (2) `validate --strict` failed the partition over
6 `29005` series with no data point at all — 2 municipalities (`Gatova`/Castellón,
`Palmerola`/Girona) outside this platform's 4 provinces. Since `--strict` is an optional
contract check (not integrity), and failing the whole extraction over an out-of-scope
municipality wouldn't protect anything real, `--strict` was removed from `make ine-validate`
and the DAG — but not from the source's own internal Makefile (which still fetches only
`31304` by default, where this check never failed). The count of series with no value is
still reported in the `validate` output, it just stopped being fatal.

## Third source: INE Callejero

[sources/ine-callejero-source/](sources/ine-callejero-source/), added on 2026-08-25,
confirms the previous section's prediction: a complete sibling package, not an extension of
the population package. **Zero structural change to the platform** was needed beyond
registering the name in `SUPPORTED_MANIFEST_VERSIONS` — `land.py`, `verify.py`, and
`manifest.py`, generalized in the previous phase, worked without touching code, including the
single-axis partition (`ingestion_date` only, no warehouse or province as a formal axis).

**No API — the first source of this type.** The Callejero is only published for manual,
semiannual download. That broke an implicit assumption of the two previous sources (that
"extract" fetches data over the network): here `extract` ingests files a human has already
downloaded, with no HTTP request at all. The verb was kept for CLI uniformity, with the
meaning documented explicitly in the source's `CONTRACT.md` — swapping the verb would break
the Makefile/DAG symmetry with no real gain.

**Undocumented file layout — measured, not assumed, with a tooling gotcha along the way.**
The download brought no "Diseño de Registro" at all. The real encoding is ISO-8859-1
(Latin-1), confirmed with `file`; a `grep` straight on the raw files (before discovering
this) returned empty even with the text right there — discovered afterward that this
environment's `grep` is a wrapper around `ugrep -I`, which **ignores files that look
binary**, and a Latin-1 file with high bytes (accents) triggers that heuristic. The fix was
checking with `grep -a` or converting with `iconv` first. The same encoding, when reading the
files back into DuckDB via `read_csv`, needed the exact name `'latin-1'` (with a hyphen) —
`'latin1'` is rejected with a list of ~700 supported encodings, none of them under that exact
name. Neither gotcha would have been caught without testing against the real file.

**Per-file column layout** (byte offsets, no delimiter — read in DuckDB via
`read_csv(..., delim=E'\x01', hive_partitioning=1, filename=true)`, a delimiter that never
appears in the data, to bring the whole line in as one column): `SECC` is just a 10-digit
code; `VIAS`/`PSEU` have code + name in 2-3 redundant forms (different widths, same text);
`UP` is the most complex — 604 characters, with the MUNICIPALITY name at one position
(`[94:314]`) and the name of the NÚCLEO/entity within it at another (`[459:529]`), confirmed
by comparing real content (`"ABRERA"` repeated for several entities, each with a different
núcleo name — `"CAN VILALBA"`, `"SANT MIQUEL"`, `"*DISEMINADO*"`), not assumed from
positional similarity with the other files. See the source's `CONTRACT.md § 2` for the full
table.

**`TRAM` was brought in during a second round, after being deliberately left out in the
first.** The original decision was not to inspect `TRAM` (the heaviest of the 5 — 14-28 MB
per province) until the Orders simulation needed house-number granularity. It reopened once
it became clear that none of the other 4 files carries a postal code (CEP) or a
"neighborhood" with real meaning outside Valencia — and the INE's official page on the
Callejero explicitly confirms that it's `TRAM` that carries "el distrito postal de cada
tramo" (the postal district of each segment).

**Decoded by measurement, confirmed against official documentation — not assumed in either
direction.** New layout (273 chars) inspected from scratch: section code at `[0:10]` (same
format as `SECC`), entity/núcleo suffix at `[13:20]` (same format as `UP`), street id at
`[20:25]` (same format as `VIAS`) OR pseudo-street id at `[25:30]` (same format as `PSEU`) —
mutually exclusive, confirmed with no exception across 305 thousand real rows from the 4
provinces. The intermediate block (`[42:58]`, 16 chars) resisted a first pass with a naive
regex (`\S+` glued adjacent fields together with no space). We found the official PDF
["Diseños de registro de los ficheros de intercambio de información
INE-Ayuntamientos"](https://idapadron.ine.es/repositorio/DisReg/disregok.PDF) (IDA-Padrón,
2015) via search — it describes the *exchange* format for variations between the INE and
municipalities (Ayuntamientos), not the snapshot we downloaded, but it names the "Tramero"
fields in the same order: `CUN CVIA CPSVIA MANZ CPOS TINUM EIN CEIN ESN CESN`. Using that
order to cut the bytes, it matched: `CPOS` (postal code, 5 digits) always with the correct
province prefix in 304,905/304,952 rows (99.985% — the 47 exceptions only in Barcelona),
`TINUM` never outside `{0,1,2}`, and the `EIN`/`ESN` range always respecting the parity that
`TINUM` declares (even/odd) — zero exceptions across all four. Also validated against real
geography: the city of Valencia has 30 distinct postal codes (46001-46026 + exceptions), and
specific pedanías match the real postal code of the area (Pinedo/El Saler → 46012, southern
zone of the city). The rest of the record (part of `[58:273]`) remains undecoded — it
includes a field repeated at the end that mirrors `CPOS`+`TINUM`+`EIN`+`ESN` already captured
at the beginning; nothing beyond that is extracted or asserted in Silver.

**`warehouse_province_map` is not generated by this source.** The seed
(`platform/dbt/seeds/warehouse_province_map_seed.csv`) was derived from the Callejero
manually during development (`scripts/derive_warehouse_province_map.py`, cross-referencing
the municipality name in `UP` against the existence of sections in `SECC`), and exists
independent of the source itself. The Callejero source doesn't know warehouses exist — it
produces `province_code`/`municipality_code` the way the INE publishes them, with no
reference to `mad1`/`bcn1`/`svq1`/`vlc1`. The link is a decision of this platform, not a
property of the INE, and only enters via `JOIN` in Silver/Gold.

**`warehouse_service_area` — same mechanism, different question.** `municipality = wh` (1:1)
is not the same as "area the warehouse serves" (N neighboring municipalities). Querying only
`warehouse_province_map` makes a real, adjacent municipality (e.g. Albal, a neighbor of
Valencia, but administratively independent — with its own town hall, postal code, and
municipality code) look like it "doesn't exist" in the warehouse's geography, when in fact it
was simply outside the query's scope. The source for the list couldn't be invented or
estimated by proximity (the Callejero has no coordinate or adjacency) — we used the INE's
"Functional Urban Area" (AUF, formerly LUZ): the single official methodology for the whole
country (a municipality enters a city's AUF if ≥15% of the employed population commutes there
for work), downloaded as an Excel file from `ine.es` and parsed with the stdlib's
`zipfile`+`xml.etree` (an `.xlsx` is just a zip of XML — no new dependency had to be added).
Each of the resulting 370 municipalities was cross-validated against the real Callejero
before entering the seed (`scripts/derive_warehouse_service_area.py`): confirmed that at
least one `SECC` section exists with that province+municipality prefix, and the name used is
the real `UP`'s name (not the INE's Excel text), to match exactly
`silver_callejero_population_units.municipality_name`. **Consciously accepted limitation**:
Madrid's official AUF has 166 municipalities, but 38 fall in Ávila, Guadalajara, or Toledo —
provinces this platform has never downloaded from the Callejero (only 08/28/41/46). Barcelona
has the same problem at a smaller scale (2 of 135, in Tarragona). Sevilla (46/46) and
Valencia (63/63) have no such gap — both AUFs fit entirely within the provinces already
landed. Explicit user decision: use what's already downloaded now, instead of extending the
Callejero source to 4 more provinces just for the border municipalities.

## Fourth source: simulated OLTP (Phase 1 — Customers)

[sources/simulated-oltp-source/](sources/simulated-oltp-source/), added on 2026-08-27. It is
the **first derived source** in the repository: the other three are upstream of external
data, this one consumes the Silver that they produced and hands back a new RAW. The inverted
direction is the point — without the previous three, generating a synthetic customer would
mean inventing their address; with them, the customer is invented and where they live is not.

**The tension that had to be resolved before writing a line.** Every Source in this repo is
FROZEN (`dependencies = []`, enforced by AST in `tests/test_dependencies.py`), and
`duckdb`/`boto3` are platform-only dependencies by explicit design (`platform/pyproject.toml`:
"the two sets never meet"). A Source that needs data from the Lakehouse can't open a
connection without breaking that boundary.

The way out reuses a precedent the Callejero had already created, one level up the chain:
there, `extract` doesn't fetch over the network, it ingests files already prepared under
`--in`. Here the **platform** gained a subcommand (`retail-platform export-oltp-reference`)
that queries Silver and writes three flat JSON files; the Source's `extract` reads them using
only the stdlib. Neither side imports the other's code — they share a **physical** contract,
the same mechanism `land.py`/`manifest.py` already use with any Source's manifest. **Zero
structural change to the platform** beyond registering the name in
`SUPPORTED_MANIFEST_VERSIONS`.

**What adversarial review caught before implementation.** Three rounds of review against the
real Lakehouse, and most of the value came from measuring instead of assuming:

- **The municipality-name join zeroed out Valencia.** The initial design matched
  `tramos.unit_code` against the aggregated row of `population_units`. Measured: **0 of
  7,194** `vlc1` tramos — València is the only one of the 4 hub municipalities with real
  núcleos/pedanías, so no tramo hangs off the aggregate row. Fixed to match by
  `(province_code, municipality_code)`: 7,194/7,194.
- **The dbt seeds aren't reachable by `connect_lakehouse()`.** `warehouse_service_area_seed`
  and `warehouse_province_map_seed` have no `location =` in `dbt_project.yml`, so dbt-duckdb
  materializes them inside the local `retail.duckdb` and they never become parquet under
  `silver/`, which is all that function sees. The export reads both CSVs directly.
- **The 103 `age_label` values of table 31304 don't form a partition.** Alongside the 101
  single ages sit two overlapping aggregates, `Total` and `85 y más años`. Naively summing all
  103 gives **2.03×** the correct value (measured for Madrid/2022: 27,724,421 against
  13,650,010, which is exactly the `Total` label). Without `where age_label not in (...)`, the
  age pyramid of every customer would come out wrong and **no test in the previous plan would
  catch it**.
- **`silver_ine_population_series` is duplicated across two `ingestion_date`** with identical
  rows (1,547,496 each). The export pins `max(ingestion_date)`.
- **Filtering by `numbering_type='0'` isn't enough to avoid fabricating a number.** There are
  14 tramos with `numbering_type='2'` and a `0000..0000` range: drawing from `[0,0]` would
  yield house number 0. 0 is excluded from the sampleable set.
- **The traceability the plan promised was false.** `(street_code, postal_code)` alone
  identifies only **13.3%** of the tramos in scope (105,112 combinations for 216,594 tramos;
  the largest covers 70). Not even a 7-column key is enough — the real grain has 11. Resolved
  with `candidate_index`: an integer pointing to the exact row in the reference.
- **Neither of the two INE sources spells a municipality the same way.** In **370 of 370**
  cases the Callejero uses uppercase with the article in parentheses (`BRUC (EL)`) and table
  29005 uses mixed case with the article postposed (`Bruc, El`). This was discovered because
  validation failed 200/200 customers on the first real run. Both names live in the reference
  under distinct labels, and what's ever compared is always the **code**.

**A defect in our own Silver, found in passing — and fixed.** Three municipalities were
getting two population rows each (Arroyomolinos and El Molar in `mad1`, Torrent in `vlc1`).
The measured cause is neither an INE anomaly nor a seed collision (0 collisions across the 4
provinces): the RAW 29005 table is national and carries two series with an identical `Nombre`
for homonymous municipalities in different provinces, and the `inner join ...
on municipality_name` matches by name alone. It was logged as technical debt at the Phase 1
delivery and **closed the same day**, using the INE's official code instead of a heuristic —
see "Homonym fanout in the population Silver," below.

**Vocabulary choices: preserve, don't rename.** The first design created an
`address_precision` field with values `house`/`street` to say whether the address had a
number. The repo already had the answer: `numbering_type`, with `accepted_values
["0","1","2"]`, models exactly that fact. Inventing new vocabulary would collapse `1` and `2`
into one thing, erasing the parity — which is exactly the guarantee worth auditing. The
customer carries `numbering_type` verbatim, and `house_number` stays `null` (never absent)
when there's no number: an optional key would make the partition's `schema_fingerprint`
depend on the seed, because the fingerprint is the union of the observed keys.

**Reproducibility had to be built, not inherited.** This is the first source with
randomness — there was no use of `random` anywhere in the repo before. Two real risks: none
of the join's 6 Silver models has an `ORDER BY` (the order comes from DuckDB's scan and isn't
stable), and CPython's `str` hash is randomized per process. The export's three queries carry
an explicit `ORDER BY`, and the generator never iterates a `set` or a reconstructed `dict`.
The guarantee is verified by running the same seed in subprocesses with different
`PYTHONHASHSEED`.

**Result measured on the first real run** (Callejero `2026-08-25`, population `2026-08-26`):
216,591 address candidates (3 orphans excluded), 370 municipalities, 800 customers across 4
partitions, **200/200 coherent in each warehouse**, 14 with no house number, 2 on a
pseudo-street. Injecting a postal code from outside the AUF makes `oltp-validate` fail with
exit code 1 — verified end to end, not only in a unit test.

**Deliberately out of Phase 1:** Silver model and Airflow DAG. No source in this repo got a
Silver model before it had a consumer. **Both entered Phase 2** (`silver_customer`,
`silver_oltp_manifest`, `simulated_oltp_customers.py`), together with the missing
consumer — the dimensional model.

**The base is reloadable, and that is a property of the code, not a promise.** Verified by
reading `customers_generator.py`: the loop is `for index in range(count)` over a single
`random.Random(seed)` consumed in fixed order, and **nothing before the loop depends on
`count`**. So `generate(ref, wh, N, seed, data)[:M] == generate(ref, wh, M, seed, data)` —
growing the base is **append-only**, without touching the frozen Source. Proved end to end:
regenerating from 200 to 5,000 customers preserved the original 200 **byte for byte** across
all four warehouses, with the sha256 checked. `mad1`'s AUF coverage rose from 46 to 125 of
128 municipalities.

The two conditions that are **not** additive, stated explicitly: another `ingestion_date`
preserves the sampled age and shifts `birth_year` (the same person, older); another seed
swaps the people behind the same ids. The manifest logs both in `history`, with the seed and
the `count` of each previous run. That's why `DIM_CUSTOMER` is SCD2 and not a fixed table.

Orders, stock, and delivery remain out of scope — see "Phase 2: analytical layer on
Snowflake."

## Phase 1 operational consolidation

Three problems that only showed up when asking "what happens if I run this again?" — none
was visible on a single run, and all three gave a wrong result silently.

**The default timeout didn't fit these tables, and neither the Makefile nor the DAG corrected
it.** The Source uses `DEFAULT_TIMEOUT = 30.0`, sized for a common API call. These aren't
common: 264 MB (31304) and 125 MB (29005) in a single `GET`. The extraction that worked on
this machine was a **manual** invocation with `--timeout 240 --max-retries 3` — the automated
path would have stalled. Both paths now pass the same values
(`INE_TIMEOUT`/`INE_MAX_RETRIES` in the Makefile, `RETAIL_INE_TIMEOUT`/`RETAIL_INE_MAX_RETRIES`
in compose and in the DAG). The Source's generic default stays as is: **whoever knows the size
of the table is whoever requests it**, and that's written in the caller.

**Re-extracting the same release duplicated rows in Silver, with no visible error.** The
reference models stack every `ingestion_date` on purpose — the history is deliberate — but
nothing marked which one was current. Measured: `silver_ine_population_series` with 1,547,496
rows on **each** of two dates, i.e. 3,094,992 total; any unfiltered count came out doubled.
The seven models started exposing `is_latest_ingestion` (`ingestion_date =
max(ingestion_date) over ()`), a dbt test guarantees the flag marks exactly one date per
model, and `export-oltp-reference` swapped its scattered `max(ingestion_date)` calls for
`where is_latest_ingestion`. The Mercadona models did **not** gain the column: there, the
several dates are the product, not a side effect.

**`data/` grew without limit and nothing was ever deleted.** After `land` + `verify-landing`,
the local copy is redundant. `retail-platform prune-local` removes the local partition, but
only after **two** checks: the local copy against its own manifest, and the destination
against that same manifest. The first isn't redundant — the smoke test against real MinIO
showed that without it a corrupted local file was deleted as if it were intact, because
`verify-landing` compares the object with the manifest and the object was still fine. There's
no `--force`, and the target never enters a `*-refresh`: deleting data is a decision for
whoever operates it.

## Phase 2: analytical layer on Snowflake

Added on 2026-08-27. This is the first time this repository crosses the boundary between two
database engines.

**The question wasn't "how do I get data into Snowflake," it was "how much shouldn't go."**
Measured when the decision was made (2026-08-27): Silver had 3,796,213 rows, of which
**3,094,992 (81.5%)** were `silver_ine_population_series` — **national** population, of which
only **57,072 (1.8%)** fell within the scope of the 4 provinces. Another 517 thousand are
Callejero address resolution, which serves the customer generator, not the analyst.
**146,240 rows, 3.85%,** crossed over, and the other 95% stayed in S3. Loading the whole
Silver would mean paying for storage on 27× the useful data.

**The ratio is NOT a property of the pipeline, and saying it "hovers around 5%" was a reading
error that Phase 3 undid.** It is a function of **how much of each source falls within
scope** — and as the sources grow at different rates, the ratio moves on its own, with nobody
touching the cut:

| measured on | Silver | crosses over | ratio | what changed |
|---|---:|---:|---:|---|
| 2026-08-27 (Phase 2) | 3,796,213 | 146,240 | **3.85%** | only catalog, population, and customers |
| 2026-08-29 (Phase 3) | 4,087,507 | 406,855 | **9.95%** | Orders came in, and is born inside the AUFs |
| 2026-09-01 (Phase 6) | 7,098,881 | 3,327,809 | **46.9%** | customers ×14 and orders ×14, both 100% in scope |

What stays fixed is the denominator that does **not** cross over: `silver_ine_population_series`
has 3,094,992 national rows of which 1.8% are in scope, and the Callejero has 517 thousand
of address resolution that serve the generator, not the analyst. Without Orders the cut is
19.2%.

**Reading 46.9% as "the cut loosened" is the same mistake as reading 5% as a property.** No
rule changed since Phase 2: geographic scope, latest ingestion, grain dedup. What changed was
the proportion between a national source that barely enters and two synthetic ones that enter
whole — and that is exactly why the number comes out of `make warehouse-evidence` and not out
of this paragraph.

The number at any given moment comes out of `make warehouse-evidence`, not this paragraph.
What does **not** change over time is the structure of the decision: the cut is geographic
scope + latest ingestion + grain dedup, and no business rule.

**The cut is deliberately dumb:** geographic scope, latest ingestion, grain dedup. No business
rule — if a domain `case when` shows up in `snowflake_export.py`, it's in the wrong place.
That's what keeps the same logic from existing in two engines and diverging.

**The one declared exception** is excluding aggregates that the source mixes with the detail
(`sex_label = 'Total'`). It's not a business rule, it's avoiding double counting: summing the
three labels gives double the population. It's the same trap that inflated the age pyramid
2.03× via `age_label` in Phase 1, and it never fails — it produces a plausible number.

**The DDL comes from the query itself.** A hand-written DDL is a second place where the
schema lives, and the two diverge on the first day someone adds a column to the cut. Since
STAGE is a 1:1 mirror, its schema *is* the query's result.

### The Iceberg trigger didn't fire in Phase 2 — and it fired in Phase 3

Logged here as it was, because the sequence matters: during Phase 2 the pre-written trigger
was *"when Gold `dim_product` needs SCD2 via `MERGE` into an existing table,"* and **it did
not fire** — SCD2 is derived from the full history, not accumulated by MERGE, because RAW
keeps every snapshot and the dimension is always reconstructible. Snowflake absorbed the rest
(native MERGE, RBAC, BI) with no new broker or catalog.

The trigger that was left — *"a second engine needing to **write** the same table"* —
fired in Milestone 6 of Phase 3, and by **concurrency, not volume**. See the section on the
concurrent projection, further below.

### Four defects that only showed up at runtime

None of them produce invalid SQL. All of them produce **valid SQL pointing at the wrong
place**, which is the category no offline test catches.

- **`@%TABLE` was following the wrong schema.** A table stage resolves against the *current
  session* schema, and Snowflake's `create schema` **switches** the current schema. Since
  `ensure_schemas` creates STAGE, GOLD, and MART in that order, the session ended up on MART
  and `PUT` was looking for `RETAIL.MART.%STG_PRODUCT_PRICE`. Fixed by always qualifying it;
  it became a test in `test_snowflake_load.py`.
- **`GOLD_GOLD` and `GOLD_MART`.** dbt **concatenates** the profile's schema with the model's
  by default — behavior meant for several developers on a shared database. Here GOLD/MART/STAGE
  are the *layers*, with a fixed name and a grant target. The `generate_schema_name` macro
  switched to using the absolute name; the right isolation, when it's needed, is by
  **database**, not by schema prefix.
- **A made-up `source_name`.** The test linking price to an ingestion run was filtering by
  `'mercadona_catalog'`; the real value, measured in the manifest, is
  `'mercadona_catalog_api'`. Getting this wrong doesn't fail one partition: it makes the whole
  join not match, and all **14** fail at once, as if the model were broken.
- **`FILTER (WHERE …)` and `WINDOW … AS (…)` don't exist in Snowflake.** DuckDB accepts both,
  so the SQL passed local `dbt parse` and only broke on the real engine. Swapped for
  `count_if()` and for repeated windows.

### Governance verified, not asserted

Three roles, one per pipeline **verb**: `RETAIL_LOADER` writes STAGE and doesn't read GOLD;
`RETAIL_TRANSFORMER` reads STAGE and writes GOLD/MART; `RETAIL_READER` only reads MART.

**Verification mattered more than the grants**, for two measured reasons:

1. **`DEFAULT_SECONDARY_ROLES = ('ALL')`** is the default for modern Snowflake accounts: the
   session activates *every* role the user has besides the primary one. Since this user also
   has ACCOUNTADMIN, `RETAIL_READER` could read GOLD and STAGE with no problem — while `show
   grants to role RETAIL_READER` kept showing only MART. **An RBAC check run from an admin's
   session without turning this off always passes by mistake.** The check runs with `use
   secondary roles none`.
2. **`grant all on schema` doesn't reach the tables that already exist** inside it — they
   belong to whoever created them. `RETAIL_TRANSFORMER` could create tables in GOLD and
   couldn't read the ones already there. This only showed up because the verification
   existed; it's what failed.

`snowflake-bootstrap` applies the grants **and proves the isolation matrix** before returning
success.

### Measured result

STAGE re-verified count by count (146,240 rows at the phase's delivery); **102 dbt tests** on
the `snowflake` target, 0 errors; **215** on the `dev` target, unchanged. The closing crosses
the three paths: sum of `MART_MARKET_COVERAGE.customers` = current `DIM_CUSTOMER` = STAGE
base = **20,000**.

> **Phase 6 resized the base by population, and that number is 286,826 today.** The
> `102`/`215`/`146,240` above are also from this phase, not the current one — see "Real
> scale," at the top, for the current state. What this section establishes isn't the value:
> it's that **the three sums keep matching each other**, and that keeps holding at any size.

`DIM_PRODUCT` has 4,962 versions for 4,959 products (3 with more than one version, 7 flagged
as ambiguous identity). And the 08-17 to 08-23 gap shows up as seven days with zero in
`DIM_DATE` — which is exactly what the full calendar and `FACT_INGESTION_RUN` exist to make
visible.

### Out of this phase, with the trigger written

Orders, Order Items, Stock, Replenishment, Delivery, Events, Kafka, Spark, Iceberg,
`BRIDGE_PRODUCT_CATEGORY`, `DIM_CENSUS_SECTION`, `DIM_ADDRESS`.

**Orders, Order Items, and Events entered in Phase 3** — as a Source, RAW, and Silver in
Milestone 3, and the `models/warehouse/` tree in Milestone 7. **Kafka and Iceberg also came
off this list**, in Milestones 5 and 6, each with the trigger that fired written down. What
remains is Stock, Replenishment, Delivery, Spark, and the three dimensions — see the Phase 3
section.

**Real blocker for `FACT_DELIVERY`:** route and delivery time require a coordinate, and the
Callejero has no coordinate or adjacency — already logged in this document. Without
geocoding, "route" would be invented, exactly what Phase 1's golden rule forbids. Honest
proxy: distance between postal-code/municipality centroids, labeled as a proxy. Trigger for
the real thing: a fifth source with coordinates (CartoCiudad from the IGN, or OSM).

## Phase 3: Orders as events (fifth source)

Added on 2026-08-28. This is the first time this repository models **flow** instead of a
snapshot, and the first source whose partition **holds no state**.

**The golden rule, moved down one level.** Phase 1 said "the customer is invented; where they
live is not." Here: **the order is invented; who buys, what's bought, how much it costs, and
where they live is not.** The customer comes from `silver_customer`, the product and price
come from `silver_product_price` for the same warehouse on the same date. Nothing here
fabricates a product, price, customer, or postal code — and `orders-validate` re-checks all
three, order by order, against the same reference that generated the partition.

### What would keep this from being theater

This document had already refused the easy version of an event model: *"publishing the batch
output itself to a topic and consuming it back would add a broker to maintain and zero
information."* Three decisions exist just to avoid that trap, and all three are verified, not
asserted.

**1. The fold is non-trivial.** Substitution and removal of a line change the basket **after**
placement, so `net_amount` isn't derivable from `gross_amount_placed` — the order's value only
exists after folding the log. Measured over the window from 2026-08-24 to 08-27, across
120,693 lines:

| Mechanism | Lines | Effect on value |
|---|---|---|
| fulfilled with no change | 108,194 | 0.00 |
| substituted | 4,670 | **+10,318.99** |
| removed | 2,321 | **−15,153.72** |

An **inverted** dbt test (`assert_order_fold_is_not_trivial`) fails when *no* basket changes —
a sibling of `assert_geography_postal_code_is_not_a_key`, and for the same reason: it guards
the design's rationale, not the data.

**2. There is a single truth.** The RAW partition contains `order_events.jsonl` and nothing
else. There is no `orders.json` with the folded state sitting alongside it, on purpose: two
representations of the same truth diverge. The state lives in `silver_order` and is always
reconstructible — the same principle that makes `DIM_PRODUCT` a **derived** SCD2 from history
instead of one accumulated by `MERGE`.

**3. Kafka will be justified by the consumer, not the producer.** The written trigger requires
both halves — a simulator that emits continuously **and** a consumer that needs latency below
the batch. This phase delivers the first; **the broker doesn't come in until the second
exists**, and that's why the Kafka row in the trigger table stays as it is.

### Two partitioning decisions that need to be written down

**`ingestion_date` is the date of the ORDER, not the event.** Every event of an order lives in
the partition of the day it was placed, even when it crosses midnight. Measured: **4,567 of
44,456 events (10.3%) occur after midnight of the order's day.** The alternative —
partitioning by event date — would leave a given day's partition impossible to close: it
would only be complete two days later, when that day's last order finished, and `_SUCCESS`
would lose its meaning. Silver exposes **both** `ingestion_date` and `event_date`, because both
questions are legitimate and different.

**The file is NDJSON, and it's the only departure from canonical form among the five
Sources.** The other four write a JSON array with `indent=2`. A log is read line by line and
grows by append; each line is a complete, independent event — the same byte on disk and,
later, in the topic — and DuckDB's `read_json(format='newline_delimited')` consumes it
directly. The guarantee that matters is preserved: same content ⇒ same bytes ⇒ same SHA-256,
because every line is canonical and the line order is deterministic.

### Reproducibility on a new axis

Phase 1 proved that **growing the customer base is additive**. Here what's additive is the
**time axis**: each `(warehouse, day)` derives its own seed from
`sha256("<seed>|<wh>|<day>")`, and no day depends on another day's draw. Consequence verified
end to end:

> Adding a day to the window leaves the already-generated partitions **byte for byte
> identical**.

`sha256` and not `hash()`, because CPython's `str` hash is randomized per process — deriving
the sub-seed from it would make the whole partition depend on `PYTHONHASHSEED`. Verified in a
subprocess with three values.

**The three conditions that are NOT additive**, stated explicitly: another `seed`, another
reference, and another **assumptions table**. All three swap the orders behind the same
`order_id`s, and all three are logged in `config` and in `history`. The third is new in this
phase, and that's why the assumptions seed's `sha256` travels all the way to the manifest.

### Assumptions: synthetic, declared, and with no default

No source ingested by this platform measures sales, basket, purchase cadence, or product
availability. Every assumption lives in `platform/dbt/seeds/order_premises_seed.csv` and is
labeled `synthetic` — a different label **fails the export**, because calling any of these
`observed` or `proxy` would promise data nobody measured.

**There is no default for any assumption.** A missing key fails the generation, instead of the
generator picking a number. A default hidden in code would, by definition, be an undeclared
assumption.

`daily_order_rate` is the only one with **no** observational anchor at all: that's why it lives
in a seed anyone can edit and rebuild, and not in a constant.

### Vocabulary choices, again: preserve instead of promise

`order_line_removed` carries `reason = "unavailable"`, and **not** `"out_of_stock"`. No stock
fact exists in any source of this platform; naming it as if it did would promise data nobody
measured. It's the same discipline that made Phase 1 preserve the INE's `numbering_type`
instead of inventing an `address_precision`.

For the same reason, delivery is modeled as a **window** (`delivery_slot`, a commercial
promise on a fixed grid) and never as a route: the Callejero has no coordinate or adjacency,
and `FACT_DELIVERY` remains blocked exactly where it was.

### What adversarial review caught before and during implementation

- **Product choice had to be uniform.** The initial design would weight products by category.
  No source in this repo measures sales, turnover, or basket composition — weighting would
  invent a distribution nobody measured, the same prohibition Phase 1 applied to the tramo.
  Declared consequence: **the category mix mirrors the assortment size**, and that is a
  consequence of an assumption, not a claim about the market.
- **The `payload` can't be inferred.** Letting DuckDB infer it produces a `STRUCT` with the
  union of fields across the 12 types — measured, 32 fields — and that union depends on what the
  sample saw. A day with no returns at all would have no `returned_amount` in the struct, and a
  downstream model referencing it **would stop compiling by sheer luck of the draw**. The schema
  is declared in `columns =` and the payload travels through as JSON.
- **The schema fingerprint had the same problem, in reverse.** In the other four Sources it's
  the union of *observed* keys; here that would make two correct partitions diverge whenever a
  day had no returns. It switched to covering the **entire declared vocabulary**, and changes
  when the code changes — which is exactly what it exists to detect.
- **`totals_of` couldn't raise an exception.** The first version folded the log to count, and a
  tampered log made `validate` exit with **code 3 (unhandled exception)** instead of **1
  (validation failed)** — erasing the difference between bad data and a validator bug.
  `fold(strict=False)` returns an `INVALID` sentinel and the totals mismatch gives it away.
- **Two of the eight failure proofs were false.** While proving that each new dbt test is
  capable of failing, two injections didn't catch anything — and the defect was in the
  **injections**, not the tests: one used a value that could coincide with the real data, and
  the other wrote `select *, 0 as substituted_lines`, where the `*` already carried the column
  and the alias collided. Worth logging because it's the failure mode of a check on a check.

### A source characteristic that changes how value is measured **[source]**

Mercadona's `unit_price` spans **four orders of magnitude**: median 2.25, p90 6.15,
maximum 3,663.00. The extremes are real — frozen seafood and Iberian ham sold by
weight (`Alistado mediano congelado` 3,663.00; `Jamón de bellota ibérico 100%` 532.00).

Measured consequence: **9 of the 4,670 substitutions fell above 100.00 and account for 42%
of the substituted value.** Excluding those nine, the substitute's average (3.288) and the
original's (3.258) are practically equal — in other words, **there is no bias in the
substitution rule**, there's a tail. Any mart using the arithmetic mean of basket value will
be dominated by a handful of lines; the guidance is median or percentile, and it's written in
the model's `schema.yml`.

### Measured result

Window from 2026-08-24 to 2026-08-27, 4 warehouses, 16 partitions:

| Metric | Value |
|---|---|
| Orders | 6,400 |
| Events | 44,456, across 12 types |
| Order lines | 120,693 |
| Value placed / picked | 879,449.74 / 821,121.93 |
| Partitions reconciled manifest ↔ Silver | 16 of 16, zero divergence |
| Coherence (real customer, product, and price) | 16 of 16 partitions, `--strict` |
| Source suites | 631 (145 + 136 + 95 + 125 + **130**) |
| Platform tests | 178 at Milestone 3, 210 at Milestone 4, 239 at Milestone 5, 262 at Milestone 6, 273 at Milestone 7, **288** at Milestone 8 |
| dbt nodes on the `dev` target | 303 at Milestone 3, 312 at Milestone 6, **319** at Milestone 7 (was 215) |
| dbt nodes on the `snowflake` target | 102 before Phase 3, **170** at Milestone 7 |

### Out of this phase, with the trigger written

Stock, Replenishment, Delivery, Spark, `BRIDGE_PRODUCT_CATEGORY`,
`DIM_CENSUS_SECTION`, `DIM_ADDRESS`, and Debezium/Kafka Connect.

**The OLTP with outbox, Kafka, and Iceberg came off this list** — they entered in
Milestones 4, 5, and 6, after the gate opened. **The Orders `models/warehouse/` tree came in
at Milestone 7.** **Stock, Replenishment, and Spark came in during Phase 7**, and it's worth
logging *how*: Spark's volume trigger did **not** fire; it came in for interop and for shape,
with the unfavorable measurement published. What remains is **Delivery** (no coordinate in
the Callejero — trigger: CartoCiudad/IGN or OSM) and the three dimension structures, with
their triggers intact.

**The gate is deliberate.** Kafka and Iceberg exist to serve the fold, and the fold has only
just gotten on its feet. Turning them on before the batch path was proven would make
`orders-reconcile` compare two wrong things and **pass** — the same failure mode that
`DEFAULT_SECONDARY_ROLES` produced in Phase 2, when an RBAC check passed by mistake.

## Phase 3, second half: the OLTP and the transactional outbox

Date: 2026-08-28. **Milestone 4 of the plan.** The rule that came to govern the following
milestones: *each step proves the property that justifies the next step's technology.*
Milestone 3 proved non-trivial event sourcing; this one proves **atomicity + outbox**.

### The literal trigger, and what's still missing from it

The trigger written for Kafka is *"a genuinely event-driven source: POS, webhook,
**CDC off an OLTP**"*, plus *"emits continuously **and** a consumer below the batch."*
What this milestone delivers is the **first half of the first half**: the event now gets
born inside the transaction that changes the order.

This isn't an implementation detail — it's the difference between an outbox and a
*dual-write*. Republishing the state after writing it means having two writes that can
disagree; writing both in the same transaction means having one. **The Kafka row in the
trigger table remains not adopted**, and stays that way until Milestone 5 runs: declaring
adoption before the thing exists is the same class of defect this plan closed elsewhere.

### What exactly is being proven, and where each proof lives

| Property | Where it's proven | Why it can't be proven elsewhere |
|---|---|---|
| The transaction boundary: outbox and state between the same start and the same `commit`, with no commit in between | `platform/tests/fake_pg.py`, offline, in `make test` | It's the defect actually made in practice — one extra `commit()` — and it's a matter of **shape**, visible in the call log |
| That `rollback` **undoes** | `make orders-prove-atomicity`, against real Postgres | Atomicity is a property of the engine. A double that undoes only demonstrates that the double undoes |
| That the outbox didn't lose, duplicate, or alter anything | `orders-outbox --verify`, across the 16 partitions | Counting rows doesn't prove it; reproducing the **manifest's sha256** does |
| Replay idempotence | both | `event_id unique` is the database's job; skipping instead of refusing is the code's job |
| Refusal of an out-of-order event | both | It's the guard that gives meaning to `key = order_id` in Milestone 5 |

The real proof's injection **doesn't touch the platform's code — it touches the
database**: a trigger that raises an exception on `insert`. It's a failure the applier can
neither predict nor handle, which is exactly the kind of failure the transaction exists
against.

And it's done **in both directions**, which is the half people forget:

1. the insert into `outbox` blows up → no order, no line, no event survives;
2. the insert into `orders` blows up → **no outbox row survives**.

The second matters as much as the first: an outbox that survived a state that didn't
change **would publish an event that never happened**. Proving only one side would prove
half the pattern. Verified by injecting dual-write into the applier: proof (1) keeps passing
and proof (2) fails with `outbox=1` — and the next partition fails in cascade, because the
outbox thinks it applied something the state doesn't have.

### Three independent guards that need to agree

| # | Guard | Refuses |
|---|---|---|
| 1 | `outbox.event_id` UNIQUE | reapplying the same event — and replay **skips**, doesn't fail |
| 2 | `orders.last_sequence_no` | an out-of-order event |
| 3 | `orders.status` in `from_states` | an invalid transition |

Guard 2 is the one that **turns the Kafka partition key from preference into
requirement**. If the OLTP accepted an out-of-order event, preserving order by `order_id` in
the broker would be decoration. It's because it refuses that `key = order_id` comes to mean
something.

The state machine is **re-declared** here, from the Source's `CONTRACT.md` §5 — never
imported. Same principle as `verify.py` rereading the object instead of trusting what it
just wrote. If the two declarations diverge, `orders-apply` fails on the first affected
transition.

### The outbox reconstitutes the log byte for byte

`outbox.event_json` stores **the log's canonical line, verbatim** — not a
re-serialization. The envelope's columns exist to **route** (key, order, filter), and four
`CHECK` constraints tie each one to the JSON itself: a row can't be routed under a key that
disagrees with the payload it carries. It's the engine that guarantees this, not convention.

Consequence: reordering the outbox rows by the Source's canonical order and recomposing the
file, the `sha256` matches the partition's manifest. **16 of 16.** It's the strongest proof
this milestone has to give, and it cost nothing.

`payload` is deliberately **not** a column: `event_json::jsonb -> 'payload'` answers any
query without storing a second copy. Storing `jsonb` instead of text would have been worse
in a non-obvious way — `jsonb` reorders keys by its own criteria, and `event_id` and
`sha256` would stop being verifiable end to end.

### One transaction per event, not per order or per partition

One transaction per partition would prove *"the whole day is atomic,"* which no store
guarantees. The cut has to be the same as a real OLTP's: **one state change is one closed
piece of business.** Measured cost: 44,456 transactions in 85 s, ~520 events/s. It's slow
compared to a `COPY`, and it's the price for the property to mean what it promises.

### What the OLTP found in Silver — two independent folds disagreeing

Here is the concrete payoff of the project's rule. Replicating the same log through a
completely different path — incremental and transactional, instead of a window function
over the whole log — and comparing the two states found **two defects in Milestone 3**
that no existing test caught, because both were internally consistent.

**1. `net_amount` was answering two questions under one name.** 298 of the 6,400 orders (196
canceled, 102 with payment refused) die before picking. Silver leaves `net_amount` null —
*"picking never happened."* The OLTP, in its first version, initialized it with the placed
value — *"how much the order is still worth."* Two legitimate questions, one name.
`orders-reconcile`, in Milestone 6, would have compared the two thinking it was comparing
two answers. Fixed in the OLTP: the column is born **undetermined** and only gets filled
when an event determines it.

**2. Silver was asserting a picking that the log never declared.** `line_status` was
`case ... else 'fulfilled'`: every line that wasn't substituted or removed became
*fulfilled* — **including the 5,508 lines of the 298 orders that never reached picking**.
`order_picked` is the only event that declares picking, and it doesn't occur for those
orders. The `else` was an assertion **by the model**, not the source — the most direct
possible violation of this repository's golden rule, and it went through three reviews
because the number balanced with everything else.

The vocabulary got five values, **identical on both sides**:
`placed | fulfilled | substituted | removed | not_picked`. `placed` covers an order still
in flight — it doesn't occur in this window, and it exists so the vocabulary is complete
instead of complete by luck. Identical vocabulary on both sides isn't aesthetics: a
translation table between two folds is exactly where *"compares two wrong things and
passes"* lives.

After the fix, the two folds agree on **everything**:

| Comparison | Rows | Attributes | Divergences |
|---|---|---|---|
| `orders` (OLTP) × `silver_order` | 6,400 | status, net, gross, lines, picked, customer, wh | **0** |
| `order_line` (OLTP) × `silver_order_line` | 120,693 | status, value, fulfilled product, qty, price, product | **0** |

Neither defect would have shown up by adding one more test to Silver: both were
internally coherent, and the test that would catch them would have to know the right
answer. What found them was **a second, independent implementation of the same fold**.
It's the argument for the lambda pair Milestone 6 is going to build, made before Milestone
6 existed.

### Infrastructure decisions

**Postgres kept separate from Airflow's metadata.** Not for organization: the OLTP is a
*modeled* component of the simulation, needs its own `wal_level=logical` (which requires a
restart and affects the whole server), and *"restarting the OLTP"* can't mean *"restarting
Airflow's brain."*

**`wal_level=logical` turned on now, with no use yet.** The outbox is drained by polling —
it reads the table, not the WAL. The setting is turned on because it's the only one that
requires a server restart: leaving it on from the start allows plugging in Debezium
(Milestone 4b) without bringing the database down and without losing whatever is in the
outbox. Cost: a few bytes per write.

**`profiles: ["stream"]`.** `make up` still only brings up MinIO — the README's promise. The
separation lives in the compose file, not in the operator's memory.

**`orders-oltp-init --reset` exists, and `prune-local --force` doesn't.** The asymmetry is
deliberate: `prune-local` targets a landed partition, which may be the only copy; the OLTP is
a **replica** of the log, which `orders-apply` rebuilds in 85 s. Nothing in it is the only
copy of anything.

### What was left out of this milestone

The replay DAG. `orders-apply` is already a limited, idempotent verb, well suited for an
Airflow task, but the useful graph (`apply >> wait_for_drain >> reconcile`) needs Milestones
5 and 6 to exist. Writing a one-task DAG now would be a facade.

## Phase 3, third half: transport, replay, and idempotent consumption

Date: 2026-08-28. **Milestone 5.** The framing matters more than the software: this is
*transport + replay + delivery semantics + idempotent consumption*, not "bring up a broker
and publish messages." Counting messages proves that something traveled; it doesn't prove
it traveled intact, nor what happens when something dies mid-flight, nor that reprocessing
is safe. The four properties are independent and each needed its own proof.

### Delivery semantics: the choice, and the side you err on

The pipeline is **at-least-once from the outbox to the broker**, and not exactly-once.
The reason is structural and has no cheap fix: marking `published_at` in Postgres and
receiving Kafka's ack are two writes in two systems, and there's no transaction spanning
them. The choice is which side to err on:

| Order | Dying midway produces | |
|---|---|---|
| publish → ack → mark | **duplicate** | chosen |
| mark → publish | **loss** | refused |

Losing is irreversible; duplicating is absorbable downstream. That's why the consumer has
to be idempotent **by obligation, not by elegance**.

**`enable.idempotence=true` doesn't fix this, and conflating the two things is the most
common mistake here.** It eliminates duplicates generated by *retries within the producer's
session*. A duplicate generated by the process dying between the ack and the outbox commit
is outside its reach — it's a matter of design, not transport.

And this isn't a documentation caveat: `make orders-prove-stream` **reproduces the
window**. It returns 500 outbox rows to the queue (exactly what a crash after the ack
produces), republishes, and the topic ends up with more messages than the log has events.
Measured: 44,456 → 44,956.

### Deduplication without a growing set

The consumer doesn't keep a set of `event_id`. It compares `sequence_no` against the
`last_sequence_no` already in the read model:

| Comparison | Verdict |
|---|---|
| `seq <= last` | duplicate — discards silently |
| `seq == last + 1` | applies |
| `seq > last + 1` | **gap — refuses loudly** |

Two consequences that matter more than the memory savings:

**It's bounded by construction.** One integer per order. A set of `event_id` grows without
bound and forces inventing an expiration policy — and every expiration policy is a window
where the duplicate gets through again.

**It makes `key = order_id` a load-bearing piece.** The comparison is only valid because
ordering by key is guaranteed: a duplicate always arrives *after* the original. If it
didn't, it would be classified as a gap. The key stops being a configuration detail and
becomes a function's assumption — and the proof against the broker checks both things:
every order on a single partition, and every repeat at an offset greater than its original.

**A gap is loss, and loss has to hurt.** `seq > last + 1` means an event didn't arrive.
Advancing the offset past it would make the loss permanent and invisible. The consumer
stops.

### The offset is committed after the write

`enable.auto.commit` is **false**, and that's the second choice that decides everything:
automatic commit runs on a *timer*, not on the write, and delivers at-most-once without
anyone having chosen it. The order is write the projection → commit the projection → commit
the offset. Dying midway redelivers the batch, and deduplication discards it:
**at-least-once on delivery, exactly-once effect on the projection**.

No counting test can see this ordering. That's why it's asserted against doubles that
write to a **shared journal** — the interleaving between the two sides is the property, and
it doesn't exist in two separate journals.

### Where each proof lives, again

| Property | Offline (`make test`) | Against the broker (`make orders-prove-stream`) |
|---|---|---|
| Duplicate/gap verdict | `decide` is pure, tested with no dependencies | — |
| Order: write → offset commit | doubles' shared journal | — |
| Key = `order_id`, nothing marked before the ack | producer double | — |
| Order by key, repeat after the original | — | only the broker can guarantee it |
| Byte-for-byte faithful transport | — | 16 sha256 |
| Real at-least-once | — | the window is reproduced |
| Replay with no effect | doubles | and against the whole topic |

Six semantic inversions were injected into the code and each one failed the right test:
offset before the write, asynchronous commit, marking before the flush, marking only what
passed, a constant key, a gap treated as a no-op.

### Replay: rewind without erasing

`orders-replay` rewinds the group to the beginning and **does not erase the
projection**. This is deliberate: reprocessing the whole topic *on top of* the existing
state is what proves idempotent consumption. Erasing first would prove that the fold is
deterministic — a different thing, already proven since Milestone 2.

Measured: 44,956 messages reprocessed, **0 applied**, identical projection digest. And the
whole plan rebuilt from empty volumes produces the **same digest**
(`d769f727f805736a…`): the projection is reconstructible from scratch, not just stable.

### Three independent folds, now

`silver_order` (window function over the log), `orders` in the OLTP (incremental,
transactional), and `live_order_state` (incremental, in memory, fed by the broker). Three
paths, one number: **6,400 orders, zero divergences** in state, sequence, counts, and
values.

Milestone 4 already showed what this buys — two disagreeing folds found two defects that no
test caught. The third fold found no new defect, and that's information too.

### Two findings worth more written down than fixed

> **Superseded in Phase 7.** The two findings below stopped being merely logged: the
> assumptions were reconciled. The text stays as it was because the reason they survived
> three phases is what matters — the missing piece was the distinction between *adjusting an
> assumption until the output pleases*, which is refused, and *making two assumptions
> mutually coherent*, which is a model correction. See Phase 7 and
> `assert_order_premises_are_internally_coherent`.

**1. The `sla_minutes_picking = 90` assumption is unreachable by construction.** The
consumer computes the picking time and flags `sla_breached` — the mechanism works and is
tested. But `basket_lines_max × minutes_per_line_picked = 40 × 2 = 80 min`, and the longest
picking time observed across 6,400 orders was exactly **80.00 min**. The threshold can't
fire.

It was left as is. Adjusting a declared assumption until the check lights up is the
opposite of verifying — and "the process is comfortably within SLA" is a legitimate state
of the world, not a defect. It's logged that the alert **is not exercised by this data**,
and therefore doesn't count as proof of anything.

**2. The perfectly uniform distribution across partitions is an artifact of the key.**
Measured: 1,600 orders in each of the 4 partitions, and exactly 100 in each within *every*
one of the 16 (warehouse, day) groups. This is not the partitioner's merit: the variable
part of `order_id` is a dense sequential counter with leading zeros, and murmur2's low bits
track the last digits linearly — the result is a complete residue system. With a sparse
`order_id` or a UUID, the balance would become merely statistical. **It's not a guarantee
and shouldn't become an assumption.**

### A defect in the proof, not in the system

The first version of the order check required that the list of `sequence_no` for each
order, ordered by offset, be increasing. It passed on the first run and **failed 121 orders
on the second** — because the second run had the first run's duplicates in the topic, and a
republished duplicate was produced *later*, so appearing later is the correct behavior. The
raw sequence reads `1, 2, …, 1`.

The proof was failing the broker for doing exactly the right thing. Kafka's guarantee is
about **production order**, not about the list of offsets. Fixed for the two properties
that deduplication actually uses: the *first* appearance of each `sequence_no` comes in
order, and every repeat comes after its original.

Worth logging because it's the third time in this project that the check was wrong and the
system right — and all three only showed up because the check was run more than once,
against state that was no longer clean.

### What Kafka bought, and what remains hypothetical

**It bought, and it's measurable**: a decoupling point where a second consumer enters
without touching the producer; replay from an arbitrary offset; and a place where
at-least-once plus idempotence are *exercised* instead of assumed.

**Remains hypothetical**: that anyone needs the latency. The written trigger called for "a
consumer whose usefulness EXPIRES if it arrives in the next day's batch." There is now a
consumer that keeps live state below the batch — the mechanism. Whether any decision
changes because the number arrives in seconds instead of the next day is a product
question, and **replay of historical data can't answer it**. It's written this way on
purpose.

**And the broker is not the origin.** The canonical log is still written to disk before
entering the OLTP and the topic. This is deliberate — trading byte-for-byte reproducibility
for the broker as the source of truth would be a bad deal, and deterministic replay is
exactly what allows running the same day a hundred times and comparing. But it means this
pipeline demonstrates **transport semantics**, not a genuinely event-driven origin. Anyone
reading this looking for the latter won't find it.


## Phase 3, fourth half: the concurrent projection in Iceberg

Date: 2026-08-28. **Milestone 6.** The trigger was *"a second engine needing to
**write** the same table,"* and now `live_order_state` has two writers by design: the
streaming consumer (`orders-project --sink iceberg`) and the batch rebuild
(`orders-rebuild-projection`), with DuckDB reading the same table while both write.

**The trigger fired for CONCURRENCY, not volume.** At this volume an atomic
`os.replace`-rewritten parquet would work. What Iceberg buys here is snapshot isolation
between two writers and a reader, plus time travel on the projection. Writing this down is
the difference between a decision and a fad.

### The closed experiment came first

`make spike-iceberg` answered nine questions against the real stack **before a single
line of the projection existed**, because the plan flagged "DuckDB might not read
pyiceberg's SQL catalog" as the most fragile assumption. If it fell apart mid-build, the
rework would be expensive and the temptation worse: work around it with a path that almost
works and call it a projection.

Two answers changed the design:

**1. DuckDB reads by `metadata_location`, and only by it.** It refuses to discover the
current metadata by scanning storage — *"globbing the filesystem to locate the latest
version is disabled by default as this is considered unsafe and could result in reading
uncommitted data."* The shortcut exists (`SET unsafe_enable_version_guessing = true`), was
measured, works, and was **refused**: reading uncommitted metadata is exactly what a
concurrent read with two writers cannot do.

Whoever knows which metadata is current is the **catalog**. `make silver` asks it and
passes the answer as a var. Authority stays in a single place, with no unsafe flag and no
reimplementing catalog convention.

**2. The experiment disproved my assertion, not Iceberg.** The first version of Q5 required
that "both writes survive" from two references loaded at the same time, and it produced a
`CommitFailedException`. That's optimistic concurrency working: the second commit starts
from a snapshot that is no longer current and **has** to be refused. If it passed silently,
it would be a lost update — and then there would have been a real reason not to use
Iceberg. Rewritten for the correct property: refuse → reload → retry commits → both
present.

**A measured dependency trap:** `pyiceberg[s3fs]` drags in an `aiobotocore` that pins an
old botocore and breaks the `boto3` the platform uses for RAW. pip accepts the install and
the damage shows up in a different module. `PyArrowFileIO` does the same job.

### Retry isn't enough: the merge has to be monotonic

This is the part the experiment doesn't catch and that decides whether two writers
actually work.

Reloading and retrying resolves the **commit** conflict — and still loses data. If the
other writer has already written the order at `sequence_no` 7 and our attempt loads 5, a
blind retry writes 5 over it. **The commit passes. The table regresses. Nothing fails**,
because from Iceberg's point of view nothing is wrong: the branch was where expected.

That's why every attempt rereads the state of the affected keys and drops its own rows that
don't advance. It's the same `last_sequence_no` guard that protects the OLTP (Milestone 4)
and the consumer (Milestone 5), now protecting the concurrent write — the third time the
same invariant pays off.

Measured in the proof: a writer with `seq=5` against a table at `seq=7` produces
`rows_dropped_as_stale=1, rows_written=0` and **no upsert is emitted**. Not writing is the
correct outcome, not a failure.

### The lambda pair, with the cut it should have

`orders-rebuild-projection --through 2026-08-26` covers the settled history; streaming
covers the live tail. Measured:

| | Orders | Time |
|---|---:|---:|
| Batch (12 partitions, 33,349 events) | 4,800 | **3.2 s** |
| Streaming (the whole topic on top) | 1,600 | 60 s |

`written_by` in the table: `{rebuild: 4800, stream: 1600}` — both writers made their
mark, and the column is **queryable provenance**, not a claim about the log. Streaming
discarded 33,849 events as duplicates: they were the orders the batch had already brought
to final state, and the consumer's dedup recognized them by reading the state the **other**
writer had written. The two mechanisms compose.

### Four paths, one digest

The read model today has four independent productions, and all of them give the same
`d769f727f805736a…`:

| Path | How |
|---|---|
| `live_order_state` in Postgres | incremental fold, Postgres sink |
| `live_order_state` in Iceberg, streaming only | same fold, different storage |
| `live_order_state` in Iceberg, batch + streaming | two writers, monotonic merge |
| Rebuild from scratch, empty volumes | Milestone 5, cold start |

**Two different storage engines producing a byte-for-byte identical digest** is a
stronger result than any count.

### What the agreement between the two writers does NOT prove

`orders-rebuild-projection` and `orders-project` share `fold_event`. Their agreeing
shows they **don't step on each other** — not that they're correct. Confusing the two would
be the same defect this project has already logged twice: a check that compares something
with itself and passes.

The evidence of correctness comes from `orders-reconcile`, which compares against
`silver_order` — a SQL window function over the whole log, with not one line of code in
common with the other two. **Three independent folds, 6,400 orders, zero divergences.** And
the same invariant became a dbt test (`assert_live_projection_matches_batch_fold`), because
a pipeline where divergence only shows up when someone remembers to run a command doesn't
have verification — it has a habit.

### A real defect the proof found

`IcebergProjection._refresh()` was reloading `TABLE_NAME` — a **hardcoded** identifier.
While there was only one table, it worked. In the first test that used a probe table, a
conflict made the probe's writer reload the **production** table and write into it: four
synthetic rows entered `live_order_state`, and it was reconciliation, three steps further
down, that caught it.

A hardcoded identifier in a refresh function is always this: it works until a second object
exists, and then it writes to the wrong place with no error at all.

Two consequences became permanent:

- the proof gained a **sentinel** — it counts the production table's rows before and after
  the parts that use the probe. A proof that uses a probe has to watch the target it
  *shouldn't* touch;
- the injection check now requires reconciliation to fail **because of the tampered row**,
  and to skip the injection if the base is already dirty. In the run with the defect, it
  "passed" because `not ok` was already true — a classic false positive.

### The measured cost of copy-on-write

The Iceberg sink processed the whole topic in **4 min 48 s** against **14 s** for the
Postgres sink — ~20×. The cause isn't the format itself: pyiceberg's `upsert` is
copy-on-write, so every batch rewrites the data files, and the monotonic guard reads the
table before every attempt. With 90 batches of 500 messages, that's ~90 reads and ~90
rewrites of 6,400 rows.

Numbers for scale: 268 snapshots in one pass, 80 in the pass with the lambda cut.

**It was not optimized, and here's why:** the two sinks answer different questions.
Postgres is the low-latency read model; Iceberg is the one that accepts two writers and
keeps history. Tuning the Iceberg batch to get close to Postgres would trade latency for
throughput with nobody having asked for it. **Trigger to revisit**: the projection leaving
the 10⁴-row order of magnitude, when the per-commit cost stops being negligible and
merge-on-read (positional delete) becomes worth what it costs in complexity.


## Phase 3, fifth half: orders in the warehouse

The phase's last leap is the most conventional one — three STAGE, three FACT, three
MART — and it's where its most expensive defect showed up. Worth telling it in that order.

### What came in

| Layer | Objects |
|---|---|
| STAGE | `STG_ORDER` (6,400) · `STG_ORDER_LINE` (120,693) · `STG_ORDER_EVENT` (44,456) · `STG_ORDER_PREMISE` (30) |
| GOLD | `FACT_ORDER` · `FACT_ORDER_ITEM` · `FACT_ORDER_EVENT` · `FACT_ORDER_PREMISE` |
| MART | `MART_ORDER_FUNNEL` · `MART_FULFILLMENT_SLA` · `MART_BASKET_DAILY` |

The destination went from 236,797 to **406,855 rows**; `DIM_CUSTOMER` doubled to 40,000
because the 2026-08-24 base, generated in Milestone 0, had never crossed the boundary.

`FACT_ORDER` is an **accumulating snapshot**, and the pattern only exists because there are
events: one row per order that fills in as it progresses, with eleven milestones and the
durations between them. A state snapshot would say *where* the order is; never *how long it
took to get there*.

### A timestamp 56 million years in the future, and 166 green nodes on top of it

The first STAGE load put **every** timestamp in the year **56,648,666**. DuckDB only
annotates the timestamp's unit in parquet's modern `LogicalType` and leaves the legacy
`ConvertedType` at `NONE`; Snowflake's reader ignores the former by default, falls back to
the latter, finds no unit at all, and assumes **milliseconds**. 1.787×10¹⁵ microseconds
become 1.787×10¹⁵ milliseconds.

What did **not** catch it:

- the loader's re-check — it compares row **counts**, and those were correct;
- dbt's 166 nodes, **all green** — the durations turned into big numbers, not nulls or
  errors, and no type changed;
- `assert_gold_grains_are_unique` — the grain stayed unique;
- `assert_order_funnel_totals_match_fact_order` — a funnel is built from
  `count_if(milestone is not null)`, and "not null" stays exact even when the instant is
  shifted;
- `assert_fact_order_amount_equals_sum_of_items` — money doesn't go through a timestamp.

What caught it was a human reading **80,000,060 minutes of picking time** in a mart.

It only showed up now because it was the **first time a `TIMESTAMP` crossed the
boundary**: through Milestone 6 the whole cut only had `DATE`, which travels as `date32`
with no unit ambiguity.

The fix is `use_logical_type = true` in `file_format`. What stayed permanent is the test:
`assert_order_milestones_are_plausible_against_the_order_date` anchors every milestone
against `order_date` — which arrived by **a different path**. Comparing milestones against
each other would have happily passed, because they were all shifted by the same factor and
the relative order stayed correct.

The test was proven against the **real** defect, not an injection: STAGE was still
corrupted when it first ran, and it failed on the 6,400 orders.

### The derived DDL found a count turning into `FLOAT`

Smaller, same family. `sum()` over `INTEGER` returns `HUGEINT` in DuckDB; parquet has no
`INT128`, so the write downgrades the column to `DOUBLE` — and `substituted_lines` and
`removed_lines`, which are event counts, would reach the warehouse declared as `FLOAT`. No
value was corrupted (none exceeds 40), but the type ends up asserting "this can have a
fractional part," which is false.

What caught it was the DDL being **derived from the cut itself**. A hand-written DDL would
have said `NUMBER(38,0)`, and the mismatch between what the file has and what the table
declares would only show up in `COPY INTO` — or never.

### SCD2 finally pays for itself, and two independent paths agree

Up to now `DIM_CUSTOMER` and `DIM_PRODUCT` were SCD2 **with no fact pointing to a
version**. `FACT_ORDER` resolves the customer version current on the order date via a
*range join*; `FACT_ORDER_ITEM` resolves two product versions — the ordered one and the
**fulfilled** one, which differ across the 4,670 substituted lines.

And there's a coincidence that became a check: the version resolved by the *range join* is
the same one the Source wrote to `customer_ingestion_date` inside the `order_placed` event,
across the **6,400** orders. These are two paths that never touch — the roster choice in
Python, at generation time, and an interval join in SQL, in the warehouse. As long as they
coincide, SCD2 is being resolved the way the phase promised; when they diverge, one of the
two sides changed its mind about what "current version" means, and that needs to be a
failure, not a dashboard discovery.

### A funnel built on milestones, and the 61 orders that prove why

`order_status` holds the state of the **last** event. A returned order has status
`RETURNED` — **and was delivered**. Measured: counting `order_status = 'DELIVERED'` gives
**5,985**; counting `delivered_at is not null` gives **6,046**. Those are the 61 returned
ones, and a funnel built on status would produce a delivery rate 1% lower than the real one
with nothing failing.

A milestone is monotonic; status isn't. A funnel is by definition a count of stages
**reached**, so the right column is the instant.

### Two findings logged instead of fixed

> **Superseded in Phase 7.** The two findings below stopped being merely logged: the
> assumptions were reconciled. The text stays as it was because the reason they survived
> three phases is what matters — the missing piece was the distinction between *adjusting an
> assumption until the output pleases*, which is refused, and *making two assumptions
> mutually coherent*, which is a model correction. See Phase 7 and
> `assert_order_premises_are_internally_coherent`.

**`sla_minutes_picking = 90` is unreachable by construction.** Picking takes
`minutes_per_line_picked` (2) × number of lines, and `basket_lines_max` is 40 — a ceiling of
80. p50 = 36, p90 = 62, **max = 80**. Zero violations, and not because operations are good:
because the three assumptions don't intersect. Lowering the threshold until the alert
fires would be adapting the assumption to the desired outcome. The mart carries
`sla_minutes`, `max_picking_minutes`, and `orders_breaching_sla` **side by side** — a reader
sees 90, sees 80, and sees 0, and understands the zero.

**The delivery window is almost never met, and the skew is toward EARLY.** Of the 6,046
deliveries, **5,166 arrive before the window opens**, 471 within it, 409 after. A median of
4.6 h to delivery against 12.8 h to the window's start: `slot_lead_hours` draws 2–24 h while
the sum of milestones delivers in ~4.6 h. Two assumptions declared separately and never
reconciled.

What changed wasn't the seed, it was **what gets published**: `orders_delivered_before_slot`
and `orders_delivered_after_slot` travel separately, because arriving early and arriving
late are **opposite** operational problems and "outside the window" doesn't say which one
is happening.

### The assumptions cross the boundary, not a copy of them

`sla_minutes_picking` has an owner: the seed the generator read, whose `sha256` is in
every RAW partition's manifest. Rewriting it as a dbt var would create the second copy that
diverges on the first edit — and **nothing would fail**, because counting zero violations
against the wrong threshold looks exactly like counting zero against the right one. Hence
`STG_ORDER_PREMISE` and `FACT_ORDER_PREMISE`: metadata promoted to a fact for the same
reason as `FACT_INGESTION_RUN`.

The `currency` var stays a var, and that difference is the point: Mercadona doesn't declare
currency in any field, so the assumption **has no other home**.

A trap measured along the way: `dbt seed` only recreates the table with `--full-refresh`.
Changing `column_types` on a seed that already exists is a silent no-op until someone
forces it.

### Every test was seen red

`make warehouse-prove-tests` injects, into the warehouse's **real** data, the specific
defect each of the five tests claims to catch; demands red; undoes it; and demands green
again. It finishes by running the whole suite and checking a count sentinel. It only
touches GOLD and MART, which are entirely reconstructible from STAGE.

After what happened with the timestamps, a green test that was never seen red is not
evidence of anything.

### `make stream-evidence`

It mirrors `make warehouse-evidence`, with a different trigger: there the reason is
**expiration** (the account is a trial); here it's that the streaming half **isn't covered
offline** — `make test` runs with no network, and the broker, OLTP, and Iceberg only exist
while `make stream-up` is up.

`docs/stream-evidence/README.md` logs the three plans and the three folds agreeing on 6,400
orders. No number written by hand. It's **tolerant of a plane deliberately turned off**:
every missing section appears as a declared absence, never as zero — *"the outbox has 0
events"* and *"the OLTP didn't respond"* fit in the same table cell and mean opposite
things.

## Verification dashboard: the third role finally worn

Streamlit on top of `MART`, 22 indicators in 7 groups. The declared purpose isn't to
*show data* — it's to **verify the indicators before rebuilding them in Power BI**, a tool
where the obviously wrong measure and the right one look exactly the same.

### The BI role stops being decorative

The three roles have existed since Phase 2. The load started wearing `RETAIL_LOADER` and
dbt `RETAIL_TRANSFORMER` when that debt got closed; **`RETAIL_READER` still had no
consumer**. This dashboard is the first one, and the consequence is concrete: it reads
`MART` and is *refused by the engine* on `GOLD` and `STAGE` — verified live, on screen
itself, with `use secondary roles none`.

This has a project effect worth more than convenience: when an indicator needs something
the role can't reach, that's **information**, not an obstacle. That's how the model's
biggest gap showed up (below).

### CONTRACT is generated, not written

`streamlit/indicators.py` carries, for each indicator, the question, the grain, the type
(observed/synthetic), and the SQL **in the same object**. `streamlit/CONTRACT.md` is derived
from it.

The reason is the usual one in this repository, and here it bites harder: CONTRACT exists
so someone can read the query alongside the explanation and decide whether the indicator is
right. If the two lived in separate files, they'd diverge on the first tweak — and **the
check would keep passing**, because nobody reads a SQL query and a piece of text side by
side looking for a disagreement. An offline test fails if the file on disk isn't what the
generator produces, and it was proven capable of failing.

### The three traps the dashboard exists to publish

| Trap | Measured |
|---|---|
| **Lost value has two causes** | `SUM(gross) − SUM(net)` = 58,327.81 mixes a basket that shrank during picking (4,834.73) with an order that died before it (53,493.08). The sum ties out exactly; a single number hides which is happening, and they're different areas — store operations vs. payment |
| **Average ticket has two denominators** | revenue/picked = 134.57; revenue/placed = 128.30. The second divides the revenue of the picked ones by the total including those who never got there |
| **`orders_touching_category` isn't additive** | summing a day's 151 categories gives far more than that day's 1,600 orders |

And the two things Phase 3 had already logged come back here as an on-screen warning,
because this is where someone would misread them: `orders_breaching_sla = 0` is only
legible next to the threshold (90) and the observed maximum (80); and the 8% window
adherence needs the three counts, because **5,166 of the 6,046 deliveries arrive before the
window opens** — arriving early and arriving late are opposite problems.

### The gap the exercise revealed

**No mart joins customer with order.** `MART_CUSTOMER_BASE` has customers with no
orders; `MART_ORDER_FUNNEL` and `MART_BASKET_DAILY` have aggregated orders with no
customer. The link exists in `FACT_ORDER.customer_sk`, in GOLD — out of `RETAIL_READER`'s
reach.

Consequence: **there is no repeat purchase, LTV, cohort, RFM, or revenue per customer**, and
those are exactly the indicators a strategic dashboard is usually asked to have. It requires
no new source — it requires a mart with customer grain and order measures. It's logged as
the most actionable gap on the list, with the trigger written down, instead of approximated
by some number that would *look* like it answers it.

### Where the dependencies live, and why

`streamlit`, `pandas`, `pyarrow`, and `altair` go in
`[project.optional-dependencies]` of `platform/pyproject.toml`, **outside**
`dependencies`. `infra/Dockerfile.airflow` installs exactly that list, and ~150 MB of UI has
no business in an image that doesn't render a dashboard. Same venv, though: the app needs
the Snowflake connector that's already there, and a second venv would duplicate the
connector just to avoid duplicating Streamlit.

### Why the smoke test isn't a `curl`

Streamlit returns **HTTP 200 with the page skeleton even when the script dies on the
first `select`** — rendering happens client-side. `make dashboard-check` runs the real
script via `AppTest` and requires zero exceptions; it's the only way to exercise all 25
queries. It's left out of `make test` because it requires a live account.

## The Silver gate lived in six files, and none of them agreed

Found on 2026-08-31 by a real run: `mercadona_catalog_daily` was failing **every day**
on its last task. The four warehouses extracted, validated, landed, and verified
successfully; `silver` fell over with *"no version-hint could be found."*

**The data was right the whole time. It was the gate that lived in the wrong file.**

### The decision, and its six copies

Not all 22 Silver models can always be built, and there are two legitimate reasons: a
source that hasn't landed anything yet makes `read_json` **fail** (not return zero rows),
and `silver_live_order_state` can only be read when the Iceberg catalog responds, because
the metadata path comes from it and never from a storage scan.

That decision existed in six places:

| Where | Gates it had |
|---|---|
| the Makefile's `silver` target | all five — four sources + Iceberg |
| `simulated_orders_events` | one — its own source |
| `ine_population_on_demand` | one — its own source |
| `ine_callejero_on_demand` | one — its own source |
| `simulated_oltp_customers` | one — its own source |
| **`mercadona_catalog_daily`** | **none** |

Mercadona always has data, so nobody missed its gate — until Milestone 6 created a
model that **has nothing to do with that DAG's source** and that it started trying to build
every day.

### Why nothing caught it

`make silver` passed — it has the full gate. `make test` passed — it doesn't look at the
DAG. The dbt suite never got to run. Only the real run failed, and a day later, which is
the maximum distance between cause and symptom in this repository.

### What was left in place

One verb: `retail_platform silver-build`, on top of `silver_gate.py`. `plan()` is
**pure** — it receives what was observed and returns `--exclude`/`--vars` — so the whole
decision is exercisable with no MinIO and no catalog. The Makefile and all five DAGs call
the same verb.

Plus a source check, because **a single gate is only worth it as long as it stays the only
one**: the suite requires that no Silver DAG assemble its own `dbt build`, and that every
source model be assigned to some source in `SOURCE_MODELS`. The second is what catches a
*new* model — whoever creates one and forgets to register it reproduces this exact defect.
Both were proven capable of failing before being accepted.

### The second finding, which the first was hiding

With the right gate in place, the Airflow container started **excluding** the
projection — and excluding it always. `orders_projection.catalog()` falls back to a default
`localhost:5433`, which inside the container is the container itself.

**This fails nothing**: the build stays green with one table missing, which is the worst
kind of success. Resolved by giving the service its own address
(`ICEBERG_CATALOG_URI: postgresql+psycopg://oltp:oltp@oltp-postgres:5432/iceberg_catalog`),
which is the difference between *"can't"* and *"didn't try."*

And, while verifying, a third one showed up: the **running Airflow image predated
Milestone 5** — no `psycopg`, `confluent-kafka`, or `pyiceberg`. `Dockerfile.airflow`
already had the import check that breaks the build when a dependency is missing; it was
right and nobody had run it. Rebuilt, Airflow builds all 319 nodes.

## Two orchestration defects that only showed up with two warehouses

Both invisible with a single warehouse, and both silent: the DAG finished `success`
while failing to transform newly landed data.

1. **`silver`'s default `trigger_rule`.** `all_success` makes the shared task get
   skipped when *any* upstream is skipped. With `mad1` short-circuiting because it was
   already landed, `silver` was skipped even with `bcn1` having just landed.
   Fixed to `NONE_FAILED_MIN_ONE_SUCCESS`.

2. **`ShortCircuitOperator`'s `ignore_downstream_trigger_rules`.** The default is `True`,
   and with it the short-circuit skips **all** downstream tasks **ignoring each task's
   `trigger_rule`** — including the one that had just been fixed. The symptom was exactly
   the same, which makes the second defect easy to confuse with the first fix having
   failed. With `False`, the gate only skips its own branch and `silver` goes back to
   deciding by its own rule.

Verified in the mixed scenario: `mad1` short-circuits, `bcn1` goes through
`extract → validate → land → verify`, and `silver` **runs**.

## Demand calibration against MAPA 2025 (Phase 4)

Up to now, the Orders simulator chose products **uniformly across the catalog**, and
that was declared as an assumption: *"the category mix mirrors the assortment's SIZE."*
That stopped holding once an observational anchor appeared that didn't exist before — the
MAPA's **Informe del Consumo Alimentario en España 2025**, which measures volume, value,
average price, and channel of Spanish household consumption.

### The finding that opened the phase wasn't about demand

The investigation started from a symptom: "Marisco y pescado" had **3.38% of units and
22.82% of revenue**, with an average price paid of **€27.09** against a catalog whose most
expensive product in mad1 cost **€24.05**. An average price above the assortment's maximum
cannot come from product choice.

RAW resolved it. When `selling_method = 1` and `unit_size` is null, the Mercadona API
returns `unit_price = reference_price × 99` — the price of the **weight-selector's
ceiling**, not of anything a household buys. The factor is exactly `99.000` in **10
product×warehouse combinations**, and the actually purchasable portion sits in
`min_bunch_amount`, a field **that had been in RAW since the first partition and that
Silver was discarding**.

| price range | products | units | revenue | % of revenue |
|---|---|---|---|---|
| ≤ €30 | 4,921 | 204,393 | 622,812.08 | 75.85% |
| €30–100 | 6 | 203 | 8,818.30 | 1.07% |
| **> €100** | **12** | **275** | **189,491.55** | **23.08%** |

**12 products out of 4,939 — 0.24% of the assortment — were producing 23% of revenue**,
and none of the 947 tests failed, because the number stayed internally consistent. It's
the same class of defect as the millisecond timestamp in Milestone 7: a wrong unit of
measure doesn't break any total.

`unit_price` **remains untouched** in `silver_product_price` — a faithful projection of the
source is an invariant. What came in were derived columns alongside it:
`purchasable_unit_price`, `price_basis`, `net_content_kg_l`, and the three bulk fields the
model was throwing away.

### The answer to the question that was asked

*"Are high-priced products receiving excessive demand because they raise the Order's
value?"* — **No.** Price enters no draw, neither before nor after this phase. The
mechanism was the opposite: the choice was *indifferent* to price, and it was that
indifference, over a catalog with 12 badly scaled prices, that concentrated the revenue.

### What the calibration does, and what it refuses to do

```
demand group   P(g)  <- MAPA VOLUME target (kg/L), tilted by the e-commerce channel
       |
product within group  <- UNIFORM (no source measures per-SKU turnover)
       |
quantity / price       <- unchanged / OBSERVED
       |
order value             <- consequence, never the goal
```

Price appears in no arrow pointing at demand. **Volume and value diverge on
purpose**: in MAPA, seafood is 0.81% of volume and 2.88% of value, and a simulator that
equated them would be wrong.

**The boundary, which matters more than the calibration.** MAPA measures the resident's
household consumption — it doesn't measure an online store order, a basket, or a cadence.
That's why `daily_order_rate`, `basket_lines_*`, and `quantity_max` **remain `synthetic` and
received no calibration at all**. Turning the benchmark into a source for those numbers
would turn it into a false representation of reality.

### Three things the report doesn't support, logged instead of invented

| Data | Why not | What was done |
|---|---|---|
| **Monthly seasonality by category** | The monthly charts are **images**: only the axis labels come through in text. There are five monthly numbers in prose, all of them totals. And the window covers only August. | A slot created **neutral** across the 12 months, applied to the order rate (never to the mix, where a global factor would normalize away). A pair of tests proves both that the mechanism works *and* that the delivered profile is neutral. |
| **E-commerce by category** | Only 18 of the 102 blocks carry the channel row. | A **fine** tilt on the 18, a **coarse** one (1.1% fresh / 2.8% rest over 2.2% total) on the rest. Every group carries `channel_basis` and the report states which rule produced it. |
| **Non-food (~30% of units)** | Outside the report's universe. | Slice kept with a declared aggregate assumption, **never added** to the calibrated block. |

Also logged: the PDF's cover page says *"Informe del consumo alimentario en España
2024"* while the entire body reports **2025**. It's a residue from the previous edition.
The numbers come from the body, and the discrepancy is in the CONTRACT — the source isn't
adjusted, the finding is logged.

### What changed, measured over the same window

| dimension | BEFORE | AFTER |
|---|---:|---:|
| units | 204,871 | 204,824 |
| kg or liter | 126,550 | 144,425 |
| revenue | 821,121.93 | 583,154.43 |
| EUR/kg | 6.49 | 4.04 |

| group, % of volume | BEFORE | TARGET | AFTER |
|---|---:|---:|---:|
| MARISCOS_MOLUSCOS_CRUSTACEOS | 11.44 | 0.43 | 0.43 |
| FRUTAS_FRESCAS | 3.11 | 9.17 | 9.51 |
| HORTALIZAS_FRESCAS | 2.16 | 6.02 | 5.62 |
| PATATAS | 1.22 | 4.53 | 4.67 |

Mean absolute error against the target: **0.098 point**. **The 29% drop in revenue is
the correction working, not a regression** — 23% of it was 12 products with an API-ceiling
price.

### A design decision that only showed up at runtime

The Iceberg projection merges state **monotonically**, dropping rows with a lower
`last_sequence_no` — that's how the two writers coexist. That merge assumes, without
saying so, that an `order_id` always refers to the same order. Changing
`demand_model_version` is the CONTRACT's **fourth non-additive condition**, and there the
assumption falls apart: the rebuild dropped **4,028 rows** as "older" and left the
projection with two universes mixed together. `orders-reconcile` caught it. `--reset` came
into existence because of this, is destructive on purpose, and never happens on its own.

### What became verifiable

- `make demand-check-mapping` — the catalog's **444 triples** match exactly **one** rule
  in the mapping; zero with no rule, zero ambiguous, zero dead rules. No silent default.
- Coverage is checked against the **category tree**, not against the cut: the catalog's
  dedup hides triples that exist in the source, and checking against it would be measuring
  the tie-break instead. Three correct rules looked dead before this was noticed.
- `make demand-reality-check` generates `docs/demand-evidence/README.md` with BEFORE · MAPA
  · TARGET · AFTER across the three dimensions, and **exits 1** if the worst deviation
  passes a wide, declared threshold. The threshold isn't a quality score: it exists to
  catch a calibration that has gone silently inert. Proven — BEFORE fails at 11.007
  points; the current state passes at 0.660.

## Customer consumption profile (Phase 5)

Phase 4 calibrated **aggregate** demand. What was left out was that every customer was
buying the same expected basket: a 22-year-old in Sevilla and a 78-year-old in Barcelona
were drawing from the same distribution. This phase swaps `P(group)` for `P(group |
cohort)`.

### The finding that opened the phase, again, wasn't about demand

**18.01% of customers were under 18 years old** — 3,602 of 20,000, with
`age_at_ingestion` ranging from 0 to 100. There was a newborn account holder.

This **was not a defect in the OLTP Source**: its contract declares that age comes from the
INE's provincial *population* distribution (table 31304), and that is exactly what it
delivers — a faithful projection of the resident population. What had never been declared
was the difference between **resident** and **whoever places an order**.

As long as age did nothing, this was harmless. It's the same shape as the previous phase's
finding: a number that's internally consistent and only becomes an error once someone
starts using it. Once age got wired to demand, 18% of the base would enter MAPA's
`-35 years` bracket while being a child, and the calibration would come out wrong by
construction with no total breaking.

The fix lives where the question lives: `min_buyer_age = 18` in `order_premises_seed.csv`,
an assumption of the **order domain**. The customer base was not touched and remains what
its contract says it is.

> **This paragraph was wrong, and Phase 6 undid it.** "The fix lives where the question
> lives" assumes the question was *who can buy*. It was also *who can exist*, and that
> second question went unanswered: the newborn account holder stayed in the registry, only
> blocked from buying. A registry is not a census. See
> [Real density of the customer base (Phase 6)](#real-density-of-the-customer-base-phase-6).

### Two bridges, and three refusals

| MAPA cut | does the customer have it? | verdict |
|---|---|---|
| age of the shopper (4 brackets) | `birth_year`, from INE 31304 | **used** — governs the mix |
| autonomous community (17) | `province_code`, from the Callejero | **used** — mix and frequency |
| household life cycle (9 types) | no household composition | **refused** |
| socioeconomic level (5 levels) | no income | **refused** |
| shopper sex | has it — but the report only publishes it for *away-from-home* consumption | **refused** |

Life cycle is the report's richest cut and it comes complete. The blocker isn't the
data, it's the attribute: assigning household composition to a customer that doesn't have
one would be **inventing the attribute** — the same prohibition Phase 1 applied to
tramo-level density. **Logged trigger:** if a future phase ingests households by INE
province, the cut opens, and the MAPA data will already be in the seed.

### The extraction: the numbers were in charts, and the charts have labels

Only **17 of the sections** carry the demographic table in text; the rest are images.
But the charts carry **printed numeric labels**, and reading a label is extraction, not
estimation. ~45 pages were read to cover the 39 weighable groups, each reading checked by
two independent checksums: the four volume brackets sum to 100.00, and the population ones
sum to `8.89 + 30.33 + 31.34 + 29.44` — the same four numbers in **every** section, because
they're the universe. A misread digit breaks one of the two sums, and `load_cohort_age`
fails.

Two discrepancies from the source itself were logged instead of smoothed over: page 206
publishes `30.5 / 31.7 / 29.0` for population where every other page publishes
`30.3 / 31.3 / 29.4`, and page 84 labels Madrid with `13.78` where the others label it
`13.86`.

### The aggregate doesn't move, and that's the acceptance criterion

Without a fix, the aggregate mix would drift off target just because our age pyramid
isn't MAPA's — the previous phase's calibration would get undone sideways, with nothing
failing. An *iterative proportional fitting* adjusts one factor per group until the average
of the per-cohort weights, weighted by the real cohort distribution **across orders**,
reproduces the `v1` weights.

Measured: convergence in **6 iterations**, largest deviation **1.0×10⁻¹⁰**. The error
against MAPA's target came out at 0.075 average points, against 0.098 before — the
difference is sampling noise.

This gives the phase a clean criterion, and a consequence for whoever reads it:
**looking for the effect in a total finds nothing.** It lives entirely in the conditional,
and that's why `MART_DEMAND_COHORT` and the reality check's cohort section exist.

### The constraint that only showed up when measuring

Normalizing the whole cohort at once, `NO_FOOD` and `SIN_BENCHMARK` — which carry a
neutral index due to **absence of evidence** — came out with **0.60×** the share in 65+
against under-35. The model would end up claiming that people over 65 buy 40% less
drugstore per basket line. Nobody measured that: it was a residue of the normalization, and
it was **bigger than most of the effects that actually are measured**.

IPF started running **within each block**, with each one's share held constant across
every cohort. This gives `food_line_share` back the status the seed grants it — a declared,
uniform assumption — and makes a neutral index truly mean "no effect," instead of "effect
left over from the arithmetic."

### What changed, measured over the same window

| | BEFORE (v1) | AFTER (v2) |
|---|---:|---:|
| orders | 6,400 | **5,248** (−18.0%) |
| units | 204,824 | 169,445 (−17.3%) |
| revenue (EUR) | 583,154.43 | 481,201.94 (−17.5%) |
| **EUR per kg** | 4.04 | **4.06** (+0.5%) |

The first three drop by the same ~18%: it's minors dropping out of buying. The fourth
stays put, and it's what proves that the **mix** didn't move — a volume drop with no
change in composition.

The four warehouses stopped being copies: **bcn1 places 1,436 orders against mad1's
1,176**, 22.1% more, against the 22.7% that the two communities' per-capita consumption
predicts (Cataluña 620.82 kg-L per person/year · Madrid 505.86). The index is renormalized
over the four communities served, so the window's **total** doesn't move — what changes is
the split.

> This holds while the warehouse bases are equal. Phase 6 sized them by population and
> the order flipped: with 128,771 customers against 95,498, mad1 started placing more
> orders than bcn1 despite the lower index. Both effects keep existing; the bigger one
> won.

And the conditional, which is the phase's product:

| group | LT35 % | GE65 % | × |
|---|---:|---:|---:|
| CARNE_CONEJO | 0.01 | 0.08 | 6.08 |
| VINO | 0.50 | 2.44 | 4.89 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0.25 | 0.80 | 3.16 |
| … | | | |
| PASTAS | 2.34 | 0.94 | 0.40 |
| ARROZ | 1.40 | 0.44 | 0.31 |

### What became verifiable

- `make demand-reality-check` gained the **Cohort propensity** section, with the table
  above and the count per warehouse. The page declares absence when the window predates
  the layer — `cohorts: None`, not an empty dictionary, which would be indistinguishable
  from "I measured and there was nothing there."
- The default BEFORE became the **immediately previous state**
  (`before_mapa_2025_v2`). Keeping `before_mapa_2025_v1` as the default would make the
  revenue drop from Phase 4's price fix get read as if it belonged to this phase.
- `assert_buyer_age_band_matches_the_customer_birth_year` recomputes the band against
  `birth_year` — `not_null` and `accepted_values` would pass with a swapped label, because
  a swapped label is still one of the four valid bands.
- `assert_no_order_comes_from_a_minor` reads the threshold from the seed **and** keeps a
  floor of 18. The first half alone passes if someone lowers the assumption to zero — this
  was measured while writing the test, and the second half exists because of it.
- `assert_buyer_age_band_is_stable_across_the_window` catches a window regenerated halfway,
  and allows for birthdays: it requires the transition to be to the **next** band and
  forward in time.

## Real density of the customer base (Phase 6)

Phase 5 handled minors **in the wrong domain**. The finding was correct, the
measurement was correct, and the fix answered half the question: `min_buyer_age` prevents
a child from *buying*, not from *existing* as an account holder. The argument backing the
choice — "`silver_customer` is a faithful projection of the resident population, and the
Source's contract says exactly that" — was also correct, and that's exactly why it was
misleading: **the projection's faithfulness is not the property in question.** A customer
base is not a census.

### The second defect, which nobody had looked for

Opening up the registry surfaced what was sitting right next to it: **5,000 customers
per warehouse**, an equal number for AUFs that differ **4.6×** in population (mad1 has
7.10 million residents in its service area; svq1 has 1.59).

Nothing failed. The 216,591 addresses were real and verified customer by customer against
the Callejero; the totals balanced; `assert_customer_reconciles_with_manifest` matched; the
grain was unique. The only thing wrong was that **density didn't exist** — and density,
unlike a sum, shows up in no total. It took a test that redid the math from the INE for it
to become a visible failure.

### The model

```
observed municipal population (INE 29005, year=2025, ref 2024-12-31)
  × the municipality's province adult share (INE 31304, ages >= min_customer_age)
  × penetration rate
  = customers for that warehouse
```

Three decisions, each with a reason that isn't aesthetic:

**The math is per municipality, not per warehouse.** Today each warehouse falls in a
single province, so the two forms give the same integer. Multiplying a warehouse's whole
population by a single adult share would *presume* that; the per-municipality form stays
correct if a warehouse ever spans provinces, and the other form silently becomes wrong.

**The adult share is measured BEFORE truncation.** Measuring it after returns 100% across
every province — a plausible number, one that would fail nothing, and whose effect would
be sizing the whole base by the total population as if it were all adult. The two queries
share the same CTE over the whole pyramid so that one's numerator never stops being the
same universe as the other's denominator.

**The denominator is adult, not total.** The base is made of adults; distributing it by
total population would give weight to people who can't have an account. Measured: against
allocation by total population, svq1 would lose 29 customers and mad1 would gain 16 — a
0.15% difference, but the right denominator costs the same as the wrong one.

**The total is a consequence, not a quota.** With a 20,000 quota split up, adding a
municipality to the service area would *take* customers away from the other warehouses.
This way, it adds them.

### The rate, and the two assumptions it carries

2.2% is e-commerce's share of total food volume in 2025 (report, section 3). It's the
**only observed number** available for sizing a customer base in this repository, and it
already existed: `demand_profile_seed.channel_reference_pct`, labeled `observed` since
Phase 4.

`customer_premises_seed` **points** to it instead of copying it, and the export only knows
how to follow that pointer — an arbitrary pointer would let the base be sized by any number
from any seed. The dbt test checks the pointer and fails if it changes target.

Two assumptions turn a volume share into a people share, and **neither is measured**:

1. **The online buyer consumes like the average.** Under it, 2.2% of volume ↔ 2.2% of
   people.
2. **These four warehouses model the AUF's entire online channel**, not one operator
   within it. Applying one operator's market share would require a source not ingested
   here.

### Three layers against minors, and why three

| layer | where | what it catches |
|---|---|---|
| the delivered distribution is already adult | `oltp_reference._age_sql` | the generator *cannot* draw age 7 |
| the reference is distrusted | `reference_data._require_customer_scope` | an old-schema reference, and half-applied truncation — a header saying 18 with a child in the rows |
| the landed data is re-checked | `validate._check_minimum_age` | anything that slipped past the first two |

The middle one exists because of experience: the header is the **promise**, and a
promise with no check is what produced the 18.01% the first time.

### What the phase changed, measured

| | BEFORE | AFTER |
|---|---:|---:|
| customers | 20,000 | 286,826 |
| minors | 3,602 (18.01%) | **0** |
| age range | 0 … 100 | 18 … 100 |
| base eligible to order | 16,398 | 286,826 |
| orders in the 4-day window | 5,248 | 91,788 |
| events | 36,596 | 636,848 |
| average error against MAPA's target | 0.075 pt | **0.070 pt** |

The error against the benchmark **improved** without the calibration being touched: IPF
reconverged in 6 iterations over a different cohort distribution — `LT35` stopped
containing children — and the aggregate stayed where it was.

### The closing proof the rate created

Sizing the base as 2.2% of the people *because* 2.2% of the volume is online creates an
obligation no previous phase had: the model should then produce 2.2% of the household
consumption of those same AUFs. That is checkable against the report itself.

| food scope, 4-day window | expected channel | model | ratio |
|---|---:|---:|---:|
| kg or liter | 2,134,114 | 1,944,027 | 0.91× |
| revenue (EUR) | 7,203,504 | 6,793,690 | 0.94× |

**Nothing was adjusted to make this tie out.** The rate entered in this phase;
`daily_order_rate`, `basket_lines_*`, and `quantity_max` entered in Phase 3, chosen with no
relationship to it and with no source measuring them. The two halves meet in this table
for the first time.

`NO_FOOD` stays out of the numerator: the report's per capita figure is for food and
beverages and doesn't cover drugstore items — adding it would compare two universes and
inflate the ratio with nothing actually wrong. `SIN_BENCHMARK` stays in, because these are
food groups the report doesn't detail but that belong to the same universe the per capita
figure measures.

The remaining distance **should not be closed** by tweaking `daily_order_rate`: no source
in this repository measures purchase cadence or online basket size, so there's no criterion
to decide which of the two sides is wrong. As long as that's true, the ratio is an
**observation**, not a target. **Trigger:** a source that measures household purchase
frequency or average ticket by channel turns this row into a test.

### The side effect that inverted the previous phase

| wh | customers | orders | regional index |
|---|---:|---:|---:|
| mad1 | 128,771 | 37,332 | 0.89 |
| bcn1 | 95,498 | 33,976 | 1.10 |
| vlc1 | 34,295 | 11,656 | 1.05 |
| svq1 | 28,262 | 8,824 | 0.96 |

Phase 5 had bcn1 in the lead due to Cataluña's per-capita consumption. Now Madrid's
population dominates, and the order flips. Both effects keep existing and the bigger one
won — which is measurement, not choice, and is the kind of thing that only shows up once
both dimensions get observed at the same time.

### A measured non-determinism, and the warning it became

Two runs of `export-oltp-reference` produce different `adult_share` values **in the
double's last bit** — 0.8224668719886548 against 0.8224668719886545 — because DuckDB's
parallel aggregation doesn't fix the floating-point sum's order. It's not a module defect
and it **doesn't propagate**: all four targets come out identical, and a base generated
from two different exports has the same `sha256`.

But it only doesn't propagate because none of the four products lands near a `.5`. The
tightest margin is mad1's: **128,770.524404**, 0.024 away from a tie — about 135
residents. If Madrid grows or shrinks by that much in the next 29005, the target starts
alternating between 128,770 and 128,771 from export to export, the base changes size with
nothing having been decided, and the dbt test (which tolerates 1 customer **exactly because
of this**) starts passing by luck. `test_a_alocacao_nao_fica_na_beira_do_arredondamento`
exists to warn before that happens.

### What became verifiable

- `assert_no_customer_is_a_minor` scans **every** `ingestion_date`, not just the
  current one: orders pin `customer_ingestion_date` and the export resolves each customer's
  latest version, so filtering by `is_latest_ingestion` would leave a back door open. It
  reads the threshold from the seed **and** keeps a floor of 18 — measured: with the
  threshold at zero, the first half passes.
- `assert_customer_base_follows_the_declared_population_allocation` redoes the allocation
  from `silver_ine_population_by_municipality` × the adult share from
  `silver_ine_population_series` and compares. It's the test that a uniform regeneration
  fails, and it also checks the rate's **pointer**.
- `--count` became optional and `count_source` entered the manifest. Without this, a base
  generated with a manual override would be indistinguishable from one derived from the
  population.
- Four **non-additive** conditions declared in the CONTRACT, not three: seed,
  `ingestion_date`, `min_customer_age`, and the rate/allocation rule. All four swap the
  people behind the same `customer_id`s.
- Regenerating the registry **forces** regenerating the orders. It's not a change to
  Orders' logic — `assert_buyer_age_band_matches_the_customer_birth_year` fails, and it was
  what flagged this during the phase. No file in the order domain was changed.
- **The stream plane was brought back into convergence**, not just the lakehouse: the
  transactional OLTP was reset and reapplied (636,848 events, one transaction each), the
  outbox was drained into Kafka, and the Postgres sink consumed the whole topic —
  **39,751 duplicates discarded by `sequence_no`**, which are the previous universe's
  events being correctly rejected as older. `make orders-reconcile` ties out across all
  three paths, 91,788 orders.
- **The Iceberg sink was deliberately NOT drained**, and the reason is written on the
  evidence page itself. The table was already in its final state:
  `orders-rebuild-projection` is the second writer and writes straight from RAW, without
  going through the topic. Draining the remaining 628,848 events would take ~9 hours of
  copy-on-write commits to discard them all as equal-or-older, without changing a single
  row. What proves convergence is `orders-reconcile`, not a consumer's offset — and the
  page says so, instead of letting the lag look like a defect.

## Phase 7: the closing — the gate over Iceberg, stock, the seal, and the three defects it found

**Date: 2026-09-01.** This phase was conducted against
[`AI_ENGINEERING_CONSTRAINTS.md`](AI_ENGINEERING_CONSTRAINTS.md), written by whoever
operates the project before the phase began. It forbids adopting technology to boost the
technology count, requires each one to have explicit responsibility and evidence of need,
and puts the agent's preference as the **last** decision criterion. What follows is the
record of how each decision survived — or didn't — that text.

### The gate was about Iceberg, not about Spark

The repository's most fragile claim wasn't Spark: it was a clause in ARCHITECTURE
itself. Iceberg was adopted in Phase 3 with **two** justifications — atomic commit with
optimistic concurrency, and **interop between engines**. The first had been proven since
then. The second was **asserted and never demonstrated**, for four phases, because both
writers were Python using the same library.

`make spike-spark-iceberg` is a closed experiment, in the mold of `make spike-iceberg`, run
**before a single line of this phase existed**. It's **18 questions** in one file
(`scripts/spike_spark_iceberg.py`), split into five stages — read, write, concurrent, shape,
cleanup. The **main goal was interoperability**: the catalog is a pyiceberg `SqlCatalog`,
and Spark needs to open it as `org.apache.iceberg.jdbc.JdbcCatalog`.

**Both outcomes were declared before running**, and both were acceptable:

| outcome | declared consequence |
|---|---|
| green | interop stops being an assertion; Spark comes in; the rest of the phase proceeds |
| red | Spark does **not** come in **and** the interop clause is **struck** from Iceberg's justification in ARCHITECTURE. What's left — atomic commit and optimistic concurrency — remains proven and remains enough |

It came out **green, 18/18**. Spark reads `projection.live_order_state` with the
**same count** as pyiceberg (91,788 in the intermediate window), creates and writes a table
that pyiceberg reads back, and the optimistic conflict **between different engines**
completes the full cycle that section 4 of the constraints requires: detect → reload →
reapply → retry, with no guessed `metadata_location`, no manual metadata reconstruction, no
blind overwrite, and with an old `seq` **not** overwriting a new one. Two questions (H5, H6)
exist just to check that the experiment **did not** touch production: row counts and
catalog schema intact afterward.

Two technical details worth logging. The `iceberg_tables` pyiceberg creates is exactly the
Java `JdbcCatalog`'s V1 schema — that's what lets the two see each other with no
translation. And `io-impl` is `S3FileIO`, not S3A: pyiceberg wrote `s3://` paths with
`PyArrowFileIO`, and S3A would require `s3a://`. The side effect is that the image doesn't
carry `hadoop-aws` plus the AWS SDK v1 bundle — about 200 MB that didn't have to go in.

### The shape SQL can't express, measured: S7b

Question S7b is the one that turned Spark's second justification into a number. It
runs the **same input** through the running sum SQL knows how to do —
`sum(...) over (partition by ... order by ...)` — and compares it against the loop.

The stock balance is not a running sum: it's a running sum whose **inputs are generated by
decisions made from the state itself**. The balance drops below the reorder point, an order
is issued, it arrives `lead_time` days later and changes the next balance, which decides
whether there's a new order. A window function **reads** the whole partition and does not
**write** back into it.

Measured on the 30-day test case: the running sum in SQL **diverges on 14 of the 30 days**
and reaches a balance of **−70** — negative stock, which no operation would ever have.
After that, the justification stopped being a paragraph.

### What the measurement publishes against Spark

`make spark-evidence` runs the **same loop** on both engines. The function is
*imported* from `jobs/spark/stock_ledger.py` on both paths — it's not an approximate
reimplementation — so the only thing that varies is who iterates over the groups: a `for`
in one process, or Spark distributing it. If they were two implementations, the comparison
would be measuring the skill of whoever wrote each one.

The **seven metrics come out identical**, and without that equality the time comparison
would mean nothing, because the two sides would be measuring different things: 173,970
lines · 17,397 series · 6,315,644 demand · 6,310,606 fulfilled · 5,038 stockout · 17,397
orders issued · 17,357 arrivals.

| run | pure Python | Spark (job) | Spark with JVM and container | ratio |
|---|---:|---:|---:|---:|
| the first one, at Milestone F | 17.3 s | 54.8 s | 60.3 s | ~3.2× |
| the one published in [`docs/spark-evidence/`](docs/spark-evidence/README.md) | 16.6 s | 54.1 s | 58.9 s | ~3.3× |

**Both are here on purpose, and the divergence between them is part of the finding.**
Wall-clock time isn't reproducible: the number changes from run to run, and the first pair
was overwritten by the next regeneration before reaching a commit — only the second has an
artifact in the repository. What does **not** change is the claim: in both runs pure Python
is **~3× faster**, and Spark's time **includes** the JVM startup with no discount, because
whoever runs the job pays that cost.

It's the project's own rule applied to itself — a measured number lives on a generated
page, the prose carries the magnitude and not the decimal.

**The volume trigger did not fire, and that is measured too.** ARCHITECTURE declares
Spark's trigger as *"a partition DuckDB can't hold in memory."* The natural candidate is
the basket self-join — every pair of products bought together, the basis of any affinity
analysis: **37,899,395 pairs, 8,789,258 distinct, in ~1.5 s and ~2.4 GB** on one DuckDB
node. The final window **doubled** that number relative to the intermediate one (18.3M
pairs) and the time stayed in the seconds, which makes the claim stronger, not weaker.

**`written_by` is what makes "three writers" a queryable fact** instead of a
documentation phrase: `platform` and `rebuild` are Python, `spark` is the JVM. The property
that justified Iceberg since Phase 3 only stopped being an assertion once that list gained
a name that isn't Python.

### The conclusion, and what it is not

**Spark was not adopted for performance.** At this volume it loses, the number is
published, and a page that only published favorable results would prove nothing.

It was adopted for three reasons:

1. **The stock ledger has cumulative state whose output depends on the prior state** — the
   shape SQL doesn't express, measured in S7b.
2. **The spike demonstrated real interoperability with Iceberg** — 18/18, including the
   full optimistic-conflict cycle between different engines.
3. **The project needed to validate multiple engines writing to the same catalog** — which
   was the undemonstrated half of Iceberg's justification.

**This is not Spark propaganda, and the distinction matters.** DuckDB did not become
"wrong": it remains the engine for the whole Silver layer and all 25 models. What changed is
that **one** table now has a writer whose shape SQL can't express. And the project's
default path does **not** depend on Spark: `silver_gate.py` pulls from the build whatever
depends on a table that doesn't exist, and
`test_O_SPARK_E_OPCIONAL_e_isto_e_o_que_prova` fails if any gate node stops being
covered. `make silver` runs green on a tree where Spark never ran — 364 nodes — and green
again with the ledger in it, 391.

**What Phase 7 did NOT prove, and is written down:** that Spark **scales**. It runs
`local[*]` — driver and executor in the same JVM, with no shuffle between nodes. A pretend
cluster would prove neither scale nor interoperability; it would prove that compose brings
up containers.

### Milestone B: two contradictory assumptions, and the distinction that authorized fixing them

The project had the right rule and a missing distinction. The rule: **refuse to adjust
an assumption until the output pleases**. The distinction: tweaking an assumption to
improve a number is one thing; making two assumptions **mutually coherent** is another. The
first is refused; the second is a model correction.

Two contradictions, measured before any change:

| assumption | was | what contradicted it |
|---|---|---|
| `slot_lead_hours_min` / `_max` | 2 h / 24 h | the cycle declared by the other rows of the **same seed** never exceeds 8.5 h. Measured over 86,803 delivered orders: **73,124 (84%) arrived before the window opened** — promising for after something that was already delivered |
| `sla_minutes_picking` | 90 min | the arithmetic ceiling is 80 (`basket_lines_max` × `minutes_per_line_picked`). **No possible basket reached the threshold**, and `orders_breaching_sla` was structurally zero |

Neither is a bad result. Both are an internally contradictory model, and three phases
had "logged instead of fixed" them.

**Both became derived**, not chosen: `slot_lead_hours_min` = `ceil(min_cycle/60)` = 1 h;
`slot_lead_hours_max` = `floor(max_cycle/60)` = 8 h; `sla_minutes_picking` =
`floor(triangular_p90(basket_lines_min, basket_lines_max+1, basket_lines_mode)) ×
minutes_per_line_picked` = 60. The `+1` is not a detail: the generator draws with
`rng.triangular(low, high+1, mode)` and truncates. And `sla_picking_percentile = 0.90` came
in as its **own seed row**, so the policy stays visible — without it, the threshold would be
a free number between 28 and 80, and adjusting it until the indicator pleases would be
indistinguishable from calibrating it.

**The guard against this very correction's own risk.** Once you touch this, it becomes
trivial to keep touching it until the KPI looks good, which is exactly what's forbidden. So
`assert_order_premises_are_internally_coherent` checks the **derivation**, never the
result, across four independent clauses. Measured: swapping 60 for **75** — a plausible
value, within the band the other clauses allow — **fails** on the exact-derivation clause.
Six injections were attempted and all six failed.

### Milestone C: zero stockouts across 86,520 lines — and the same failure recreated two hours later

The stock job's first run produced **zero stockouts across 86,520 lines**. That was not
good operations: `opening_days_of_demand` was **7** and the observed window was **5
days**, so the **opening** stock covered the whole period. Zero stockout was **arithmetic,
not measurement** — a condition structurally incapable of producing a violation.

**It's exactly the same family as the structurally-zero `orders_breaching_sla` that
Milestone B had just fixed** — recreated in the stock domain two hours later, by the same
person who had written the fix. A threshold no basket reaches and a stock no window
consumes are the same thing: a number that can only give one result. That's why the rule
became a **test** instead of a paragraph: `assert_stock_window_can_exercise_replenishment`
requires the observed window to be **greater** than the opening coverage.

**What this test deliberately does NOT require:** that a stockout **occur**. Operations
that don't run out of stock are a legitimate state of the world, and requiring a stockout
would be asking the model to produce the number that pleases. What it requires is that a
stockout be **possible** — that the result depend on demand and not on the assumptions'
arithmetic.

**Zero violations is not evidence of operational quality.** It's the first thing to
suspect.

Today the ledger measures: 5,038 units of stockout across 590 of the 173,970 lines. Seven
injections were attempted against the ledger's three tests (balance conservation,
`lead_time` compliance, window exercisability) and all seven failed.

### Milestone D: the single regeneration, and what the correction started measuring

The final window is **derived**, not chosen: it has to exceed `opening_days_of_demand`
(7 days, otherwise it falls into the defect above) and the ceiling is external —
**2026-09-01**, as far as the observed catalog reaches. Lowering the stock policy to fit
more cycles would be choosing an assumption for its effect on the chart.

Result: 9 days, **206,523 orders**, 3,892,062 lines, 1,433,723 events, with the **three
folds agreeing** — batch, Iceberg projection, and Postgres sink.

**Milestone B's fix started measuring something**, and the numbers were declared as a
consequence before being measured:

| indicator | before | now |
|---|---:|---:|
| deliveries **before** the window opens | 84.0% | 42.5% |
| deliveries **within** the window | 8.5% fixed | **24.8%**, and it varies |
| picking SLA violations | 0 (structural) | **19,136 = 9.694%** |

The 9.694% is against a theoretical 9.747% derived from the basket distribution's
declared p90 — a difference of 0.053 point, within the sampling noise expected for
197,402 orders (standard error ≈ 0.065 point). **Correction to this very sentence, found in
a later audit**: the previous version said "agreement to the third decimal," which is
arithmetically false — 9.694 and 9.747 already diverge at the first decimal place. What
the agreement shows is more modest and still real: both round to 9.7%, and the distance
between them is what sampling would predict, not a systematic bias. This became
`assert_picking_breach_rate_matches_the_declared_percentile`, a test of **distributional**
conformance, not of counting.

`make demand-reality-check` was run afterward: mean absolute error of **0.111 point**
against the TARGET, across the volume table's 39 rows — a number read from
[`docs/demand-evidence/README.md`](docs/demand-evidence/README.md), not copied by hand.
("0.066 pt across 34 groups" used to be here; it didn't correspond to any run persisted in
the repository — probably a leftover from an intermediate pass never reconciled with the
final regeneration, found in the same audit.) The slot and SLA fix **did not move** the
demand mix, which is correct — it belongs to a different domain.

**A caveat the evidence page doesn't connect in the text, and worth logging here.** The
biggest deviation in the volume table is HUEVOS (1.238 point), and HUEVOS's EUR/kg diverges
from MAPA by 5.8× (22.24 against 3.85) — the worst in the whole document. Both things have
the SAME cause, and it isn't price: `net_content_kg_l` is null for eggs sold by unit (no
source measures weight per egg, correctly), and covers only 18% of HUEVOS lines — the
liquid pasteurized egg whites. `eur_por_kg = revenue/kg` sums revenue over ALL lines and kg
only over that 18%; the result is the whole category divided by a seventh of it, not an
observed price. The page already warns that HUEVOS "doesn't measure the calibration, it
measures the conversion's coverage" — but that caveat only lives in the volume table; the
EUR/kg table, which advertises itself as "the comparable column with no conversion at
all," doesn't inherit it. Trigger to fix: the same `linhas_sem_kg` group list already
computed in `demand_check.py` needs to suppress (or flag) the EUR/kg of those groups, not
just the volume deviation.

### Milestone E: Silver's parquet survives the model's exclusion

When `silver_gate` pulls `silver_stock_ledger` from the build — because the Iceberg
table doesn't exist on this machine — **the parquet from the last successful build stays
behind** in object storage. The Snowflake export reads it without knowing it's stale.

It happened for real, and it had already been published: **`MART_STOCK_HEALTH` was
describing a 5-day window while every other mart described 9**, and **no test failed**.
Every total balanced — within each domain.

Mitigated by `assert_stock_ledger_covers_the_order_window`, which compares the two
domains' windows **in the warehouse**, which is where they finally meet. The new test
failed against the real data on its first run — a natural injection, the only kind not
suspected of having been written to pass.

**This is not closed, and shouldn't be presented as closed:** the mitigation is
**per-domain** and the class is **general**. Any model the gate excludes leaves stale
parquet behind, and nothing checks this generically. It stays in the debt list as an open
item.

### The consumer was counting WRITES and calling them ORDERS

The phase's last defect, and the most instructive one. Consuming the 1,433,723 events,
the CLI printed **`SLA breached 31,908`** while the Iceberg projection and `silver_order`
agreed on **19,136**.

`orders` and `sla_breaches` were **integers summed batch by batch**. An order whose events
fall across different batches was counted **once per batch** — and from `order_picked`
onward the state carries `sla_breached = true`, so every subsequent batch touching the
order rewrote it as breached, and the sum counted each of those writes. Event counting
**is** additive across batches; order counting **is not**.

**Nothing broke, and that's the point.** The number was plausible, had the right order of
magnitude, and the label said something different from what it measured. No counting test
catches this. What caught it was **two folds disagreeing** — for the third time in this
project, and all three times they found a real defect.

Fixed by swapping the integers for **sets of identifiers**, with `orders` and
`sla_breaches` becoming properties derived from the set's size. The test that pins this down
needed a **fourth** batch to reproduce the defect: an order only starts being breached at
`order_picked`, which is the third one.

### Milestone G: RAW is sealed, not reproduced

`make freeze` walks RAW, reads every `_manifest.json`, and writes
[`docs/FREEZE.md`](docs/FREEZE.md): **81 partitions, 2,221,069 records, 2.29 GB**, with an
aggregate `capture_id` —
`cec10cb5931f284f463a68ccc707c7612da1bab81caf4d41936a7a7f0ec568ef`. `make freeze-check`
rereads RAW and compares.

**The seal covers the data, not the run.** Each `content_sha256` is the hash of the
**ordered** list of `(path, sha256, bytes, records)`; `run_id`, `started_at_utc`,
`finished_at_utc`, `duration_seconds`, and `history` are **deliberately left out**.
Including them would make a byte-identical re-land break the seal, and an alarm that fires
with no cause trains reviewers to ignore it — the same reason the dashboard's
`CONTRACT.md` carries the source's sha256 and not the generation date.

**The property this guarantees is `same frozen RAW → reproducible downstream`**, not that
RAW is reproducible: the Mercadona API is live, the Callejero is a semiannual manual
download, and MAPA's URL points to "últimos datos" (the latest data). On a fresh machine
the operator runs `make freeze` and seals **their own** capture.

**If a regeneration makes `make freeze-check` fail, the right move is not to fix the
seal.** That behavior is intentional: a window that grows after closing invalidates every
number already published about it. A partition-name injection was refused; changing one
byte fails it.

### Milestone H: 3,910 lines of state documentation became 1,328

The README had 1,435 lines and ARCHITECTURE had 2,475, with the **same subject in two
places aging at different rates**. The size is, by itself, a challenge to complexity.

The narrative moved to this file — **17 sections moved literally, nothing rewritten and
nothing lost** — future scope went to [BACKLOG.md](BACKLOG.md), and a **tested line
ceiling** keeps the two from growing back without that being a decision that shows up in
the diff.

> **Note from the documentation closing, 2026-09-02.** The README grew again, on purpose
> and once: the section *"The fourteen closing questions"* came in, a one-line-per-question
> index pointing to where the evidence lives, and the ceiling rose from 700 to 760 lines. The
> ceiling change is in `test_documentacao.py`'s diff, which is exactly what the mechanism
> was built to provoke.

**The change exposed two classes of reference that were never checked.**
`LinksRelativosTest` verified that the **file** exists — and a broken anchor points at a
file that exists, so it passed and the reader landed at the top of the document with no
idea they'd missed their spot. An entire index can rot this way. Two new checkers cover the
`§ "…"` label and the anchor, across every heading level and explicit HTML anchors.

And it's worth logging against myself: the **first version** of those checkers was wrong
twice — it only scanned `##`, reporting two false positives, and it didn't know about
`<a id="...">`, reporting 22. A wrong checker costs more than no checker, because it
teaches people to ignore the result.

### Debt went from five to eight, and that is a result

**Three of the new items were discovered by Phase 7, and two of them measure what it
built itself.** A debt list that only shrinks is a sign nobody is looking. The current
list, with problem, impact, status, and next step, is in
[ARCHITECTURE.md § "Technical debt"](ARCHITECTURE.md).

The most uncomfortable item is the one place in the project where **volume actually
hurt**: rebuilding the projection is **O(n²)** — 414 commits and ~55 min for 206,523
orders, because every batch does an `upsert` against the whole table. In Phase 3, with
6,400 orders, this took seconds and was invisible. The irony is worth writing down: Spark's
justification says the volume trigger didn't fire, and it fired **here**, in the Python
path, and not in the analysis.

---

## Change request — how to change something after the freeze

The project is frozen. That doesn't mean immutable; it means a change goes through a
record, not an impulse. Copy the block below and answer **every** line — if any of them
can't be answered, the change isn't ready to be made.

```
## CR-NNN · <title>

Need             What concrete problem exists?
Evidence         What test, log, or measurement demonstrates it exists?
Insufficiency    Why isn't the current solution enough?
Component        Who should be responsible for the change?
Contracts        What data contracts are affected?
Regression       What tests might break?
Semantics        Does any field change meaning?
Provenance       Will we still know where the data came from?
Reproducibility  Does the same input keep producing the same result?
Cost             Is the added complexity justified?
Proof            What test will demonstrate the change improved the system?
Loss             What stops being true?
```

**What doesn't count as a need**, and the list is literal: *"it's used in the
market,"* *"it looks more professional,"* *"it's a best practice,"* *"companies use it,"*
*"it might be useful in the future,"* *"it looks good on a résumé."*

An idea with no CR goes to [BACKLOG.md](BACKLOG.md).

### CR-001 · Chain `warehouse_load` after `mercadona_catalog_daily`

```
Need             The Mercadona catalog already had a daily cron and already finished by
                 rebuilding Silver — but Snowflake (RETAIL.MART, what the dashboard reads)
                 only updated with a manual `make warehouse-trigger`. Without someone
                 remembering, MART sat stale.
Evidence         orchestration/airflow/dags/warehouse_load.py already anticipated the hook,
                 in its own docstring: "Whoever wants to chain it uses TriggerDagRunOperator."
                 Not a new feature — activating an extension point that already existed.
Insufficiency    Manual triggering depends on someone remembering; without that, MART ages
                 silently, with no test or alarm noticing.
Component        Orchestration (Airflow). One file: mercadona_catalog_daily.py.
Contracts        None. No dbt model, SQL, or schema changed — only the orchestration.
Regression       make test (1,095/1,095 green), airflow dags list-import-errors (zero),
                 airflow tasks list --tree confirming the new chain.
Semantics        No field changes meaning.
Provenance       Unchanged — the same chain land → verify-landing → silver-build →
                 export-snowflake → load-snowflake → dbt build, just triggered on its own.
Reproducibility  Yes. The same input keeps producing the same output; only the trigger changed.
Cost             One task (TriggerDagRunOperator), zero new services, zero new cron on
                 warehouse_load (still schedule=None, fired by trigger).
Proof            Ran live, not just as a dry run: docs/screens/airflow-warehouse-load-trigger.png
                 shows the new chain in the real Airflow graph, and
                 docs/screens/minio-mercadona-partitions-09-04.png shows the partitions for
                 2026-09-03 and 2026-09-04 already landed by the cron — after the freeze,
                 exactly as the plan predicted.
Loss             docs/FREEZE.md stops matching the live RAW byte-for-byte starting on
                 2026-09-02 — DOCUMENTED and expected behavior of the seal (Milestone G), not
                 a regression from this change: make freeze-check will flag the new Mercadona
                 partitions as "outside the seal," because that's exactly what it's for.
                 Orders and synthetic customers stay frozen; only the real catalog grows.
```

Decision logged on 2026-09-04. Implemented, verified live against Airflow and MinIO, and
queried afterward via `docs/screens/snowflake-price-evolution-query.png` — the newly loaded
MART answering a real `RETAIL_READER` query against `MART_PRICE_EVOLUTION`, 1,697 rows, with
`snapshot_date` already covering 2026-09-01.

### CR-002 · Translate the project to English (reading layer only)

```
Need             The project was entirely in Portuguese. A prior pass this same session had
                 already translated the dashboard to Spanish; the request changed to English
                 for the whole project before any screenshot collection was done.
Evidence         docs/screens/dashboard-commercial-en.png and
                 docs/screens/dashboard-inventory-en.png — the live dashboard, driven with a
                 headless browser (Playwright/Chromium) against the real Snowflake account,
                 zero console errors, every UI label in English.
Insufficiency    A dashboard-only translation leaves README/ARCHITECTURE/DECISIONS/BACKLOG/
                 AI_ENGINEERING_CONSTRAINTS.md and the 5 generated evidence pages in
                 Portuguese — inconsistent for an English-speaking reader.
Component        Documentation (5 root docs), the 5 evidence-page generator scripts under
                 platform/src/retail_platform/, and streamlit/{app.py,indicators.py,
                 contract.py}. FAQ.md deliberately excluded — personal, Portuguese,
                 gitignored, out of scope by explicit request.
Contracts        None. No dbt model, SQL structure, or Snowflake schema changed. The one
                 gray area: display-only string literals embedded in indicators.py's SQL
                 (funnel stage names, leakage reasons, loss causes, delivery-window
                 outcomes — e.g. 'Colocado' -> 'Placed') were translated, because they are
                 constructed inline (`select 'Label' as column`) and never compared against
                 anything downstream. Genuine warehouse-computed enum values
                 (`change_type`, `sex_label`, category and product names) were left
                 untouched on purpose — those come from dbt models and real Mercadona/INE
                 data; translating them would mean editing warehouse SQL and rebuilding
                 Snowflake data, which is out of scope for a text-only pass.
Regression       make test (1,095/1,095 green), make dashboard-check against the live
                 account (13 metrics / 7 tabs / 20 tables / 0 warnings, zero exception),
                 platform/tests/test_documentacao.py (10/10 — cross-file section citations
                 and anchors all resolve after every heading translation).
Semantics        None.
Provenance       Unaffected.
Reproducibility  Unaffected — same SQL, same data, same tests; only prose and display
                 labels changed language.
Cost             A large one-time editing effort across roughly 30 files (5 root docs, 5
                 evidence-page/generator pairs, 3 dashboard files, and a handful of test
                 files whose assertions hardcoded Portuguese report strings). No new
                 abstraction, no new dependency, no ongoing cost.
Proof            The two screenshots above, plus every test run listed under Regression.
Loss             The Spanish-language dashboard screenshots taken earlier this same session
                 (docs/screens/, timestamps 10:22-10:28) are now stale against the English
                 UI. Left in place rather than deleted — that's the user's call, not the
                 agent's.
```

### CR-003 · Screenshot proof for four demonstrations that were only ever prose

```
Need             The dashboard screenshots (CR-002) covered the read path. Four other
                 demonstrations this project makes — RBAC isolation, the Spark/Iceberg
                 interop spike, the OLTP transaction-atomicity proof, and Kafka stream
                 semantics (transport, delivery, idempotent replay) — existed only as prose
                 claims and test-suite green checks, never as captured terminal output a
                 reader could look at directly.
Evidence         docs/screens/proof-rbac-isolation.png — a live probe against the real
                 Snowflake account: RETAIL_READER reading MART_CUSTOMER_BASE (286,826 rows)
                 and being refused on GOLD.DIM_CUSTOMER and STAGE.STG_CUSTOMER with the
                 literal Snowflake error ("002003 (02000): SQL compilation error"), same for
                 RETAIL_LOADER denied on MART and GOLD. Session opened with
                 `use secondary roles none` — without it the check passes by accident, see
                 `check_isolation`'s own docstring.
                 docs/screens/proof-spark-iceberg-interop.png — `make spike-spark-iceberg`,
                 18/18 checks green (VEREDITO: APROVADO), against the real Iceberg catalog
                 the pyiceberg writer built.
                 docs/screens/proof-oltp-atomicity.png — `make orders-prove-atomicity`, all
                 checks green (APROVADO): two independent trigger-injected failures (outbox
                 insert, orders insert) each roll back cleanly, a full partition replays
                 idempotently, and an out-of-sequence event is refused rather than silently
                 skipped.
                 docs/screens/proof-stream-semantics.png — `make orders-prove-stream` after a
                 full stream-plane reset (below): transport fidelity holds for all 36
                 partitions (topic reread reproduces every manifest sha256), at-least-once
                 duplication is reproduced on demand (500 events, exactly), duplicate
                 consumption is a no-op on the projection, replay over an already-built
                 projection changes nothing, a forged out-of-sequence event is refused, and
                 the three independent folds (Silver, OLTP, streaming projection) agree on
                 206,523 orders. The one check that stays red — "no order breaches the
                 declared 60-minute picking SLA" — is, per the script's own comment, an
                 intentional, permanent characteristic: the mechanism runs correctly and
                 measures a real 80.00-minute maximum separation, but the script's author
                 chose not to adjust the declared threshold or the data just to make the
                 check go green, calling that "the opposite of verifying." `make
                 orders-prove-stream` therefore exits 1 in its correct, steady state — the
                 screenshot's own footer shows exactly that, and only that one line.
Insufficiency    Terminal output alone doesn't reproduce the run; it documents that a
                 specific run, on a specific date, against a specific live account (or
                 local containers), produced this exact output. The commands are
                 reproducible; the screenshot is a snapshot of one execution of them.
Component        docs/screens/ only. No code, no test, no documentation prose changed. The
                 stream plane's live state (Kafka topic, OLTP replica, Postgres projection)
                 was reset and rebuilt as an operational side effect of capturing proof 4 —
                 not a code or schema change, and confined to this project's own Docker
                 containers.
Contracts        None. All four commands are read-only or self-contained (the atomicity
                 proof empties and rebuilds its own OLTP tables; the RBAC probe only runs
                 `select count(*)`; the Spark spike creates and deletes its own namespace,
                 verified by its own H5/H6 checks that production tables were untouched;
                 the stream proof consumes and republishes but never touches Silver or the
                 warehouse).
Regression       Re-running each command reproduced the same verdict; no test suite was
                 touched. `make test` still 1,095/1,095 after the stream-plane reset.
Semantics        None.
Provenance       Unaffected.
Reproducibility  Unaffected — no data, model, or script changed, only screenshots added.
Cost             Four terminal captures via the same Playwright pipeline built for CR-002,
                 rendered as styled terminal screenshots instead of browser screenshots.
                 The fourth cost substantially more than a screenshot: see Loss.
Proof            The four screenshots above.
Loss             The first attempt at proof 4 FAILED (7 checks red) — not because the
                 underlying mechanism is broken, but because the Kafka topic and OLTP
                 replica had been up and exercised manually for hours this session before
                 this agent touched them; the topic held more messages than a single fresh
                 publish of the full 36-partition log would produce, so the
                 transport-fidelity check (topic reread = manifest sha256) failed, and that
                 cascaded into the fold-agreement checks. A separate, unprompted
                 `orders-prove-atomicity` run moments earlier compounded this by emptying
                 the OLTP replica down to a single partition. Asked the user before taking
                 the destructive fix (delete+recreate the Kafka topic, reset the OLTP
                 tables, truncate the Postgres projection, reload all 36 partitions,
                 republish once) — user said yes. After the reset, the same command passed
                 every check except the one documented above as permanent by design. The
                 stream plane is left in this fully-reloaded, freshly-published state;
                 nothing was torn back down.
```

### CR-004 · Close the six required observability signals with what already exists

```
Need             AI_ENGINEERING_CONSTRAINTS.md § 17 and BACKLOG.md's Observability row both
                 require proving useful signals exist — latency, error, throughput,
                 failures, processing, pipeline state — before instrumenting anything.
                 Nobody had run that proof; absence was assumed, which is itself an
                 unverified claim, the same disease this project rejects everywhere else.
Evidence         `make observability-prove-signals` (new script, this CR) measured the six
                 signals against the real system, not intention:
                   - processing: 5/5 Sources covered, proportionally. Three have a
                     contract-documented RunLogger mirroring timestamped progress to
                     `_run.log` (mercadona_catalog_api 226.8-338.8 s, ine_population_api
                     2,098-5,120 s, ine_callejero 0.4 s, all measured from local partitions).
                     The two synthetic Sources (simulated_oltp 0.7-2.1 s, simulated_orders
                     1.8-6.0 s) don't have one, and the same reasoning that justified
                     RunLogger on the other three doesn't apply to them.
                   - throughput/failures: already reached GOLD for the 3 sources with a `wh`
                     axis, via FACT_INGESTION_RUN (declared_rows, failure_count,
                     anomaly_count, complete) — measured present before this CR.
                   - latency: duration_seconds/started_at_utc/finished_at_utc existed in
                     raw_manifest.sql, silver_oltp_manifest.sql and
                     silver_orders_manifest.sql (Silver, zero nulls across the 90 historical
                     partitions measured) and were dropped by a fixed column list in
                     snowflake_export.py's STG_INGESTION_RUN spec before reaching
                     STAGE/GOLD — confirmed by reading the SQL, not inferred.
                   - error: cli.py had exactly 5 `except Exception as exc:` blocks that
                     printed only str(exc), never a traceback — reproduced live by forcing
                     an unmapped RuntimeError through main()'s final handler.
                   - pipeline state: grep across all 6 DAGs for `on_failure_callback`
                     returned zero matches; a failed task left no record beyond the
                     scheduler's own internal state.
Insufficiency    Sufficient already for processing (5/5, proportional) and for
                 throughput/failures at GOLD (3/5, the sources with a `wh` axis). NOT
                 sufficient for: latency at GOLD (existed two layers upstream, silently
                 dropped), error diagnosis in cli.py (traceback lost on every unmapped
                 exception, defeating the exit-code-3 distinction this project already
                 relies on elsewhere), and task-level pipeline state in Airflow (a failure
                 left only the UI, no structured trace).
Component        Platform (`retail_platform.snowflake_export`, `retail_platform.cli`,
                 `retail_platform.snowflake_evidence`), dbt
                 (`models/warehouse/gold/fact_ingestion_run.sql` + `schema.yml`),
                 orchestration (all 6 DAG files), plus a new script
                 (`scripts/prove_observability_signals.py`) and Makefile target
                 (`observability-prove-signals`). The 5 Sources are explicitly OUT of this
                 CR's component list — FROZEN, and the evidence showed they don't need it.
Contracts        `STG_INGESTION_RUN` and `FACT_INGESTION_RUN` gain 3 columns
                 (`started_at_utc`, `finished_at_utc`, `duration_seconds`), additive — no
                 existing column changes name, type, or meaning. Two new `not_null` tests on
                 the Gold model (zero nulls measured across 90 historical partitions before
                 adding them). No Source CONTRACT.md changes — nothing about the manifest
                 schema changed, only what the platform projects downstream from fields the
                 manifest already declared.
Regression       Found the hard way: widening the STAGE projection broke 7 tests in
                 test_snowflake_export.py (fixture tables didn't have the 3 new columns —
                 DuckDB BinderException) and 1 in test_snowflake_evidence.py
                 (test_ha_amostra_para_cada_mart assumed a strict 1:1 between AMOSTRAS and
                 mart files on disk, and FACT_INGESTION_RUN is deliberately not a mart).
                 Fixed both — the fixtures now carry realistic timestamps, and the mart-count
                 test names the one exception explicitly instead of loosening silently. `make
                 test` back to 1,095/1,095 green. `make silver` (391/391) and `dbt build
                 --target snowflake` (202/202, including the new not_null tests, against the
                 real account) both green. `DagBag` reload against the live scheduler:
                 zero import errors across all 6 DAGs after adding the callback.
Semantics        No field changes meaning. `declared_rows` stays "rows/events declared by
                 the source"; `duration_seconds` is simply visible one layer further
                 downstream than before.
Provenance       Unchanged. The 3 new Gold columns come from the same manifest the rest of
                 FACT_INGESTION_RUN already reads; no new source of truth introduced.
Reproducibility  Yes — the change is purely additive projection, not a new computation.
Cost             Two SQL projections widened, one dbt model +6 lines (18 → 24, both prose
                 headers updated to match), 5 exception sites in cli.py gained
                 `logging.exception()` plus one `logging.basicConfig()` call (stdlib, zero
                 new dependency), one small `_log_task_failure` function duplicated across 6
                 DAG files (matching this project's own no-shared-module convention in that
                 folder — `_run()` is already duplicated the same way), one entry added to
                 `snowflake_evidence.py`'s existing `AMOSTRAS` dict. No new file format, no
                 new service, no new dependency, no page created from scratch.
                 EXPLICITLY REJECTED as part of this same CR, and recorded rather than
                 silently skipped: extending FACT_INGESTION_RUN to ine_population_api and
                 ine_callejero (no proven consumer — moved to BACKLOG.md with a trigger); the
                 full `application -> OTel -> Collector -> Grafana/Prometheus` stack (no
                 proven need for cross-service trace correlation or real-time alerting —
                 BACKLOG.md's Observability row narrowed to name exactly that remainder).
Proof            `make observability-prove-signals`, before → after: latency NAO → SIM,
                 error NAO → SIM, pipeline state NAO → SIM (processing, throughput/failures
                 were already SIM; coverage stays PARCIAL 3/5, by the decision above, not by
                 gap). `make silver && make warehouse-refresh && make warehouse-evidence` run
                 live against the real Snowflake trial account (2026-09-08): `dbt build
                 --target snowflake` 202/202 including the 3 new not_null tests, and
                 docs/warehouse-evidence/README.md § FACT_INGESTION_RUN shows 90 real rows,
                 with the 8-row sample carrying non-null started_at_utc/duration_seconds and
                 a computed rows_per_second (throughput) for every row — mercadona_catalog_api
                 across 4 warehouses × 2 days, 226.8-285.0 s, 16.0-20.2 rows/s. A forced
                 unmapped RuntimeError through cli.py's main() showed a full traceback where
                 it previously showed one line. `DagBag(...).import_errors == {}` against the
                 live scheduler, and `task.on_failure_callback` confirmed attached on a real
                 task instance.
Loss             `fact_ingestion_run.sql`'s "18 LINES" framing (and schema.yml's matching
                 description) stopped matching the real line count and was updated in the
                 same change, to the same discipline this project applies to every other
                 hand-written number. Ingestion for ine_population_api and ine_callejero
                 remains without a queryable "was this observed" fact — an accepted, recorded
                 gap, not an oversight.
```

Decision logged on 2026-09-08. The Snowflake-side proof (`make warehouse-refresh` +
`make warehouse-evidence`) ran against the live trial account with the user's explicit
go-ahead, as a separate approval from the code change itself — the same distinction CR-001
already drew between writing something and watching it run for real.
