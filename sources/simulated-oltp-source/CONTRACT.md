# Source Contract — `simulated_oltp`

Contract between this Source and any component that consumes its snapshots.
Everything here is enforced by code and covered by tests, except where marked **[source]** —
a characteristic of the INE data, outside this Source's control.

**Structural difference from the other three Sources: this one is DERIVED.** The other three
are upstream of external data (Mercadona API, INE API, Callejero files); this one consumes
the Silver they produced and returns a new RAW. The inversion of direction is deliberate,
and it is what allows generating synthetic customers without inventing geography.

**There is no network at any step.** `extract` here makes no request at all: it reads three
flat JSON files the platform precomputed from Silver with `retail-platform
export-oltp-reference`, and generates the customers from them. The verb is kept for
interface uniformity; the meaning changes — same precedent as `ine_callejero`'s
`extract --in <dir>`, one level up the chain.

**Why the reference exists:** every Source in this repo is FROZEN (`dependencies = []`,
verified by AST in `tests/test_dependencies.py`), and `duckdb`/`boto3` are platform-only
dependencies by design. A Source cannot open a connection to the Lakehouse without breaking
that boundary. The reference is a **physical** contract between the two sides: neither
imports the other's code.

## 1. Snapshot identity

| Key | Value | Where it lives |
|---|---|---|
| `source` | `simulated_oltp` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | partition directory and `partition.ingestion_date` |
| `wh` | warehouse (`mad1`, `bcn1`, `svq1`, `vlc1`) | partition directory and `partition.warehouse` |

One partition **per warehouse**, as in the Mercadona source. It is not an aesthetic choice:
`platform/src/retail_platform/manifest.py` derives `axis_name = "wh" if warehouse else
None`, and those are the only two layouts the manifest reader understands. It also gives
failure isolation — if `vlc1` fails, the other three are not affected.

```
<root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/
├── customers.json
├── _manifest.json
└── _SUCCESS          (only exists when complete = true)
```

Top-level blocks of `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `reference`, `totals`, `schema_fingerprint`, `files[]`,
`failures[]`, `anomalies[]`, `history[]`.

## 2. Guaranteed content

`customers.json` is a **JSON array** of objects, one per customer. Every object has exactly
these fields, always present:

| Field | Role | Origin |
|---|---|---|
| `customer_id` | synthetic customer identity | generated: `cust_<wh>_<6-digit index>` |
| `wh` | lineage: which warehouse generated it | `--wh` argument |
| `province_code`, `province_name` | lineage: province | `warehouse_province_map_seed` |
| `municipality_code`, `municipality_name` | lineage: municipality | INE 29005 |
| `candidate_index` | lineage: **which tramo** | index in `address_candidates.json` |
| `street_name` | street or pseudo-street | Callejero (`VIAS`/`PSEU`) |
| `postal_code` | postal code | Callejero (`TRAM`, field `CPOS`) |
| `numbering_type` | `"0"`/`"1"`/`"2"`, verbatim | Callejero (`TRAM`, field `TINUM`) |
| `house_number` | integer **or `null`** | drawn within the tramo's real range |
| `first_name`, `last_name` | name | curated list embedded in the package |
| `sex_label` | `"Hombres"` / `"Mujeres"` | sampled by municipal proportion |
| `birth_year` | year of birth | `year(ingestion_date) − sampled age`, with age always `>= min_customer_age` |

**The customer is invented; the place where they live is not.** Municipality, street,
postal code and numbering range always come from a real Callejero row. Nothing is built out
of loose parts, and no fictitious postal code, municipality or street is generated.

### How many customers each warehouse has

**It is no longer an argument.** When `--count` is omitted, the target comes from
`customer_allocation`, in the header of `municipality_population_weights.json` — never from
a fixed number passed in from outside, which would treat FUAs whose population differs by
**4.6×** as if they were the same size, without anything in the totals exposing the error:

```
observed municipal population (INE 29005)
  × adult share of that municipality's province (INE 31304, ages >= min_customer_age)
  × penetration rate
  = customers of that warehouse
```

| wh | served pop. | adult share | adult pop. | customers |
|---|---:|---:|---:|---:|
| mad1 | 7,104,034 | 82.393 % | 5,853,206 | 128,771 |
| bcn1 | 5,277,804 | 82.247 % | 4,340,819 | 95,498 |
| vlc1 | 1,885,230 | 82.689 % | 1,558,871 | 34,295 |
| svq1 | 1,585,157 | 81.041 % | 1,284,630 | 28,262 |
| | **15,852,225** | | **13,037,526** | **286,826** |

**The total is a consequence, not a quota.** Each warehouse comes from its own population;
adding a municipality to the service area adds customers, instead of taking them from the
others.

`--count` still works and becomes an **explicit override**, recorded in the manifest as
`config.count_source: "cli"` — without that, a hand-typed base would be indistinguishable
from a population-derived one.

### The penetration rate, and the two assumptions it carries

The rate is **2.2 %**: e-commerce's share of total food volume in 2025 (MAPA report,
section 3). It is an **observed** number, and it is **not** declared in the customer-base
domain — `customer_premises.customer_penetration_source` points to
`demand_profile.channel_reference_pct`, where it was measured. Copying it would create two
places to change it.

Two assumptions, **neither measured**, turn a volume share into a share of people:

1. **The online shopper consumes like the average.** Under it, 2.2 % of the volume
   corresponds to 2.2 % of the people. No source in this repository measures customer
   penetration.
2. **These four warehouses model the FUA's entire online channel**, not one operator within
   it. Applying an operator's market share would require a source this repository does not
   ingest.

### Sampling chain

```
warehouse → municipality weighted by observed municipal population (INE 29005)
          → UNIFORM tramo among that municipality's valid candidates
          → house number within the real range, respecting parity
          → sex by observed municipal proportion (INE 29005)
          → age by the ADULT PROVINCIAL distribution (INE 31304) — proxy, truncated
          → name from an embedded curated list
```

The tramo choice is **uniform on purpose, and that is not population density**: there is no
population per street or per tramo in any source this platform ingests. Weighting tramos
would invent a distribution nobody measured. Population weight enters only once, in the
municipality choice.

### Demographic composition, explicitly

| Dimension | How it is obtained | Nature |
|---|---|---|
| Municipality | **observed** municipal population (29005, `year=2025`, `reference_date` 2024-12-31, cifras oficiales, integers) | observed |
| Sex | **observed** municipal distribution (same table) | observed |
| Age | **ADULT provincial** distribution (31304, `year=2022`, `reference_date` 2022-06-30, `fk_periodo=27`, `fk_tipo_dato=2`), truncated at `min_customer_age` and renormalized | **proxy** |
| Base size | warehouse adult population × penetration rate | derived |

#### The customer base is not the resident population

INE's age distribution is **population-based**, and population includes minors — **a
customer base is not a census**. While age drives nothing downstream, a three-year-old
account holder is harmless; from the moment it starts guiding demand, the same base without
an age floor would produce a relevant fraction of account holders below the age of
majority, without anything breaking in the totals.

The minimum age is a **platform** assumption (`customer_premises.min_customer_age`), not this
Source's, and arrives already applied: `province_age_distribution.json` carries only ages
`>= 18`, renormalized to sum to 1 per province, with `min_customer_age` declared in the
header next to the two aggregate labels that were already excluded. This Source **distrusts
the promise**: it rejects a reference without the header, rejects a reference whose rows
contradict the header, and `validate` recomputes the age of each landed customer.

Four declared approximations, none hidden:

1. **Age is provincial, not municipal.** There is no age band per municipality in the tables
   this platform ingests — 29005 has no age column and 31304 only has province. Every
   municipality in a province gets the same pyramid shape.
2. **Different vintages.** The real lag between the two inputs is **2 years and 6 months**
   (2024-12-31 vs 2022-06-30), measured by `reference_date` and not by `year`.
3. **Different natures.** 31304 with `fk_tipo_dato=2` carries **estimates** (non-integer
   values, e.g. Madrid 2022 = 6,825,005.307136); 29005 carries cifras oficiales. Mixing the
   two is a second approximation.
4. **The customer is an adult individual, not a household.** A supermarket account belongs,
   in practice, to a household, and Spain has about 2.5 people per household. Modeling that
   would require household composition per province, which this platform **does not
   ingest** — the same prohibition that blocked the household life-cycle cut in Phase 5.
   Declared consequence: the base is a fraction of **adults**, not of **households**.

In addition, **sex and age are sampled independently**. 31304 has age crossed with sex, and
that real correlation (women dominate the older bands) is discarded by construction. The
truncation at 18 accentuates this slightly — the older bands, where the difference is
larger, weigh more in the base — and it is still not modeled.

## 3. Guarantees

1. **Atomic write.** No file is observable in a partial state (temporary file in the same
   directory + `os.replace`, with `fsync`).
2. **Canonical form.** `customers.json` and `_manifest.json` are serialized with
   `ensure_ascii=False, sort_keys=True, indent=2` + trailing newline, so that the SHA-256
   depends only on the content.
3. **Verifiable integrity.** The manifest declares `sha256`, `bytes` and `records`;
   `validate` recomputes all three from disk.
4. **Reproducibility by seed.** The same `(reference, wh, count, seed, ingestion_date)`
   produces a **byte-for-byte identical** `customers.json`. A single
   `random.Random(seed)` instance is always consumed in the same order; nothing iterates a
   `set` or a rebuilt `dict`, so the output does not depend on `PYTHONHASHSEED` (verified in
   a subprocess, with three different values).
5. **No clock.** `birth_year` derives from `ingestion_date`, never from `datetime.now()`.
6. **Closed inventory.** Every file inside the partition is declared in the manifest.
7. **Immutability.** A partition with `complete: true` cannot be rewritten without an
   explicit `--overwrite`.
8. **Completion marker.** `_SUCCESS` exists if and only if `complete: true`, and is always
   written **after** the manifest.
9. **Provenance preserved.** `reference{}` records the `ingestion_date` of each Silver
   input; `history[]` accumulates each previous run **with the seed it used** — without
   that, an `--overwrite` would swap the people behind the same `customer_id` without
   leaving a trace.
10. **Geospatial coherence verified.** `validate` re-reads the reference and checks,
    customer by customer, that the address matches the source row field by field and that
    the municipality belongs to that warehouse's Functional Urban Area.
11. **No fabricated address.** `house_number` is `null` — never an invented number —
    whenever the source does not support a real number (see section 6).
12. **Totals re-checked.** `totals.customer_rows`, `municipalities_used`,
    `house_number_null` and `bytes` are recomputed from the re-read, and `config.count` must
    match the real count.
13. **Stable `schema_fingerprint`.** It is the union of the observed keys. That is why
    `house_number` is **always present**, even when null: an optional key would make the
    partition fingerprint depend on the seed.
14. **Distrusted reference.** A reference that is missing, unreadable, has empty `rows` or
    lacks a required field fails the generation — it never silently produces biased
    customers.
15. **No account holder below the minimum age.** In three independent layers: the platform
    delivers the distribution already truncated, `reference_data` rejects a reference whose
    header is missing or whose rows contradict it, and `validate` recomputes
    `year(ingestion_date) − birth_year` for **each landed customer**. The minimum age always
    comes from the reference, never from a constant in the package.
16. **The base size is derived, not typed.** Without `--count`, it comes from
    `customer_allocation`; with `--count`, the manifest records `count_source: "cli"`. There
    is no default: a reference without a target **fails** instead of picking a number.

### Non-additive conditions

Growing the base with the **same** seed, date and scope is additive — the first N customers
come out byte-for-byte identical. These four things are **not**: they swap the people behind
the same `customer_id`, require `--overwrite`, and force regenerating the orders that point
to those customers.

| condition | where it lives |
|---|---|
| `seed` | manifest `config.seed` |
| `ingestion_date` | `birth_year = year(ingestion_date) − age` |
| `min_customer_age` | `customer_premises_seed`, reflected in `reference.min_customer_age` |
| penetration rate or allocation rule | `demand_profile_seed` / `customer_premises_seed`, reflected in `reference.penetration_pct` and `reference.allocation_rule` |

All four are recorded in the manifest precisely so that two partitions with the same count
and the same appearance are distinguishable.

## 4. Consumer obligations

1. **Iterate `manifest.files[]`.** `files[].path` is relative to the snapshot root, two
   levels above the partition (`ingestion_date=…/wh=…/`).
2. **Treat the partition as immutable.** Do not write inside it.
3. **`customer_id` is a partition key, not a persistent identity.** These are two different
   guarantees:
   - **Uniqueness:** holds across the whole `ingestion_date` — `wh` is embedded in the id
     itself, so `cust_mad1_000042` and `cust_vlc1_000042` never collide.
   - **Referent stability:** holds only within `(wh, ingestion_date, seed)`. An
     `--overwrite` with another seed keeps the same ids and swaps the people behind them.
     Concrete proof: `birth_year` derives from `ingestion_date`, so **the same seed on
     another day produces another person under the same id**.

   Customer identity across days is neither provided nor derivable from here. See
   section 5.
4. **The address is an assignment, not identity.** The geographic fields are a *reference*
   to a real Callejero row, not part of the customer's identity. Two consequences that are
   **not bugs**:
   - `customer_id` never derives from any geographic field;
   - two customers may receive the same tramo **and the same `house_number`**, i.e. an
     identical address. Expect repetition; it is not removed here.
5. **Trace the tramo by `candidate_index`, not by postal code + street.** Measured: within
   the scope of the 4 FUAs (216,594 tramos), `(street_code, postal_code)` produces 105,112
   combinations, of which only 28,886 (**13.3%**) are unique — the largest covers 70 tramos.
   The real tramo grain has 11 columns. `candidate_index` points to the exact row of the
   reference declared in `manifest.reference`, and is the only exact audit path.
6. **`house_number: null` means "does not exist", not "unknown".** It is qualified by the
   companion field `numbering_type` next to it. See section 6.

## 5. What the Source does not do

- **Does not talk to the Lakehouse.** No `duckdb`, no `boto3`, no dbt, no importing anything
  from the platform. The reference arrives as flat JSON.
- **Does not touch the network.** No socket, at any step.
- **Does not provide persistent customer identity.** There is no merge across days, and this
  Source produces no Silver model — the consumer versions the identity. Since Phase 2 that
  exists on the platform side (`silver_customer` and the SCD2 dimension `DIM_CUSTOMER`), and
  it remains the consumer's responsibility, not this Source's. The guarantee here is the
  same as always: `customer_id` is a partition key.
- **Does not generate Orders, stock or delivery.** Phase 1 is strictly Customers.
- **Does not normalize the source vocabulary.** `numbering_type` and `sex_label` carry the
  INE labels verbatim.
- **Does not fix the Silver.** Where the upstream data has a defect (see section 6), this
  Source defends itself explicitly and measurably, but does not repair it.

## 6. Source scope and limits **[source]**

Everything measured against the real Lakehouse (Callejero `ingestion_date=2026-08-25`,
population `2026-08-26`), within the scope of the 4 FUAs.

- **216,594 tramos with a postal code** in the 370 municipalities of the FUAs (`mad1`=128,
  `bcn1`=133, `svq1`=46, `vlc1`=63 municipalities). No municipality belongs to two
  warehouses.
- **3 orphan tramos** match neither a street nor a pseudo-street. They are excluded from the
  candidates and the count is kept in `excluded_rows.no_street_or_pseudo_match`. That leaves
  **216,591 candidates**.
- **838 tramos are pseudo-streets** (`street_id = "00000"`), where the name comes from
  `PSEU`.
- **9,024 tramos (~4.2%) have `numbering_type = "0"`**: INE declares that the street has no
  numbering scheme. Drawing a number here would fabricate geography — the same prohibition
  as for the postal code, one level down. `house_number` stays `null`.
- **14 tramos have `numbering_type = "2"` and range `0000..0000`.** Filtering by
  `numbering_type` alone does **not** catch this case: drawing in `[0,0]` would give house
  number 0, which does not exist. 0 is excluded from the sampleable set, and a range without
  a number of the declared parity also falls back to `null`.
- **Number qualifiers are not carried.** 4,444 and 4,473 rows of the dataset have a
  qualifier (`A`/`B`/`Z`…) at the ends of the range. A `house_number` "12" for a tramo whose
  real interval is `0012-A..0012-Z` is an approximation.
- **The two INE products spell the same municipality differently in 370 of 370 cases**: the
  Callejero uses upper case with the article in parentheses (`"BRUC (EL)"`), table 29005
  uses mixed case with the article postponed (`"Bruc, El"`). Both coexist in the reference
  under distinct labels (`municipality_name` and `municipality_name_callejero`); the
  customer carries the 29005 one. **The comparable key is always the code, never the name.**
- **National homonyms: resolved upstream, re-checked here.** The RAW 29005 table is national
  (~8,200 municipalities) and carries series with an identical `Nombre` for homonymous
  municipalities in different provinces. Until 2026-08-27 the name join in
  `silver_ine_population_by_municipality` matched both, and three FUA municipalities ended
  up with two diverging rows:

  | Municipality | Correct series | Intruding series |
  |---|---|---|
  | Arroyomolinos (28/015, `mad1`) | `DPOP12967` = 38,075 | `DPOP4729` = 816 (Cáceres) |
  | Molar, El (28/086, `mad1`) | `DPOP13174` = 9,999 | `DPOP19423` = 295 (Tarragona) |
  | Torrent (46/244, `vlc1`) | `DPOP21778` = 90,928 | `DPOP7960` = 182 (Girona) |

  Fixed in the model with the **official INE code** (`ine_ambiguous_series_seed`, derived
  from `VALORES_SERIE/{COD}`), not by a value heuristic. The export only **verifies** the
  invariant — one series per `(municipality, sex, year)` — and **refuses** if it breaks,
  instead of picking a value on its own.
- **The 103 `age_label`s of 31304 do not form a partition.** Alongside the 101 single ages
  live two overlapping aggregates, `"Total"` and `"85 y más años"`. Naively summing the 103
  gives **2.03×** the correct value (measured on Madrid/2022: 27,724,421 vs 13,650,010,
  which is exactly the `"Total"` label), and `"85 años"` would collide with
  `"85 y más años"` in the same bucket when cast to integer. Both are excluded, and
  `"100 y más años"` — a legitimate terminal bucket — remains.
- **`silver_ine_population_series` is duplicated across two `ingestion_date`s** with
  identical rows. The export pins `max(ingestion_date)`.

## 7. Exit codes

| Code | `extract` | `validate` |
|---|---|---|
| 0 | partition complete | validation passed |
| 1 | — | validation failed |
| 2 | fatal failure: nothing was generated, partition unusable | — |
| 3 | unhandled exception (no traceback escapes) | same |

`validate` **requires `--reference`**, unlike the other three Sources: the central guarantee
— every customer lives at a real address in their warehouse's FUA — is only verifiable by
re-reading the same reference that generated the partition. A validator that only checks
checksums would prove integrity, not coherence.

## 8. Contract version

`manifest_version: 1`. Each Source has its own counter — they are independent contracts. The
platform declares the version it can read in
`retail_platform.SUPPORTED_MANIFEST_VERSIONS["simulated_oltp"]` and rejects any other,
instead of interpreting by guesswork.
