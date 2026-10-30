# Source Contract — `simulated_orders`

Contract between this Source and any component that consumes its snapshots.
Everything here is enforced by code and covered by tests, except where marked **[source]** —
a characteristic of the upstream data, outside this Source's control.

**Structural difference from the other four Sources: this one delivers a LOG, not a snapshot.**
The other four produce the state of something at an instant. This one produces the sequence of
events that made the state change, and the state exists nowhere in the partition — it is the
**fold** of the events, and the consumer is the one who materializes it.

**It is the second DERIVED source**, like `simulated_oltp`: it consumes the Silver the others
produced and returns a new RAW. Phase 1's golden rule still holds, shifted one level:

> The **order** is invented; who buys, what is bought, what it costs and where they live are not.

**There is no network at any step.** `extract` here makes no request at all: it reads five flat
JSON files (`customers`, `catalog`, `calendar`, `premises`, `demand_profile`) that the platform
pre-computed from Silver and the versioned seeds with `retail-platform
export-orders-reference`, and generates the events from them. The verb is kept for interface
uniformity — same precedent as `extract --in <dir>` in `ine_callejero` and `extract
--reference` in `simulated_oltp`.

## 1. Snapshot identity

| Key | Value | Where it lives |
|---|---|---|
| `source` | `simulated_orders` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) — **the ORDER date** | partition directory and `partition.ingestion_date` |
| `wh` | warehouse (`mad1`, `bcn1`, `svq1`, `vlc1`) | partition directory and `partition.warehouse` |

One partition **per warehouse and per day**. The two axes are not an aesthetic choice:
`platform/src/retail_platform/manifest.py` derives `axis_name = "wh" if warehouse else None`,
and those are the only two layouts the manifest reader understands.

```
<root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/
├── order_events.jsonl
├── _manifest.json
└── _SUCCESS          (only exists when complete = true)
```

**`ingestion_date` is the ORDER date, not the event date.** Every event of an order lives in
the partition of the day the order was *placed*, even when `occurred_at` crosses midnight — an
order placed at 22:40 and delivered at 9:00 the next day leaves **all** its events here.

The alternative — partitioning by event date — would make a day's partition impossible to
close: it would only be complete two days later, when that day's last order finished, and
`_SUCCESS` would lose its meaning. See obligation 4.2.

Top-level blocks of `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `reference`, `totals`, `schema_fingerprint`, `files[]`,
`failures[]`, `anomalies[]`, `history[]`.

## 2. Guaranteed content

`order_events.jsonl` is **NDJSON**: one line per event, each line a complete, independent JSON
object. **It is not a JSON array**, and that is the only deviation from the canonical form
relative to the other four Sources — justified in guarantee 2.

### 2.1 Envelope — identical in every event

| Field | Role | Origin |
|---|---|---|
| `event_id` | event identity | `sha256("<order_id>|<sequence_no>")[:32]` |
| `event_type` | one of the 12 in the vocabulary | state machine |
| `event_version` | `payload` version | `1` |
| `occurred_at` | event instant, ISO-8601 UTC | `ingestion_date` + declared offsets |
| `order_id` | aggregate **and** broker partition key | `ord_<wh>_<YYYYMMDD>_<6 digits>` |
| `sequence_no` | position in the aggregate, `1..N` with no gaps | emission order |
| `wh` | lineage: which warehouse | `--wh` argument |
| `producer` | who emitted it | `simulated_orders` |
| `payload` | type-specific | see 2.2 |

**`recorded_at` does not exist here, on purpose.** Delay and reordering are properties of the
*transport*, and whoever consumes measures them — on the other side of the broker, with their
own clock. Stamping them here would invent a delay nobody observed, and would destroy the
byte-for-byte reproducibility of the log.

### 2.2 Event vocabulary and state machine

| # | `event_type` | state after | requires state | payload |
|---|---|---|---|---|
| 1 | `order_placed` | `PLACED` | — | `customer_id`, `customer_ingestion_date`, `buyer_age_band`, `province_code`, `municipality_code`, `postal_code`, `price_as_of`, `price_source`, `delivery_slot_start`, `delivery_slot_end`, `line_count`, `gross_amount`, `lines[]` |
| 2 | `order_payment_authorized` | `CONFIRMED` | `PLACED` | `payment_method`, `authorized_amount` |
| 3 | `order_payment_failed` | `PAYMENT_FAILED` ⏹ | `PLACED` | `decline_reason` |
| 4 | `order_cancelled` | `CANCELLED` ⏹ | `PLACED`, `CONFIRMED` | `cancelled_by`, `reason` |
| 5 | `order_picking_started` | `PICKING` | `CONFIRMED` | *(empty)* |
| 6 | `order_line_substituted` | `PICKING` | `PICKING` | `line_no`, `source_product_id`, `substitute_source_product_id`, `substitute_unit_price`, `quantity` |
| 7 | `order_line_removed` | `PICKING` | `PICKING` | `line_no`, `source_product_id`, `quantity`, `reason` |
| 8 | `order_picked` | `PICKED` | `PICKING` | `picked_line_count`, `picked_amount` |
| 9 | `order_dispatched` | `IN_TRANSIT` | `PICKED` | *(empty)* |
| 10 | `order_delivered` | `DELIVERED` | `IN_TRANSIT` | `delivered_within_slot`, `delivered_line_count`, `delivered_amount` |
| 11 | `order_delivery_failed` | `DELIVERY_FAILED` ⏹ | `IN_TRANSIT` | `reason` |
| 12 | `order_returned` | `RETURNED` ⏹ | `DELIVERED` | `returned_line_count`, `returned_line_no`, `returned_amount` |

⏹ = terminal state: no event can leave it. **`DELIVERED` is not terminal** — a return can
still come afterwards, which is why it does not close the aggregate.

Each row of `lines[]` carries `line_no`, `source_product_id`, `category_id`, `subgroup_id`,
`demand_group`, `quantity` and `unit_price`.

`demand_group` is the label the basket was calibrated against (§2.6). It travels **on the
event**, and is not rediscovered by a downstream join: what matters is the group in force at
order time, not what the mapping would say today.

`unit_price` is the price of the **purchasable portion**, which differs from the raw API
`unit_price` in the 10 product×warehouse combinations sold in bulk without `unit_size` — there
the source returns `reference_price × 99`, the ceiling of the weight selector (obligation 5 of
the Mercadona contract).

### 2.3 The fold is non-trivial, and that is the reason the Source exists

Events 6 and 7 change the basket **after** placement. So:

> `picked_amount` **cannot be derived** from `gross_amount`. The order's value only exists
> after folding the log.

If it could be derived, the log would be a date stamp and the event model would be decoration.
An inverted dbt test watches exactly that on the consumer side.

### 2.4 What is real and what is invented

| Dimension | Nature |
|---|---|
| Customer, and their municipality/postal code | **observed** (via `silver_customer`, whose address comes from the Callejero) |
| Ordered product | **observed** — exists in that warehouse's catalog on that day |
| Price paid | **observed** — it is the `unit_price` of that `(warehouse, date, product)` |
| Warehouse and date | **observed** |
| Demand group of each line | **calibrated against a benchmark** — see 2.6 |
| Buyer's age and autonomous community | **observed** (INE 31304 and Callejero, via `silver_customer`) |
| Per-group propensity of each cohort | **calibrated against a benchmark** — see 2.7 |
| Basket, cadence, times, substitution/cancellation/return rates | **declared synthetic** — see 2.5 |

Three natures, and the distinction between the second and the third is new in this phase:
**observed** comes from a source this platform ingests; **benchmark** comes from an external
official source it does *not* ingest and uses only as a distribution reference;
**synthetic** has no anchor at all. A benchmark is not our data — calling it that would turn it
into a false representation of reality.

### 2.5 Premises — all synthetic, all declared

No source ingested by this platform measures sales, baskets, purchase cadence or product
availability. Every premise lives in `platform/dbt/seeds/order_premises_seed.csv`, is labeled
`synthetic` (never `observed`, never `proxy` — a different label **fails** the export), and its
`sha256` travels to `config.premises_sha256` in the manifest.

`daily_order_rate` has **no observational anchor at all**: that is why it lives in a seed anyone
can edit and rebuild, and not in a constant hidden in the generator. There is no default for
any premise — a missing key fails the generation.

**Since CR-017 the premises fall in two groups, and the seed says which is which:**

| Group | Premises | Origin |
|---|---|---|
| Rules a real operator publishes | no delivery on Sunday (`delivery_closed_isoweekday`), slots 07:00-22:00 (`delivery_open_hour`/`close_hour`), one-hour slot, never delivered on the order day (`slot_lead_day_weight_*` starts at 1), 50 EUR minimum (`min_order_amount_eur`) | Mercadona online service conditions, cited in each row |
| Shapes nobody in this repo measures | customer cadence (`cadence_weight_*`, `order_skip_rate`), weekday (`order_weekday_weight_*`), placement hour in LOCAL time (`order_hour_weight_*`), slot hour, on-time/early/late split, fresh vs other substitution and removal rates, quantity tiers by price | declared, `synthetic` |

The cadence weights and `order_skip_rate` are declared so that they **reproduce**
`daily_order_rate` (`assert_order_premises_are_internally_coherent`): the cadence replaced the
daily lottery without moving the rate the MAPA channel closure was built on.

### 2.6 Demand model — calibrated against MAPA 2025

`demand_model_version` travels in `config.demand_model_version` in the manifest. Version in
force: **`mapa_2025_v2`**. `v2` adds the cohort layer described in 2.7; everything below
describes the aggregate and still holds word for word.

**The chain, with price outside the demand path:**

```
demand group       P(g)  <- MAPA VOLUME target (kg/L), tilted by the e-commerce channel
       |
product in group         <- popularity_weight (own brand x2, price relative to the group
       |                    median ^ elasticity FITTED against MAPA EUR/kg) — platform-computed
quantity                 <- w(k)=1/2^(k-1), up to the product's quantity_max (price tiers)
       |
unit price               <- OBSERVED (Mercadona purchasable portion)
       |
order value              <- consequence, never a target
```

Price appears in no arrow pointing at the demand GROUP. An expensive category can have low
volume and high revenue — in MAPA, shellfish are **0.81% of volume and 2.88% of value** —, and
that is a result of the model, not a defect. Inside a group, since CR-017, price does weigh the
choice of product — relative to the group median, so the group's share of lines is untouched.

**What the benchmark calibrates:** the probability of a demand group appearing on a line.
**What it does not calibrate, and cannot:** `daily_order_rate`, `basket_lines_min/mode/max` and
`quantity_max`. MAPA measures residents' household consumption — it does not measure
online-store orders, baskets or purchase cadence. Those remain `synthetic` in
`order_premises_seed.csv`.

**The bridge between a kg target and line draws** is the average kg per line *as drawn* —
package size x expected units, weighted by the same `popularity_weight` the generator uses:
`P(g) ∝ volume_target_g / avg_kg_per_line_g`. A group whose typical package
weighs 1 kg needs fewer lines than one whose typical product weighs 100 g to deliver the same
volume. Groups with less than 50% of the assortment convertible to kg/L (`reference_format` in
`ud`, `dz`) do **not** get a volume target — measured: `HUEVOS` converts 18% and falls into the
declared line-share fallback.

**Seasonality: neutral across all 12 months, for lack of numerical evidence.** The report's
monthly charts are images; there are only five monthly numbers in prose, all for total food and
never per category. Moreover the window only covers August. The mechanism exists, applies to
the **order rate** (never to the mix — a global factor on the mix would normalize away and do
nothing) and has a test proving that a non-neutral profile changes the output.

The eight versioned seeds — `mapa_2025_benchmark_seed.csv`,
`demand_category_mapping_seed.csv`, `demand_profile_seed.csv`,
`demand_seasonality_seed.csv`, `demand_cohort_age_seed.csv`,
`demand_cohort_region_seed.csv`, `mapa_2025_region_seed.csv` and `ine_ccaa_map_seed.csv` —
have their `sha256` in `reference.demand_seeds_sha256`.

### 2.7 Per-cohort buyer propensity

What `v2` adds is one floor above the aggregate: **P(group) becomes P(group | cohort)**.

The cohort has two dimensions, and they are the **only** two in which an observed customer
attribute coincides with a published cut of the report:

| Dimension | Where it comes from in the customer | MAPA cut |
|---|---|---|
| `age_band` ∈ {`LT35`,`35_49`,`50_64`,`GE65`} | `birth_year`, from INE 31304's provincial age distribution | age of the person responsible for purchasing |
| `region` (autonomous community) | `province_code`, from the Callejero | autonomous community |

**What was left out, and why.** Household life cycle (9 types) and socioeconomic level
(5 levels) are the report's richest cuts, and come with full volume and per-capita figures. The
blocker is not the data — it is the attribute: the customer has no household composition or
income, and assigning them would be **inventing the attribute**, the same prohibition Phase 1
applied to per-segment density. The customer does have the buyer's sex, but the report only
publishes it for *out-of-home* consumption. **Recorded trigger:** if a future phase ingests
INE households per province, the life-cycle cut opens up.

**The affinity index** takes numerator and denominator from the same chart, and is therefore
dimensionless:

```
idx(g, band)            = volume_share(g, band) / population_share(band)
raw_weight(g, cohort)   = w_g  x  idx_age(g, band)  x  idx_region(g, ccaa)
```

**Two new premises, named and declared in `demand_profile_seed.csv`:**

1. `cohort_independence = multiplicative` — the report publishes the two **marginals** and
   never the age × community cross. Multiplying assumes conditional independence. It is a
   premise, not a measurement.
2. `region_frequency_basis = per_capita_volume` — regional intensity shows up as **order
   frequency**, never as basket size. The report gives kg/year and **does not publish household
   purchase frequency** (`actos de compra` only appears in the out-of-home chapter), so
   splitting the intensity between the two would be inventing the split.

**Why the index cannot be read as a share.** MAPA's `% Población` is the population that
*lives in households* whose purchasing person is in that band — a household with a 40-year-old
buyer carries the children into `35_49`. Our customer is an individual. That is why the number
enters as a **relative** index between bands, and never as an absolute share.

**The aggregate does not move, and that is the acceptance criterion.** An *iterative
proportional fitting* adjusts one factor per group until the average of the per-cohort
weights — weighted by the real distribution of cohorts **among the orders** — reproduces the
`v1` aggregate weights. Without it, the aggregate mix would drift off target merely because our
age pyramid is not MAPA's, and the previous phase's calibration would be quietly undone
without anything failing.

**IPF runs WITHIN each block**, and the constraint was found by measuring. Normalizing the
whole cohort at once, `NO_FOOD` and `SIN_BENCHMARK` — which have a neutral index for *lack of
evidence* — came out at 0.60× the share in 65+ versus under 35. The model would come to claim
that people over 65 buy 40% less drugstore per basket line: nobody measured that, it was a
normalization residue, and it was **larger than most of the measured effects**.

**Minimum buyer age.** `min_buyer_age = 18`, in `order_premises_seed.csv`. INE's age
distribution, consumed by the OLTP Source, is **population-wide** — and population includes
minors. The difference between **resident** and **who places an order** only matters from the
moment age governs demand, as happens here. This Source applies the floor taken from the
premise, never from a constant of its own; the customer base itself is not changed.

**The band is resolved on the order day**, not at export, and travels stamped in
`order_placed.buyer_age_band` — same precedent as `demand_group`: it is what chose that
basket's weight vector. A customer whose birthday falls within the window has two orders in
different bands, and that is correct.

## 3. Guarantees

1. **Atomic writes.** No file is observable in a partial state (temporary file in the same
   directory + `os.replace`, with `fsync`).
2. **Canonical form, adapted to the log.** Each line of `order_events.jsonl` is
   `json.dumps(ensure_ascii=False, sort_keys=True)` **without indentation**, and the file ends
   with a newline; `_manifest.json` keeps the indented form of the other four Sources. The
   deviation is deliberate: a log is read line by line and grows by append, each line is a
   complete event — the same bytes on disk and on the topic — and DuckDB's
   `read_json(format='newline_delimited')` consumes it directly. The guarantee that matters is
   preserved: same content ⇒ same bytes ⇒ same SHA-256.
3. **Total, deterministic ordering.** Lines are sorted by
   `(occurred_at, order_id, sequence_no)`. `occurred_at` alone ties; the tie-break guarantees
   that events of the same order never come out of order.
4. **Verifiable integrity.** The manifest declares `sha256`, `bytes` and `records`; `validate`
   recomputes all three from disk.
5. **Per-seed reproducibility, with a per-day sub-seed.** Each `(wh, order_date)` derives its
   own `random.Random` from `sha256("<seed>|<wh>|<order_date>")`. `sha256` and not `hash()`,
   because CPython's `str` hash is randomized per process. Verified in a subprocess, with
   three `PYTHONHASHSEED` values.
6. **Adding a day is additive.** Since no day depends on another day's draw, generating
   `D+1` leaves the partition of `D` **byte-for-byte identical**. It is the analogue of
   Phase 1's prefix property, on a different axis.

   **The four NON-additive conditions**, which swap the orders behind the same ids:
   another `seed`; another reference (customers or catalog re-ingested); another
   `order_premises_seed.csv`; and another **`demand_model_version`**. All four are recorded
   in `config`, and regenerating under any of them requires `--overwrite`.

   `min_buyer_age` enters through the third: it is a premise, and changing it changes **who**
   is in the eligible set — hence, who is drawn. It is not a fifth condition, it is a case of
   one that already existed, and it is worth saying because intuition suggests otherwise: an
   *eligibility* rule looks like a filter, and it is a draw. Resizing the customer base
   (another reference) enters through the second condition, even though this Source does not
   change a line: since `buyer_age_band` is stamped on the event, the test
   `assert_buyer_age_band_matches_the_customer_birth_year` fails when the person behind a
   `customer_id` changes, and the whole window must be regenerated. **Customer and order are
   separate domains in the code and coupled in the data.**

   **Consequence for consumers of the Iceberg projection:** it merges state
   **monotonically**, discarding a row with a lower `last_sequence_no` — which assumes that an
   `order_id` always refers to the same order. Changing `demand_model_version` breaks that
   assumption (the `order_id` stays the same, the order behind it does not), and a rebuild
   without reset mixes the two universes in the same table. After any of the four non-additive
   conditions, use `make orders-rebuild-projection PROJECTION_RESET=1`.
7. **No clock.** Every `occurred_at` derives from `ingestion_date` plus declared offsets,
   never from `datetime.now()`.
8. **Deterministic `event_id`.** Derives from `(order_id, sequence_no)`, never from `uuid4()` —
   without that the downstream consumer could not deduplicate across replays.
9. **Contiguous `sequence_no`.** `1..N` per order, with no gaps, verified on re-read.
10. **State machine enforced at generation.** The generator advances the state through the same
    function `validate` uses to re-check: an impossible transition fails at emission time, not
    three layers further down.
11. **Closed inventory.** Every file inside the partition is declared in the manifest.
12. **Immutability.** A partition with `complete: true` cannot be rewritten without an explicit
    `--overwrite` — including when the manifest was removed and only `_SUCCESS` remained.
13. **Completion marker.** `_SUCCESS` exists if and only if `complete: true`, and is always
    written **after** the manifest.
14. **Provenance preserved.** `reference{}` records the customer and catalog `ingestion_date`s,
    that day's `price_as_of` and `price_source`, and the premises digest.
    `history[]` accumulates each previous run **with the seed and premises digest it used** —
    the two inputs that swap the orders behind the same `order_id`.
15. **Coherence verified against the reference.** `validate` re-reads the reference and checks,
    order by order: the customer belongs to the partition's warehouse; every product exists in
    that `(warehouse, date)` catalog; the price paid is the observed one, compared as `Decimal`
    and never as `float`; `gross_amount` matches the placed lines; `picked_amount` matches the
    fulfilled lines after substitutions and removals.
16. **`schema_fingerprint` stable by construction.** Unlike the other four Sources, the
    fingerprint covers the **whole declared vocabulary** — envelope, line fields and payload of
    the 12 types — and not the union of observed keys. A day with no returns would have no
    `order_returned` in the observed set, and two correct partitions would diverge by chance.
    It changes when the **code** changes, which is what it exists to detect.
17. **Totals re-checked.** Every field of `totals` is recomputed from the re-read, and
    `config.orders` must match `totals.order_rows`.
18. **Distrusted reference.** A reference that is missing, unreadable, has empty `rows`, lacks a
    required field, has a non-positive price or a `price_source` outside the vocabulary fails
    the generation — it never silently produces biased orders.
19. **No order is born before its customer.** An order whose customer only appears in a later
    generation fails the generation.

## 4. Consumer obligations

1. **Iterate `manifest.files[]`.** `files[].path` is relative to the snapshot root, two levels
   above the partition (`ingestion_date=…/wh=…/`).
2. **`ingestion_date` is the ORDER date.** For "events that occurred on day D", filter by
   `occurred_at`, **not** by `ingestion_date`. Silver exposes both columns
   (`ingestion_date` and `event_date`) precisely because both questions are legitimate and
   different.
3. **The state is the fold, and materializing it is your job.** There is no `orders.json` in
   the partition, and that is not an omission: two representations of the same truth drift
   apart. Fold the log.
4. **Sort by `sequence_no` within the order, never by `occurred_at`.** Two events of the same
   order may share the same instant; only `sequence_no` is total.
5. **Deduplicate by `event_id`.** It is deterministic on purpose: a replay delivers the same
   events, and the consumer must be able to recognize them.
6. **Treat the partition as immutable.** Do not write inside it.
7. **`order_id` is unique across the whole window, not just the partition.** `wh` and the day
   are embedded in the id itself, so `ord_mad1_20260824_000042` and `ord_mad1_20260825_000042`
   never collide. The **stability of the referent**, however, only holds within `(wh, day,
   seed, premises, reference, demand model)`: an `--overwrite` with another seed keeps the same
   ids and swaps the orders behind them. The manifest records both in `history`.
8. **Read money as decimal, never as float.** Every monetary value travels as a string, for the
   same reason as obligation 4.4 of the Mercadona contract.
9. **`price_source` qualifies the price, and is not decorative.** `carried_forward` means that
   day had no catalog snapshot for that warehouse and the last known price was carried
   forward. A mart that compares revenue across days without looking at this column compares
   prices of different vintages.

## 5. What the Source does not do

- **It does not talk to the Lakehouse.** No `duckdb`, no `boto3`, no dbt, no import of anything
  from the platform. The reference arrives as flat JSON.
- **It does not touch the network.** No socket, at any step.
- **It does not talk to a broker, database or catalog.** No `confluent-kafka`, no `psycopg`, no
  `pyiceberg`. The OLTP, the outbox, the topic and the live projection exist **one level
  further**, on the platform side. Here only a canonical log is written to disk — and it is
  precisely because the log is byte-reproducible that a replay through the broker can be
  checked against it.
- **It does not materialize the order state.** See obligation 4.3.
- **It does not provide persistent order identity across runs.** `order_id` is a key of the
  window; the referent is only stable under the same `(seed, premises, reference, demand
  model)`.
- **It does not model stock, replenishment, routes or delivery time.** See section 6.
- **It does not fix Silver.** Where the upstream data has a limit (section 6), this Source
  defends itself explicitly and measurably, but does not repair it.

## 6. Source scope and limits **[source]**

All measured against the real Lakehouse on 2026-08-28 (catalog `2026-08-24`…`2026-08-27`,
customers `2026-08-24` and `2026-08-27`).

- **There is no transactional fact in any external source of this repo.** The Mercadona
  contract records: *"No known endpoint exposes sales, orders or stock."* Every order here is
  synthetic, and that is stated, not implied.
- **There is no stock fact.** That is why `order_line_removed` carries
  `reason = "unavailable"` and **not** `"out_of_stock"`: naming it as if there were stock would
  promise data nobody measured. The rate is declared, not the consequence of a balance.
- **The product weight is a model, not an observation.** No source measures sales, turnover
  or basket composition. Until CR-017 the product was drawn uniformly within its group, and on
  the real catalog that made perfume the top non-food category and put two whole Iberian hams
  on a line. The weight now favours own brands and cheaper-than-the-group products, with the
  elasticity fitted so the basket's EUR/kg matches what MAPA measures households pay; the fit
  and its error travel in `catalog.json` (`product_choice`).
- **The delivery slot is a commercial promise on a fixed grid, never a route.** The Callejero
  has no coordinates or adjacency; computing a route or route time would be inventing
  geography. Since CR-017 the slot is chosen first, on the operating calendar, and picking and
  dispatch are scheduled backwards from it; `delivered_within_slot` therefore reads back the
  declared `delivery_on_time_rate` — it is not a measure of real punctuality. Trigger for the real thing: a sixth source with coordinates (IGN's
  CartoCiudad, or OSM).
- **The window of days is bounded by the catalog, and the bound is measured.** The 4
  warehouses have a catalog from `2026-08-24` to `2026-08-27`; `mad1` also has `08-15` and
  `08-16`, with a gap from `08-17` to `08-23`. A day with no snapshot **before or on** it for
  that warehouse **fails the reference export** instead of generating an invented price.
- **The effective substitution rate is slightly below `substitution_rate`.** When no free
  product of the same subgroup — nor, on a second attempt, of the same category — is
  available, the line stays fulfilled and no event is emitted. Inventing a substitute outside
  that warehouse's assortment would be worse: it would be inventing assortment.
- **A customer orders on a cadence, at most once per scheduled day.** Every 7/14/21/28 days on
  their own weekday, from a hash of the customer; the day's draw only decides who skips.
  Declared; no source in this repo measures purchase cadence.
- **The partition day is the UTC day of `order_placed`.** Hours are drawn in local time
  (Europe/Madrid, stdlib `zoneinfo`) and stamped in UTC; local hours that fall on the previous
  UTC day (00h-01h in summer time) are not drawn.
- **The customer's sex, age and address do not influence the basket.** Crossing demographics
  with consumption would require consumption data that does not exist. The correlation is
  discarded by construction, as Phase 1 discarded the real correlation between sex and age.

## 7. Exit codes

| Code | `extract` | `validate` |
|---|---|---|
| 0 | partition complete | validation passed |
| 1 | — | validation failed |
| 2 | fatal failure: nothing was generated, partition unusable | — |
| 3 | unhandled exception (no traceback escapes) | same |

`validate` **requires `--reference`**, like the customers Source and for the same reason: the
core guarantees — the product exists in that catalog, the price is the observed one, the
customer belongs to that warehouse — can only be verified by re-reading the same reference that
generated the partition. A validator that only checks checksums would prove integrity, not
coherence.

`--date` is **required** for `extract`, unlike the other four Sources. There the partition is
"today's snapshot" and defaulting to today makes sense; here `ingestion_date` is the order
date, and falling back to the current day would generate orders on a day that may not even
have a catalog, or outside the exported window.

## 8. Contract version

`manifest_version: 1`. Each Source has its own counter — they are independent contracts. The
platform declares the version it can read in
`retail_platform.SUPPORTED_MANIFEST_VERSIONS["simulated_orders"]` and refuses any other, instead
of interpreting by guesswork.
