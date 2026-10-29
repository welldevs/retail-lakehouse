# Source Contract — `mercadona_catalog_api`

Contract between this Source and any component that consumes its snapshots.
Everything here is enforced by code and covered by a test, except where marked **[source]** —
a characteristic of the upstream API, outside this Source's control.

## 1. Snapshot identity

Each complete run produces a snapshot identified by three keys:

| Key | Value | Where it lives |
|---|---|---|
| `source` | `mercadona_catalog_api` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | partition directory and `partition.ingestion_date` |
| `warehouse` | e.g. `mad1` | partition directory and `partition.warehouse` |

```
<root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/
├── categories/categories.json
├── catalog/category_id=<id>.json
├── _manifest.json
├── _SUCCESS          (only exists when complete = true)
└── _run.log
```

Top-level blocks of `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint`, `files[]`, `failures[]`,
`anomalies[]`, `history[]`.

**Compatibility within a manifest version:** a missing list is equivalent to an empty
list. Snapshots written before `anomalies[]` was introduced do not have the field, and must
be read as "no anomaly recorded". Consumers must use
`manifest.get("anomalies", [])`, never direct indexing.

## 2. Guaranteed content

The snapshot preserves **category**, **product**, **price**, **source metadata** and
**ingestion metadata**.

| Field | Path |
|---|---|
| Level-1 category (tree) | `categories/categories.json` → `results[].name` |
| Level-2 category (tree) | `categories/categories.json` → `results[].categories[].name` |
| Level-2 category (catalog) | `catalog/category_id=<id>.json` → `name` |
| Level-1 category (catalog) | `.categories[].products[].categories[0].name` |
| Subgroup | `.categories[].name` |
| Product | `.categories[].products[].display_name` |
| Price | `.categories[].products[].price_instructions.unit_price` |

**The JSON is the physical contract.** There is no relational schema: the structure of the
API response is preserved as received, and the Source does not define types, primary keys
or normalization.

## 3. Guarantees

1. **Atomic write.** No data file is observable in a partial state. Every write uses a
   temporary file + `os.replace`, with `fsync` before the swap.
2. **Canonical form.** Every file is `json.dumps(ensure_ascii=False, sort_keys=True,
   indent=2)` + trailing newline. The same content always produces the same bytes and the
   same SHA-256, regardless of the order in which the API returned the keys.
3. **Verifiable integrity.** `_manifest.json` declares the `sha256`, `bytes` and `records` of
   each file. `validate` recomputes all three from disk.
4. **Closed inventory.** Every file inside the partition, at any depth, is declared in the
   manifest. The extraction sweeps the directory at the end and declares valid catalog
   files the run did not visit; `validate` sweeps recursively and treats an undeclared file
   as an error.
5. **Immutability.** A partition with `complete: true` cannot be rewritten without an
   explicit `--overwrite` — even when `_manifest.json` was removed and only `_SUCCESS`
   remains. A reused file whose content diverged from the previous manifest is
   re-downloaded from the source and recorded in `anomalies[]`, never promoted to the new truth.
6. **`lang` fixed per partition.** Reopening a partition with another language is refused
   (unless `--overwrite`, which rewrites the whole partition), which makes a mixed-language
   partition impossible.
7. **Safe resume.** An incomplete partition can be completed by re-running the same
   command. A file that is unreadable, has an unexpected shape or is not in canonical form
   is re-downloaded from the source, not propagated.
8. **Provenance preserved.** `history[]` accumulates the summary of each previous run over
   the partition: `run_id`, window, requests, retries and totals.
9. **Schema change detection.** `schema_fingerprint` records the sorted set of keys of
   `product` and of `price_instructions` plus a hash. `validate` recomputes and compares.
10. **Completion marker.** `_SUCCESS` exists if and only if `complete: true`. An
    inconsistency between the two is a validation error.
11. **Identity persisted before the fan-out.** A preliminary manifest (`complete: false`)
    is written right after the category tree. An interruption midway through the
    extraction leaves the partition with an identity on disk — `source.lang`, `partition`,
    `run_id` — so guarantees 5 and 6 still hold on resume.
12. **Canonical form verified on reuse.** A file already present is only reused if its
    bytes are exactly the canonical serialization of its own content. A file with the same
    JSON value but a different spelling is re-downloaded from the source, so the declared
    SHA-256 is always the canonical digest.
13. **Anomalies recorded.** `anomalies[]` lists events handled during the extraction —
    non-canonical file, content diverging from the previous manifest, unreadable file not
    visited. `validate` reports them as warnings, without failing an already corrected
    partition.
14. **Coverage verified.** `validate` checks that every level-2 category in the tree has a
    catalog file, except in a partial partition (`--limit`), where it becomes a warning.
15. **Totals re-checked.** `totals.product_rows`, `totals.unique_product_ids`,
    `totals.catalog_files` and `totals.bytes` are recomputed from the re-read, and `records`
    and `bytes` are re-checked file by file — including the tree's `records`.
16. **A recorded failure is fatal in validation.** Any entry in `failures[]` fails the
    partition.

## 4. Consumer obligations

1. **Iterate `manifest.files[]`.** Do not `glob` the directory: the manifest is the
   authoritative inventory. **`files[].path` is relative to the snapshot root, not to the
   partition** — that is, it starts at `ingestion_date=…/wh=…/`. To resolve it:
   `os.path.join(partition, "..", "..", entry["path"])`. Guarantee 4 makes `glob` and the
   manifest equivalent in a validated partition; the rule exists for partitions you have
   not validated.
2. **Treat the partition as immutable.** Do not write inside it.
3. **Load `ingestion_date` and `warehouse` as columns.** No payload contains the
   warehouse: that identity exists only in the path and in the manifest. Ingesting the
   content without the path loses the warehouse irrecoverably.
4. **Read `unit_price` as a string.** It is a string in 100% of records. Converting to
   `float` loses decimal precision in currency.
5. **`unit_price` is not always the price of a purchasable unit.** When
   `price_instructions.selling_method = 1` (weight selector, `bunch_selector: true`) **and**
   `unit_size` is null, the API returns `unit_price = reference_price × 99` — the price of
   the selector's **ceiling**, not of anything a household buys **[source]**. Measured on
   2026-08-31 over the 9 catalog partitions: the factor is exactly `99,000` in 10
   product×warehouse combinations (105 rows at the snapshot grain), with `unit_price`
   reaching **3.663,00** for a loose frozen item at **37.00 per kg**. In the other 21 with
   the same `selling_method` the factor is `0,200`, which is the `unit_size` itself, and
   those are correct.

   The price of the purchasable portion is **`reference_price × min_bunch_amount`**, and
   both fields come from the source:

   ```
   Alistado mediano congelado   reference_price 37.00/kg   min_bunch_amount 0.15
     unit_price           3.663,00   <- selector ceiling, do NOT use
     purchasable portion      5.55   <- 37.00 x 0.15
   ```

   Consequence for anyone summing money: using `unit_price` here inflates revenue by three
   orders of magnitude on one row. In a price-indifferent synthetic basket, these 10
   rows produced **16.6% of all revenue** across 6,400 orders, and no totals test
   failed — because the number remained internally consistent.
6. **`min_bunch_amount` and `increment_bunch_amount` are the purchase granularity**, not
   decorative metadata. A loose product is only purchasable in multiples of
   `increment_bunch_amount`; treating it as a whole unit invents a package that does not
   exist.
7. **Distinguish `source_product_id` from business identity.** They are two things, and the
   source only delivers the first.
   - **`source_product_id`** is `product.id` (string). It is the **source's** key and the
     only one available for joins — use it. Within a snapshot it is consistent: repeated
     appearances of the same id differ only in the `categories[]` array, which reflects the
     file the row was read from, and never in price or name.
   - **Business identity** — "the same commercial item over time" — **is neither provided
     nor derivable** from this source. Without `ean`/`gtin`/`barcode` (§6), two facts make
     it undecidable: the same `display_name` appears under different ids in the same
     snapshot, with different prices; and an id can be replaced by another between
     consecutive snapshots with identical name, category and subgroup **[source]**.
     Resolving it is the consumer's job, with an external key or a matching rule declared
     as a premise.
8. **Do not infer currency from the payload** — the API does not declare it **[source]**.
9. **Check `schema_fingerprint` across snapshots** if you depend on the shape of the fields.
10. **Expect product repetition.** A product can appear in more than one category and in
   more than one subgroup. It is source semantics **[source]** and is not removed here.

## 5. What the Source does not do

Deduplication · dimensional modeling · normalization · facts and dimensions · Silver
transformation · Gold/Marts · enrichment · analytical history · metric computation · SCD ·
price-change detection · ingestion into Snowflake · upload to S3 · Spark · dbt ·
Airflow · Kafka.

Each run is a **snapshot in time**. Temporal interpretation belongs to the downstream
layer. The fields `unit_price`, `reference_price`, `bulk_price`, `previous_unit_price`,
`price_decreased`, `tax_percentage`, `selling_method`, `bunch_selector`, `min_bunch_amount`,
`increment_bunch_amount` and `unit_size` are preserved in the payload without
interpretation — which includes **not correcting** the ceiling `unit_price` described in
obligation 5. Correcting it here would erase the evidence of what the API returned; the
purchasable portion is derived one layer up, in `silver_product_price`, next to the
original value.

## 6. Source scope and limits **[source]**

- A snapshot covers **one warehouse**. `complete: true` means "every level-2 category of
  this `(ingestion_date, wh)` was written", not "complete catalog of the chain".
- Only **level-2** category ids are reachable at `/api/categories/{id}/`; level-1 ids
  respond `404`/`410`.
- There is no cross-source key: `ean`, `gtin`, `barcode`, `brand` and `origin` do not exist
  in this endpoint's payloads. `product.id` is the **source key**: it identifies the record
  within the snapshot, and carries **no** guarantee of temporal stability — the source may
  reissue the same commercial item under another id, and does not signal when it does.
  `price_decreased` and `previous_unit_price` do not cover that case: they are attributes of
  the new id, which does not know the old one. See §4.5.
- No known endpoint exposes sales, orders or stock. This Source has no transactional
  fact.
- `robots.txt` of `tienda.mercadona.es` declares `Disallow: /api`, and the Source sends a
  browser User-Agent. See README, section "Source restrictions".

## 7. Exit codes

| Code | `extract` | `validate` |
|---|---|---|
| 0 | no target category failed | passed |
| 1 | partial failure, recorded in `failures[]` | failed |
| 2 | fatal failure: unusable partition or invalid use | — |
| 3 | unhandled exception | unhandled exception |

## 8. Contract version

`manifest_version: 2`. `validate` refuses a manifest whose version differs from the one
supported by the code, instead of interpreting it by guesswork.
