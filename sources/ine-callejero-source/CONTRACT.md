# Source Contract — `ine_callejero`

Contract between this Source and any component that consumes its snapshots.
Everything here is enforced by code and covered by a test, except where marked **[source]** —
a characteristic of the INE dataset, outside this Source's control.

**Structural difference from the other two Sources: there is no API.** The Callejero is
only distributed for manual download on the INE website. `extract` here makes no
network request — it ingests files that were already downloaded and placed in a
local directory (`--in`). The verb is kept for interface uniformity with the
other Sources; the meaning changes. What the three share (atomic writes,
closed inventory, immutability, safe resume) is generic partition hygiene,
kept verbatim.

## 1. Snapshot identity

| Key | Value | Where it lives |
|---|---|---|
| `source` | `ine_callejero` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | partition directory and `partition.ingestion_date` |

No second partition axis — neither by warehouse nor by province: the artifact is an
intake of the whole official dataset, not a slice.

```
<root>/ingestion_date=YYYY-MM-DD/
├── provinces/province=<code>/<ORIGINAL name of the downloaded file>
├── _manifest.json
├── _SUCCESS          (only exists when complete = true)
└── _run.log
```

`provinces/province=<code>/` is DIRECTORY structure inside the partition —
equivalent to `tables/` in the population source — not a formal axis on the
platform side. The landed file name is the same as the download (e.g.
`VIAS.P28.D260630.G260702`), not a generic name: it preserves provenance (INE date and
generation) without needing another mechanism.

Top-level blocks of `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `totals`, `files[]`, `failures[]`, `history[]`.

## 2. Guaranteed content

**The Callejero downloads contain 5 files per province: `SECC`, `UP`, `VIAS`,
`PSEU` and `TRAM`. This Source ingests all 5.** `TRAM` (street segments) was added after the
other 4 because it is the only one that links, in a single record, census section +
entity/nucleus + street or pseudo-street + **postal code** + number range — the other 4 have no
postal code.

**Actual encoding: ISO-8859-1 (Latin-1)**, lines terminated with CRLF — measured with `file`
against the real files, not assumed. **RAW preserves the original bytes**: this
Source does not decode, does not convert encoding, does not normalize line endings. The
recorded SHA-256 is over the file exactly as INE distributes it.

**`SECC`/`UP`/`VIAS`/`PSEU` have no postal code nor any official concept of an urban
"neighborhood"** — measured by inspecting the real content, not assumed from a lack of searching.
`TRAM` has the postal code (field `CPOS`, see the table below). The closest thing to a
"neighborhood" is still the population unit (nucleus) name in `UP` — sub-municipal, but not an
official administrative neighborhood division (that belongs to each city's council, not
to INE). Measured: Madrid/Barcelona/Sevilla city are a single entity with no named nucleus
in `UP`; València has 16 named nuclei/pedanías (a history of annexing villages —
l'Horta), which is why it is the only one of the 4 warehouses where the nucleus approximates a
"neighborhood" with any meaning.

**Measured layout of each file** (0-indexed positions, `[start:end)`), against the
real files of the 4 warehouse provinces. For `SECC`/`UP`/`VIAS`/`PSEU`, there is no
official documentation attached to the download — reconstructed from evidence. For `TRAM`, the
layout below was measured AND cross-checked against the "Diseños de registro de los ficheros de
intercambio de información INE-Ayuntamientos" (INE, published by IDA-Padrón — see
section 6), which names the fields of the "Tramero" in the same order observed in the real bytes
(`CPRO CMUN DIST SECC LSECC SUBSC CUN CVIA CPSVIA MANZ CPOS TINUM EIN CEIN ESN CESN`)
— that document describes the *exchange* format for changes (INE↔Ayuntamientos), not the
full snapshot we download, so the offsets below are still the ones measured
against the real file, only confirmed by the official field order/names, not copied
from the document:

| Dataset | Width | Fields |
|---|---|---|
| `SECC` | 11 chars (10 of code + 1 trailing space) | `[0:2]` province · `[2:5]` municipality · `[5:7]` district · `[7:10]` section |
| `VIAS` | 132 chars | `[0:10]` code (province+municipality+street id) · `[10:38]` short name · `[38:46]` date · `[47:52]` street id · `[52:57]` street type (CALLE/PLAZA/CMNO/PRAJE...) · `[57:107]` full name · `[107:132]` alternative short name |
| `PSEU` | 127 chars | `[0:10]` code (province+municipality+id) · `[10:63]` name · `[63:71]` date · `[72:77]` id · `[77:127]` full name — no type field (pseudo-streets are not classified by street type) |
| `UP` | 604 chars | `[0:12]` code (province+municipality(3)+suffix(7), suffix `0000000` = MUNICIPALITY aggregate row) · `[15:23]` date · `[94:314]` MUNICIPALITY name · `[459:529]` POPULATION UNIT/nucleus name (e.g. `*DISEMINADO*` for scattered population). Redundant fields at other widths (70/50/25 chars, plus two repetitions) exist but are not extracted in this version — they are abbreviated forms of the same two pieces of information, confirmed by comparing the content, not assumed. |
| `TRAM` | 273 chars | `[0:10]` **section** code (same format as `SECC`) · `[13:20]` `CUN` — entity/nucleus suffix (same format as `UP`) · `[20:25]` `CVIA` — street id (same format as `VIAS`, `00000` when not applicable) · `[25:30]` `CPSVIA` — pseudo-street id (same format as `PSEU`, `00000` when not applicable; mutually exclusive with `CVIA` — measured on 305 thousand lines across the 4 provinces, 0 "both filled" exceptions) · `[30:42]` `MANZ` — cadastral block (almost always blank; the official document confirms it was never implemented in practice) · `[42:47]` **`CPOS` — postal code**, 5 digits, always with the correct province prefix (measured: 304,905/304,952 correct lines across the 4 provinces, 47 exceptions only in Barcelona, 0.015%) · `[47:48]` `TINUM` — numbering type (`0`=no numbering, `1`=odd, `2`=even) · `[48:52]`+`[52:53]` `EIN`+`CEIN` — lower end of the number range + duplicate qualifier (letter A-Z or blank) · `[53:57]`+`[57:58]` `ESN`+`CESN` — upper end + qualifier · `[61:69]` reference date (`YYYYMMDD`, same date as the file). The rest of the record (up to 273 chars) has not been decoded — it includes at least one repeated field at the end that mirrors `CPOS`+`TINUM`+`EIN`+`ESN` already captured at the start; nothing beyond that is extracted or claimed. |

Validated against real addresses: "Alcalá" in municipality 079 (Madrid), "Gran Vía" in 3
different municipalities, "Abrera"/"Can Vilalba"/"Sant Miquel" (Barcelona), municipalities
079/019/091/250 = Madrid/Barcelona/Sevilla/València — the same codes already used in the platform seed
`warehouse_province_map_seed.csv` (an independent source), exposed to Silver
as a pass-through by the `warehouse_province_map` model. `CPOS` validated against
real geography: València city has 30 distinct postal codes (46001-46026 + exceptions), and
specific nuclei/pedanías match the known real postal code of the area (e.g. Pinedo/El Saler
→ 46012, southern part of the city — geographically correct).

**The fixed-width text is the physical contract.** There is no relational schema; the Source does not
parse fields — it only copies the file and records structural facts (line count,
dominant width) for revalidation. The table above documents what was measured to
guide the consumer (Silver); it is neither enforced nor checked by this Source's code.

## 3. Guarantees

1. **Atomic writes.** No file is observable in a partial state.
2. **Bytes preserved.** The landed file is identical, byte for byte, to the file in
   `--in`. No reserialization.
3. **Verifiable integrity.** `_manifest.json` declares `sha256`, `bytes`,
   `line_count` and `dominant_line_width` for each file. `validate` recomputes all
   four from disk.
4. **Closed inventory.** Every file inside the partition is declared in the manifest.
5. **Immutability.** A partition with `complete: true` cannot be rewritten without an
   explicit `--overwrite`.
6. **Divergence requires an explicit decision.** An already landed file (in an
   incomplete partition being resumed) whose content diverges from the current file in `--in` is
   neither silently overwritten nor silently kept — it becomes a recorded failure,
   resolved only with `--overwrite`.
7. **Safe resume.** An incomplete partition can be completed by rerunning the
   same command, with `--in` containing the missing files.
8. **Provenance preserved.** `history[]` accumulates the summary of each previous run;
   the original file name (INE date and generation) is preserved in the landed path.
9. **Completion marker.** `_SUCCESS` exists if and only if `complete: true`.
10. **Identity persisted before the fan-out.** A preliminary manifest (`complete:
    false`) is written as soon as the partition is created, before any file is
    copied.
11. **Coverage verified.** `validate` checks that every (province,
    dataset) combination in `manifest.config` has a landed file.
12. **Totals rechecked.** `totals.files_landed` and `totals.bytes` are recomputed from the
    reread.
13. **A recorded failure is fatal in validation.** Any entry in `failures[]` fails
    the partition.
14. **A layout change is a warning, not corrected.** A dominant line width
    different from the one recorded in the manifest becomes a warning in `validate` (fatal only with
    `--strict`) — a signal to the consumer that INE may have changed the format; this
    Source does not try to guess or adapt.

**Deliberately without an equivalent here** (they exist in the other Sources, but make no
sense in this one): canonical reserialization form (there is no JSON here to reserialize);
`schema_fingerprint` of named keys (fixed-width text has no keys);
detection of "file outside the canonical form" (there is no form other than the original).

## 4. Consumer obligations

1. **Iterate `manifest.files[]`.** `files[].path` is relative to the snapshot root,
   starting at `ingestion_date=…/`.
2. **Treat the partition as immutable.** Do not write inside it.
3. **Parse the fields yourself, using the table in section 2 as a starting point —
   and revalidate against a real sample before relying on an offset.** This Source does not
   guarantee that the layout measured here remains valid for a future `ingestion_date`;
   it only guarantees the bytes it copied.
4. **Do not assume an update cadence.** The Callejero is published twice a year by
   INE, with no guarantee of different content between publications.
5. **`warehouse → province/municipality` does not come from this Source.** That relationship is a
   platform decision, not INE's — it lives in the seed `warehouse_province_map_seed.csv`, exposed
   to Silver by the `warehouse_province_map` model (a pass-through of the seed to object
   storage), consumed via JOIN in Silver/Gold. This Source does not know warehouses
   exist.

## 5. What the Source does not do

Field parsing · deduplication · dimensional modeling · normalization · facts and
dimensions · Silver transformation · Gold/Marts · warehouse → province mapping ·
service-area/logistics computation · ingestion into object storage · Spark · dbt ·
Airflow.

Each run ingests a **faithful copy** of the requested files. Interpretation
(extracting street, section, joining with warehouse) belongs to the downstream layer.

## 6. Source scope and limits **[source]**

- The Callejero is published by INE without an API — manual download only, twice a year. The
  January publication = reference Dec 31 of the previous year; the July publication = reference
  Jun 30 of the same year.
- Official INE page on the Callejero (what it is, composition of the 5 files,
  cadence): [Cartografía secciones censales y callejero de Censo Electoral](https://www.ine.es/ss/Satellite?L=es_ES&c=Page&cid=1259952026632&pagename=ProductosYServicios%2FPYSLayout).
  The official "Diseño de registro" used to confirm the field order of `TRAM`
  (section 2) is at [idapadron.ine.es/repositorio/DisReg/disregok.PDF](https://idapadron.ine.es/repositorio/DisReg/disregok.PDF)
  — it describes the INE↔Ayuntamientos *exchange* format (`marzo 2015`), not the full
  downloaded snapshot; used only to name/order fields, the offsets themselves are still
  measured against the real bytes.
- File layout **not officially documented inside the download itself** (no
  "Diseño de Registro" attached to the zip); the table in section 2 was reconstructed from
  evidence, with `TRAM` additionally cross-checked against the document above.
- None of the 5 files contains population density. Only `TRAM` contains a postal code.
- An administrative "neighborhood" (council district) does not exist in any of the 5 files.

## 7. Exit codes

| Code | `extract` | `validate` |
|---|---|---|
| 0 | every target (province, dataset) combination was landed | passed |
| 1 | partial failure, recorded in `failures[]` | failed |
| 2 | fatal failure: partition unusable or invalid usage | — |
| 3 | unhandled exception | unhandled exception |

## 8. Contract version

`manifest_version: 1`. A counter independent of the other two Sources — they are separate
contracts. `validate` rejects a manifest whose version differs from the one supported by the code.
