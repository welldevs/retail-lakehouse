"""Derives, for the municipality NAMES that repeat across all of Spain, which Tempus3
series belongs to which municipality — by the official INE CODE, not by heuristic.

WHY THIS EXISTS
---------------
`silver_ine_population_by_municipality` resolves the municipality code by joining the NAME
that comes in "Nombre" (table_id=29005 carries no code) with `ine_municipality_codes_seed`. The
seed is scoped to 08/28/41/46 and has no internal name collision — measured. But the RAW
side is NATIONAL (~8,200 municipalities), and there are homonyms in other provinces with
IDENTICAL "Nombre" text. The join by name matches our province's row AND the homonym from
outside, and the result is silent fanout:

    "Torrent. Total. Total habitantes. Personas. "  ->  DPOP21778 = 90.928 (Valencia, 46244)
    "Torrent. Total. Total habitantes. Personas. "  ->  DPOP7960  =    182 (Girona,  17197)

The docstring of scripts/derive_municipality_codes.py already anticipated exactly this ("a
Spain-wide join by name alone would be ambiguous for 3 of those 18 names"); what went
unnoticed is that the model does that Spain-wide join, because RAW is not filtered at
extraction. The existing grain test does not catch it, because the two series have
DIFFERENT series_code — the grain (ingestion_date, series_code, year, fk_periodo) stays unique.

HOW THE AMBIGUITY IS RESOLVED
-----------------------------
Source: `GET /ES/VALORES_SERIE/{COD}` of the Tempus3 API (the same API that
ine-population-source already consumes). For each series it returns the VALUES of the
variables that define it, and the municipality value carries `Codigo` = province(2)+municipality(3)
— the official code, the same scheme as the Callejero and the warehouse_* seeds. Confirmed live
on both Torrent series: 46244 and 17197.

Only the series whose municipality name is ambiguous IN RAW and exists in the seed of the
4 provinces are queried — today 3 names, ~18 calls. There is no way to derive this offline: the
DATOS_TABLA/29005 payload has only COD, Nombre, FK_Escala, FK_Unidad and Data. No code, no
province, no MetaData (verified).

Usage:
    python3 scripts/derive_ambiguous_series.py
"""

from __future__ import annotations

import csv
import glob
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

SERIES_ENDPOINT = "https://servicios.ine.es/wstempus/js/ES/VALORES_SERIE"
USER_AGENT = "spark-retail-lakehouse-scripts/1.0 (derive_ambiguous_series; contato via repositorio)"

RAW_GLOB = "data/ine/ingestion_date=*/tables/table_id=29005.json"
CODES_SEED = Path("platform/dbt/seeds/ine_municipality_codes_seed.csv")
OUT_PATH = Path("platform/dbt/seeds/ine_ambiguous_series_seed.csv")

DELAY_SECONDS = 0.5


def latest_raw() -> Path:
    """The most recent landed partition. RAW is immutable: re-reading changes nothing."""
    found = sorted(glob.glob(RAW_GLOB))
    if not found:
        raise SystemExit(
            f"no payload in {RAW_GLOB}. Run `make ine-refresh` before this script."
        )
    return Path(found[-1])


def municipality_of(series_name: str) -> str:
    return series_name.split(". ")[0].strip()


def sex_of(series_name: str) -> str:
    parts = series_name.split(". ")
    return parts[1].strip() if len(parts) > 1 else ""


def fetch_official_code(series_code: str) -> str:
    """Official code (province+municipality) of the series, from the INE API itself."""
    request = urllib.request.Request(
        f"{SERIES_ENDPOINT}/{series_code}", headers={"User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    # The municipality value is the only one whose Codigo has 5 numeric digits: the others are
    # sex ("0"), measure ("0") and unit (""). Verified on the real ambiguous series.
    candidates = [
        item.get("Codigo")
        for item in payload
        if isinstance(item, dict)
        and isinstance(item.get("Codigo"), str)
        and item["Codigo"].isdigit()
        and len(item["Codigo"]) == 5
    ]
    if len(candidates) != 1:
        raise SystemExit(
            f"{series_code}: expected exactly one 5-digit municipality code, "
            f"found {candidates}. The response shape changed — not guessing."
        )
    return candidates[0]


def main() -> None:
    raw_path = latest_raw()
    print(f"reading {raw_path} ...")
    with raw_path.open(encoding="utf-8") as handle:
        raw = json.load(handle)

    with CODES_SEED.open(encoding="utf-8") as handle:
        seed_names = {row["municipality_name"] for row in csv.DictReader(handle)}
    print(f"seed of the 4 provinces: {len(seed_names)} names")

    # Groups by (name, sex): more than one series for the same pair = national homonym.
    grouped: dict[tuple, list[str]] = {}
    for series in raw:
        if not isinstance(series, dict) or "Nombre" not in series:
            continue
        key = (municipality_of(series["Nombre"]), sex_of(series["Nombre"]))
        grouped.setdefault(key, []).append(series["COD"])

    ambiguous = {
        key: sorted(codes)
        for key, codes in grouped.items()
        if len(codes) > 1 and key[0] in seed_names
    }
    names = sorted({key[0] for key in ambiguous})
    total_series = sum(len(codes) for codes in ambiguous.values())
    print(f"ambiguous names affecting the scope: {len(names)} -> {names}")
    print(f"series to query: {total_series}\n")

    if not ambiguous:
        print("no ambiguity: the seed comes out empty (header only), and the model filters nothing.")

    rows = []
    for (name, sex), codes in sorted(ambiguous.items()):
        for series_code in codes:
            official = fetch_official_code(series_code)
            rows.append(
                {
                    "series_code": series_code,
                    "municipality_name": name,
                    "sex_label": sex,
                    "province_code": official[:2],
                    "municipality_code": official[2:],
                }
            )
            print(f"  {series_code:12} {name!r:20} {sex:8} -> {official}")
            time.sleep(DELAY_SECONDS)

    rows.sort(key=lambda r: (r["municipality_name"], r["sex_label"], r["series_code"]))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "series_code",
                "municipality_name",
                "sex_label",
                "province_code",
                "municipality_code",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    inside = sum(1 for r in rows if r["province_code"] in {"08", "28", "41", "46"})
    print(f"\nOK: {len(rows)} rows written to {OUT_PATH}")
    print(f"     {inside} within scope (kept by the model), "
          f"{len(rows) - inside} outside (discarded by the model)")


if __name__ == "__main__":
    main()
