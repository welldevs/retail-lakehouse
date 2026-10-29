"""Derives the INE code (province+municipality) of every municipality within this
platform's scope (08/28/41/46), to join with INE Tempus3 series that only carry the
municipality NAME in "Nombre" (e.g. table_id=29005) — same spirit as the other derive_*
scripts, applied to a live source instead of a manually downloaded file.

Source: `GET /ES/VALORES_VARIABLE/19` of the INE Tempus3 API (the same API that
ine-population-source already consumes) — variable 19 = "Municipios", returns in a single call
`{"Nombre", "Codigo", ...}` for all ~8,200 municipalities in Spain. `Codigo` is
province(2)+municipality(3), the same scheme the Callejero and the warehouse_* seeds already use.
Confirmed live: the spelling of `Nombre` here matches DATOS_TABLA/29005 exactly
(0 divergences in a sample of 1,501 names) — it does NOT match the Callejero spelling
(silver_callejero_population_units.municipality_name), which comes in UPPERCASE and
moves the definite article to a parenthesized suffix (e.g. "AMETLLA DEL VALLES (L')" vs
"L'Ametlla del Valles" here) — which is why this script looks the code up in the SAME
endpoint family that table_id=29005 uses, instead of reusing the already-landed Callejero.

A municipality name is NOT a safe key at national scope: measured that 18 of the ~8,200
municipalities in Spain share their name with another municipality in ANOTHER province (e.g.
"Arroyomolinos" exists in Madrid [28015] and in Caceres [10023]). No collision happens
WITHIN this platform's 4 provinces (verified against the full payload before
writing this script) — which is why the seed below, scoped to 08/28/41/46, can be joined
by name without real ambiguity, even though a Spain-wide join by name alone would be
ambiguous for 3 of those 18 names.

Usage:
    python3 scripts/derive_municipality_codes.py
"""

from __future__ import annotations

import csv
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

ENDPOINT = "https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLE/19"
USER_AGENT = "spark-retail-lakehouse-scripts/1.0 (derive_municipality_codes; contato via repositorio)"

# The same 4 provinces already landed by the Callejero (ine-callejero-source CONTRACT.md) —
# this platform's scope, not an arbitrary cut.
TARGET_PROVINCES = {"08", "28", "41", "46"}

TIMEOUT_SECONDS = 120.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 5.0


def fetch_municipalities() -> list[dict]:
    """GET VALORES_VARIABLE/19 with retry — a ~900KB payload; the network has already shown
    silent truncation on large responses from this API during this investigation."""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        request = urllib.request.Request(
            ENDPOINT, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            backoff = BACKOFF_BASE_SECONDS * (2**attempt)
            print(f"  attempt {attempt + 1}/{MAX_RETRIES} failed ({exc}); waiting {backoff:.0f}s")
            time.sleep(backoff)
    raise SystemExit(f"could not fetch {ENDPOINT} after {MAX_RETRIES + 1} attempts: {last_error}")


def main() -> int:
    print(f"fetching {ENDPOINT} ...")
    municipalities = fetch_municipalities()
    print(f"{len(municipalities)} municipalities received (all of Spain)")

    codes = [m["Codigo"] for m in municipalities]
    if len(codes) != len(set(codes)):
        raise SystemExit("duplicate code in the payload — unexpected, not writing an inconsistent seed")
    if any(len(c) != 5 for c in codes):
        raise SystemExit("code outside the 5-digit pattern — unexpected, check the payload")

    in_scope = [m for m in municipalities if m["Codigo"][:2] in TARGET_PROVINCES]
    print(f"{len(in_scope)} municipalities within scope (provinces {sorted(TARGET_PROVINCES)})")

    names_in_scope = [m["Nombre"] for m in in_scope]
    if len(names_in_scope) != len(set(names_in_scope)):
        raise SystemExit(
            "duplicate municipality name WITHIN scope — unexpected (measured beforehand that it "
            "does not happen); a join by name would no longer be safe, not writing the seed like this"
        )

    rows = sorted(
        (
            {
                "province_code": m["Codigo"][:2],
                "municipality_code": m["Codigo"][2:],
                "municipality_name": m["Nombre"],
            }
            for m in in_scope
        ),
        key=lambda r: (r["province_code"], r["municipality_code"]),
    )

    out_path = Path("platform/dbt/seeds/ine_municipality_codes_seed.csv")
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["province_code", "municipality_code", "municipality_name"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nOK: {len(rows)} (municipality) rows written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
