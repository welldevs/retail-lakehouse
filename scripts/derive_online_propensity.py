"""Derives how likely a resident is to buy groceries online, by AGE and by HABITAT SIZE, from
the INE's ICT household survey — so the synthetic customer base looks like the people who
actually shop online, not like the census.

WHY THIS EXISTS
---------------
Until CR-017 the customer base was the resident adult population scaled by one rate (2.2%):
every age band and every municipality got the same penetration. The dashboard showed it —
180 customers per 10,000 inhabitants in all four provinces, 65+ placing 23% of the orders.
Online grocery is not like that, and the INE measures by how much.

SOURCE
------
INE, Encuesta sobre Equipamiento y Uso de Tecnologías de Información y Comunicación en los
Hogares (TIC-H), table 50116: "Compras realizadas, por motivos particulares, a través de
Internet por características demográficas y tipo de formato". The row used is
"Alguna compra de un producto en formato físico de corta duración" — the INE's product group
that contains food — as a percentage of all persons aged 16 to 74 ("Total personas").

The INE universe ends at 74. The seed says so and does not extrapolate: who uses it decides
what 75+ gets, and writes the decision down (see customers_reference.py).

Usage:
    python3 scripts/derive_online_propensity.py
"""

from __future__ import annotations

import csv
import io
import urllib.request
from pathlib import Path

TABLE_ID = "50116"
CSV_URL = f"https://www.ine.es/jaxiPx/files/tpx/es/csv_bdsc/{TABLE_ID}.csv"
USER_AGENT = "spark-retail-lakehouse-scripts/1.0 (derive_online_propensity; contato via repositorio)"
OUT_PATH = Path("platform/dbt/seeds/ine_online_propensity_seed.csv")

POPULATION = "Total personas (16 a 74 años)"
PRODUCT = "Alguna compra de un producto en formato físico de corta duración"

# INE label -> (dimension, band, low, high). `high` empty = open band.
BANDS = {
    "Total Personas": ("total", "16_74", 16, 74),
    "Edad: De 16 a 24 años": ("age", "16_24", 16, 24),
    "Edad: De 25 a 34 años": ("age", "25_34", 25, 34),
    "Edad: De 35 a 44 años": ("age", "35_44", 35, 44),
    "Edad: De 45 a 54 años": ("age", "45_54", 45, 54),
    "Edad: De 55 a 64 años": ("age", "55_64", 55, 64),
    "Edad: De 65 a 74 años": ("age", "65_74", 65, 74),
    "Hábitat: Menos de 10.000 habitantes": ("habitat", "lt_10k", 0, 9999),
    "Hábitat: De 10.000 a 20.000 habitantes": ("habitat", "10k_20k", 10000, 19999),
    "Hábitat: De 20.000 a 50.000 habitantes": ("habitat", "20k_50k", 20000, 49999),
    "Hábitat: De 50.000 a 100.000 habitantes": ("habitat", "50k_100k", 50000, 99999),
    # The INE merges provincial capitals into this band. In the four served provinces every
    # capital (Madrid, Barcelona, Valencia, Sevilla) is above 100,000, so population alone
    # classifies them correctly.
    "Hábitat: Más de 100.000 habitantes y capitales de provincia": ("habitat", "gt_100k", 100000, ""),
}


def fetch() -> list[list[str]]:
    request = urllib.request.Request(CSV_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        text = response.read().decode("utf-8-sig")
    return list(csv.reader(io.StringIO(text), delimiter=";"))


def main() -> int:
    rows = fetch()
    found: dict[str, str] = {}
    for population, label, product, value in rows[1:]:
        if population == POPULATION and product == PRODUCT and label in BANDS:
            found[label] = value.replace(".", "").replace(",", ".")
    missing = sorted(set(BANDS) - set(found))
    if missing:
        raise SystemExit(f"INE table {TABLE_ID} no longer carries: {missing}. Nothing written.")

    fields = ["dimension", "band", "age_or_population_low", "age_or_population_high",
              "online_pct", "ine_table", "provenance", "note"]
    with open(OUT_PATH, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(fields)
        for label, (dimension, band, low, high) in BANDS.items():
            writer.writerow([
                dimension, band, low, high, found[label], TABLE_ID, "observed",
                f"INE TIC-H, '{PRODUCT}', % of '{POPULATION}', row '{label}'.",
            ])
    print(f"wrote {OUT_PATH} ({len(BANDS)} rows) from {CSV_URL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
