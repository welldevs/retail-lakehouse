"""Derives each warehouse's service area (the municipalities of its INE AUF) and re-checks
each municipality against the Callejero — same spirit as derive_warehouse_province_map.py,
applied to a second question: not "where the warehouse is" (1 municipality), but "which
neighboring municipalities are in the same Functional Urban Area" (N municipalities).

Area source: INE "Areas Urbanas Funcionales" (AUF, formerly LUZ) — a single, official
methodology for the whole country: a municipality belongs to a city's AUF if >=15% of its
employed population commutes to the core city for work (a different threshold
for small municipalities, see the INE methodology). File downloaded by hand from
https://www.ine.es/uaudit_imagenes/AUF_mun.xlsx ("Areas Urbanas Funcionales" page on
ine.es) — NO third-party library to read .xlsx: an .xlsx is a zip of XML, so
zipfile+xml.etree (stdlib) are enough to extract the sheet (single tab: AUF | Codigo
municipal | Nombre municipio).

Known, deliberate limitation: the official Madrid AUF has 166 municipalities (38 outside
province 28 — Avila/Guadalajara/Toledo) and Barcelona's has 135 (2 outside province
08 — Tarragona). This Source only ingests the Callejero for 08/28/41/46 (the source's
CONTRACT.md), so the border municipalities stay OUT of the service area derived
here — they were not downloaded, there is no SECC/UP/VIAS/TRAM to cross-check. Sevilla
(46/46 in province 41) and Valencia (63/63 in province 46) do not have that problem.

Each municipality's name comes from the Callejero ITSELF (UP, the municipality's aggregate
row, suffix 0000000) — not from the INE Excel text — so it matches
silver_callejero_population_units.municipality_name exactly (same source, same spelling).

Usage:
    python3 scripts/derive_warehouse_service_area.py <AUF_mun.xlsx> <callejero-directory>
"""

from __future__ import annotations

import csv
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

# wh -> (INE AUF, warehouse province code, warehouse municipality code)
WAREHOUSES = {
    "mad1": ("Madrid", "28", "079"),
    "bcn1": ("Barcelona", "08", "019"),
    "svq1": ("Sevilla", "41", "091"),
    "vlc1": ("Valencia", "46", "250"),
}

# Provinces whose Callejero has already been downloaded and landed (ine-callejero-source CONTRACT.md).
LANDED_PROVINCES = {"08", "28", "41", "46"}


def read_auf_xlsx(path: Path) -> list[tuple[str, str, str]]:
    """Reads the sheet (AUF, municipal code, name) without any third-party library."""
    with zipfile.ZipFile(path) as zf:
        shared = [
            "".join((t.text or "") for t in si.findall(".//s:t", XLSX_NS))
            for si in ET.fromstring(zf.read("xl/sharedStrings.xml")).findall("s:si", XLSX_NS)
        ]
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))

    rows: list[tuple[str, str, str]] = []
    for row in sheet.findall(".//s:sheetData/s:row", XLSX_NS):
        cells: dict[str, str | None] = {}
        for c in row.findall("s:c", XLSX_NS):
            col = "".join(ch for ch in c.get("r", "") if ch.isalpha())
            v = c.find("s:v", XLSX_NS)
            val = v.text if v is not None else None
            if c.get("t") == "s" and val is not None:
                val = shared[int(val)]
            cells[col] = val
        if cells.get("A") and cells.get("A") != "AUF":
            rows.append((cells["A"], cells.get("B", ""), cells.get("C", "")))
    if not rows:
        raise SystemExit(f"empty sheet or unexpected format: {path}")
    return rows


def find_file(root: Path, prefix: str, province_code: str) -> Path:
    matches = sorted(root.glob(f"**/{prefix}.P{province_code}.*"))
    if not matches:
        raise SystemExit(f"could not find {prefix}.P{province_code}.* under {root}")
    if len(matches) > 1:
        raise SystemExit(f"more than one {prefix}.P{province_code}.* under {root}: {matches}")
    return matches[0]


def secc_prefix_exists(root: Path, province_code: str, municipio_code: str) -> bool:
    path = find_file(root, "SECC", province_code)
    prefix = f"{province_code}{municipio_code}"
    with path.open(encoding="latin-1", newline="") as f:
        return any(line.startswith(prefix) for line in f)


def municipio_name_from_up(root: Path, province_code: str, municipio_code: str) -> str:
    path = find_file(root, "UP", province_code)
    target_prefix = f"{province_code}{municipio_code}0000000"
    with path.open(encoding="latin-1", newline="") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if line.startswith(target_prefix):
                return line[94:314].strip()
    raise SystemExit(f"UP {path.name}: no aggregate row (suffix 0000000) for {target_prefix!r}")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    xlsx_path, callejero_root = Path(argv[1]), Path(argv[2])
    if not xlsx_path.is_file():
        print(f"not a file: {xlsx_path}", file=sys.stderr)
        return 2
    if not callejero_root.is_dir():
        print(f"not a directory: {callejero_root}", file=sys.stderr)
        return 2

    auf_rows = read_auf_xlsx(xlsx_path)
    print(f"{len(auf_rows)} rows read from {xlsx_path.name}")

    output_rows: list[dict] = []
    for wh, (auf_name, home_province, home_municipio) in WAREHOUSES.items():
        members = [r for r in auf_rows if r[0] == auf_name]
        if not members:
            raise SystemExit(f"AUF {auf_name!r} not found in {xlsx_path.name}")

        in_scope = [(code, name) for _, code, name in members if code[:2] in LANDED_PROVINCES]
        out_of_scope = len(members) - len(in_scope)
        print(
            f"{wh:6} AUF={auf_name:10} total={len(members):4} "
            f"in-scope={len(in_scope):4} out-of-scope={out_of_scope:4}"
        )

        home_code = f"{home_province}{home_municipio}"
        if not any(code == home_code for code, _ in in_scope):
            raise SystemExit(
                f"{wh}: home municipality {home_code} does not appear in AUF {auf_name!r} — "
                f"unexpected, check WAREHOUSES"
            )

        for code, _auf_name_text in sorted(in_scope):
            province_code, municipio_code = code[:2], code[2:]
            if not secc_prefix_exists(callejero_root, province_code, municipio_code):
                raise SystemExit(
                    f"{wh}: municipality {code} (AUF {auf_name!r}) has no SECC section "
                    f"in province {province_code} — the INE/AUF code does not match the "
                    f"landed Callejero; not including it without that confirmation"
                )
            real_name = municipio_name_from_up(callejero_root, province_code, municipio_code)
            output_rows.append(
                {
                    "wh": wh,
                    "province_code": province_code,
                    "municipality_code": municipio_code,
                    "municipality_name": real_name,
                    "is_home_municipality": code == home_code,
                }
            )

    out_path = Path("platform/dbt/seeds/warehouse_service_area_seed.csv")
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["wh", "province_code", "municipality_code", "municipality_name", "is_home_municipality"],
        )
        writer.writeheader()
        for row in output_rows:
            writer.writerow(row)

    print(f"\nOK: {len(output_rows)} (municipality, warehouse) rows written to {out_path}")
    print("All municipalities confirmed against real SECC; names came from the real UP (not the INE Excel).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
