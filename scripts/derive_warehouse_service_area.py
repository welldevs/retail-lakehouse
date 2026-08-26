"""Deriva a area de atendimento de cada warehouse (municipios da AUF do INE) e reconfere
cada municipio contra o Callejero — mesmo espirito de derive_warehouse_province_map.py,
aplicado a uma segunda pergunta: nao "onde o armazem fica" (1 municipio), mas "quais
municipios vizinhos estao na mesma Area Urbana Funcional" (N municipios).

Fonte da area: "Areas Urbanas Funcionales" do INE (AUF, ex-LUZ) — metodologia unica e
oficial para todo o pais: um municipio pertence a AUF de uma cidade se >=15% da sua
populacao empregada comuta para a cidade-nucleo por motivo de trabalho (limiar diferente
para municipios pequenos, ver metodologia do INE). Arquivo baixado manualmente de
https://www.ine.es/uaudit_imagenes/AUF_mun.xlsx (pagina "Areas Urbanas Funcionales" de
ine.es) — SEM biblioteca de terceiros pra ler .xlsx: um .xlsx e um zip de XML, entao
zipfile+xml.etree (stdlib) bastam pra extrair a planilha (aba unica: AUF | Codigo
municipal | Nombre municipio).

Limitacao conhecida, deliberada: a AUF oficial de Madrid tem 166 municipios (38 fora da
provincia 28 — Avila/Guadalajara/Toledo) e a de Barcelona tem 135 (2 fora da provincia
08 — Tarragona). Esta Source so incorpora Callejero de 08/28/41/46 (CONTRACT.md da
source), entao os municipios de fronteira ficam FORA da area de atendimento derivada
aqui — nao foram baixados, nao tem SECC/UP/VIAS/TRAM pra cruzar. Sevilla (46/46 na
provincia 41) e Valencia (63/63 na provincia 46) nao tem esse problema.

O nome de cada municipio vem do PROPRIO Callejero (UP, linha agregada do municipio,
sufixo 0000000) — nao do texto do Excel do INE — pra bater exatamente com
silver_callejero_population_units.municipality_name (mesma fonte, mesma grafia).

Uso:
    python3 scripts/derive_warehouse_service_area.py <AUF_mun.xlsx> <diretorio-do-callejero>
"""

from __future__ import annotations

import csv
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

# wh -> (AUF do INE, codigo de provincia do warehouse, codigo de municipio do warehouse)
WAREHOUSES = {
    "mad1": ("Madrid", "28", "079"),
    "bcn1": ("Barcelona", "08", "019"),
    "svq1": ("Sevilla", "41", "091"),
    "vlc1": ("Valencia", "46", "250"),
}

# Provincias cuja Callejero ja foi baixada e landada (CONTRACT.md da ine-callejero-source).
LANDED_PROVINCES = {"08", "28", "41", "46"}


def read_auf_xlsx(path: Path) -> list[tuple[str, str, str]]:
    """Le a planilha (AUF, codigo municipal, nome) sem nenhuma biblioteca de terceiros."""
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
        raise SystemExit(f"planilha vazia ou formato inesperado: {path}")
    return rows


def find_file(root: Path, prefix: str, province_code: str) -> Path:
    matches = sorted(root.glob(f"**/{prefix}.P{province_code}.*"))
    if not matches:
        raise SystemExit(f"nao achei {prefix}.P{province_code}.* dentro de {root}")
    if len(matches) > 1:
        raise SystemExit(f"mais de um {prefix}.P{province_code}.* dentro de {root}: {matches}")
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
    raise SystemExit(f"UP {path.name}: nenhuma linha agregada (sufixo 0000000) para {target_prefix!r}")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    xlsx_path, callejero_root = Path(argv[1]), Path(argv[2])
    if not xlsx_path.is_file():
        print(f"nao e um arquivo: {xlsx_path}", file=sys.stderr)
        return 2
    if not callejero_root.is_dir():
        print(f"nao e um diretorio: {callejero_root}", file=sys.stderr)
        return 2

    auf_rows = read_auf_xlsx(xlsx_path)
    print(f"{len(auf_rows)} linhas lidas de {xlsx_path.name}")

    output_rows: list[dict] = []
    for wh, (auf_name, home_province, home_municipio) in WAREHOUSES.items():
        members = [r for r in auf_rows if r[0] == auf_name]
        if not members:
            raise SystemExit(f"AUF {auf_name!r} nao encontrada em {xlsx_path.name}")

        in_scope = [(code, name) for _, code, name in members if code[:2] in LANDED_PROVINCES]
        out_of_scope = len(members) - len(in_scope)
        print(
            f"{wh:6} AUF={auf_name:10} total={len(members):4} "
            f"dentro-do-escopo={len(in_scope):4} fora-do-escopo={out_of_scope:4}"
        )

        home_code = f"{home_province}{home_municipio}"
        if not any(code == home_code for code, _ in in_scope):
            raise SystemExit(
                f"{wh}: municipio-sede {home_code} nao aparece na AUF {auf_name!r} — "
                f"inesperado, confira WAREHOUSES"
            )

        for code, _auf_name_text in sorted(in_scope):
            province_code, municipio_code = code[:2], code[2:]
            if not secc_prefix_exists(callejero_root, province_code, municipio_code):
                raise SystemExit(
                    f"{wh}: municipio {code} (AUF {auf_name!r}) sem nenhuma secao SECC "
                    f"em provincia {province_code} — codigo do INE/AUF nao bate com o "
                    f"Callejero landado; nao vou incluir sem essa confirmacao"
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

    print(f"\nOK: {len(output_rows)} linhas (municipio, warehouse) escritas em {out_path}")
    print("Todos os municipios confirmados contra SECC real; nomes vieram do UP real (nao do Excel do INE).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
