"""Derives and re-checks the wh -> province/municipality mapping against the INE Callejero.

The Callejero has no API (manual source, downloaded from the INE website, twice a year) and
did not come with a "Diseno de Registro" (official field layout). The codes below were
reconstructed from evidence, crossing TWO independent sources inside the Callejero
itself — not assumed:

  1. UP (Unidades Poblacionales): the row whose code ends in 7 zeros is the whole
     municipality (the others are nuclei/entities within it); the city name appears
     on that row.
  2. SECC (census sections): the province+municipality prefix must appear in at least
     one census section — this confirms it is a real municipality, not a misreading
     of UP.

The real file encoding is ISO-8859-1 (Latin-1), confirmed with `file`; opening as
UTF-8 fails silently (decode error or, worse, lost matches in text search).

Usage:
    python3 scripts/derive_warehouse_province_map.py <callejero-directory>

Re-runnable against any future Callejero download (it is semiannual) to re-check
the codes, not only against this round's `temp/`.
"""

from __future__ import annotations

import sys
from pathlib import Path

# wh -> (province code, municipality code, name to search for in UP)
WAREHOUSES = {
    "mad1": ("28", "079", "MADRID"),
    "bcn1": ("08", "019", "BARCELONA"),
    "svq1": ("41", "091", "SEVILLA"),
    "vlc1": ("46", "250", "VAL"),  # "VALÈNCIA"/"VALENCIA" — partial match, no accent
}


def find_file(root: Path, prefix: str, province_code: str) -> Path:
    matches = sorted(root.glob(f"**/{prefix}.P{province_code}.*"))
    if not matches:
        raise SystemExit(f"could not find {prefix}.P{province_code}.* under {root}")
    if len(matches) > 1:
        raise SystemExit(f"more than one {prefix}.P{province_code}.* under {root}: {matches}")
    return matches[0]


def check_up(root: Path, province_code: str, municipio_code: str, name_hint: str) -> str:
    path = find_file(root, "UP", province_code)
    target_prefix = f"{province_code}{municipio_code}0000000"
    with path.open(encoding="latin-1", newline="") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if line.startswith(target_prefix):
                if name_hint not in line.upper():
                    raise SystemExit(
                        f"UP {path.name}: row {target_prefix!r} does not contain {name_hint!r}: {line!r}"
                    )
                return line.strip()
    raise SystemExit(f"UP {path.name}: no row starting with {target_prefix!r}")


def check_secc(root: Path, province_code: str, municipio_code: str) -> int:
    path = find_file(root, "SECC", province_code)
    prefix = f"{province_code}{municipio_code}"
    count = 0
    with path.open(encoding="latin-1", newline="") as f:
        for line in f:
            if line.startswith(prefix):
                count += 1
    if count == 0:
        raise SystemExit(f"SECC {path.name}: no census section with prefix {prefix!r}")
    return count


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    root = Path(argv[1])
    if not root.is_dir():
        print(f"not a directory: {root}", file=sys.stderr)
        return 2

    print(f"{'wh':6} {'prov':4} {'munic':5} {'sections':8} name (UP)")
    for wh, (province_code, municipio_code, name_hint) in WAREHOUSES.items():
        up_line = check_up(root, province_code, municipio_code, name_hint)
        n_secc = check_secc(root, province_code, municipio_code)
        print(f"{wh:6} {province_code:4} {municipio_code:5} {n_secc:8} {up_line}")

    print("\nOK: the 4 codes match in UP (name) and SECC (census sections exist).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
