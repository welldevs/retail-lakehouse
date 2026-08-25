"""Deriva e reconfere o mapeamento wh -> provincia/municipio contra o Callejero do INE.

O Callejero nao tem API (source manual, baixada do site do INE, semestral) e nao veio
com "Diseno de Registro" (layout oficial dos campos). Os codigos abaixo foram
reconstruidos por evidencia, cruzando DUAS fontes independentes dentro do proprio
Callejero — nao presumidos:

  1. UP (Unidades Poblacionais): a linha cujo codigo termina em 7 zeros e o municipio
     inteiro (as demais sao nucleos/entidades dentro dele); o nome da cidade aparece
     nessa linha.
  2. SECC (Secoes censitarias): o prefixo provincia+municipio deve aparecer em pelo
     menos uma secao censitaria — confirma que e um municipio real, nao um erro de
     leitura do UP.

Encoding real dos arquivos e ISO-8859-1 (Latin-1), confirmado com `file`; abrir como
UTF-8 falha silenciosamente (decode error ou, pior, matches perdidos em busca de texto).

Uso:
    python3 scripts/derive_warehouse_province_map.py <diretorio-do-callejero>

Reexecutavel contra qualquer download futuro do Callejero (e semestral) para reconferir
os codigos, nao so contra o `temp/` desta rodada.
"""

from __future__ import annotations

import sys
from pathlib import Path

# wh -> (codigo de provincia, codigo de municipio, nome pra buscar no UP)
WAREHOUSES = {
    "mad1": ("28", "079", "MADRID"),
    "bcn1": ("08", "019", "BARCELONA"),
    "svq1": ("41", "091", "SEVILLA"),
    "vlc1": ("46", "250", "VAL"),  # "VALÈNCIA"/"VALENCIA" — busca parcial, sem acento
}


def find_file(root: Path, prefix: str, province_code: str) -> Path:
    matches = sorted(root.glob(f"**/{prefix}.P{province_code}.*"))
    if not matches:
        raise SystemExit(f"nao achei {prefix}.P{province_code}.* dentro de {root}")
    if len(matches) > 1:
        raise SystemExit(f"mais de um {prefix}.P{province_code}.* dentro de {root}: {matches}")
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
                        f"UP {path.name}: linha {target_prefix!r} nao contem {name_hint!r}: {line!r}"
                    )
                return line.strip()
    raise SystemExit(f"UP {path.name}: nenhuma linha comecando com {target_prefix!r}")


def check_secc(root: Path, province_code: str, municipio_code: str) -> int:
    path = find_file(root, "SECC", province_code)
    prefix = f"{province_code}{municipio_code}"
    count = 0
    with path.open(encoding="latin-1", newline="") as f:
        for line in f:
            if line.startswith(prefix):
                count += 1
    if count == 0:
        raise SystemExit(f"SECC {path.name}: nenhuma secao censitaria com prefixo {prefix!r}")
    return count


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    root = Path(argv[1])
    if not root.is_dir():
        print(f"nao e um diretorio: {root}", file=sys.stderr)
        return 2

    print(f"{'wh':6} {'prov':4} {'munic':5} {'secoes':7} nome (UP)")
    for wh, (province_code, municipio_code, name_hint) in WAREHOUSES.items():
        up_line = check_up(root, province_code, municipio_code, name_hint)
        n_secc = check_secc(root, province_code, municipio_code)
        print(f"{wh:6} {province_code:4} {municipio_code:5} {n_secc:7} {up_line}")

    print("\nOK: os 4 codigos batem em UP (nome) e SECC (secoes censitarias existem).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
