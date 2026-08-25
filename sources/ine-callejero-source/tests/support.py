"""Apoio da suite: diretorio de entrada com arquivos de amostra do Callejero.

Nenhum teste toca rede nem os 110 MB de arquivos reais (fora do repositorio, so em
temp/ localmente). As larguras usadas aqui sao as MEDIDAS contra os arquivos reais (ver
CONTRACT.md secao 2): SECC=10 chars, VIAS=132, PSEU=127, UP=604 — o suficiente para
exercitar intake/validate (que operam por linha e por nome de arquivo, nao por campo
semantico) com dados estruturalmente equivalentes aos de producao.
"""

from __future__ import annotations

import argparse
import os


def _pad(text: str, width: int) -> str:
    return text[:width].ljust(width)


def secc_line(province: str, municipio: str, distrito: str = "01", seccion: str = "001") -> str:
    return f"{province}{municipio}{distrito}{seccion} \r\n"


def vias_line(province: str, municipio: str, via_id: str, name: str, date: str = "20260630") -> str:
    code = f"{province}{municipio}{via_id}"
    return (
        code
        + _pad(name, 28)
        + date
        + " "
        + via_id
        + _pad("CALLE", 5)
        + _pad(name, 50)
        + _pad(name, 25)
        + "\r\n"
    )


def pseu_line(province: str, municipio: str, pseudovia_id: str, name: str, date: str = "20260630") -> str:
    code = f"{province}{municipio}{pseudovia_id}"
    return code + _pad(name, 53) + date + " " + pseudovia_id + _pad(name, 50) + "\r\n"


def up_line(province: str, municipio: str, suffix: str, municipio_name: str) -> str:
    code = f"{province}{municipio}{suffix}"
    # Layout real tem 604 chars com o nome do municipio a partir da coluna 94; um
    # fixture nao precisa reproduzir todas as colunas redundantes, so a largura total.
    return _pad(code + "   20260630" + " " * 71 + municipio_name, 604) + "\r\n"


DEFAULT_LINES = {
    "SECC": lambda province, municipio: [secc_line(province, municipio)],
    "UP": lambda province, municipio: [up_line(province, municipio, "0000000", "MUNICIPIO TESTE")],
    "VIAS": lambda province, municipio: [vias_line(province, municipio, "00001", "RUA TESTE")],
    "PSEU": lambda province, municipio: [pseu_line(province, municipio, "00001", "PSEUDOVIA TESTE")],
}


def write_callejero_file(
    directory: str,
    dataset: str,
    province: str,
    *,
    municipio: str = "001",
    date: str = "260630",
    gen: str = "260702",
    lines: list[str] | None = None,
    subdir: str | None = None,
) -> str:
    """Grava um arquivo de amostra do Callejero, com o nome oficial do dataset.

    `subdir` imita a estrutura aninhada dos downloads reais (call_pXX_.../call_pXX_.../)
    quando fornecido; por padrao grava direto em `directory`, o que tambem e um caso
    valido (intake varre recursivamente).
    """
    filename = f"{dataset}.P{province}.D{date}.G{gen}"
    target_dir = os.path.join(directory, subdir) if subdir else directory
    os.makedirs(target_dir, exist_ok=True)
    path = os.path.join(target_dir, filename)
    content_lines = lines if lines is not None else DEFAULT_LINES[dataset](province, municipio)
    with open(path, "w", encoding="latin-1", newline="") as handle:
        handle.writelines(content_lines)
    return path


def build_input_dir(
    root: str,
    provinces: tuple[str, ...] = ("28",),
    datasets: tuple[str, ...] = ("SECC", "UP", "VIAS", "PSEU"),
    include_tram: bool = False,
) -> str:
    """Monta um diretorio --in de amostra: um arquivo por (provincia, dataset)."""
    in_dir = os.path.join(root, "in")
    for province in provinces:
        subdir = f"call_p{province}_726/call_p{province}_072026"
        for dataset in datasets:
            write_callejero_file(in_dir, dataset, province, subdir=subdir)
        if include_tram:
            write_callejero_file(in_dir, "TRAM", province, subdir=subdir, lines=["linha de tram\r\n"])
    return in_dir


def extract_args(out: str, in_dir: str, **overrides) -> argparse.Namespace:
    defaults = {
        "out": out,
        "in_dir": in_dir,
        "provinces": ["28"],
        "date": "2026-01-01",
        "overwrite": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def validate_args(partition: str, strict: bool = False) -> argparse.Namespace:
    return argparse.Namespace(partition=partition, strict=strict)
