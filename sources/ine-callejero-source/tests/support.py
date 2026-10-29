"""Suite support: input directory with sample Callejero files.

No test touches the network or the 110 MB of real files (outside the repository, only in
temp/ locally). The widths used here are the ones MEASURED against the real files (see
CONTRACT.md section 2): SECC=10 chars, VIAS=132, PSEU=127, UP=604, TRAM=273 — enough to
exercise intake/validate (which work per line and per file name, not per semantic field)
with data structurally equivalent to production.
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
    # The real layout has 604 chars with the municipality name from column 94 on; a
    # fixture does not need to reproduce every redundant column, only the total width.
    return _pad(code + "   20260630" + " " * 71 + municipio_name, 604) + "\r\n"


def tram_line(
    province: str,
    municipio: str,
    *,
    distrito: str = "01",
    seccion: str = "001",
    entity_suffix: str = "0000000",
    via_id: str = "00000",
    pseudovia_id: str = "00000",
    cpos: str | None = None,
    tinum: str = "1",
    ein: str = "0001",
    esn: str = "0001",
    date: str = "20260630",
) -> str:
    """TRAM (street segment) line: section + entity/nucleus + street/pseudo-street +
    postal code + number range.

    Offsets MEASURED against the 4 real files and confirmed against INE's official Diseno
    de Registro (CONTRACT.md section 2): [0:10] section (province+municipality+district+
    section, same as SECC) · [13:20] CUN (entity_suffix, same as UP) · [20:25] CVIA
    (via_id, same as VIAS when != 00000) · [25:30] CPSVIA (pseudovia_id, same as PSEU when
    != 00000 — mutually exclusive with CVIA) · [30:42] MANZ (manzana, usually blank) ·
    [42:47] CPOS (postal code) · [47:48] TINUM (0=no numbering, 1=odd, 2=even) · [48:52]
    EIN + [52:53] CEIN (lower end of the number range + qualifier) · [53:57] ESN + [57:58]
    CESN (upper end + qualifier) · [61:69] reference date. The rest of the record (up to
    273 chars) has not been decoded yet — left blank here, like the redundant UP fields.
    """
    cpos = cpos if cpos is not None else f"{province}000"[:5].ljust(5, "0")
    head = (
        f"{province}{municipio}{distrito}{seccion}"
        + "   "
        + entity_suffix
        + via_id
        + pseudovia_id
        + _pad("", 12)
        + cpos
        + tinum
        + ein
        + " "
        + esn
        + " "
    )
    assert len(head) == 58, f"tram_line: head should have 58 chars, has {len(head)}"
    body = head + "   " + date
    return _pad(body, 273) + "\r\n"


DEFAULT_LINES = {
    "SECC": lambda province, municipio: [secc_line(province, municipio)],
    "UP": lambda province, municipio: [up_line(province, municipio, "0000000", "TEST MUNICIPALITY")],
    "VIAS": lambda province, municipio: [vias_line(province, municipio, "00001", "TEST STREET")],
    "PSEU": lambda province, municipio: [pseu_line(province, municipio, "00001", "TEST PSEUDOVIA")],
    "TRAM": lambda province, municipio: [tram_line(province, municipio, via_id="00001")],
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
    """Write a sample Callejero file with the dataset's official name.

    `subdir`, when given, mimics the nested layout of the real downloads
    (call_pXX_.../call_pXX_.../); by default it writes straight into `directory`, which is
    also a valid case (intake scans recursively).
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
    datasets: tuple[str, ...] = ("SECC", "UP", "VIAS", "PSEU", "TRAM"),
) -> str:
    """Build a sample --in directory: one file per (province, dataset)."""
    in_dir = os.path.join(root, "in")
    for province in provinces:
        subdir = f"call_p{province}_726/call_p{province}_072026"
        for dataset in datasets:
            write_callejero_file(in_dir, dataset, province, subdir=subdir)
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
