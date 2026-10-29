"""Structural facts observed in a Callejero file, without interpreting its content.

The files are fixed-width text in ISO-8859-1 with CRLF line endings (measured against the
real files — see CONTRACT.md section 2). This Source does not parse fields: it only
observes structural facts (how many lines, which widths appear) for the manifest and for
revalidation — the same role `schema_fingerprint` plays in the population source, adapted
to a format without named keys.

A width divergence between two successive ingestion_dates signals that INE changed the
file layout — information for the consumer (Silver), not something this Source tries to
fix or validate against a fixed expected value: the layout observed today is not assumed
to be stable forever.
"""

from __future__ import annotations

ENCODING = "latin-1"  # ISO-8859-1, measured. Never converted on write (CONTRACT.md).


def line_stats(path: str) -> dict:
    """Line count and width distribution (without the terminator) of the file.

    Defensive: an empty file, or one unreadable in the expected encoding, does not raise
    here — it returns empty stats. The caller (intake/validate) decides whether that is a
    failure, not this function.
    """
    widths: dict[int, int] = {}
    count = 0
    try:
        with open(path, encoding=ENCODING, newline="") as handle:
            for raw_line in handle:
                count += 1
                width = len(raw_line.rstrip("\r\n"))
                widths[width] = widths.get(width, 0) + 1
    except OSError:
        return {"line_count": 0, "widths": {}}
    return {"line_count": count, "widths": widths}


def dominant_width(stats: dict) -> int | None:
    """The most frequent width, or None if the file is empty.

    Does not claim that EVERY line has that width — it only reports the most common one. A
    file with mixed widths still produces a result (not an error here); the caller decides
    what to do with it.
    """
    widths = stats.get("widths") or {}
    if not widths:
        return None
    return max(widths.items(), key=lambda item: item[1])[0]
