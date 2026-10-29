"""Canonical form of the snapshot files.

Every snapshot file is written with the same deterministic serialization, so the file's
SHA-256 depends only on the payload content. This is the only function in the package that
writes data files.

Writing is ATOMIC: it writes to a temporary file in the same directory and calls os.replace. A
process interrupted mid-write never leaves a partial file at the final location.

Identical to the other four sources in this repo for JSON, on purpose: the consumer
recomputes the digest the same way for any partition, whichever source it comes from.

DELIBERATE DEVIATION: `write_jsonl`
-----------------------------------
This Source's data file is a LOG, and a log is not an array. The other four write a JSON
array with `indent=2`; here `order_events.jsonl` is NDJSON — one line per event, each line
canonical and compact. Three reasons, in this order:

  1. a log is read line by line and grows by append; an array needs the whole file in
     memory both to write and to read;
  2. each line is a complete, independent event, which is exactly what crosses a broker
     further down — the same bytes on disk and on the topic;
  3. DuckDB's `read_json(..., format='newline_delimited')` consumes NDJSON directly.

The guarantee that matters is preserved: the same content produces the same bytes and the
same SHA-256, because each line is serialized with `sort_keys` and the ORDER of the lines is
determined by the generator, never by iterating a `set` or `dict`.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile

# Canonical form parameters. Any reimplementation that changes them produces a different
# digest for the same content.
JSON_ENSURE_ASCII = False
JSON_SORT_KEYS = True
JSON_INDENT = 2
JSON_TRAILING_NEWLINE = "\n"


class CorruptFileError(Exception):
    """Existing file on disk that cannot be read as valid UTF-8 JSON."""


def canonical_bytes(payload) -> bytes:
    """Serialize the payload in the snapshot's canonical form."""
    text = json.dumps(
        payload,
        ensure_ascii=JSON_ENSURE_ASCII,
        sort_keys=JSON_SORT_KEYS,
        indent=JSON_INDENT,
    )
    return (text + JSON_TRAILING_NEWLINE).encode("utf-8")


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def write_json(path: str, payload) -> tuple[str, int]:
    """Write the payload atomically in canonical form. Returns (sha256, bytes)."""
    blob = canonical_bytes(payload)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        # Includes KeyboardInterrupt: the temporary file never becomes the final file.
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return digest(blob), len(blob)


def write_jsonl(path: str, rows) -> tuple[str, int, int]:
    """Write an NDJSON log atomically. Returns (sha256, bytes, lines).

    Each line is `json.dumps(..., sort_keys=True, ensure_ascii=False)` WITHOUT indentation, and
    the file ends with a newline. Line order is the caller's responsibility: it is part of the
    content and therefore of the digest.
    """
    chunks = []
    count = 0
    for row in rows:
        chunks.append(
            json.dumps(row, ensure_ascii=False, sort_keys=JSON_SORT_KEYS).encode("utf-8")
        )
        chunks.append(b"\n")
        count += 1
    blob = b"".join(chunks)

    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".jsonl")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return digest(blob), len(blob), count


def read_jsonl(path: str) -> tuple[list, bytes]:
    """Read an NDJSON log. Raises CorruptFileError on the first invalid line."""
    blob = read_bytes(path)
    rows = []
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except ValueError as exc:
            raise CorruptFileError(f"{path}: line {number}: {exc}") from exc
    return rows, blob


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def read_json(path: str) -> tuple[object, bytes]:
    """Read a snapshot file. Raises CorruptFileError if it is not valid UTF-8 JSON."""
    blob = read_bytes(path)
    try:
        return json.loads(blob.decode("utf-8")), blob
    except (ValueError, UnicodeDecodeError) as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc
