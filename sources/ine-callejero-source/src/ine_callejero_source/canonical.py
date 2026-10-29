"""The two file forms this Source writes: the JSON manifest and the verbatim dataset.

Unlike the other Sources (which reserialize the payload into a canonical JSON form), the
Callejero files are fixed-width text whose format is defined by INE itself — this Source
does NOT rewrite them, it only copies them. "Canonical" for a dataset here means "the same
bytes as the downloaded file", not a serialization of our own; the SHA-256 is over the
file as it arrived. `_manifest.json` is still real JSON, written in the same deterministic
form as the other Sources.

Both writes are ATOMIC: temp file in the same directory + fsync + os.replace. A process
interrupted midway never leaves a partial file at the final location.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile

# Canonical form of the MANIFEST (JSON). Same parameters as the other Sources: any
# reimplementation that changes them produces a different digest for the same content.
JSON_ENSURE_ASCII = False
JSON_SORT_KEYS = True
JSON_INDENT = 2
JSON_TRAILING_NEWLINE = "\n"


class CorruptFileError(Exception):
    """Manifest on disk that cannot be read as valid UTF-8 JSON."""


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _atomic_write(path: str, blob: bytes) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        # Includes KeyboardInterrupt: the temp file never becomes the final file.
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


# ---- manifest (JSON) ---------------------------------------------------------------


def canonical_json_bytes(payload) -> bytes:
    text = json.dumps(
        payload,
        ensure_ascii=JSON_ENSURE_ASCII,
        sort_keys=JSON_SORT_KEYS,
        indent=JSON_INDENT,
    )
    return (text + JSON_TRAILING_NEWLINE).encode("utf-8")


def write_json(path: str, payload) -> tuple[str, int]:
    blob = canonical_json_bytes(payload)
    _atomic_write(path, blob)
    return digest(blob), len(blob)


def read_json(path: str) -> tuple[object, bytes]:
    with open(path, "rb") as handle:
        blob = handle.read()
    try:
        return json.loads(blob.decode("utf-8")), blob
    except (ValueError, UnicodeDecodeError) as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc


# ---- dataset (verbatim) ------------------------------------------------------------


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def copy_verbatim(source_path: str, target_path: str) -> tuple[str, int]:
    """Copy source_path to target_path atomically. Returns (sha256, bytes).

    No decoding, encoding conversion or line-ending normalization happens here: the
    bytes written are exactly the bytes read. CONTRACT.md section 2 promises this
    explicitly.
    """
    blob = read_bytes(source_path)
    _atomic_write(target_path, blob)
    return digest(blob), len(blob)
