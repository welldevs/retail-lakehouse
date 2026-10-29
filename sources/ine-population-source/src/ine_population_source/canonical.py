"""Canonical form of the snapshot files.

Every snapshot file is written with the same deterministic serialization, so that the
file's SHA-256 depends only on the payload content. This is the only function that writes
data files in the package.

The write is ATOMIC: it writes to a temporary file in the same directory and calls
os.replace. A process interrupted mid-write never leaves a partial file in the final place.
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


def read_json(path: str) -> tuple[object, bytes]:
    """Read a snapshot file. Raises CorruptFileError if it is not valid UTF-8 JSON."""
    with open(path, "rb") as handle:
        blob = handle.read()
    try:
        return json.loads(blob.decode("utf-8")), blob
    except (ValueError, UnicodeDecodeError) as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc
