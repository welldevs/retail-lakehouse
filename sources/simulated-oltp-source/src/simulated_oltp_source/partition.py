"""Identity, path and metadata of a snapshot partition.

A partition is the Source's unit of delivery:

    <root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/

The `wh=` axis is not an aesthetic choice: `platform/src/retail_platform/manifest.py` derives
`axis_name = "wh" if warehouse else None`, and those are the only two layouts the platform's
manifest reader understands. One partition per warehouse also gives failure isolation — if
`vlc1` fails, `mad1`/`bcn1`/`svq1` are not affected — same as Mercadona.

Rules enforced here, not just documented:
  - partition tokens are validated (no path separator or '..');
  - a complete partition is immutable: it can only be rewritten with --overwrite;
  - the _SUCCESS marker only exists when the partition is complete.
"""

from __future__ import annotations

import os
import re

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, read_json, write_json

MANIFEST_NAME = "_manifest.json"
SUCCESS_NAME = "_SUCCESS"
CUSTOMERS_FILE = "customers.json"

DEFAULT_WAREHOUSES = ("mad1", "bcn1", "svq1", "vlc1")

# Tokens that become directory names. Deliberately restrictive: prevents path traversal
# coming from a CLI parameter.
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PartitionError(Exception):
    """Invalid partition use: malformed token or immutability violation."""


def validate_token(name: str, value: str) -> str:
    if not isinstance(value, str) or not SAFE_TOKEN.match(value):
        raise PartitionError(
            f"invalid {name}: {value!r}. Use only letters, digits, '-' and '_' "
            f"(1 to 64 characters, starting with a letter or digit)."
        )
    return value


def validate_date(value: str) -> str:
    if not isinstance(value, str) or not ISO_DATE.match(value):
        raise PartitionError(f"invalid date: {value!r}. Expected format: YYYY-MM-DD.")
    return value


def partition_path(root: str, ingestion_date: str, warehouse: str) -> str:
    validate_date(ingestion_date)
    validate_token("wh", warehouse)
    return os.path.join(root, f"ingestion_date={ingestion_date}", f"wh={warehouse}")


def customers_path(partition: str) -> str:
    return os.path.join(partition, CUSTOMERS_FILE)


def manifest_path(partition: str) -> str:
    return os.path.join(partition, MANIFEST_NAME)


def success_path(partition: str) -> str:
    return os.path.join(partition, SUCCESS_NAME)


def read_manifest(partition: str) -> dict | None:
    """Read the partition manifest. None if it does not exist; error if it is corrupt."""
    path = manifest_path(partition)
    if not os.path.exists(path):
        return None
    payload, _ = read_json(path)
    if not isinstance(payload, dict):
        raise CorruptFileError(f"{path}: manifest is not a JSON object")
    return payload


def assert_writable(partition: str, overwrite: bool) -> dict | None:
    """Validate reopening an existing partition. Returns the previous manifest.

    Prevents uses the contract forbids:
      - rewriting an already complete partition without asking explicitly;
      - reopening a partition whose manifest is unreadable, as if it were new;
      - ignoring the _SUCCESS marker when the manifest was removed.
    """
    try:
        previous = read_manifest(partition)
    except CorruptFileError as exc:
        if not overwrite:
            raise PartitionError(
                f"previous manifest unreadable ({exc}). Use --overwrite to rewrite the "
                f"partition from scratch, or extract to another ingestion_date."
            ) from exc
        previous = None

    if previous is not None and not isinstance(previous.get("files"), list):
        if not overwrite:
            raise PartitionError(
                "previous manifest has no 'files' list. Use --overwrite to rewrite "
                "the partition from scratch."
            )
        previous = None

    if previous is None:
        # Without a readable manifest, the _SUCCESS marker still witnesses a complete
        # partition: without this check, deleting the manifest would bypass immutability.
        if os.path.exists(success_path(partition)) and not overwrite:
            raise PartitionError(
                "partition marked complete by _SUCCESS, but without a readable manifest. "
                "Use --overwrite to rewrite it deliberately."
            )
        return None

    if previous.get("complete") and not overwrite:
        raise PartitionError(
            "partition is already complete and is immutable. Use --overwrite to rewrite it "
            "deliberately, or extract to another ingestion_date."
        )
    return previous


def build_history(previous: dict | None) -> list[dict]:
    """Run history, so that provenance survives re-runs.

    Keeps the seed of each previous run: without it, an --overwrite with another seed
    would swap the people behind the same customer_id without leaving a trace.
    """
    if previous is None:
        return []
    history = list(previous.get("history") or [])
    totals = previous.get("totals") or {}
    config = previous.get("config") or {}
    history.append(
        {
            "run_id": previous.get("run_id"),
            "started_at_utc": previous.get("started_at_utc"),
            "finished_at_utc": previous.get("finished_at_utc"),
            "duration_seconds": previous.get("duration_seconds"),
            "complete": previous.get("complete"),
            "seed": config.get("seed"),
            "count": config.get("count"),
            "customer_rows": totals.get("customer_rows"),
        }
    )
    return history


def write_manifest(partition: str, manifest: dict) -> tuple[str, int]:
    manifest["manifest_version"] = MANIFEST_VERSION
    return write_json(manifest_path(partition), manifest)


def mark_success(partition: str, complete: bool, run_id: str) -> None:
    """Create _SUCCESS when the partition is complete; remove it when it is not.

    Data lake convention: the consumer can decide whether to read the partition by looking
    at a single file, without parsing the manifest.
    """
    path = success_path(partition)
    if complete:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"{run_id}\n")
    elif os.path.exists(path):
        os.unlink(path)
