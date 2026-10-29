"""Identity, path and metadata of a snapshot partition.

A partition is the Source's unit of delivery:

    <root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/

`ingestion_date` HERE IS THE ORDER DATE, NOT THE EVENT DATE
-----------------------------------------------------------
Every event of an order lives in the partition of the day the order was PLACED, even when
`occurred_at` crosses midnight (an order placed at 22:40 and delivered at 9:00 the next day
leaves all its events here).

The alternative — partitioning by event date — would make a day's partition impossible to
close: it would only be complete two days later, when that day's last order finished, and
`_SUCCESS` would lose its meaning. As a declared consequence, a consumer that wants "events
that occurred on day D" filters by `occurred_at`, not by `ingestion_date`; Silver exposes both
columns. See CONTRACT.md section 4.

The `wh=` axis is not an aesthetic choice: `platform/src/retail_platform/manifest.py` derives
`axis_name = "wh" if warehouse else None`, and those are the only two layouts the platform's
manifest reader understands. One partition per warehouse also gives failure isolation — if
`vlc1` fails, `mad1`/`bcn1`/`svq1` are unaffected — just like Mercadona.

Rules enforced here, not merely documented:
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
EVENTS_FILE = "order_events.jsonl"

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


def events_path(partition: str) -> str:
    return os.path.join(partition, EVENTS_FILE)


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
        # Without a readable manifest, the _SUCCESS marker still attests a complete
        # partition: without this check, deleting the manifest would bypass immutability.
        if os.path.exists(success_path(partition)) and not overwrite:
            raise PartitionError(
                "partition marked complete by _SUCCESS, but without a readable manifest. "
                "Use --overwrite to rewrite it deliberately."
            )
        return None

    if previous.get("complete") and not overwrite:
        raise PartitionError(
            "partition is already complete and immutable. Use --overwrite to rewrite it "
            "deliberately, or extract to another ingestion_date."
        )
    return previous


def build_history(previous: dict | None) -> list[dict]:
    """Run history, so that provenance survives re-runs.

    Keeps the seed AND the premises digest of each previous run. Those are the two inputs
    that change the orders under the same order_id: an --overwrite with another seed swaps the
    orders, and an --overwrite with another premises table swaps the rates behind them.
    Without both recorded, neither swap would leave a trace.
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
            "orders": config.get("orders"),
            "premises_sha256": config.get("premises_sha256"),
            "event_rows": totals.get("event_rows"),
            "order_rows": totals.get("order_rows"),
        }
    )
    return history


def write_manifest(partition: str, manifest: dict) -> tuple[str, int]:
    manifest["manifest_version"] = MANIFEST_VERSION
    return write_json(manifest_path(partition), manifest)


def mark_success(partition: str, complete: bool, run_id: str) -> None:
    """Create _SUCCESS when the partition is complete; remove it when it is not.

    Data lake convention: the consumer can decide whether to read the partition by looking at
    a single file, without parsing the manifest.
    """
    path = success_path(partition)
    if complete:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"{run_id}\n")
    elif os.path.exists(path):
        os.unlink(path)
