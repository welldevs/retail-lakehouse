"""Identity, path and metadata of a snapshot partition.

A partition is the Source's unit of delivery:

    <root>/ingestion_date=YYYY-MM-DD/
      provinces/
        province=<code>/
          <ORIGINAL name of the downloaded file>   (one per dataset: SECC, UP, VIAS, PSEU, TRAM)

No second partition axis: the artifact represents an intake of the whole official dataset,
not a slice by warehouse or by province. "provinces/province=<code>/" is DIRECTORY
structure inside the partition — equivalent to "tables/" in the population source — not a
formal axis on the platform side (manifest.py still sees only `ingestion_date=...`).

The landed file name is the same as the original download (e.g.
"VIAS.P28.D260630.G260702"), not a generic name like "VIAS.txt": this preserves provenance
(INE date and generation) without needing another mechanism.

Rules enforced here, not merely documented:
  - partition tokens are validated (no path separator or '..');
  - a complete partition is immutable: it can only be rewritten with --overwrite;
  - the _SUCCESS marker exists only when the partition is complete.
"""

from __future__ import annotations

import os
import re

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, read_json, write_json

MANIFEST_NAME = "_manifest.json"
SUCCESS_NAME = "_SUCCESS"
RUN_LOG_NAME = "_run.log"
PROVINCES_DIR = "provinces"

# Datasets this Source ingests. The Callejero downloads contain 5 files per province
# (SECC, UP, VIAS, TRAM, PSEU) and all 5 are ingested (CONTRACT.md section 2) — TRAM was
# added after SECC/UP/VIAS/PSEU because it is the only one of the 5 that links street,
# census section, entity/nucleus and postal code (CPOS) in a single record; the other 4
# carry no postal code.
DATASETS = ("SECC", "UP", "VIAS", "PSEU", "TRAM")

# The 4 warehouse provinces (mad1=28, bcn1=08, svq1=41, vlc1=46). CLI default, same in
# spirit as INE_TABLES in the population source — configurable, not hard-coded beyond the
# default.
DEFAULT_PROVINCES = ("08", "28", "41", "46")

PROVINCE_TOKEN = re.compile(r"^\d{2}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Callejero file name as INE distributes it, e.g. "VIAS.P28.D260630.G260702".
# Used both to recognize input files in --in and already landed files.
INPUT_FILE = re.compile(
    r"^(?P<dataset>SECC|UP|VIAS|PSEU|TRAM)\.P(?P<province>\d{2})\.D(?P<date>\d{6})\.G(?P<gen>\d{6})$"
)


class PartitionError(Exception):
    """Invalid partition use: malformed token or immutability violated."""


def validate_province(value: str) -> str:
    if not isinstance(value, str) or not PROVINCE_TOKEN.match(value):
        raise PartitionError(
            f"invalid province code: {value!r}. Expected 2 digits, e.g. '28'."
        )
    return value


def validate_date(value: str) -> str:
    if not isinstance(value, str) or not ISO_DATE.match(value):
        raise PartitionError(f"invalid date: {value!r}. Expected format: YYYY-MM-DD.")
    return value


def partition_path(root: str, ingestion_date: str) -> str:
    validate_date(ingestion_date)
    return os.path.join(root, f"ingestion_date={ingestion_date}")


def province_dir(partition: str, province: str) -> str:
    validate_province(province)
    return os.path.join(partition, PROVINCES_DIR, f"province={province}")


def dataset_path(partition: str, province: str, original_filename: str) -> str:
    """Landed path of a dataset, with the original name preserved."""
    if not INPUT_FILE.match(original_filename):
        # Message kept byte-identical: intake persists str(exc) into the manifest's
        # failures[].error, which is part of the data contract.
        raise PartitionError(f"nome de arquivo fora do padrao do Callejero: {original_filename!r}")
    return os.path.join(province_dir(partition, province), original_filename)


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

    Same logic as the population source: prevents rewriting a complete partition without
    --overwrite, reopening a partition with an unreadable manifest as if it were new, or
    ignoring _SUCCESS when the manifest was removed.
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
        if os.path.exists(success_path(partition)) and not overwrite:
            raise PartitionError(
                "partition marked complete by _SUCCESS, but has no readable manifest. "
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
    if previous is None:
        return []
    history = list(previous.get("history") or [])
    totals = previous.get("totals") or {}
    history.append(
        {
            "run_id": previous.get("run_id"),
            "started_at_utc": previous.get("started_at_utc"),
            "finished_at_utc": previous.get("finished_at_utc"),
            "duration_seconds": previous.get("duration_seconds"),
            "complete": previous.get("complete"),
            "files_landed": totals.get("files_landed"),
        }
    )
    return history


def write_manifest(partition: str, manifest: dict) -> tuple[str, int]:
    manifest["manifest_version"] = MANIFEST_VERSION
    return write_json(manifest_path(partition), manifest)


def mark_success(partition: str, complete: bool, run_id: str) -> None:
    path = success_path(partition)
    if complete:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"{run_id}\n")
    elif os.path.exists(path):
        os.unlink(path)
