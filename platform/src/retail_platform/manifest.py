"""Reading of a partition's _manifest.json, treated as a CONTRACT.

This module implements the consumer obligations of CONTRACT.md section 4. It
deliberately does NOT import mercadona_catalog_source: the platform consumes the physical
contract (canonical JSON + manifest), the same way an outside consumer would. If the
Source package changed internally, this reading would remain valid.

Rules enforced here, not just documented:
  - _SUCCESS and complete: true, BOTH (guarantee 10: inconsistency is an error);
  - a supported manifest_version, instead of guesswork (section 8);
  - files[].path is relative to the snapshot ROOT, not the partition (obligation 4.1);
  - a path escaping the snapshot root is refused;
  - the manifest's ingestion_date/wh checked against the path on disk.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field

from . import SUPPORTED_MANIFEST_VERSIONS

MANIFEST_NAME = "_manifest.json"
SUCCESS_NAME = "_SUCCESS"
RUN_LOG_NAME = "_run.log"

# Files that exist in the partition and are NOT declared in files[]. The Source's
# validate.py treats them the same way when scanning for orphans; the platform needs the
# same list so it does not flag as orphan a file the contract expects.
UNDECLARED_FILES = (MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME)


class ManifestError(Exception):
    """Partition that does not satisfy the reading contract."""


@dataclass(frozen=True)
class FileEntry:
    """One entry of files[]. `path` is relative to the snapshot root."""

    path: str
    sha256: str
    bytes: int
    records: int | None
    stage: str | None
    category_id: object
    local_path: str

    def digest_on_disk(self) -> tuple[str, int]:
        """Recomputes sha256 and size of the local file. Does not trust the manifest."""
        with open(self.local_path, "rb") as handle:
            blob = handle.read()
        return hashlib.sha256(blob).hexdigest(), len(blob)


@dataclass(frozen=True)
class Partition:
    path: str
    root: str
    ingestion_date: str
    # Second partition axis, besides ingestion_date. Two shapes known today: with an axis
    # (Mercadona: axis_name="wh") and with no axis at all (INE: both None, the source
    # returns every province in a single payload). There is no third case to generalize
    # for now.
    axis_name: str | None
    axis_value: str | None
    run_id: str
    complete: bool
    source_name: str
    lang: str
    files: list[FileEntry]
    totals: dict
    schema_fingerprint: dict
    anomalies: list = field(default_factory=list)

    @property
    def warehouse(self) -> str | None:
        """Compatibility with Mercadona consumers. None for a source without that axis
        (e.g. INE) — use axis_name/axis_value for the general case."""
        return self.axis_value if self.axis_name == "wh" else None

    @property
    def prefix_suffix(self) -> str:
        """Hive suffix of the partition, as it appears in files[].path and in the object key."""
        suffix = f"ingestion_date={self.ingestion_date}"
        if self.axis_name is not None:
            suffix += f"/{self.axis_name}={self.axis_value}"
        return suffix

    def undeclared_paths(self) -> list[tuple[str, str]]:
        """(local path, name) of the expected-but-undeclared files that exist."""
        found = []
        for name in UNDECLARED_FILES:
            candidate = os.path.join(self.path, name)
            if os.path.exists(candidate):
                found.append((candidate, name))
        return found


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def read(partition_path: str) -> Partition:
    """Reads and validates a partition's manifest. Raises ManifestError if unusable."""
    manifest_file = os.path.join(partition_path, MANIFEST_NAME)
    if not os.path.exists(manifest_file):
        raise ManifestError(f"manifest not found: {manifest_file}")
    try:
        with open(manifest_file, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ManifestError(f"unreadable manifest: {manifest_file}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ManifestError(f"manifest is not a JSON object: {manifest_file}")

    source_name = (manifest.get("source") or {}).get("name", "")
    if source_name not in SUPPORTED_MANIFEST_VERSIONS:
        raise ManifestError(
            f"source.name {source_name!r} unknown to this platform "
            f"(expected one of {sorted(SUPPORTED_MANIFEST_VERSIONS)})"
        )
    expected_version = SUPPORTED_MANIFEST_VERSIONS[source_name]
    version = manifest.get("manifest_version")
    if version != expected_version:
        raise ManifestError(
            f"manifest_version {version!r} not supported for {source_name!r} "
            f"(this platform reads {expected_version})"
        )

    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise ManifestError("manifest without the 'files' list")

    # Guarantee 10: _SUCCESS exists if and only if complete. We require both, because
    # trusting either one alone would accept a partition the Source considers invalid.
    complete = bool(manifest.get("complete"))
    has_success = os.path.exists(os.path.join(partition_path, SUCCESS_NAME))
    if complete != has_success:
        raise ManifestError(
            f"_SUCCESS {'present' if has_success else 'missing'} contradicts "
            f"complete={complete}"
        )
    if not complete:
        raise ManifestError(
            "incomplete partition (complete: false). The platform only consumes complete "
            "partitions: complete it with a new extract before landing."
        )
    if manifest.get("failures"):
        raise ManifestError(
            f"{len(manifest['failures'])} category(ies) in failures[]: partition failed "
            f"by the Source's own contract (guarantee 16)"
        )

    partition_block = manifest.get("partition") or {}
    ingestion_date = partition_block.get("ingestion_date")
    warehouse = partition_block.get("warehouse")
    if not ingestion_date:
        raise ManifestError("manifest without partition.ingestion_date")

    # axis_name is always "wh" when a warehouse is declared: it is the only axis known
    # today besides ingestion_date. Without it (e.g. INE), the partition has only
    # ingestion_date.
    axis_name = "wh" if warehouse else None
    axis_value = warehouse

    # Obligation 4.1: files[].path starts at the snapshot ROOT, not at the partition. Go up
    # one level per partition axis present, plus ingestion_date itself.
    levels_up = 2 if axis_name is not None else 1
    root = os.path.abspath(os.path.join(partition_path, *([".."] * levels_up)))

    # The path on disk and the manifest must agree. A mismatch means a moved or renamed
    # partition, and the axis only exists in the path and the manifest (obligation 4.3):
    # accepting the mismatch would lose the identity irrecoverably.
    if axis_name is not None:
        expected_tail = os.path.join(
            f"ingestion_date={ingestion_date}", f"{axis_name}={axis_value}"
        )
    else:
        expected_tail = f"ingestion_date={ingestion_date}"
    if not os.path.abspath(partition_path).endswith(expected_tail):
        raise ManifestError(
            f"partition path ({partition_path}) does not match the manifest "
            f"({expected_tail})"
        )

    files: list[FileEntry] = []
    for entry in entries:
        if not isinstance(entry, dict) or "path" not in entry:
            raise ManifestError(f"malformed manifest entry: {entry!r}")
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            raise ManifestError(f"path outside the snapshot root: {entry['path']}")
        files.append(
            FileEntry(
                path=entry["path"],
                sha256=entry.get("sha256", ""),
                bytes=entry.get("bytes", -1),
                records=entry.get("records"),
                stage=entry.get("stage"),
                category_id=entry.get("category_id"),
                local_path=target,
            )
        )

    return Partition(
        path=os.path.abspath(partition_path),
        root=root,
        ingestion_date=ingestion_date,
        axis_name=axis_name,
        axis_value=axis_value,
        run_id=manifest.get("run_id", ""),
        complete=complete,
        source_name=source_name,
        lang=(manifest.get("source") or {}).get("lang", ""),
        files=files,
        totals=manifest.get("totals") or {},
        schema_fingerprint=manifest.get("schema_fingerprint") or {},
        anomalies=manifest.get("anomalies") or [],
    )
