"""Independent validation of a snapshot partition.

Rereads every file declared in the manifest, recomputing checksum and line statistics
instead of accepting the recorded values. It also scans the directory to detect files the
manifest does not declare.

Two kinds of problem:
  - INTEGRITY -> always fatal (exit 1).
  - COVERAGE/QUALITY -> measured and reported; fatal only with --strict.

Coverage here means "every (province, dataset) combination configured in manifest.config
has a landed file" — the same data the intake run itself declared as its target, like the
role of manifest.config.tables in the population source.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json
from .partition import DATASETS, MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME, manifest_path, success_path
from .schema import dominant_width, line_stats

EXIT_OK = 0
EXIT_FAILED = 1


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def run(args) -> int:
    partition = args.partition
    errors: list[str] = []
    warnings: list[str] = []

    path = manifest_path(partition)
    if not os.path.exists(path):
        print(f"ERROR: manifest not found at {path}")
        return EXIT_FAILED
    try:
        manifest, _ = read_json(path)
    except CorruptFileError as exc:
        print(f"ERROR: manifest unreadable: {exc}")
        return EXIT_FAILED
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        print(f"ERROR: manifest has no 'files' list: {path}")
        return EXIT_FAILED

    root = os.path.abspath(os.path.join(partition, ".."))
    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        errors.append(
            f"manifest_version {version!r} differs from the supported one ({MANIFEST_VERSION})"
        )

    print(f"partition ........ {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"complete ......... {manifest.get('complete')}")

    # ---- Integrity of the declared files ----------------------------------
    checked = 0
    declared: set = set()
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or "path" not in entry:
            errors.append(f"malformed manifest entry: {entry!r}")
            continue
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            errors.append(f"path outside the snapshot root: {entry['path']}")
            continue
        declared.add(os.path.abspath(target))
        if not os.path.exists(target):
            errors.append(f"missing file: {entry['path']}")
            continue
        blob = read_bytes(target)
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum mismatch: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"size mismatch: {entry['path']}")
        stats = line_stats(target)
        if stats["line_count"] != entry.get("line_count"):
            errors.append(
                f"line count mismatch in {entry['path']}: "
                f"manifest={entry.get('line_count')} file={stats['line_count']}"
            )
        observed_width = dominant_width(stats)
        if observed_width != entry.get("dominant_line_width"):
            warnings.append(
                f"dominant line width diverged in {entry['path']}: "
                f"manifest={entry.get('dominant_line_width')} file={observed_width} "
                f"(may indicate an INE layout change)"
            )
    print(f"files ............ {checked}/{len(manifest['files'])} read and checked")

    if manifest.get("failures"):
        errors.append(
            f"{len(manifest['failures'])} province/dataset combination(s) recorded "
            f"as failed in the manifest"
        )

    # ---- Files on disk not declared in the manifest -----------------------
    orphans: list[str] = []
    known_root_files = {MANIFEST_NAME, SUCCESS_NAME, RUN_LOG_NAME}
    partition_abs = os.path.abspath(partition)
    for base, _, names in os.walk(partition_abs):
        for name in sorted(names):
            candidate = os.path.abspath(os.path.join(base, name))
            if base == partition_abs and name in known_root_files:
                continue
            if candidate not in declared:
                orphans.append(os.path.relpath(candidate, partition_abs))
    print(f"orphans .......... {len(orphans)}")
    if orphans:
        errors.append(f"file(s) on disk not in the manifest: {orphans}")

    # ---- _SUCCESS marker consistent with 'complete' ------------------------
    has_success = os.path.exists(success_path(partition))
    if bool(manifest.get("complete")) != has_success:
        errors.append(
            f"_SUCCESS {'present' if has_success else 'absent'} contradicts "
            f"complete={manifest.get('complete')}"
        )

    # ---- Coverage: every configured (province, dataset) combination has a file --
    configured_provinces = list((manifest.get("config") or {}).get("provinces") or [])
    configured_datasets = list((manifest.get("config") or {}).get("datasets") or DATASETS)
    expected = {(p, d) for p in configured_provinces for d in configured_datasets}
    present = {
        (entry.get("province"), entry.get("dataset"))
        for entry in manifest["files"]
        if isinstance(entry, dict)
    }
    print(f"coverage ......... {len(present)}/{len(expected)} province/dataset combinations")
    missing = expected - present
    if missing:
        errors.append(f"configured combination(s) without a file: {sorted(missing)}")

    # ---- Totals rechecked against the reread -------------------------------
    totals = manifest.get("totals") or {}
    observed_totals = {
        "files_landed": checked,
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} mismatch: manifest={totals.get(key)} observed={observed}"
            )

    if args.strict and warnings:
        errors.append(f"{len(warnings)} line-width warning(s) (--strict)")

    print()
    for item in warnings:
        print(f"WARNING: {item}")
    if errors:
        print(f"FAILED ({len(errors)} problem(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integrity, coverage and totals validated.")
    return EXIT_OK
