"""Independent validation of a snapshot partition.

Re-reads and re-parses every file declared in the manifest, recomputing checksums, counts
and the schema fingerprint instead of accepting the recorded values. It also scans the
directory to detect files the manifest does not declare.

Two kinds of problem:
  - INTEGRITY -> always fatal (code 1).
  - QUALITY   -> measured and reported; fatal only with --strict.

Coverage here is "every configured table_id (manifest.config.tables) has a file": unlike
Mercadona, there is no external category tree to discover what is expected — the table_ids
are the run's own configuration.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_json
from .partition import MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME, manifest_path, success_path
from .schema import count_series, fingerprint, is_mapping, series_of

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
        print(f"ERROR: unreadable manifest: {exc}")
        return EXIT_FAILED
    if not is_mapping(manifest) or not isinstance(manifest.get("files"), list):
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

    # ---- Integrity of the declared files -----------------------------------
    checked = 0
    declared: set = set()
    payloads: list[object] = []
    for entry in manifest["files"]:
        if not is_mapping(entry) or "path" not in entry:
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
        with open(target, "rb") as handle:
            blob = handle.read()
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum mismatch: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"size mismatch: {entry['path']}")
        try:
            payload, _ = read_json(target)
        except CorruptFileError as exc:
            errors.append(f"invalid JSON in {entry['path']}: {exc}")
            continue
        if entry.get("stage") == "tables":
            observed = count_series(payload)
            payloads.append(payload)
        else:
            observed = entry.get("records")
        if observed != entry.get("records"):
            errors.append(
                f"count mismatch in {entry['path']}: "
                f"manifest={entry.get('records')} file={observed}"
            )
    print(f"files ............ {checked}/{len(manifest['files'])} read and checked")

    if manifest.get("failures"):
        errors.append(
            f"{len(manifest['failures'])} table(s) recorded as failed in the manifest"
        )

    anomalies = manifest.get("anomalies") or []
    print(f"anomalies ........ {len(anomalies)}")
    for item in anomalies:
        if is_mapping(item):
            warnings.append(f"recorded anomaly: {item.get('kind')} at {item.get('path')}")

    # ---- Files on disk not declared in the manifest ------------------------
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

    # ---- Coverage: every configured table_id has a file --------------------
    configured = {str(t) for t in (manifest.get("config") or {}).get("tables") or []}
    present = {
        str(entry.get("table_id"))
        for entry in manifest["files"]
        if is_mapping(entry) and entry.get("stage") == "tables"
    }
    print(f"coverage ......... {len(present)}/{len(configured)} configured tables")
    missing = configured - present
    if missing:
        errors.append(f"configured table_id(s) without a file: {sorted(missing)}")

    # ---- Quality: every series has at least one point with a Valor ---------
    rows = 0
    points = 0
    without_value = 0
    for entry in manifest["files"]:
        if not is_mapping(entry) or entry.get("stage") != "tables":
            continue
        target = os.path.join(root, entry["path"])
        if not os.path.exists(target):
            continue
        try:
            payload, _ = read_json(target)
        except CorruptFileError:
            continue  # already reported as an integrity error
        for series in series_of(payload):
            rows += 1
            data = series.get("Data") if is_mapping(series) else None
            data = data if isinstance(data, list) else []
            if not any(is_mapping(p) and p.get("Valor") is not None for p in data):
                without_value += 1
            points += sum(1 for p in data if is_mapping(p))

    print(f"series ........... {rows}")
    print(f"data points ...... {points}")
    print(f"series w/o value . {without_value}")

    if rows == 0:
        print("completeness ..... n/a (no series read)")
    else:
        print(f"completeness ..... {100.0 * (1 - without_value / rows):.2f}% of series with a value")

    # ---- Schema fingerprint -------------------------------------------------
    declared_fp = manifest.get("schema_fingerprint") or {}
    observed_fp = fingerprint(payloads)
    same_fp = declared_fp.get("sha256") == observed_fp["sha256"]
    print(f"schema ........... {observed_fp['sha256'][:16]} ({'same' if same_fp else 'MISMATCH'})")
    if not same_fp:
        errors.append(
            f"schema_fingerprint mismatch: manifest={declared_fp.get('sha256')} "
            f"observed={observed_fp['sha256']}"
        )

    # ---- Totals re-checked against the re-read -----------------------------
    totals = manifest.get("totals") or {}
    table_entries = [
        e for e in manifest["files"] if is_mapping(e) and e.get("stage") == "tables"
    ]
    observed_totals = {
        "series_count": rows,
        "data_point_count": points,
        "table_files": len(table_entries),
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if is_mapping(e)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} mismatch: manifest={totals.get(key)} observed={observed}"
            )

    if table_entries and rows == 0:
        errors.append(
            f"no series read in {len(table_entries)} table file(s): "
            f"the source format may have changed"
        )
    if args.strict and without_value:
        errors.append(f"{without_value} series without any point with a value (--strict)")

    print()
    for item in warnings:
        print(f"WARNING: {item}")
    if errors:
        print(f"FAILED ({len(errors)} problem(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integrity, coverage, schema and totals validated.")
    return EXIT_OK
