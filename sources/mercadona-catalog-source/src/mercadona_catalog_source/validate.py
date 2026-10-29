"""Independent validation of a snapshot partition.

Re-reads and re-parses every file declared in the manifest, recomputing checksums,
counts and the schema fingerprint instead of accepting the recorded values. It also
sweeps the directory to detect files the manifest does not declare.

Two kinds of problem:
  - INTEGRITY -> always fatal (code 1).
  - QUALITY   -> measured and reported; fatal only with --strict.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_json
from .partition import (
    CATALOG_DIR,
    CATEGORIES_DIR,
    MANIFEST_NAME,
    RUN_LOG_NAME,
    SUCCESS_NAME,
    categories_path,
    manifest_path,
    success_path,
)
from .schema import (
    count_level1,
    count_products,
    fingerprint,
    flatten_tree,
    is_mapping,
    iter_products,
    SchemaError,
)

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
    if not is_mapping(manifest) or not isinstance(manifest.get("files"), list):
        print(f"ERROR: manifest has no 'files' list: {path}")
        return EXIT_FAILED

    root = os.path.abspath(os.path.join(partition, "..", ".."))
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
    rows_by_entry: dict = {}
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
        if entry.get("stage") == "catalog":
            observed = count_products(payload)
            rows_by_entry[entry["path"]] = observed
            payloads.append(payload)
        elif entry.get("stage") == "categories":
            observed = count_level1(payload)
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
            f"{len(manifest['failures'])} category(ies) recorded as failed in the manifest"
        )

    # Anomalies are events already handled by the extraction (file re-downloaded from the
    # source). They stay visible for auditing, without failing a partition that was fixed.
    anomalies = manifest.get("anomalies") or []
    print(f"anomalies ........ {len(anomalies)}")
    for item in anomalies:
        if is_mapping(item):
            warnings.append(f"recorded anomaly: {item.get('kind')} at {item.get('path')}")

    # ---- Files on disk not declared in the manifest ------------------------
    # Recursive sweep: a shallow listdir would let catalog/backup/old.json and any
    # subdirectory created inside the partition slip through, and the closed-inventory
    # guarantee would only hold for the first level.
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

    # ---- Coverage: every level-2 category has a file ----------------------
    tree_file = categories_path(partition)
    level2: list = []
    if not os.path.exists(tree_file):
        errors.append("category tree missing: categories/categories.json")
    else:
        try:
            tree, _ = read_json(tree_file)
            level2 = flatten_tree(tree)
        except (CorruptFileError, SchemaError) as exc:
            errors.append(f"category tree unreadable: {exc}")

    expected = {node["id"] for node in level2}
    present = {
        entry.get("category_id")
        for entry in manifest["files"]
        if is_mapping(entry) and entry.get("stage") == "catalog"
    }
    print(f"coverage ......... {len(present)}/{len(expected)} level-2 categories")
    limit = (manifest.get("config") or {}).get("limit")
    missing = expected - present
    if missing and limit is None:
        errors.append(f"level-2 categories without a file: {sorted(missing)}")
    elif missing and limit is not None:
        warnings.append(
            f"{len(missing)} category(ies) without a file (expected: partial partition, limit={limit})"
        )

    # ---- Quality: product, category and price ------------------------------
    lineage = {node["id"]: node for node in level2}
    rows = 0
    unique_ids: set = set()
    missing_name = missing_price = missing_category = 0
    for entry in manifest["files"]:
        if not is_mapping(entry) or entry.get("stage") != "catalog":
            continue
        target = os.path.join(root, entry["path"])
        if not os.path.exists(target):
            continue
        try:
            payload, _ = read_json(target)
        except CorruptFileError:
            continue  # already reported as an integrity error
        has_lineage = entry.get("category_id") in lineage
        for group, product in iter_products(payload):
            rows += 1
            if "id" in product:
                unique_ids.add(product["id"])
            if not product.get("display_name"):
                missing_name += 1
            price = (product.get("price_instructions") or {}).get("unit_price")
            if price is None or str(price).strip() == "":
                missing_price += 1
            if not has_lineage or not group.get("name"):
                missing_category += 1

    duplicates = rows - len(unique_ids)
    print(f"rows ............. {rows}")
    print(f"unique products .. {len(unique_ids)} ({duplicates} repeated rows)")
    print(f"missing name ..... {missing_name}")
    print(f"missing price .... {missing_price}")
    print(f"missing category . {missing_category}")

    incomplete = missing_name + missing_price + missing_category
    if rows == 0:
        print("completeness ..... n/a (no rows read)")
    else:
        print(
            f"completeness ..... {100.0 * (1 - incomplete / (rows * 3)):.2f}% "
            f"of the in-scope fields"
        )

    # ---- Schema fingerprint ------------------------------------------------
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
    catalog_entries = [
        e for e in manifest["files"] if is_mapping(e) and e.get("stage") == "catalog"
    ]
    observed_totals = {
        "product_rows": rows,
        "unique_product_ids": len(unique_ids),
        "catalog_files": len(catalog_entries),
        "bytes": sum(
            e.get("bytes", 0) for e in manifest["files"] if is_mapping(e)
        ),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} mismatch: manifest={totals.get(key)} observed={observed}"
            )

    if catalog_entries and rows == 0:
        errors.append(
            f"no product read in {len(catalog_entries)} catalog file(s): "
            f"the source format may have changed"
        )
    if args.strict and incomplete:
        errors.append(f"{incomplete} in-scope field(s) missing (--strict)")

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
