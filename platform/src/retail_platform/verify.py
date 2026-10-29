"""Independent verification of a landed partition.

Same principle as the Source's validate.py: REREADS and RECOMPUTES instead of trusting
what was written. Downloads each declared object, recomputes SHA-256 and size, and scans
the whole prefix for objects the manifest does not declare.

The local manifest is the authoritative inventory (obligation 4.1). Besides checking the
objects against it, verification requires the landed _manifest.json to be byte-identical
to the local one: without that, a tampered inventory at the destination would get to
dictate what is "correct" at the destination.
"""

from __future__ import annotations

import hashlib
import os

from .land import object_key
from .manifest import SUCCESS_NAME, UNDECLARED_FILES, Partition, read

MANIFEST_NAME = "_manifest.json"


def _iter_keys(client, bucket: str, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents") or []:
            yield item["Key"], item["Size"]


def _get(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def verify(config, partition_path: str) -> tuple[list[str], dict]:
    """Checks the landed partition. Returns (errors, summary)."""
    partition: Partition = read(partition_path)
    client = config.client()
    bucket = config.raw_bucket
    prefix = f"{object_key(partition, partition.prefix_suffix)}/"

    errors: list[str] = []
    declared: set[str] = set()
    checked = 0
    total_bytes = 0

    # ---- Declared objects: download, recompute, compare ----------------------
    for entry in partition.files:
        key = object_key(partition, entry.path)
        declared.add(key)
        try:
            blob = _get(client, bucket, key)
        except Exception as exc:  # object missing or unreadable
            errors.append(f"object missing or unreadable: {key}: {type(exc).__name__}")
            continue
        checked += 1
        total_bytes += len(blob)
        observed = hashlib.sha256(blob).hexdigest()
        if observed != entry.sha256:
            errors.append(
                f"sha256 mismatch: {key} manifest={entry.sha256} object={observed}"
            )
        if len(blob) != entry.bytes:
            errors.append(
                f"size mismatch: {key} manifest={entry.bytes} object={len(blob)}"
            )

    # ---- Files expected but not declared -------------------------------------
    for name in UNDECLARED_FILES:
        local_path = os.path.join(partition.path, name)
        key = object_key(partition, f"{partition.prefix_suffix}/{name}")
        if not os.path.exists(local_path):
            continue
        declared.add(key)
        try:
            blob = _get(client, bucket, key)
        except Exception as exc:
            errors.append(f"object missing or unreadable: {key}: {type(exc).__name__}")
            continue
        checked += 1
        total_bytes += len(blob)
        with open(local_path, "rb") as handle:
            local_blob = handle.read()
        if blob != local_blob:
            errors.append(f"object differs from the local file: {key}")

    # ---- Orphans: objects in the prefix the manifest does not declare -------
    orphans = [key for key, _ in _iter_keys(client, bucket, prefix) if key not in declared]
    if orphans:
        errors.append(f"object(s) at the destination outside the manifest: {sorted(orphans)}")

    # ---- Completion marker -------------------------------------------------
    success_key = object_key(partition, f"{partition.prefix_suffix}/{SUCCESS_NAME}")
    remote_keys = {key for key, _ in _iter_keys(client, bucket, prefix)}
    if partition.complete and success_key not in remote_keys:
        errors.append(f"{SUCCESS_NAME} missing at the destination, but complete=true")

    summary = {
        "partition": partition.path,
        "bucket": bucket,
        "prefix": prefix,
        "declared": len(partition.files),
        "checked": checked,
        "orphans": len(orphans),
        "bytes": total_bytes,
        "run_id": partition.run_id,
    }
    return errors, summary
