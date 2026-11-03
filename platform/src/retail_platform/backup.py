"""Copies of what cannot be collected again, kept OFF this machine (CR-020).

    make mercadona-export     # CSV of the whole observed price history, from Silver
    make raw-archive          # the RAW partitions themselves, restorable as they were
    make raw-restore ARCHIVE=exports/raw/<file>.tar.gz

WHY THIS EXISTS. The Mercadona API serves only TODAY's price, and RAW lives in one MinIO
volume on one disk. A clone of the repository brings the code, never the data: if that disk
fails, every price since 2026-08-15 is gone, and no backfill can bring it back (api-guardrails
rule 3). A deploy from scratch would start a NEW capture on the day it came up.

TWO ARTIFACTS, for two different jobs:

  CSV (`mercadona-export`)  for PEOPLE and other tools: one row per product, category,
                            warehouse and collection — `silver_product_price` as is, all its
                            columns. Readable anywhere; NOT what the pipeline loads from, because
                            the pipeline reads RAW, and a CSV has already lost the JSON's shape.
  RAW archive               for a RESTORE: every object of every partition, byte for byte, with
                            its `_manifest.json`. Restored through the same `land` + `verify`
                            the daily DAG uses, so a restored capture is checked exactly like a
                            freshly collected one, and `freeze-check` can tell it is the same.

Each artifact gets a sidecar `*.manifest.json` (rows or partitions, window, sha256 of the
file) so that a copy found on a disk a year from now says what it is without being opened.

THE RESTORE NEVER OVERWRITES. `land` replaces an object whose bytes differ — right for a
retry, wrong for a restore into a bucket that already holds that partition. Here a partition
already landed is VERIFIED against its manifest: identical is skipped, different is refused.
RAW is immutable (api-guardrails rule 4), and a backup must not be the way it stops being.

Writes go to `exports/` (gitignored): a CSV that grows every day has no place in git history.
Copying it off the machine — external disk, cloud storage — is the operator's step.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
from datetime import datetime, timezone

from .freeze import MANIFEST_NAME, _content_sha256, capture_id

PRICE_TABLE = "main.silver_product_price"
ARCHIVE_MANIFEST = "ARCHIVE_MANIFEST.json"
SUCCESS_NAME = "_SUCCESS"


class BackupError(RuntimeError):
    """An export, archive or restore that must not be trusted."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_sidecar(path: str, body: dict) -> str:
    sidecar = path + ".manifest.json"
    with open(sidecar + ".tmp", "w", encoding="utf-8") as handle:
        json.dump(body, handle, indent=2)
        handle.write("\n")
    os.replace(sidecar + ".tmp", sidecar)
    return sidecar


# ------------------------------------------------------------------------------------
# CSV of the observed price history
# ------------------------------------------------------------------------------------
def export_prices(duckdb_path: str, out_dir: str) -> dict:
    """Write the whole Mercadona price history as CSV.gz, plus its sidecar manifest."""
    import duckdb

    if not os.path.exists(duckdb_path):
        raise BackupError(f"{duckdb_path} not found: run `make silver` first.")
    connection = duckdb.connect(duckdb_path, read_only=True)
    try:
        # A terminal bar in a make target's output is noise (and a connect-time option is
        # refused when the process already holds another DuckDB connection).
        connection.execute("set enable_progress_bar = false")
        first, last, collections, warehouses, rows = connection.execute(f"""
            select min(ingestion_date)::varchar, max(ingestion_date)::varchar,
                   count(distinct ingestion_date), count(distinct warehouse), count(*)
            from {PRICE_TABLE}""").fetchone()
        if not rows:
            raise BackupError(f"{PRICE_TABLE} is empty: nothing to export.")
        columns = connection.execute(f"""
            select column_name, data_type from information_schema.columns
            where table_schema = 'main' and table_name = 'silver_product_price'
            order by ordinal_position""").fetchall()
        os.makedirs(out_dir, exist_ok=True)
        target = os.path.join(out_dir, f"mercadona_prices_{first}_{last}.csv.gz")
        # The order is part of the file: the same Silver gives the same bytes (and sha256).
        connection.execute(f"""
            copy (select * from {PRICE_TABLE}
                  order by ingestion_date, warehouse, source_product_id, category_id)
            to '{target}.tmp' (header, delimiter ',', compression gzip)""")
    finally:
        connection.close()
    os.replace(target + ".tmp", target)
    body = {
        "kind": "mercadona_price_history_csv",
        "source_table": PRICE_TABLE,
        "created_at": _now(),
        "from": first, "to": last,
        "collections": collections, "warehouses": warehouses, "rows": rows,
        "columns": [{"name": n, "type": t} for n, t in columns],
        "grain": "(ingestion_date, warehouse, source_product_id, category_id)",
        "prices": "purchasable_unit_price is the comparable price; unit_price is the source's "
                  "raw value (a x99 selector ceiling for some weight-sold products)",
        "file": os.path.basename(target),
        "bytes": os.path.getsize(target),
        "sha256": _sha256_file(target),
    }
    body["sidecar"] = _write_sidecar(target, body)
    body["path"] = target
    return body


# ------------------------------------------------------------------------------------
# RAW archive
# ------------------------------------------------------------------------------------
def _partitions(client, bucket: str, sources: list[str] | None) -> dict[str, list[str]]:
    """{'<source>/<partition_key>': [object keys]} for every partition with a manifest."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for item in page.get("Contents") or []:
            if not sources or item["Key"].split("/", 1)[0] in sources:
                keys.append(item["Key"])
    partitions = {k[: -len("/" + MANIFEST_NAME)]: [] for k in keys
                  if k.endswith("/" + MANIFEST_NAME)}
    for key in sorted(keys):
        parts = key.split("/")
        # The partition is the longest prefix of the key that has a manifest.
        for i in range(len(parts) - 1, 0, -1):
            prefix = "/".join(parts[:i])
            if prefix in partitions:
                partitions[prefix].append(key)
                break
    return dict(sorted(partitions.items()))


def archive_raw(config, out_dir: str, sources: list[str] | None = None) -> dict:
    """Pack every object of every RAW partition into one tar.gz, plus its sidecar manifest."""
    client = config.client()
    bucket = config.raw_bucket
    partitions = _partitions(client, bucket, sources)
    if not partitions:
        raise BackupError(f"no partition with a {MANIFEST_NAME} under s3://{bucket}/"
                          + (f" for {', '.join(sources)}" if sources else ""))
    incomplete = [p for p, keys in partitions.items() if f"{p}/{SUCCESS_NAME}" not in keys]
    if incomplete:
        raise BackupError(f"{len(incomplete)} partition(s) without {SUCCESS_NAME}, e.g. "
                          f"{incomplete[0]}: an archive of an incomplete landing would restore "
                          "it as if it were complete.")

    records = []
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = os.path.join(out_dir, f"raw_{stamp}.tar.gz")
    objects = 0
    total = 0
    with tarfile.open(target + ".tmp", "w:gz", compresslevel=6) as tar:
        for prefix, keys in partitions.items():
            source, _, partition_key = prefix.partition("/")
            for key in keys:
                blob = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                info = tarfile.TarInfo(name=key)
                info.size = len(blob)
                info.mtime = 0  # the archive's bytes depend on the data, not on when it ran
                tar.addfile(info, io.BytesIO(blob))
                objects += 1
                total += len(blob)
                if key.endswith("/" + MANIFEST_NAME):
                    manifest = json.loads(blob)
                    files = manifest.get("files") or []
                    records.append({
                        "source": source, "partition_key": partition_key,
                        "files": len(files),
                        "records": sum(int(f.get("records") or 0) for f in files),
                        "bytes": sum(int(f.get("bytes") or 0) for f in files),
                        "content_sha256": _content_sha256(manifest),
                    })
        inner = {"created_at": _now(), "bucket": bucket, "capture_id": capture_id(records),
                 "partitions": records}
        payload = json.dumps(inner, indent=2).encode("utf-8")
        info = tarfile.TarInfo(name=ARCHIVE_MANIFEST)
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    os.replace(target + ".tmp", target)

    by_source: dict[str, int] = {}
    for r in records:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
    body = {
        "kind": "raw_archive",
        "created_at": inner["created_at"],
        "bucket": bucket,
        # The same function freeze uses: an archive of the sealed partitions carries the
        # sealed capture_id, so a restore can be checked against docs/FREEZE.md by eye.
        "capture_id": inner["capture_id"],
        "partitions_by_source": by_source,
        "partitions": len(records), "objects": objects, "raw_bytes": total,
        "file": os.path.basename(target),
        "bytes": os.path.getsize(target),
        "sha256": _sha256_file(target),
        "restore": f"make raw-restore ARCHIVE={target}",
    }
    body["sidecar"] = _write_sidecar(target, body)
    body["path"] = target
    return body


def _safe_extract(archive: str, target_dir: str) -> dict:
    """Extract regular files only, refusing any member that would land outside target_dir."""
    root = os.path.realpath(target_dir)
    inner = None
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            path = os.path.realpath(os.path.join(root, member.name))
            if not member.isfile() or not (path == root or path.startswith(root + os.sep)):
                raise BackupError(f"refused archive member {member.name!r}: not a regular "
                                  "file inside the restore directory.")
        for member in tar.getmembers():
            data = tar.extractfile(member).read()
            if member.name == ARCHIVE_MANIFEST:
                inner = json.loads(data)
                continue
            path = os.path.join(root, member.name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(data)
    if inner is None:
        raise BackupError(f"{archive} has no {ARCHIVE_MANIFEST}: not an archive from raw-archive.")
    return inner


def restore_raw(config, archive: str, work_dir: str) -> dict:
    """Restore an archive into the RAW bucket: verify what is there, land what is missing."""
    from .land import land
    from .verify import verify

    sidecar = archive + ".manifest.json"
    if os.path.exists(sidecar):
        with open(sidecar, encoding="utf-8") as handle:
            expected = json.load(handle).get("sha256")
        if expected and expected != _sha256_file(archive):
            raise BackupError(f"{archive} does not match the sha256 in its sidecar: the copy "
                              "is damaged. Restore from another copy.")
    inner = _safe_extract(archive, work_dir)
    client = config.client()
    bucket = config.raw_bucket
    landed, already, refused = [], [], []
    for p in inner["partitions"]:
        prefix = f"{p['source']}/{p['partition_key']}"
        local = os.path.join(work_dir, prefix)
        present = client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/{SUCCESS_NAME}",
                                         MaxKeys=1).get("KeyCount", 0)
        if present:
            errors, _ = verify(config, local)
            (refused if errors else already).append((prefix, errors))
            continue
        land(config, local)
        errors, _ = verify(config, local)
        if errors:
            raise BackupError(f"restored {prefix} but verify failed: {errors[:3]}")
        landed.append(prefix)
    return {"capture_id": inner["capture_id"], "partitions": len(inner["partitions"]),
            "landed": landed, "already": [a for a, _ in already],
            "refused": [(r, e[:3]) for r, e in refused]}
