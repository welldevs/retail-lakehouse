"""L1 Landing: uploads a partition to the object storage, byte-for-byte identical.

Nothing is repackaged. It is the canonicalization done by the Source that makes the
manifest's SHA-256 verifiable end to end; a .tar would destroy the per-object check and
direct reading by DuckDB.

Three disciplines, inherited from the Source itself:

  1. DO NOT PROPAGATE CORRUPTION. Before uploading, the local file's sha256 is recomputed
     and checked against the manifest. The platform does not assume `validate` ran.
  2. SERVER-SIDE VALIDATION. Each PUT declares ChecksumSHA256; the object storage rejects
     the object if the bytes that arrived do not match.
  3. _SUCCESS LAST. An interrupted upload never looks complete — mirrors the Source's
     atomic write, where no file is observable in a partial state.

Idempotent: an object already present with the expected checksum is skipped, not re-sent.
"""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass, field

from .manifest import Partition, SUCCESS_NAME, read


class LandingError(Exception):
    """Partition cannot be landed safely."""


def _b64_of_hex(sha256_hex: str) -> str:
    """S3 expects the checksum as base64 of the raw digest, not hexadecimal."""
    return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")


def object_key(partition: Partition, relative_path: str) -> str:
    """Object key. The prefix starts with the name declared in partition.source_name (the
    manifest itself, not a fixed constant), so a second source lands alongside without
    reorganizing anything. The Source's hive layout (ingestion_date=.../wh=... or just
    ingestion_date=...) is preserved so DuckDB reads it with hive_partitioning=1."""
    return f"{partition.source_name}/{relative_path}"


@dataclass
class LandResult:
    partition: str
    bucket: str
    prefix: str
    uploaded: int = 0
    skipped: int = 0
    bytes_uploaded: int = 0
    objects: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.uploaded + self.skipped


def _remote_checksum(client, bucket: str, key: str) -> tuple[str | None, int | None]:
    """SHA-256 checksum (base64) and size of the remote object, or (None, None) if absent."""
    from botocore.exceptions import ClientError

    try:
        head = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None, None
        raise
    return head.get("ChecksumSHA256"), head.get("ContentLength")


def _put(client, bucket: str, key: str, blob: bytes, sha256_hex: str) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=blob,
        ChecksumAlgorithm="SHA256",
        ChecksumSHA256=_b64_of_hex(sha256_hex),
    )


def _upload_one(client, bucket: str, key: str, local_path: str, expected_sha: str | None,
                result: LandResult) -> None:
    """Uploads one file, checking the local copy first and skipping what is already correct."""
    with open(local_path, "rb") as handle:
        blob = handle.read()
    observed = hashlib.sha256(blob).hexdigest()

    if expected_sha and observed != expected_sha:
        raise LandingError(
            f"sha256 mismatch BEFORE upload in {local_path}: "
            f"manifest={expected_sha} disk={observed}. Partition corrupted on disk; "
            f"run `validate` on the Source before landing."
        )

    want = _b64_of_hex(observed)
    remote_sum, remote_len = _remote_checksum(client, bucket, key)
    if remote_sum == want and remote_len == len(blob):
        result.skipped += 1
        return

    _put(client, bucket, key, blob, observed)
    result.uploaded += 1
    result.bytes_uploaded += len(blob)
    result.objects.append(key)


def land(config, partition_path: str) -> LandResult:
    """Lands the partition. Raises LandingError before writing _SUCCESS on any failure."""
    partition = read(partition_path)
    client = config.client()
    bucket = config.raw_bucket

    result = LandResult(
        partition=partition.path,
        bucket=bucket,
        prefix=object_key(partition, partition.prefix_suffix) + "/",
    )

    # 1. Files declared in files[], each with a sha256 in the manifest.
    for entry in partition.files:
        if not os.path.exists(entry.local_path):
            raise LandingError(f"declared file missing on disk: {entry.path}")
        _upload_one(
            client, bucket, object_key(partition, entry.path),
            entry.local_path, entry.sha256, result,
        )

    # 2. Files expected by the contract but not declared: _manifest.json and _run.log.
    #    There is no declared sha256 for them (the manifest does not declare itself), so
    #    the checksum sent to the server is the one computed now.
    for local_path, name in partition.undeclared_paths():
        if name == SUCCESS_NAME:
            continue  # always last, outside this loop
        _upload_one(
            client, bucket,
            object_key(partition, f"{partition.prefix_suffix}/{name}"),
            local_path, None, result,
        )

    # 3. _SUCCESS last: it is the marker the downstream consumer reads to decide whether
    #    the partition can be consumed. Written before the data, an interruption would leave
    #    an incomplete partition announcing itself as complete.
    success_local = os.path.join(partition.path, SUCCESS_NAME)
    if not os.path.exists(success_local):
        raise LandingError(f"{SUCCESS_NAME} missing: partition is not complete")
    _upload_one(
        client, bucket,
        object_key(partition, f"{partition.prefix_suffix}/{SUCCESS_NAME}"),
        success_local, None, result,
    )
    return result
