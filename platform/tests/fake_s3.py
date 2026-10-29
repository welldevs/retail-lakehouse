"""In-memory S3 client double, in the spirit of the HTTP double the Source uses.

The point is not to simulate AWS: it is to exercise the land/verify guarantees without
network and without containers. Two fidelities matter and are implemented:

  - put_object VALIDATES the declared ChecksumSHA256, as the real server does. Without
    that, a bug in the hex->base64 conversion would pass in tests and fail in production;
  - head_object raises a real ClientError 404 (the botocore class), because
    land._remote_checksum depends on catching exactly that exception.
"""

from __future__ import annotations

import base64
import hashlib
import io

from botocore.exceptions import ClientError


def b64_sha256(blob: bytes) -> str:
    return base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii")


class ChecksumMismatch(Exception):
    """The server rejected the object: the bytes do not match the declared checksum."""


class FakePaginator:
    def __init__(self, store: dict):
        self._store = store

    def paginate(self, Bucket: str, Prefix: str = ""):
        contents = [
            {"Key": key, "Size": len(blob)}
            for (bucket, key), blob in sorted(self._store.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        # One page is enough for this platform's volume; the code under test uses the
        # paginator anyway, so the interface is the same.
        yield {"Contents": contents} if contents else {}


class FakeS3Client:
    def __init__(self):
        self.store: dict[tuple[str, str], bytes] = {}
        self.puts = 0
        self.gets = 0
        self.heads = 0

    # ---- interface consumed by land.py and verify.py ------------------------
    def put_object(self, Bucket, Key, Body, ChecksumAlgorithm=None, ChecksumSHA256=None):
        self.puts += 1
        if ChecksumSHA256 is not None and ChecksumSHA256 != b64_sha256(Body):
            raise ChecksumMismatch(
                f"declared checksum does not match the bytes sent: {Key}"
            )
        self.store[(Bucket, Key)] = Body
        return {"ChecksumSHA256": b64_sha256(Body)}

    def head_object(self, Bucket, Key, ChecksumMode=None):
        self.heads += 1
        blob = self.store.get((Bucket, Key))
        if blob is None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return {"ChecksumSHA256": b64_sha256(blob), "ContentLength": len(blob)}

    def get_object(self, Bucket, Key):
        self.gets += 1
        blob = self.store.get((Bucket, Key))
        if blob is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
            )
        return {"Body": io.BytesIO(blob)}

    def get_paginator(self, name):
        assert name == "list_objects_v2", name
        return FakePaginator(self.store)

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000):
        keys = [key for (bucket, key) in self.store if bucket == Bucket and key.startswith(Prefix)]
        return {"KeyCount": min(len(keys), MaxKeys)}

    # ---- test utilities ----------------------------------------------------
    def corrupt(self, bucket: str, key: str) -> None:
        """Tampers with already-stored bytes, simulating tampering at rest.

        Writes straight to the store, bypassing put_object: the real server would accept
        an object whose declared checksum matches the tampered bytes. That is exactly what
        verify has to catch, because it checks against the MANIFEST.
        """
        self.store[(bucket, key)] = self.store[(bucket, key)] + b" "

    def drop(self, bucket: str, key: str) -> None:
        del self.store[(bucket, key)]

    def keys(self, bucket: str) -> set[str]:
        return {key for (b, key) in self.store if b == bucket}


class FakeConfig:
    """Minimal config: land/verify use only .client() and .raw_bucket."""

    def __init__(self, client: FakeS3Client, raw_bucket: str = "retail-raw"):
        self._client = client
        self.raw_bucket = raw_bucket
        self.lakehouse_bucket = "retail-lakehouse"
        self.endpoint = "http://minio:9000"
        self.access_key = "key"
        self.secret_key = "secret"

    def client(self):
        return self._client
