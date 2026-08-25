"""Duplo de cliente S3 em memoria, no espirito do duplo de HTTP que a Source usa.

O ponto nao e simular a AWS: e poder exercitar as garantias de land/verify sem rede e sem
container. Duas fidelidades importam e estao implementadas:

  - put_object VALIDA o ChecksumSHA256 declarado, como o servidor real faz. Sem isso, um
    bug na conversao hex->base64 passaria em teste e falharia em producao;
  - head_object levanta ClientError 404 de verdade (a classe do botocore), porque
    land._remote_checksum depende de capturar exatamente essa excecao.
"""

from __future__ import annotations

import base64
import hashlib
import io

from botocore.exceptions import ClientError


def b64_sha256(blob: bytes) -> str:
    return base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii")


class ChecksumMismatch(Exception):
    """O servidor recusou o objeto: os bytes nao correspondem ao checksum declarado."""


class FakePaginator:
    def __init__(self, store: dict):
        self._store = store

    def paginate(self, Bucket: str, Prefix: str = ""):
        contents = [
            {"Key": key, "Size": len(blob)}
            for (bucket, key), blob in sorted(self._store.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        # Uma pagina basta para o volume desta plataforma; o codigo sob teste usa o
        # paginator de qualquer forma, entao a interface e a mesma.
        yield {"Contents": contents} if contents else {}


class FakeS3Client:
    def __init__(self):
        self.store: dict[tuple[str, str], bytes] = {}
        self.puts = 0
        self.gets = 0
        self.heads = 0

    # ---- interface consumida por land.py e verify.py ------------------------
    def put_object(self, Bucket, Key, Body, ChecksumAlgorithm=None, ChecksumSHA256=None):
        self.puts += 1
        if ChecksumSHA256 is not None and ChecksumSHA256 != b64_sha256(Body):
            raise ChecksumMismatch(
                f"checksum declarado nao corresponde aos bytes enviados: {Key}"
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

    # ---- utilitarios de teste ----------------------------------------------
    def corrupt(self, bucket: str, key: str) -> None:
        """Adultera bytes ja armazenados, simulando manipulacao em repouso.

        Escreve direto no store, sem passar por put_object: o servidor real aceitaria um
        objeto cujo checksum declarado corresponde aos bytes adulterados. E exatamente o
        que o verify tem de pegar, porque ele confere contra o MANIFESTO.
        """
        self.store[(bucket, key)] = self.store[(bucket, key)] + b" "

    def drop(self, bucket: str, key: str) -> None:
        del self.store[(bucket, key)]

    def keys(self, bucket: str) -> set[str]:
        return {key for (b, key) in self.store if b == bucket}


class FakeConfig:
    """Config minima: land/verify usam apenas .client() e .raw_bucket."""

    def __init__(self, client: FakeS3Client, raw_bucket: str = "retail-raw"):
        self._client = client
        self.raw_bucket = raw_bucket
        self.lakehouse_bucket = "retail-lakehouse"
        self.endpoint = "http://minio:9000"
        self.access_key = "key"
        self.secret_key = "secret"

    def client(self):
        return self._client
