"""L1 Landing: sobe uma particao para o object storage, byte a byte identica.

Nada e reempacotado. E a canonicalizacao feita pela Source que torna o SHA-256 do
manifesto verificavel ponta a ponta; um .tar destruiria a conferencia por objeto e a
leitura direta pelo DuckDB.

Tres disciplinas, herdadas da propria Source:

  1. NAO PROPAGAR CORRUPCAO. Antes de subir, o sha256 do arquivo local e recalculado e
     conferido contra o manifesto. A plataforma nao assume que `validate` rodou.
  2. VALIDACAO NO SERVIDOR. Cada PUT declara ChecksumSHA256; o object storage recusa o
     objeto se os bytes que chegaram nao corresponderem.
  3. _SUCCESS POR ULTIMO. Um upload interrompido nunca parece completo — espelha a
     escrita atomica da Source, onde nenhum arquivo e observavel em estado parcial.

Idempotente: objeto ja presente com o checksum esperado e pulado, nao reenviado.
"""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass, field

from . import SOURCE_NAME
from .manifest import Partition, SUCCESS_NAME, read


class LandingError(Exception):
    """Particao nao pode ser aterrissada com seguranca."""


def _b64_of_hex(sha256_hex: str) -> str:
    """S3 espera o checksum em base64 do digest cru, nao em hexadecimal."""
    return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")


def object_key(partition: Partition, relative_path: str) -> str:
    """Chave do objeto. O prefixo comeca pelo nome da source, e o layout hive da Source
    (ingestion_date=.../wh=...) e preservado para que o DuckDB o leia com
    hive_partitioning=1 e ganhe ingestion_date/wh como colunas sem parsing manual."""
    return f"{SOURCE_NAME}/{relative_path}"


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
    """Checksum SHA-256 (base64) e tamanho do objeto remoto, ou (None, None) se ausente."""
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
    """Sobe um arquivo, conferindo o local antes e pulando o que ja esta correto."""
    with open(local_path, "rb") as handle:
        blob = handle.read()
    observed = hashlib.sha256(blob).hexdigest()

    if expected_sha and observed != expected_sha:
        raise LandingError(
            f"sha256 divergente ANTES do upload em {local_path}: "
            f"manifesto={expected_sha} disco={observed}. Particao corrompida em disco; "
            f"rode `validate` na Source antes de aterrissar."
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
    """Aterrissa a particao. Levanta LandingError antes de gravar _SUCCESS em qualquer falha."""
    partition = read(partition_path)
    client = config.client()
    bucket = config.raw_bucket

    result = LandResult(
        partition=partition.path,
        bucket=bucket,
        prefix=object_key(partition, partition.prefix_suffix) + "/",
    )

    # 1. Arquivos declarados em files[], cada um com sha256 no manifesto.
    for entry in partition.files:
        if not os.path.exists(entry.local_path):
            raise LandingError(f"arquivo declarado e ausente em disco: {entry.path}")
        _upload_one(
            client, bucket, object_key(partition, entry.path),
            entry.local_path, entry.sha256, result,
        )

    # 2. Arquivos previstos pelo contrato e nao declarados: _manifest.json e _run.log.
    #    Nao ha sha256 declarado para eles (o manifesto nao declara a si mesmo), entao o
    #    checksum enviado ao servidor e o calculado agora.
    for local_path, name in partition.undeclared_paths():
        if name == SUCCESS_NAME:
            continue  # sempre por ultimo, fora deste laco
        _upload_one(
            client, bucket,
            object_key(partition, f"{partition.prefix_suffix}/{name}"),
            local_path, None, result,
        )

    # 3. _SUCCESS por ultimo: e o marcador que o consumidor a jusante le para decidir se a
    #    particao pode ser consumida. Gravado antes dos dados, uma interrupcao deixaria uma
    #    particao incompleta anunciando-se como completa.
    success_local = os.path.join(partition.path, SUCCESS_NAME)
    if not os.path.exists(success_local):
        raise LandingError(f"{SUCCESS_NAME} ausente: particao nao esta completa")
    _upload_one(
        client, bucket,
        object_key(partition, f"{partition.prefix_suffix}/{SUCCESS_NAME}"),
        success_local, None, result,
    )
    return result
