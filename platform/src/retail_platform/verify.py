"""Verificacao independente de uma particao aterrissada.

Mesmo principio do validate.py da Source: RELE e RECALCULA em vez de confiar no que foi
gravado. Baixa cada objeto declarado, recalcula o SHA-256 e o tamanho, e varre o prefixo
inteiro em busca de objeto que o manifesto nao declara.

O manifesto local e o inventario autoritativo (obrigacao 4.1). Alem de conferir os
objetos contra ele, a verificacao exige que o _manifest.json aterrissado seja
byte-identico ao local: sem isso, um inventario adulterado no destino passaria a ditar o
que e "correto" no destino.
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
    """Confere a particao aterrissada. Retorna (erros, resumo)."""
    partition: Partition = read(partition_path)
    client = config.client()
    bucket = config.raw_bucket
    prefix = f"{object_key(partition, partition.prefix_suffix)}/"

    errors: list[str] = []
    declared: set[str] = set()
    checked = 0
    total_bytes = 0

    # ---- Objetos declarados: baixa, recalcula, compara -----------------------
    for entry in partition.files:
        key = object_key(partition, entry.path)
        declared.add(key)
        try:
            blob = _get(client, bucket, key)
        except Exception as exc:  # objeto ausente ou ilegivel
            errors.append(f"objeto ausente ou ilegivel: {key}: {type(exc).__name__}")
            continue
        checked += 1
        total_bytes += len(blob)
        observed = hashlib.sha256(blob).hexdigest()
        if observed != entry.sha256:
            errors.append(
                f"sha256 divergente: {key} manifesto={entry.sha256} objeto={observed}"
            )
        if len(blob) != entry.bytes:
            errors.append(
                f"tamanho divergente: {key} manifesto={entry.bytes} objeto={len(blob)}"
            )

    # ---- Arquivos previstos e nao declarados --------------------------------
    for name in UNDECLARED_FILES:
        local_path = os.path.join(partition.path, name)
        key = object_key(partition, f"{partition.prefix_suffix}/{name}")
        if not os.path.exists(local_path):
            continue
        declared.add(key)
        try:
            blob = _get(client, bucket, key)
        except Exception as exc:
            errors.append(f"objeto ausente ou ilegivel: {key}: {type(exc).__name__}")
            continue
        checked += 1
        total_bytes += len(blob)
        with open(local_path, "rb") as handle:
            local_blob = handle.read()
        if blob != local_blob:
            errors.append(f"objeto difere do arquivo local: {key}")

    # ---- Orfaos: objeto no prefixo que o manifesto nao declara ---------------
    orphans = [key for key, _ in _iter_keys(client, bucket, prefix) if key not in declared]
    if orphans:
        errors.append(f"objeto(s) no destino fora do manifesto: {sorted(orphans)}")

    # ---- Marcador de conclusao ---------------------------------------------
    success_key = object_key(partition, f"{partition.prefix_suffix}/{SUCCESS_NAME}")
    remote_keys = {key for key, _ in _iter_keys(client, bucket, prefix)}
    if partition.complete and success_key not in remote_keys:
        errors.append(f"{SUCCESS_NAME} ausente no destino, mas complete=true")

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
