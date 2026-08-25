"""Leitura do _manifest.json de uma particao, tratado como CONTRATO.

Este modulo implementa as obrigacoes do consumidor de CONTRACT.md secao 4. Ele
deliberadamente NAO importa mercadona_catalog_source: a plataforma consome o contrato
fisico (JSON canonico + manifesto), do mesmo modo que um consumidor de fora faria. Se
o pacote da Source mudasse de forma interna, esta leitura continuaria valida.

Regras impostas aqui, e nao apenas documentadas:
  - _SUCCESS e complete: true, os DOIS (garantia 10: a incoerencia e erro);
  - manifest_version suportada, em vez de adivinhacao (secao 8);
  - files[].path e relativo a RAIZ do snapshot, nao a particao (obrigacao 4.1);
  - caminho que escapa da raiz do snapshot e recusado;
  - ingestion_date/wh do manifesto conferidos contra o caminho em disco.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field

from . import SUPPORTED_MANIFEST_VERSIONS

MANIFEST_NAME = "_manifest.json"
SUCCESS_NAME = "_SUCCESS"
RUN_LOG_NAME = "_run.log"

# Arquivos que existem na particao e NAO sao declarados em files[]. validate.py da Source
# os trata do mesmo modo ao varrer orfaos; a plataforma precisa da mesma lista para nao
# acusar de orfao um arquivo que o contrato preve.
UNDECLARED_FILES = (MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME)


class ManifestError(Exception):
    """Particao que nao satisfaz o contrato de leitura."""


@dataclass(frozen=True)
class FileEntry:
    """Uma entrada de files[]. `path` e relativo a raiz do snapshot."""

    path: str
    sha256: str
    bytes: int
    records: int | None
    stage: str | None
    category_id: object
    local_path: str

    def digest_on_disk(self) -> tuple[str, int]:
        """Recalcula sha256 e tamanho do arquivo local. Nao confia no manifesto."""
        with open(self.local_path, "rb") as handle:
            blob = handle.read()
        return hashlib.sha256(blob).hexdigest(), len(blob)


@dataclass(frozen=True)
class Partition:
    path: str
    root: str
    ingestion_date: str
    # Segundo eixo de particao, alem de ingestion_date. Duas formas conhecidas hoje: com
    # eixo (Mercadona: axis_name="wh") e sem eixo nenhum (INE: os dois None, a fonte
    # devolve todas as provincias num unico payload). Nao ha um terceiro caso a
    # generalizar por enquanto.
    axis_name: str | None
    axis_value: str | None
    run_id: str
    complete: bool
    source_name: str
    lang: str
    files: list[FileEntry]
    totals: dict
    schema_fingerprint: dict
    anomalies: list = field(default_factory=list)

    @property
    def warehouse(self) -> str | None:
        """Compatibilidade com consumidores da Mercadona. None para uma source sem esse
        eixo (ex.: INE) — use axis_name/axis_value para o caso geral."""
        return self.axis_value if self.axis_name == "wh" else None

    @property
    def prefix_suffix(self) -> str:
        """Sufixo hive da particao, como aparece em files[].path e na chave do objeto."""
        suffix = f"ingestion_date={self.ingestion_date}"
        if self.axis_name is not None:
            suffix += f"/{self.axis_name}={self.axis_value}"
        return suffix

    def undeclared_paths(self) -> list[tuple[str, str]]:
        """(caminho local, nome) dos arquivos previstos e nao declarados que existem."""
        found = []
        for name in UNDECLARED_FILES:
            candidate = os.path.join(self.path, name)
            if os.path.exists(candidate):
                found.append((candidate, name))
        return found


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def read(partition_path: str) -> Partition:
    """Le e valida o manifesto de uma particao. Levanta ManifestError se nao servir."""
    manifest_file = os.path.join(partition_path, MANIFEST_NAME)
    if not os.path.exists(manifest_file):
        raise ManifestError(f"manifesto nao encontrado: {manifest_file}")
    try:
        with open(manifest_file, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ManifestError(f"manifesto ilegivel: {manifest_file}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ManifestError(f"manifesto nao e um objeto JSON: {manifest_file}")

    source_name = (manifest.get("source") or {}).get("name", "")
    if source_name not in SUPPORTED_MANIFEST_VERSIONS:
        raise ManifestError(
            f"source.name {source_name!r} desconhecida desta plataforma "
            f"(esperado um de {sorted(SUPPORTED_MANIFEST_VERSIONS)})"
        )
    expected_version = SUPPORTED_MANIFEST_VERSIONS[source_name]
    version = manifest.get("manifest_version")
    if version != expected_version:
        raise ManifestError(
            f"manifest_version {version!r} nao suportada para {source_name!r} "
            f"(esta plataforma le {expected_version})"
        )

    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise ManifestError("manifesto sem a lista 'files'")

    # Garantia 10: _SUCCESS existe se e somente se complete. Exigimos os dois, porque
    # confiar num deles isolado aceitaria uma particao que a Source considera invalida.
    complete = bool(manifest.get("complete"))
    has_success = os.path.exists(os.path.join(partition_path, SUCCESS_NAME))
    if complete != has_success:
        raise ManifestError(
            f"_SUCCESS {'presente' if has_success else 'ausente'} contradiz "
            f"complete={complete}"
        )
    if not complete:
        raise ManifestError(
            "particao incompleta (complete: false). A plataforma so consome particao "
            "completa: complete-a com um novo extract antes de aterrissar."
        )
    if manifest.get("failures"):
        raise ManifestError(
            f"{len(manifest['failures'])} categoria(s) em failures[]: particao reprovada "
            f"pelo proprio contrato da Source (garantia 16)"
        )

    partition_block = manifest.get("partition") or {}
    ingestion_date = partition_block.get("ingestion_date")
    warehouse = partition_block.get("warehouse")
    if not ingestion_date:
        raise ManifestError("manifesto sem partition.ingestion_date")

    # axis_name e sempre "wh" quando ha um armazem declarado: e o unico eixo conhecido
    # hoje alem de ingestion_date. Sem ele (ex.: INE), a particao tem so ingestion_date.
    axis_name = "wh" if warehouse else None
    axis_value = warehouse

    # Obrigacao 4.1: files[].path comeca na RAIZ do snapshot, nao na particao. Sobe um
    # nivel por eixo de particao presente, alem do proprio ingestion_date.
    levels_up = 2 if axis_name is not None else 1
    root = os.path.abspath(os.path.join(partition_path, *([".."] * levels_up)))

    # O caminho em disco e o manifesto tem de concordar. Divergencia significa particao
    # movida ou renomeada, e o eixo so existe no caminho e no manifesto (obrigacao 4.3):
    # aceitar a divergencia perderia a identidade de forma irrecuperavel.
    if axis_name is not None:
        expected_tail = os.path.join(
            f"ingestion_date={ingestion_date}", f"{axis_name}={axis_value}"
        )
    else:
        expected_tail = f"ingestion_date={ingestion_date}"
    if not os.path.abspath(partition_path).endswith(expected_tail):
        raise ManifestError(
            f"caminho da particao ({partition_path}) nao corresponde ao manifesto "
            f"({expected_tail})"
        )

    files: list[FileEntry] = []
    for entry in entries:
        if not isinstance(entry, dict) or "path" not in entry:
            raise ManifestError(f"entrada de manifesto malformada: {entry!r}")
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            raise ManifestError(f"caminho fora da raiz do snapshot: {entry['path']}")
        files.append(
            FileEntry(
                path=entry["path"],
                sha256=entry.get("sha256", ""),
                bytes=entry.get("bytes", -1),
                records=entry.get("records"),
                stage=entry.get("stage"),
                category_id=entry.get("category_id"),
                local_path=target,
            )
        )

    return Partition(
        path=os.path.abspath(partition_path),
        root=root,
        ingestion_date=ingestion_date,
        axis_name=axis_name,
        axis_value=axis_value,
        run_id=manifest.get("run_id", ""),
        complete=complete,
        source_name=source_name,
        lang=(manifest.get("source") or {}).get("lang", ""),
        files=files,
        totals=manifest.get("totals") or {},
        schema_fingerprint=manifest.get("schema_fingerprint") or {},
        anomalies=manifest.get("anomalies") or [],
    )
