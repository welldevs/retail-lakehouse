"""Identidade, caminho e metadados de uma particao do snapshot.

Uma particao e a unidade de entrega da Source:

    <root>/ingestion_date=YYYY-MM-DD/
      provinces/
        province=<codigo>/
          <nome ORIGINAL do arquivo baixado>   (um por dataset: SECC, UP, VIAS, PSEU, TRAM)

Sem segundo eixo de particao: o artefato representa um intake do dataset oficial
inteiro, nao um recorte por warehouse nem por provincia. "provinces/province=<codigo>/"
e estrutura de DIRETORIO dentro da particao — equivalente ao "tables/" da source de
populacao — nao um eixo formal do lado da plataforma (manifest.py continua vendo so
`ingestion_date=...`).

O nome de arquivo landado e o mesmo do download original (ex.:
"VIAS.P28.D260630.G260702"), nao um nome generico tipo "VIAS.txt": isso preserva
proveniencia (data e geracao do INE) sem precisar de outro mecanismo.

Regras impostas aqui, e nao apenas documentadas:
  - tokens de particao sao validados (nada de separador de caminho ou '..');
  - uma particao completa e imutavel: so pode ser reescrita com --overwrite;
  - o marcador _SUCCESS so existe quando a particao esta completa.
"""

from __future__ import annotations

import os
import re

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, read_json, write_json

MANIFEST_NAME = "_manifest.json"
SUCCESS_NAME = "_SUCCESS"
RUN_LOG_NAME = "_run.log"
PROVINCES_DIR = "provinces"

# Datasets que esta Source incorpora. Os downloads do Callejero trazem 5 arquivos por
# provincia (SECC, UP, VIAS, TRAM, PSEU) e todos os 5 sao incorporados (CONTRACT.md
# secao 2) — TRAM entrou depois de SECC/UP/VIAS/PSEU porque e o unico dos 5 que liga
# rua, secao censitaria, entidade/nucleo e codigo postal (CPOS) num so registro; os
# outros 4 nao carregam CEP.
DATASETS = ("SECC", "UP", "VIAS", "PSEU", "TRAM")

# As 4 provincias dos warehouses (mad1=28, bcn1=08, svq1=41, vlc1=46). Default da CLI,
# igual em espirito ao INE_TABLES da source de populacao — configuravel, nao fixo no
# codigo alem do default.
DEFAULT_PROVINCES = ("08", "28", "41", "46")

PROVINCE_TOKEN = re.compile(r"^\d{2}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Nome de arquivo do Callejero tal como o INE distribui, ex.: "VIAS.P28.D260630.G260702".
# Usado tanto para reconhecer arquivos de entrada em --in quanto arquivos ja landados.
INPUT_FILE = re.compile(
    r"^(?P<dataset>SECC|UP|VIAS|PSEU|TRAM)\.P(?P<province>\d{2})\.D(?P<date>\d{6})\.G(?P<gen>\d{6})$"
)


class PartitionError(Exception):
    """Uso invalido de particao: token malformado ou imutabilidade violada."""


def validate_province(value: str) -> str:
    if not isinstance(value, str) or not PROVINCE_TOKEN.match(value):
        raise PartitionError(
            f"codigo de provincia invalido: {value!r}. Esperado 2 digitos, ex.: '28'."
        )
    return value


def validate_date(value: str) -> str:
    if not isinstance(value, str) or not ISO_DATE.match(value):
        raise PartitionError(f"data invalida: {value!r}. Formato esperado: YYYY-MM-DD.")
    return value


def partition_path(root: str, ingestion_date: str) -> str:
    validate_date(ingestion_date)
    return os.path.join(root, f"ingestion_date={ingestion_date}")


def province_dir(partition: str, province: str) -> str:
    validate_province(province)
    return os.path.join(partition, PROVINCES_DIR, f"province={province}")


def dataset_path(partition: str, province: str, original_filename: str) -> str:
    """Caminho landado de um dataset, com o nome original preservado."""
    if not INPUT_FILE.match(original_filename):
        raise PartitionError(f"nome de arquivo fora do padrao do Callejero: {original_filename!r}")
    return os.path.join(province_dir(partition, province), original_filename)


def manifest_path(partition: str) -> str:
    return os.path.join(partition, MANIFEST_NAME)


def success_path(partition: str) -> str:
    return os.path.join(partition, SUCCESS_NAME)


def read_manifest(partition: str) -> dict | None:
    """Le o manifesto da particao. None se nao existir; erro se estiver corrompido."""
    path = manifest_path(partition)
    if not os.path.exists(path):
        return None
    payload, _ = read_json(path)
    if not isinstance(payload, dict):
        raise CorruptFileError(f"{path}: manifesto nao e um objeto JSON")
    return payload


def assert_writable(partition: str, overwrite: bool) -> dict | None:
    """Valida a reabertura de uma particao existente. Retorna o manifesto anterior.

    Mesma logica da source de populacao: impede reescrever uma particao completa sem
    --overwrite, reabrir uma particao com manifesto ilegivel como se fosse nova, ou
    ignorar _SUCCESS quando o manifesto foi removido.
    """
    try:
        previous = read_manifest(partition)
    except CorruptFileError as exc:
        if not overwrite:
            raise PartitionError(
                f"manifesto anterior ilegivel ({exc}). Use --overwrite para reescrever a "
                f"particao do zero, ou extraia para outra ingestion_date."
            ) from exc
        previous = None

    if previous is not None and not isinstance(previous.get("files"), list):
        if not overwrite:
            raise PartitionError(
                "manifesto anterior sem a lista 'files'. Use --overwrite para reescrever "
                "a particao do zero."
            )
        previous = None

    if previous is None:
        if os.path.exists(success_path(partition)) and not overwrite:
            raise PartitionError(
                "particao marcada como completa por _SUCCESS, mas sem manifesto legivel. "
                "Use --overwrite para reescreve-la deliberadamente."
            )
        return None

    if previous.get("complete") and not overwrite:
        raise PartitionError(
            "particao ja esta completa e e imutavel. Use --overwrite para reescreve-la "
            "deliberadamente, ou extraia para outra ingestion_date."
        )
    return previous


def build_history(previous: dict | None) -> list[dict]:
    if previous is None:
        return []
    history = list(previous.get("history") or [])
    totals = previous.get("totals") or {}
    history.append(
        {
            "run_id": previous.get("run_id"),
            "started_at_utc": previous.get("started_at_utc"),
            "finished_at_utc": previous.get("finished_at_utc"),
            "duration_seconds": previous.get("duration_seconds"),
            "complete": previous.get("complete"),
            "files_landed": totals.get("files_landed"),
        }
    )
    return history


def write_manifest(partition: str, manifest: dict) -> tuple[str, int]:
    manifest["manifest_version"] = MANIFEST_VERSION
    return write_json(manifest_path(partition), manifest)


def mark_success(partition: str, complete: bool, run_id: str) -> None:
    path = success_path(partition)
    if complete:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"{run_id}\n")
    elif os.path.exists(path):
        os.unlink(path)
