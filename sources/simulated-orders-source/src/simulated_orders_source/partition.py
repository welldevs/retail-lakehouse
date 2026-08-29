"""Identidade, caminho e metadados de uma particao do snapshot.

Uma particao e a unidade de entrega da Source:

    <root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/

`ingestion_date` AQUI E A DATA DO PEDIDO, NAO A DATA DO EVENTO
---------------------------------------------------------------
Todo evento de um pedido mora na particao do dia em que o pedido foi COLOCADO, mesmo quando
`occurred_at` atravessa a meia-noite (um pedido das 22h40 entregue as 9h do dia seguinte
deixa todos os seus eventos aqui).

A alternativa — particionar por data do evento — deixaria a particao de um dia impossivel de
fechar: ela so estaria completa dois dias depois, quando o ultimo pedido daquele dia
terminasse, e `_SUCCESS` perderia o significado. Como consequencia declarada, um consumidor
que queira "eventos ocorridos no dia D" filtra por `occurred_at`, nao por `ingestion_date`; o
Silver expoe as duas colunas. Ver CONTRACT.md secao 4.

O eixo `wh=` nao e uma escolha estetica: `platform/src/retail_platform/manifest.py` deriva
`axis_name = "wh" if warehouse else None`, e esses sao os dois unicos formatos que o leitor
de manifesto da plataforma entende. Uma particao por warehouse tambem da isolamento de
falha — se `vlc1` falhar, `mad1`/`bcn1`/`svq1` nao sao afetados — igual ao Mercadona.

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
EVENTS_FILE = "order_events.jsonl"

DEFAULT_WAREHOUSES = ("mad1", "bcn1", "svq1", "vlc1")

# Tokens que viram nome de diretorio. Deliberadamente restritivo: impede travessia de
# caminho vinda de um parametro de CLI.
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PartitionError(Exception):
    """Uso invalido de particao: token malformado ou violacao de imutabilidade."""


def validate_token(name: str, value: str) -> str:
    if not isinstance(value, str) or not SAFE_TOKEN.match(value):
        raise PartitionError(
            f"{name} invalido: {value!r}. Use apenas letras, digitos, '-' e '_' "
            f"(1 a 64 caracteres, comecando por letra ou digito)."
        )
    return value


def validate_date(value: str) -> str:
    if not isinstance(value, str) or not ISO_DATE.match(value):
        raise PartitionError(f"data invalida: {value!r}. Formato esperado: YYYY-MM-DD.")
    return value


def partition_path(root: str, ingestion_date: str, warehouse: str) -> str:
    validate_date(ingestion_date)
    validate_token("wh", warehouse)
    return os.path.join(root, f"ingestion_date={ingestion_date}", f"wh={warehouse}")


def events_path(partition: str) -> str:
    return os.path.join(partition, EVENTS_FILE)


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

    Impede usos que o contrato proibe:
      - reescrever uma particao ja completa sem pedir explicitamente;
      - reabrir uma particao cujo manifesto esta ilegivel, como se fosse nova;
      - ignorar o marcador _SUCCESS quando o manifesto foi removido.
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
        # Sem manifesto legivel, o marcador _SUCCESS ainda testemunha uma particao
        # completa: sem esta checagem, apagar o manifesto contornaria a imutabilidade.
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
    """Historico de execucoes, para que a proveniencia sobreviva a reexecucoes.

    Guarda a seed E o digest das premissas de cada execucao anterior. Sao as duas entradas
    que mudam os pedidos sob os mesmos order_id: um --overwrite com outra seed troca os
    pedidos, e um --overwrite com outra tabela de premissas troca as taxas por tras deles.
    Sem as duas registradas, nenhuma das trocas deixaria rastro.
    """
    if previous is None:
        return []
    history = list(previous.get("history") or [])
    totals = previous.get("totals") or {}
    config = previous.get("config") or {}
    history.append(
        {
            "run_id": previous.get("run_id"),
            "started_at_utc": previous.get("started_at_utc"),
            "finished_at_utc": previous.get("finished_at_utc"),
            "duration_seconds": previous.get("duration_seconds"),
            "complete": previous.get("complete"),
            "seed": config.get("seed"),
            "orders": config.get("orders"),
            "premises_sha256": config.get("premises_sha256"),
            "event_rows": totals.get("event_rows"),
            "order_rows": totals.get("order_rows"),
        }
    )
    return history


def write_manifest(partition: str, manifest: dict) -> tuple[str, int]:
    manifest["manifest_version"] = MANIFEST_VERSION
    return write_json(manifest_path(partition), manifest)


def mark_success(partition: str, complete: bool, run_id: str) -> None:
    """Cria _SUCCESS quando a particao esta completa; remove quando nao esta.

    Convencao de data lake: o consumidor pode decidir se le a particao olhando um unico
    arquivo, sem parsear o manifesto.
    """
    path = success_path(partition)
    if complete:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"{run_id}\n")
    elif os.path.exists(path):
        os.unlink(path)
