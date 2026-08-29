"""Forma canonica dos arquivos do snapshot.

Todo arquivo do snapshot e gravado com a mesma serializacao deterministica, de modo que
o SHA-256 do arquivo dependa apenas do conteudo do payload. Esta e a unica funcao que
grava arquivos de dados no pacote.

A gravacao e ATOMICA: escreve num temporario no mesmo diretorio e faz os.replace. Um
processo interrompido no meio da escrita nunca deixa arquivo parcial no lugar final.

Identica a das outras quatro sources deste repo para o JSON, de proposito: o consumidor
recalcula o digest da mesma forma para qualquer particao, venha ela de que source vier.

DESVIO DELIBERADO: `write_jsonl`
--------------------------------
O arquivo de dados desta Source e um LOG, e log nao e array. As outras quatro gravam um
array JSON com `indent=2`; aqui `order_events.jsonl` e NDJSON — uma linha por evento, cada
linha canonica e compacta. Tres razoes, nesta ordem:

  1. um log e lido linha a linha e cresce por append; um array exige o arquivo inteiro em
     memoria tanto para escrever quanto para ler;
  2. cada linha e um evento completo e independente, que e exatamente o que atravessa um
     broker mais adiante — o mesmo byte no disco e no topico;
  3. `read_json(..., format='newline_delimited')` do DuckDB consome NDJSON direto.

A garantia que importa e preservada: mesmo conteudo produz os mesmos bytes e o mesmo
SHA-256, porque cada linha e serializada com `sort_keys` e a ORDEM das linhas e determinada
pelo gerador, nunca pela iteracao de um `set` ou `dict`.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile

# Parametros da forma canonica. Qualquer reimplementacao que os altere produz outro
# digest para o mesmo conteudo.
JSON_ENSURE_ASCII = False
JSON_SORT_KEYS = True
JSON_INDENT = 2
JSON_TRAILING_NEWLINE = "\n"


class CorruptFileError(Exception):
    """Arquivo existente em disco que nao pode ser lido como JSON UTF-8 valido."""


def canonical_bytes(payload) -> bytes:
    """Serializa o payload na forma canonica do snapshot."""
    text = json.dumps(
        payload,
        ensure_ascii=JSON_ENSURE_ASCII,
        sort_keys=JSON_SORT_KEYS,
        indent=JSON_INDENT,
    )
    return (text + JSON_TRAILING_NEWLINE).encode("utf-8")


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def write_json(path: str, payload) -> tuple[str, int]:
    """Grava o payload atomicamente na forma canonica. Retorna (sha256, bytes)."""
    blob = canonical_bytes(payload)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        # Inclui KeyboardInterrupt: o temporario nunca vira arquivo final.
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return digest(blob), len(blob)


def write_jsonl(path: str, rows) -> tuple[str, int, int]:
    """Grava um log NDJSON atomicamente. Retorna (sha256, bytes, linhas).

    Cada linha e `json.dumps(..., sort_keys=True, ensure_ascii=False)` SEM indentacao, e o
    arquivo termina em newline. A ordem das linhas e responsabilidade de quem chama: ela faz
    parte do conteudo e portanto do digest.
    """
    chunks = []
    count = 0
    for row in rows:
        chunks.append(
            json.dumps(row, ensure_ascii=False, sort_keys=JSON_SORT_KEYS).encode("utf-8")
        )
        chunks.append(b"\n")
        count += 1
    blob = b"".join(chunks)

    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".jsonl")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return digest(blob), len(blob), count


def read_jsonl(path: str) -> tuple[list, bytes]:
    """Le um log NDJSON. Levanta CorruptFileError na primeira linha invalida."""
    blob = read_bytes(path)
    rows = []
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except ValueError as exc:
            raise CorruptFileError(f"{path}: linha {number}: {exc}") from exc
    return rows, blob


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def read_json(path: str) -> tuple[object, bytes]:
    """Le um arquivo do snapshot. Levanta CorruptFileError se nao for JSON UTF-8 valido."""
    blob = read_bytes(path)
    try:
        return json.loads(blob.decode("utf-8")), blob
    except (ValueError, UnicodeDecodeError) as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc
