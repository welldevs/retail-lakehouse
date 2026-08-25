"""As duas formas de arquivo que esta Source grava: manifesto JSON e dataset verbatim.

Ao contrario das outras Sources (que reserializam o payload numa forma JSON canonica),
os arquivos do Callejero sao texto de largura fixa cujo formato e definido pelo proprio
INE — esta Source NAO os reescreve, so os copia. "Canonico" para um dataset aqui
significa "os mesmos bytes do arquivo baixado", nao uma serializacao propria; o SHA-256
e sobre o arquivo como ele chegou. O `_manifest.json` continua sendo JSON de verdade,
gravado na mesma forma determinista das outras Sources.

As duas escritas sao ATOMICAS: temporario no mesmo diretorio + fsync + os.replace. Um
processo interrompido no meio nunca deixa arquivo parcial no lugar final.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile

# Forma canonica do MANIFESTO (JSON). Mesmos parametros das outras Sources: qualquer
# reimplementacao que os altere produz outro digest para o mesmo conteudo.
JSON_ENSURE_ASCII = False
JSON_SORT_KEYS = True
JSON_INDENT = 2
JSON_TRAILING_NEWLINE = "\n"


class CorruptFileError(Exception):
    """Manifesto existente em disco que nao pode ser lido como JSON UTF-8 valido."""


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _atomic_write(path: str, blob: bytes) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
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


# ---- manifesto (JSON) -------------------------------------------------------------


def canonical_json_bytes(payload) -> bytes:
    text = json.dumps(
        payload,
        ensure_ascii=JSON_ENSURE_ASCII,
        sort_keys=JSON_SORT_KEYS,
        indent=JSON_INDENT,
    )
    return (text + JSON_TRAILING_NEWLINE).encode("utf-8")


def write_json(path: str, payload) -> tuple[str, int]:
    blob = canonical_json_bytes(payload)
    _atomic_write(path, blob)
    return digest(blob), len(blob)


def read_json(path: str) -> tuple[object, bytes]:
    with open(path, "rb") as handle:
        blob = handle.read()
    try:
        return json.loads(blob.decode("utf-8")), blob
    except (ValueError, UnicodeDecodeError) as exc:
        raise CorruptFileError(f"{path}: {exc}") from exc


# ---- dataset (verbatim) ------------------------------------------------------------


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def copy_verbatim(source_path: str, target_path: str) -> tuple[str, int]:
    """Copia source_path para target_path, atomicamente. Retorna (sha256, bytes).

    Nenhuma decodificacao, conversao de encoding ou normalizacao de fim de linha
    acontece aqui: os bytes gravados sao exatamente os bytes lidos. CONTRACT.md
    secao 2 promete isso explicitamente.
    """
    blob = read_bytes(source_path)
    _atomic_write(target_path, blob)
    return digest(blob), len(blob)
