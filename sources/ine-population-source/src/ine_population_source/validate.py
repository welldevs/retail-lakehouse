"""Validacao independente de uma particao do snapshot.

Rele e reparseia todos os arquivos declarados no manifesto, recalculando checksums,
contagens e impressao digital de schema em vez de aceitar os valores registrados. Alem
disso varre o diretorio para detectar arquivos que o manifesto nao declara.

Dois tipos de problema:
  - INTEGRIDADE -> sempre fatal (codigo 1).
  - QUALIDADE   -> medida e reportada; fatal apenas com --strict.

Cobertura aqui e "todo table_id configurado (manifest.config.tables) tem arquivo": ao
contrario da Mercadona, nao ha arvore de categorias externa para descobrir o esperado —
os table_id sao a propria configuracao da execucao.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_json
from .partition import MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME, manifest_path, success_path
from .schema import count_series, fingerprint, is_mapping, series_of

EXIT_OK = 0
EXIT_FAILED = 1


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def run(args) -> int:
    partition = args.partition
    errors: list[str] = []
    warnings: list[str] = []

    path = manifest_path(partition)
    if not os.path.exists(path):
        print(f"ERRO: manifesto nao encontrado em {path}")
        return EXIT_FAILED
    try:
        manifest, _ = read_json(path)
    except CorruptFileError as exc:
        print(f"ERRO: manifesto ilegivel: {exc}")
        return EXIT_FAILED
    if not is_mapping(manifest) or not isinstance(manifest.get("files"), list):
        print(f"ERRO: manifesto sem a lista 'files': {path}")
        return EXIT_FAILED

    root = os.path.abspath(os.path.join(partition, ".."))
    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        errors.append(
            f"manifest_version {version!r} diferente do suportado ({MANIFEST_VERSION})"
        )

    print(f"particao ......... {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"completa ......... {manifest.get('complete')}")

    # ---- Integridade dos arquivos declarados -------------------------------
    checked = 0
    declared: set = set()
    payloads: list[object] = []
    for entry in manifest["files"]:
        if not is_mapping(entry) or "path" not in entry:
            errors.append(f"entrada de manifesto malformada: {entry!r}")
            continue
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            errors.append(f"caminho fora da raiz do snapshot: {entry['path']}")
            continue
        declared.add(os.path.abspath(target))
        if not os.path.exists(target):
            errors.append(f"arquivo ausente: {entry['path']}")
            continue
        with open(target, "rb") as handle:
            blob = handle.read()
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum divergente: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"tamanho divergente: {entry['path']}")
        try:
            payload, _ = read_json(target)
        except CorruptFileError as exc:
            errors.append(f"JSON invalido em {entry['path']}: {exc}")
            continue
        if entry.get("stage") == "tables":
            observed = count_series(payload)
            payloads.append(payload)
        else:
            observed = entry.get("records")
        if observed != entry.get("records"):
            errors.append(
                f"contagem divergente em {entry['path']}: "
                f"manifesto={entry.get('records')} arquivo={observed}"
            )
    print(f"arquivos ......... {checked}/{len(manifest['files'])} lidos e conferidos")

    if manifest.get("failures"):
        errors.append(
            f"{len(manifest['failures'])} tabela(s) registradas como falha no manifesto"
        )

    anomalies = manifest.get("anomalies") or []
    print(f"anomalias ........ {len(anomalies)}")
    for item in anomalies:
        if is_mapping(item):
            warnings.append(f"anomalia registrada: {item.get('kind')} em {item.get('path')}")

    # ---- Arquivos em disco nao declarados no manifesto ---------------------
    orphans: list[str] = []
    known_root_files = {MANIFEST_NAME, SUCCESS_NAME, RUN_LOG_NAME}
    partition_abs = os.path.abspath(partition)
    for base, _, names in os.walk(partition_abs):
        for name in sorted(names):
            candidate = os.path.abspath(os.path.join(base, name))
            if base == partition_abs and name in known_root_files:
                continue
            if candidate not in declared:
                orphans.append(os.path.relpath(candidate, partition_abs))
    print(f"orfaos ........... {len(orphans)}")
    if orphans:
        errors.append(f"arquivo(s) em disco fora do manifesto: {orphans}")

    # ---- Marcador _SUCCESS coerente com 'complete' -------------------------
    has_success = os.path.exists(success_path(partition))
    if bool(manifest.get("complete")) != has_success:
        errors.append(
            f"_SUCCESS {'presente' if has_success else 'ausente'} contradiz "
            f"complete={manifest.get('complete')}"
        )

    # ---- Cobertura: todo table_id configurado tem arquivo ------------------
    configured = {str(t) for t in (manifest.get("config") or {}).get("tables") or []}
    present = {
        str(entry.get("table_id"))
        for entry in manifest["files"]
        if is_mapping(entry) and entry.get("stage") == "tables"
    }
    print(f"cobertura ........ {len(present)}/{len(configured)} tabelas configuradas")
    missing = configured - present
    if missing:
        errors.append(f"table_id(s) configurados sem arquivo: {sorted(missing)}")

    # ---- Qualidade: toda serie tem ao menos um ponto com Valor -------------
    rows = 0
    points = 0
    sem_valor = 0
    for entry in manifest["files"]:
        if not is_mapping(entry) or entry.get("stage") != "tables":
            continue
        target = os.path.join(root, entry["path"])
        if not os.path.exists(target):
            continue
        try:
            payload, _ = read_json(target)
        except CorruptFileError:
            continue  # ja reportado como erro de integridade
        for series in series_of(payload):
            rows += 1
            data = series.get("Data") if is_mapping(series) else None
            data = data if isinstance(data, list) else []
            if not any(is_mapping(p) and p.get("Valor") is not None for p in data):
                sem_valor += 1
            points += sum(1 for p in data if is_mapping(p))

    print(f"series ........... {rows}")
    print(f"pontos ........... {points}")
    print(f"series sem valor . {sem_valor}")

    if rows == 0:
        print("completude ....... n/a (nenhuma serie lida)")
    else:
        print(f"completude ....... {100.0 * (1 - sem_valor / rows):.2f}% das series com valor")

    # ---- Impressao digital de schema ---------------------------------------
    declared_fp = manifest.get("schema_fingerprint") or {}
    observed_fp = fingerprint(payloads)
    same_fp = declared_fp.get("sha256") == observed_fp["sha256"]
    print(f"schema ........... {observed_fp['sha256'][:16]} ({'igual' if same_fp else 'DIVERGENTE'})")
    if not same_fp:
        errors.append(
            f"schema_fingerprint divergente: manifesto={declared_fp.get('sha256')} "
            f"observado={observed_fp['sha256']}"
        )

    # ---- Totais reconferidos contra a releitura ----------------------------
    totals = manifest.get("totals") or {}
    table_entries = [
        e for e in manifest["files"] if is_mapping(e) and e.get("stage") == "tables"
    ]
    observed_totals = {
        "series_count": rows,
        "data_point_count": points,
        "table_files": len(table_entries),
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if is_mapping(e)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} divergente: manifesto={totals.get(key)} observado={observed}"
            )

    if table_entries and rows == 0:
        errors.append(
            f"nenhuma serie lida em {len(table_entries)} arquivo(s) de tabela: "
            f"formato da fonte pode ter mudado"
        )
    if args.strict and sem_valor:
        errors.append(f"{sem_valor} serie(s) sem nenhum ponto com valor (--strict)")

    print()
    for item in warnings:
        print(f"AVISO: {item}")
    if errors:
        print(f"FALHOU ({len(errors)} problema(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integridade, cobertura, schema e totais validados.")
    return EXIT_OK
