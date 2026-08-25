"""Validacao independente de uma particao do snapshot.

Rele todos os arquivos declarados no manifesto, recalculando checksum e estatisticas de
linha em vez de aceitar os valores registrados. Alem disso varre o diretorio para
detectar arquivos que o manifesto nao declara.

Dois tipos de problema:
  - INTEGRIDADE -> sempre fatal (codigo 1).
  - COBERTURA/QUALIDADE -> medida e reportada; fatal apenas com --strict.

Cobertura aqui e "toda combinacao (provincia, dataset) configurada em manifest.config
tem arquivo landado" — os mesmos dados que a propria execucao de intake declarou como
alvo, igual ao papel de manifest.config.tables na source de populacao.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json
from .partition import DATASETS, MANIFEST_NAME, RUN_LOG_NAME, SUCCESS_NAME, manifest_path, success_path
from .schema import dominant_width, line_stats

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
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
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
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or "path" not in entry:
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
        blob = read_bytes(target)
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum divergente: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"tamanho divergente: {entry['path']}")
        stats = line_stats(target)
        if stats["line_count"] != entry.get("line_count"):
            errors.append(
                f"contagem de linhas divergente em {entry['path']}: "
                f"manifesto={entry.get('line_count')} arquivo={stats['line_count']}"
            )
        observed_width = dominant_width(stats)
        if observed_width != entry.get("dominant_line_width"):
            warnings.append(
                f"largura de linha dominante divergiu em {entry['path']}: "
                f"manifesto={entry.get('dominant_line_width')} arquivo={observed_width} "
                f"(pode indicar mudanca de layout do INE)"
            )
    print(f"arquivos ......... {checked}/{len(manifest['files'])} lidos e conferidos")

    if manifest.get("failures"):
        errors.append(
            f"{len(manifest['failures'])} combinacao(oes) provincia/dataset registradas "
            f"como falha no manifesto"
        )

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

    # ---- Cobertura: toda combinacao (provincia, dataset) configurada tem arquivo --
    configured_provinces = list((manifest.get("config") or {}).get("provinces") or [])
    configured_datasets = list((manifest.get("config") or {}).get("datasets") or DATASETS)
    expected = {(p, d) for p in configured_provinces for d in configured_datasets}
    present = {
        (entry.get("province"), entry.get("dataset"))
        for entry in manifest["files"]
        if isinstance(entry, dict)
    }
    print(f"cobertura ........ {len(present)}/{len(expected)} combinacoes provincia/dataset")
    missing = expected - present
    if missing:
        errors.append(f"combinacao(oes) configurada(s) sem arquivo: {sorted(missing)}")

    # ---- Totais reconferidos contra a releitura ----------------------------
    totals = manifest.get("totals") or {}
    observed_totals = {
        "files_landed": checked,
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} divergente: manifesto={totals.get(key)} observado={observed}"
            )

    if args.strict and warnings:
        errors.append(f"{len(warnings)} aviso(s) de largura de linha (--strict)")

    print()
    for item in warnings:
        print(f"AVISO: {item}")
    if errors:
        print(f"FALHOU ({len(errors)} problema(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integridade, cobertura e totais validados.")
    return EXIT_OK
