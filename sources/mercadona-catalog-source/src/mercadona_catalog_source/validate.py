"""Validacao independente de uma particao do snapshot.

Rele e reparseia todos os arquivos declarados no manifesto, recalculando checksums,
contagens e impressao digital de schema em vez de aceitar os valores registrados. Alem
disso varre o diretorio para detectar arquivos que o manifesto nao declara.

Dois tipos de problema:
  - INTEGRIDADE -> sempre fatal (codigo 1).
  - QUALIDADE   -> medida e reportada; fatal apenas com --strict.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_json
from .partition import (
    CATALOG_DIR,
    CATEGORIES_DIR,
    MANIFEST_NAME,
    RUN_LOG_NAME,
    SUCCESS_NAME,
    categories_path,
    manifest_path,
    success_path,
)
from .schema import (
    count_level1,
    count_products,
    fingerprint,
    flatten_tree,
    is_mapping,
    iter_products,
    SchemaError,
)

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

    root = os.path.abspath(os.path.join(partition, "..", ".."))
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
    rows_by_entry: dict = {}
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
        if entry.get("stage") == "catalog":
            observed = count_products(payload)
            rows_by_entry[entry["path"]] = observed
            payloads.append(payload)
        elif entry.get("stage") == "categories":
            observed = count_level1(payload)
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
            f"{len(manifest['failures'])} categoria(s) registradas como falha no manifesto"
        )

    # Anomalias sao eventos ja tratados pela extracao (arquivo rebaixado da fonte). Ficam
    # visiveis para auditoria, sem reprovar uma particao que foi corrigida.
    anomalies = manifest.get("anomalies") or []
    print(f"anomalias ........ {len(anomalies)}")
    for item in anomalies:
        if is_mapping(item):
            warnings.append(f"anomalia registrada: {item.get('kind')} em {item.get('path')}")

    # ---- Arquivos em disco nao declarados no manifesto ---------------------
    # Varredura recursiva: um listdir raso deixaria passar catalog/backup/old.json e
    # qualquer subdiretorio criado dentro da particao, e a garantia de inventario fechado
    # so valeria para o primeiro nivel.
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

    # ---- Cobertura: toda categoria de nivel 2 tem arquivo ------------------
    tree_file = categories_path(partition)
    level2: list = []
    if not os.path.exists(tree_file):
        errors.append("arvore de categorias ausente: categories/categories.json")
    else:
        try:
            tree, _ = read_json(tree_file)
            level2 = flatten_tree(tree)
        except (CorruptFileError, SchemaError) as exc:
            errors.append(f"arvore de categorias ilegivel: {exc}")

    expected = {node["id"] for node in level2}
    present = {
        entry.get("category_id")
        for entry in manifest["files"]
        if is_mapping(entry) and entry.get("stage") == "catalog"
    }
    print(f"cobertura ........ {len(present)}/{len(expected)} categorias de nivel 2")
    limit = (manifest.get("config") or {}).get("limit")
    missing = expected - present
    if missing and limit is None:
        errors.append(f"categorias de nivel 2 sem arquivo: {sorted(missing)}")
    elif missing and limit is not None:
        warnings.append(
            f"{len(missing)} categoria(s) sem arquivo (esperado: particao parcial, limit={limit})"
        )

    # ---- Qualidade: produto, categoria e preco -----------------------------
    lineage = {node["id"]: node for node in level2}
    rows = 0
    unique_ids: set = set()
    sem_nome = sem_preco = sem_categoria = 0
    for entry in manifest["files"]:
        if not is_mapping(entry) or entry.get("stage") != "catalog":
            continue
        target = os.path.join(root, entry["path"])
        if not os.path.exists(target):
            continue
        try:
            payload, _ = read_json(target)
        except CorruptFileError:
            continue  # ja reportado como erro de integridade
        has_lineage = entry.get("category_id") in lineage
        for group, product in iter_products(payload):
            rows += 1
            if "id" in product:
                unique_ids.add(product["id"])
            if not product.get("display_name"):
                sem_nome += 1
            price = (product.get("price_instructions") or {}).get("unit_price")
            if price is None or str(price).strip() == "":
                sem_preco += 1
            if not has_lineage or not group.get("name"):
                sem_categoria += 1

    duplicadas = rows - len(unique_ids)
    print(f"linhas ........... {rows}")
    print(f"produtos unicos .. {len(unique_ids)} ({duplicadas} linhas repetidas)")
    print(f"sem nome ......... {sem_nome}")
    print(f"sem preco ........ {sem_preco}")
    print(f"sem categoria .... {sem_categoria}")

    incompletos = sem_nome + sem_preco + sem_categoria
    if rows == 0:
        print("completude ....... n/a (nenhuma linha lida)")
    else:
        print(
            f"completude ....... {100.0 * (1 - incompletos / (rows * 3)):.2f}% "
            f"dos campos de escopo"
        )

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
    catalog_entries = [
        e for e in manifest["files"] if is_mapping(e) and e.get("stage") == "catalog"
    ]
    observed_totals = {
        "product_rows": rows,
        "unique_product_ids": len(unique_ids),
        "catalog_files": len(catalog_entries),
        "bytes": sum(
            e.get("bytes", 0) for e in manifest["files"] if is_mapping(e)
        ),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} divergente: manifesto={totals.get(key)} observado={observed}"
            )

    if catalog_entries and rows == 0:
        errors.append(
            f"nenhum produto lido em {len(catalog_entries)} arquivo(s) de catalogo: "
            f"formato da fonte pode ter mudado"
        )
    if args.strict and incompletos:
        errors.append(f"{incompletos} campo(s) de escopo ausentes (--strict)")

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
