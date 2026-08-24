"""Extracao do catalogo: produto, categoria e preco.

Duas etapas contra a API publica da Mercadona:
  1. GET /api/categories/       -> arvore de categorias (nivel 1 + nivel 2)
  2. GET /api/categories/{id}/  -> um arquivo por categoria de nivel 2

Nenhuma transformacao e aplicada ao payload: ele e gravado na forma canonica descrita em
canonical.py, com o conteudo integro.

O manifesto e gravado DUAS vezes: um manifesto preliminar logo apos a arvore, e o
definitivo no fim. Sem o preliminar, uma execucao interrompida deixaria a particao sem
manifesto, e as travas de imutabilidade e de idioma fixo — que dependem dele — ficariam
desligadas na retomada.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from . import SOURCE_NAME
from .canonical import (
    CorruptFileError,
    canonical_bytes,
    digest,
    read_json,
    write_json,
)
from .http_client import BASE_URL, Fetcher
from .partition import (
    CATALOG_DIR,
    CATALOG_FILE,
    PartitionError,
    assert_writable,
    build_history,
    catalog_path,
    categories_path,
    mark_success,
    partition_path,
    validate_date,
    validate_token,
    write_manifest,
)
from .schema import (
    SchemaError,
    collect_product_ids,
    count_level1,
    count_products,
    fingerprint,
    flatten_tree,
    is_mapping,
)

EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


class RunLogger:
    """Log append-only da particao, espelhado no stdout."""

    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._handle = open(path, "a", encoding="utf-8")

    def __call__(self, message: str) -> None:
        line = f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {message}"
        # errors="replace": um locale nao-UTF-8 nao pode derrubar a extracao no meio.
        print(line.encode("utf-8", "replace").decode("utf-8", "replace"), flush=True)
        self._handle.write(line + "\n")
        self._handle.flush()

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()

    def __enter__(self) -> RunLogger:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


def _is_canonical(blob: bytes, payload) -> bool:
    """O arquivo em disco esta exatamente na forma canonica deste Source?

    Um arquivo que carregue o mesmo valor JSON mas outra grafia (outra indentacao, outra
    ordem de chaves, escapes Unicode) nao pode ser reaproveitado: seu SHA-256 entraria no
    manifesto como se fosse o digest canonico, e um consumidor que compare digests entre
    snapshots veria mudanca onde nada mudou.
    """
    return blob == canonical_bytes(payload)


def _build_manifest(
    *,
    run_id,
    started,
    finished,
    duration,
    complete,
    warehouse,
    lang,
    ingestion_date,
    args,
    fetcher,
    level1_count,
    level2_count,
    targeted,
    files,
    failures,
    anomalies,
    payloads,
    product_rows,
    product_ids,
    history,
) -> dict:
    return {
        "run_id": run_id,
        "started_at_utc": started.isoformat(timespec="seconds"),
        "finished_at_utc": finished.isoformat(timespec="seconds"),
        "duration_seconds": round(duration, 1),
        "complete": complete,
        "source": {
            "name": SOURCE_NAME,
            "base_url": BASE_URL,
            "wh": warehouse,
            "lang": lang,
        },
        "partition": {"ingestion_date": ingestion_date, "warehouse": warehouse},
        "config": {
            "delay_seconds": args.delay,
            "timeout_seconds": args.timeout,
            "max_retries": args.max_retries,
            "limit": args.limit,
            "overwrite": args.overwrite,
        },
        "totals": {
            "http_requests": fetcher.requests,
            "http_retries": fetcher.retries,
            "categories_level_1": level1_count,
            "categories_level_2": level2_count,
            "categories_targeted": targeted,
            "catalog_files": sum(1 for f in files if f["stage"] == "catalog"),
            "product_rows": product_rows,
            "unique_product_ids": len(product_ids),
            "bytes": sum(f["bytes"] for f in files),
        },
        "schema_fingerprint": fingerprint(payloads),
        "files": sorted(files, key=lambda f: f["path"]),
        "failures": failures,
        "anomalies": anomalies,
        "history": history,
    }


def run(args, fetcher_factory=Fetcher) -> int:
    """Executa a extracao. Retorna o codigo de saida.

    fetcher_factory existe como costura de teste: a suite injeta um cliente que responde
    do disco, para exercitar toda a maquina sem tocar a rede.
    """
    try:
        ingestion_date = validate_date(
            args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        warehouse = validate_token("wh", args.wh)
        lang = validate_token("lang", args.lang)
        if args.limit is not None and args.limit < 1:
            raise PartitionError(f"--limit deve ser >= 1 (recebido: {args.limit})")
        partition = partition_path(args.out, ingestion_date, warehouse)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, lang, args.overwrite)
    except (PartitionError, CorruptFileError) as exc:
        print(f"FALHA FATAL: {exc}", flush=True)
        return EXIT_FATAL

    started = datetime.now(timezone.utc)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{warehouse}"
    clock = time.monotonic()
    history = build_history(previous)

    with RunLogger(os.path.join(partition, "_run.log")) as log:
        log(f"== run {run_id} | particao {partition} | delay {args.delay}s | lang {lang}")
        fetcher = fetcher_factory(args.delay, args.timeout, args.max_retries, log)
        files: list[dict] = []
        failures: list[dict] = []
        anomalies: list[dict] = []
        product_ids: set = set()
        product_rows = 0
        payloads: list[object] = []
        previous_digests = {
            entry.get("path"): entry.get("sha256")
            for entry in (previous or {}).get("files", [])
            if isinstance(entry, dict)
        }

        def snapshot_manifest(complete, level1_count, level2_count, targeted):
            return _build_manifest(
                run_id=run_id,
                started=started,
                finished=datetime.now(timezone.utc),
                duration=time.monotonic() - clock,
                complete=complete,
                warehouse=warehouse,
                lang=lang,
                ingestion_date=ingestion_date,
                args=args,
                fetcher=fetcher,
                level1_count=level1_count,
                level2_count=level2_count,
                targeted=targeted,
                files=files,
                failures=failures,
                anomalies=anomalies,
                payloads=payloads,
                product_rows=product_rows,
                product_ids=product_ids,
                history=history,
            )

        # ---- Etapa 1: arvore de categorias ---------------------------------
        tree, error = fetcher.get_json("/categories/", {"lang": lang, "wh": warehouse})
        if tree is None:
            log(f"FALHA FATAL na arvore de categorias: {error}")
            return EXIT_FATAL

        # A arvore e gravada ANTES de ser interpretada: um formato inesperado nao
        # descarta um download bem-sucedido.
        tree_path = categories_path(partition)
        tree_digest, tree_size = write_json(tree_path, tree)
        files.append(
            {
                "path": os.path.relpath(tree_path, args.out),
                "stage": "categories",
                "sha256": tree_digest,
                "bytes": tree_size,
                "records": count_level1(tree),
            }
        )

        level1_count = count_level1(tree)
        try:
            level2 = flatten_tree(tree)
        except SchemaError as exc:
            # Manifesto preliminar: a arvore baixada fica declarada mesmo na saida fatal,
            # em vez de virar arquivo orfao na particao.
            write_manifest(partition, snapshot_manifest(False, level1_count, 0, 0))
            mark_success(partition, False, run_id)
            log(f"FALHA FATAL: formato da arvore de categorias mudou: {exc}")
            return EXIT_FATAL
        if not level2:
            write_manifest(partition, snapshot_manifest(False, level1_count, 0, 0))
            mark_success(partition, False, run_id)
            log("FALHA FATAL: arvore de categorias sem nenhuma categoria de nivel 2")
            return EXIT_FATAL

        targets = level2 if args.limit is None else level2[: args.limit]
        log(f"etapa 1 ok: {level1_count} categorias nivel 1, {len(level2)} nivel 2")

        # Manifesto preliminar. A partir daqui a particao tem identidade em disco: uma
        # interrupcao nao deixa mais uma particao "sem dono" que a retomada aceitaria com
        # outro idioma ou por cima de uma particao completa.
        write_manifest(
            partition, snapshot_manifest(False, level1_count, len(level2), len(targets))
        )
        mark_success(partition, False, run_id)

        # ---- Etapa 2: catalogo por categoria de nivel 2 ---------------------
        for index, category in enumerate(targets, start=1):
            label = f"[{index}/{len(targets)}] {category['id']} {category['name'][:38]}"
            try:
                target_path = catalog_path(partition, category["id"])
            except PartitionError as exc:
                failures.append(
                    {
                        "stage": "catalog",
                        "category_id": category["id"],
                        "name": category["name"],
                        "error": str(exc),
                    }
                )
                log(f"{label} FALHOU: {exc}")
                continue

            relative = os.path.relpath(target_path, args.out)
            payload = None
            blob = b""
            reused = False

            if os.path.exists(target_path) and not args.overwrite:
                try:
                    payload, blob = read_json(target_path)
                    reused = True
                except CorruptFileError as exc:
                    log(f"{label} arquivo ilegivel ({exc}); rebaixando")
                    payload = None

            if reused and not is_mapping(payload):
                log(f"{label} arquivo com formato inesperado; rebaixando")
                payload, reused = None, False

            # Um arquivo que nao esta na forma canonica nao veio deste Source. Reaproveita-lo
            # gravaria no manifesto um digest que nao e o digest canonico do conteudo.
            if reused and not _is_canonical(blob, payload):
                anomalies.append(
                    {
                        "kind": "arquivo_nao_canonico",
                        "path": relative,
                        "category_id": category["id"],
                    }
                )
                log(f"{label} arquivo fora da forma canonica; rebaixando")
                payload, reused = None, False

            # Particao e imutavel: divergencia do manifesto anterior e adulteracao ou
            # perda. Registramos a anomalia E rebaixamos da fonte, para que a reexecucao
            # seguinte nao "lave" o conteudo adulterado transformando-o na nova verdade.
            if reused and previous_digests.get(relative) not in (None, digest(blob)):
                anomalies.append(
                    {
                        "kind": "conteudo_divergente_do_manifesto_anterior",
                        "path": relative,
                        "category_id": category["id"],
                    }
                )
                log(f"{label} divergente do manifesto anterior; rebaixando")
                payload, reused = None, False

            if payload is None:
                payload, error = fetcher.get_json(
                    f"/categories/{category['id']}/", {"lang": lang, "wh": warehouse}
                )
                if payload is None:
                    failures.append(
                        {
                            "stage": "catalog",
                            "category_id": category["id"],
                            "name": category["name"],
                            "error": error,
                        }
                    )
                    log(f"{label} FALHOU: {error}")
                    continue
                if not is_mapping(payload):
                    failures.append(
                        {
                            "stage": "catalog",
                            "category_id": category["id"],
                            "name": category["name"],
                            "error": "resposta nao e um objeto JSON",
                        }
                    )
                    log(f"{label} FALHOU: resposta nao e um objeto JSON")
                    continue
                reused = False

            rows = count_products(payload)
            product_rows += rows
            product_ids |= collect_product_ids(payload)
            payloads.append(payload)

            if reused:
                file_digest, file_size = digest(blob), len(blob)
            else:
                file_digest, file_size = write_json(target_path, payload)

            files.append(
                {
                    "path": relative,
                    "stage": "catalog",
                    "category_id": category["id"],
                    "sha256": file_digest,
                    "bytes": file_size,
                    "records": rows,
                    "reused": reused,
                }
            )
            log(f"{label} {'reaproveitado' if reused else 'ok'} ({rows} produtos)")

        # ---- Inventario: arquivos ja presentes que esta execucao nao visitou --
        # Sem isto, uma execucao parcial declararia menos arquivos do que a particao
        # contem, e o manifesto deixaria de ser o inventario fechado que o contrato promete.
        declared = {entry["path"] for entry in files}
        catalog_dir = os.path.join(partition, CATALOG_DIR)
        if os.path.isdir(catalog_dir):
            for name in sorted(os.listdir(catalog_dir)):
                match = CATALOG_FILE.match(name)
                existing = os.path.join(catalog_dir, name)
                if not match or not os.path.isfile(existing):
                    continue
                relative = os.path.relpath(existing, args.out)
                if relative in declared:
                    continue
                try:
                    payload, blob = read_json(existing)
                except CorruptFileError as exc:
                    anomalies.append(
                        {"kind": "arquivo_ilegivel_nao_visitado", "path": relative, "error": str(exc)}
                    )
                    log(f"[inventario] {name} ilegivel e nao visitado nesta execucao")
                    continue
                if not is_mapping(payload) or not _is_canonical(blob, payload):
                    anomalies.append(
                        {"kind": "arquivo_nao_canonico_nao_visitado", "path": relative}
                    )
                    log(f"[inventario] {name} fora da forma canonica e nao visitado")
                    continue
                rows = count_products(payload)
                product_rows += rows
                product_ids |= collect_product_ids(payload)
                payloads.append(payload)
                category_id = match.group(1)
                files.append(
                    {
                        "path": relative,
                        "stage": "catalog",
                        "category_id": int(category_id) if category_id.isdigit() else category_id,
                        "sha256": digest(blob),
                        "bytes": len(blob),
                        "records": rows,
                        "reused": True,
                    }
                )
                log(f"[inventario] {name} declarado ({rows} produtos, nao visitado)")

        # ---- Manifesto definitivo -------------------------------------------
        complete = not failures and args.limit is None
        manifest = snapshot_manifest(complete, level1_count, len(level2), len(targets))
        write_manifest(partition, manifest)
        mark_success(partition, complete, run_id)

        totals = manifest["totals"]
        log(
            f"== fim: {totals['catalog_files']}/{len(targets)} arquivos, "
            f"{totals['product_rows']} linhas, {totals['unique_product_ids']} produtos unicos, "
            f"{totals['http_requests']} requests ({totals['http_retries']} retries), "
            f"{manifest['duration_seconds']}s, complete={complete}"
        )
        if anomalies:
            log(f"== {len(anomalies)} anomalia(s) registradas em anomalies[]")
        if failures:
            log(
                f"== {len(failures)} categoria(s) falharam; reexecute o mesmo comando "
                f"para completar a particao"
            )
        return EXIT_PARTIAL if failures else EXIT_OK
