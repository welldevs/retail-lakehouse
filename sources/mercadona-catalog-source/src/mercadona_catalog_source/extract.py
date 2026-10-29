"""Catalog extraction: product, category and price.

Two stages against Mercadona's public API:
  1. GET /api/categories/       -> category tree (level 1 + level 2)
  2. GET /api/categories/{id}/  -> one file per level-2 category

No transformation is applied to the payload: it is written in the canonical form described
in canonical.py, with its content intact.

The manifest is written TWICE: a preliminary manifest right after the tree, and the final
one at the end. Without the preliminary one, an interrupted run would leave the partition
without a manifest, and the immutability and fixed-language locks — which depend on it —
would be switched off on resume.
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
    """Append-only partition log, mirrored to stdout."""

    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._handle = open(path, "a", encoding="utf-8")

    def __call__(self, message: str) -> None:
        line = f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {message}"
        # errors="replace": a non-UTF-8 locale must not bring the extraction down midway.
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
    """Is the file on disk exactly in this Source's canonical form?

    A file carrying the same JSON value but a different spelling (other indentation, other
    key order, Unicode escapes) cannot be reused: its SHA-256 would enter the manifest as
    if it were the canonical digest, and a consumer comparing digests across snapshots
    would see a change where nothing changed.
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
    """Runs the extraction. Returns the exit code.

    fetcher_factory exists as a test seam: the suite injects a client that answers from
    memory, to exercise the whole machinery without touching the network.
    """
    try:
        ingestion_date = validate_date(
            args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        warehouse = validate_token("wh", args.wh)
        lang = validate_token("lang", args.lang)
        if args.limit is not None and args.limit < 1:
            raise PartitionError(f"--limit must be >= 1 (got: {args.limit})")
        partition = partition_path(args.out, ingestion_date, warehouse)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, lang, args.overwrite)
    except (PartitionError, CorruptFileError) as exc:
        print(f"FATAL FAILURE: {exc}", flush=True)
        return EXIT_FATAL

    started = datetime.now(timezone.utc)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{warehouse}"
    clock = time.monotonic()
    history = build_history(previous)

    with RunLogger(os.path.join(partition, "_run.log")) as log:
        log(f"== run {run_id} | partition {partition} | delay {args.delay}s | lang {lang}")
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

        # ---- Stage 1: category tree ----------------------------------------
        tree, error = fetcher.get_json("/categories/", {"lang": lang, "wh": warehouse})
        if tree is None:
            log(f"FATAL FAILURE fetching the category tree: {error}")
            return EXIT_FATAL

        # The tree is written BEFORE it is interpreted: an unexpected format does not
        # discard a successful download.
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
            # Preliminary manifest: the downloaded tree stays declared even on the fatal exit,
            # instead of becoming an orphan file in the partition.
            write_manifest(partition, snapshot_manifest(False, level1_count, 0, 0))
            mark_success(partition, False, run_id)
            log(f"FATAL FAILURE: category tree format changed: {exc}")
            return EXIT_FATAL
        if not level2:
            write_manifest(partition, snapshot_manifest(False, level1_count, 0, 0))
            mark_success(partition, False, run_id)
            log("FATAL FAILURE: category tree has no level-2 category")
            return EXIT_FATAL

        targets = level2 if args.limit is None else level2[: args.limit]
        log(f"stage 1 ok: {level1_count} level-1 categories, {len(level2)} level-2")

        # Preliminary manifest. From here on the partition has an identity on disk: an
        # interruption no longer leaves an "ownerless" partition that a resume would accept
        # with another language or on top of a complete partition.
        write_manifest(
            partition, snapshot_manifest(False, level1_count, len(level2), len(targets))
        )
        mark_success(partition, False, run_id)

        # ---- Stage 2: catalog per level-2 category --------------------------
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
                log(f"{label} FAILED: {exc}")
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
                    log(f"{label} unreadable file ({exc}); re-downloading")
                    payload = None

            if reused and not is_mapping(payload):
                log(f"{label} file with unexpected format; re-downloading")
                payload, reused = None, False

            # A file that is not in canonical form did not come from this Source. Reusing it
            # would write into the manifest a digest that is not the content's canonical digest.
            # Anomaly "kind" values are contract literals written to the manifest (kept as-is).
            if reused and not _is_canonical(blob, payload):
                anomalies.append(
                    {
                        "kind": "arquivo_nao_canonico",
                        "path": relative,
                        "category_id": category["id"],
                    }
                )
                log(f"{label} file not in canonical form; re-downloading")
                payload, reused = None, False

            # The partition is immutable: a mismatch with the previous manifest means tampering or
            # loss. We record the anomaly AND re-download from the source, so that the next
            # re-run does not "launder" the tampered content into the new truth.
            if reused and previous_digests.get(relative) not in (None, digest(blob)):
                anomalies.append(
                    {
                        "kind": "conteudo_divergente_do_manifesto_anterior",
                        "path": relative,
                        "category_id": category["id"],
                    }
                )
                log(f"{label} differs from the previous manifest; re-downloading")
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
                    log(f"{label} FAILED: {error}")
                    continue
                if not is_mapping(payload):
                    failures.append(
                        {
                            "stage": "catalog",
                            "category_id": category["id"],
                            "name": category["name"],
                            # Kept verbatim ("response is not a JSON object"): persisted.
                            "error": "resposta nao e um objeto JSON",
                        }
                    )
                    log(f"{label} FAILED: response is not a JSON object")
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
            log(f"{label} {'reused' if reused else 'ok'} ({rows} products)")

        # ---- Inventory: files already present that this run did not visit ---
        # Without this, a partial run would declare fewer files than the partition
        # contains, and the manifest would stop being the closed inventory the contract promises.
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
                    log(f"[inventory] {name} unreadable and not visited in this run")
                    continue
                if not is_mapping(payload) or not _is_canonical(blob, payload):
                    anomalies.append(
                        {"kind": "arquivo_nao_canonico_nao_visitado", "path": relative}
                    )
                    log(f"[inventory] {name} not in canonical form and not visited")
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
                log(f"[inventory] {name} declared ({rows} products, not visited)")

        # ---- Final manifest -------------------------------------------------
        complete = not failures and args.limit is None
        manifest = snapshot_manifest(complete, level1_count, len(level2), len(targets))
        write_manifest(partition, manifest)
        mark_success(partition, complete, run_id)

        totals = manifest["totals"]
        log(
            f"== end: {totals['catalog_files']}/{len(targets)} files, "
            f"{totals['product_rows']} rows, {totals['unique_product_ids']} unique products, "
            f"{totals['http_requests']} requests ({totals['http_retries']} retries), "
            f"{manifest['duration_seconds']}s, complete={complete}"
        )
        if anomalies:
            log(f"== {len(anomalies)} anomaly(ies) recorded in anomalies[]")
        if failures:
            log(
                f"== {len(failures)} category(ies) failed; re-run the same command "
                f"to complete the partition"
            )
        return EXIT_PARTIAL if failures else EXIT_OK
