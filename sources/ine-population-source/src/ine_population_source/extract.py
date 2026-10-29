"""Extraction of population series by province.

One step against the INE's Tempus3 API, once per configured table_id:
    GET /ES/DATOS_TABLA/{table_id}  -> full historical series of that table

No tree crawl: unlike Mercadona, there is no category discovery — the table_ids are passed
explicitly (--tables). No transformation is applied to the payload: it is written in the
canonical form described in canonical.py, with its content intact.

The manifest is written TWICE: a preliminary one right after the partition is created, and
the final one at the end. Without the preliminary one, an interrupted run would leave the
partition without a manifest, and the immutability lock -- which depends on it -- would be
off on resume.
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
    PartitionError,
    TABLE_FILE,
    TABLES_DIR,
    assert_writable,
    build_history,
    mark_success,
    partition_path,
    table_path,
    validate_date,
    validate_token,
    write_manifest,
)
from .schema import (
    SchemaError,
    count_data_points,
    count_series,
    fingerprint,
    is_mapping,
    validate_payload,
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
    """Is the file on disk exactly in this Source's canonical form?"""
    return blob == canonical_bytes(payload)


def _build_manifest(
    *,
    run_id,
    started,
    finished,
    duration,
    complete,
    ingestion_date,
    args,
    fetcher,
    targeted,
    files,
    failures,
    anomalies,
    payloads,
    series_count,
    data_point_count,
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
        },
        "partition": {"ingestion_date": ingestion_date},
        "config": {
            "delay_seconds": args.delay,
            "timeout_seconds": args.timeout,
            "max_retries": args.max_retries,
            "tables": list(args.tables),
            "overwrite": args.overwrite,
        },
        "totals": {
            "http_requests": fetcher.requests,
            "http_retries": fetcher.retries,
            "tables_targeted": targeted,
            "table_files": sum(1 for f in files if f["stage"] == "tables"),
            "series_count": series_count,
            "data_point_count": data_point_count,
            "bytes": sum(f["bytes"] for f in files),
        },
        "schema_fingerprint": fingerprint(payloads),
        "files": sorted(files, key=lambda f: f["path"]),
        "failures": failures,
        "anomalies": anomalies,
        "history": history,
    }


def run(args, fetcher_factory=Fetcher) -> int:
    """Run the extraction. Returns the exit code.

    fetcher_factory exists as a test seam: the suite injects a client that answers from
    disk, to exercise the whole machinery without touching the network.
    """
    try:
        ingestion_date = validate_date(
            args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        tables = [validate_token("table_id", str(t)) for t in args.tables]
        if not tables:
            raise PartitionError("--tables needs at least one table_id")
        partition = partition_path(args.out, ingestion_date)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
    except PartitionError as exc:
        print(f"FATAL FAILURE: {exc}", flush=True)
        return EXIT_FATAL

    started = datetime.now(timezone.utc)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}"
    clock = time.monotonic()
    history = build_history(previous)

    with RunLogger(os.path.join(partition, "_run.log")) as log:
        log(f"== run {run_id} | partition {partition} | delay {args.delay}s | tables {tables}")
        fetcher = fetcher_factory(args.delay, args.timeout, args.max_retries, log)
        files: list[dict] = []
        failures: list[dict] = []
        anomalies: list[dict] = []
        series_count = 0
        data_point_count = 0
        payloads: list[object] = []
        previous_digests = {
            entry.get("path"): entry.get("sha256")
            for entry in (previous or {}).get("files", [])
            if isinstance(entry, dict)
        }

        def snapshot_manifest(complete, targeted):
            return _build_manifest(
                run_id=run_id,
                started=started,
                finished=datetime.now(timezone.utc),
                duration=time.monotonic() - clock,
                complete=complete,
                ingestion_date=ingestion_date,
                args=args,
                fetcher=fetcher,
                targeted=targeted,
                files=files,
                failures=failures,
                anomalies=anomalies,
                payloads=payloads,
                series_count=series_count,
                data_point_count=data_point_count,
                history=history,
            )

        # Preliminary manifest. From here on the partition has an identity on disk: an
        # interruption no longer leaves an "ownerless" partition that a resume would accept
        # on top of a complete partition.
        write_manifest(partition, snapshot_manifest(False, len(tables)))
        mark_success(partition, False, run_id)

        # ---- One table at a time -----------------------------------------------
        for index, table_id in enumerate(tables, start=1):
            label = f"[{index}/{len(tables)}] table_id={table_id}"
            try:
                target_path = table_path(partition, table_id)
            except PartitionError as exc:
                failures.append({"stage": "tables", "table_id": table_id, "error": str(exc)})
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
                    log(f"{label} unreadable file ({exc}); downloading again")
                    payload = None

            if reused and not isinstance(payload, list):
                log(f"{label} file with unexpected shape; downloading again")
                payload, reused = None, False

            # A file that is not in canonical form did not come from this Source.
            if reused and not _is_canonical(blob, payload):
                anomalies.append(
                    {"kind": "arquivo_nao_canonico", "path": relative, "table_id": table_id}
                )
                log(f"{label} file not in canonical form; downloading again")
                payload, reused = None, False

            # The partition is immutable: a mismatch with the previous manifest means
            # tampering or loss. We record the anomaly AND download again from the source.
            if reused and previous_digests.get(relative) not in (None, digest(blob)):
                anomalies.append(
                    {
                        "kind": "conteudo_divergente_do_manifesto_anterior",
                        "path": relative,
                        "table_id": table_id,
                    }
                )
                log(f"{label} differs from the previous manifest; downloading again")
                payload, reused = None, False

            if payload is None:
                payload, error = fetcher.get_json(f"/ES/DATOS_TABLA/{table_id}", {})
                if payload is None:
                    failures.append({"stage": "tables", "table_id": table_id, "error": error})
                    log(f"{label} FAILED: {error}")
                    continue
                try:
                    validate_payload(payload)
                except SchemaError as exc:
                    failures.append(
                        {"stage": "tables", "table_id": table_id, "error": str(exc)}
                    )
                    log(f"{label} FAILED: {exc}")
                    continue
                reused = False

            rows = count_series(payload)
            points = count_data_points(payload)
            series_count += rows
            data_point_count += points
            payloads.append(payload)

            if reused:
                file_digest, file_size = digest(blob), len(blob)
            else:
                file_digest, file_size = write_json(target_path, payload)

            files.append(
                {
                    "path": relative,
                    "stage": "tables",
                    "table_id": table_id,
                    "sha256": file_digest,
                    "bytes": file_size,
                    "records": rows,
                    "reused": reused,
                }
            )
            log(f"{label} {'reused' if reused else 'ok'} ({rows} series, {points} data points)")

        # ---- Inventory: files already present that this run did not visit ----
        declared = {entry["path"] for entry in files}
        tables_dir = os.path.join(partition, TABLES_DIR)
        if os.path.isdir(tables_dir):
            for name in sorted(os.listdir(tables_dir)):
                match = TABLE_FILE.match(name)
                existing = os.path.join(tables_dir, name)
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
                if not isinstance(payload, list) or not _is_canonical(blob, payload):
                    anomalies.append(
                        {"kind": "arquivo_nao_canonico_nao_visitado", "path": relative}
                    )
                    log(f"[inventory] {name} not in canonical form and not visited")
                    continue
                rows = count_series(payload)
                points = count_data_points(payload)
                series_count += rows
                data_point_count += points
                payloads.append(payload)
                table_id = match.group(1)
                files.append(
                    {
                        "path": relative,
                        "stage": "tables",
                        "table_id": table_id,
                        "sha256": digest(blob),
                        "bytes": len(blob),
                        "records": rows,
                        "reused": True,
                    }
                )
                log(f"[inventory] {name} declared ({rows} series, not visited)")

        # ---- Final manifest -------------------------------------------------
        complete = not failures
        manifest = snapshot_manifest(complete, len(tables))
        write_manifest(partition, manifest)
        mark_success(partition, complete, run_id)

        totals = manifest["totals"]
        log(
            f"== end: {totals['table_files']}/{len(tables)} files, "
            f"{totals['series_count']} series, {totals['data_point_count']} data points, "
            f"{totals['http_requests']} requests ({totals['http_retries']} retries), "
            f"{manifest['duration_seconds']}s, complete={complete}"
        )
        if anomalies:
            log(f"== {len(anomalies)} anomaly(ies) recorded in anomalies[]")
        if failures:
            log(
                f"== {len(failures)} table(s) failed; re-run the same command "
                f"to complete the partition"
            )
        return EXIT_PARTIAL if failures else EXIT_OK
