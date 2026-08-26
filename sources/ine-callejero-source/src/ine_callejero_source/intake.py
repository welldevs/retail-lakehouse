"""Intake dos arquivos do Callejero: validar e incorporar arquivos ja baixados.

CONTRACT.md secao 2: esta Source nao busca dado por rede. Aqui "extract" significa
localizar, dentro de --in, os arquivos de cada (provincia, dataset) configurados,
copia-los verbatim para a particao e registrar manifesto. O verbo e mantido por
uniformidade com as outras Sources — nao ha requisicao HTTP nenhuma neste modulo.

O manifesto e gravado DUAS vezes: preliminar logo apos a particao ser criada, e
definitivo no fim — mesma razao da source de populacao: sem o preliminar, uma execucao
interrompida deixaria a particao sem manifesto, e a trava de imutabilidade (que depende
dele) ficaria desligada na retomada.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from . import SOURCE_NAME
from .canonical import copy_verbatim, digest, read_bytes
from .partition import (
    DATASETS,
    INPUT_FILE,
    PartitionError,
    assert_writable,
    build_history,
    dataset_path,
    mark_success,
    partition_path,
    validate_date,
    validate_province,
    write_manifest,
)
from .schema import dominant_width, line_stats

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


def discover_input_files(in_dir: str) -> dict[tuple[str, str], str]:
    """Mapeia (dataset, provincia) -> caminho absoluto, varrendo --in recursivamente.

    So reconhece arquivos cujo nome bate com o padrao oficial do Callejero
    (partition.INPUT_FILE, que lista SECC/UP/VIAS/PSEU/TRAM). Se houver mais de um
    arquivo pro mesmo (dataset, provincia) em --in, o ultimo encontrado numa varredura
    ordenada (os.walk + sorted) vence — deterministico entre execucoes, nao "o que o
    sistema de arquivos devolver primeiro".
    """
    found: dict[tuple[str, str], str] = {}
    for base, _dirs, names in os.walk(in_dir):
        for name in sorted(names):
            match = INPUT_FILE.match(name)
            if not match:
                continue
            key = (match.group("dataset"), match.group("province"))
            found[key] = os.path.join(base, name)
    return found


def _build_manifest(
    *,
    run_id,
    started,
    finished,
    duration,
    complete,
    ingestion_date,
    args,
    targeted,
    files,
    failures,
    history,
) -> dict:
    return {
        "run_id": run_id,
        "started_at_utc": started.isoformat(timespec="seconds"),
        "finished_at_utc": finished.isoformat(timespec="seconds"),
        "duration_seconds": round(duration, 1),
        "complete": complete,
        "source": {"name": SOURCE_NAME},
        "partition": {"ingestion_date": ingestion_date},
        "config": {
            "in_dir": args.in_dir,
            "provinces": list(args.provinces),
            "datasets": list(DATASETS),
            "overwrite": args.overwrite,
        },
        "totals": {
            "files_targeted": targeted,
            "files_landed": len(files),
            "bytes": sum(f["bytes"] for f in files),
        },
        "files": sorted(files, key=lambda f: f["path"]),
        "failures": failures,
        "history": history,
    }


def run(args) -> int:
    """Executa o intake. Retorna o codigo de saida.

    Nao ha costura de teste tipo `fetcher_factory`: nao existe cliente de rede a
    substituir. A suite exercita este modulo apontando --in para um diretorio de
    fixture com arquivos de amostra, criado por tests/support.py.
    """
    try:
        ingestion_date = validate_date(
            args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        provinces = [validate_province(p) for p in args.provinces]
        if not provinces:
            raise PartitionError("--provinces precisa de pelo menos uma provincia")
        if not os.path.isdir(args.in_dir):
            raise PartitionError(f"--in nao e um diretorio: {args.in_dir}")
        partition = partition_path(args.out, ingestion_date)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
    except PartitionError as exc:
        print(f"FALHA FATAL: {exc}", flush=True)
        return EXIT_FATAL

    started = datetime.now(timezone.utc)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}"
    clock = time.monotonic()
    history = build_history(previous)

    with RunLogger(os.path.join(partition, "_run.log")) as log:
        log(
            f"== run {run_id} | particao {partition} | provincias {provinces} | "
            f"datasets {list(DATASETS)} | in {args.in_dir}"
        )
        available = discover_input_files(args.in_dir)
        log(f"{len(available)} arquivo(s) reconhecidos em --in")

        files: list[dict] = []
        failures: list[dict] = []
        targeted = len(provinces) * len(DATASETS)

        def snapshot_manifest(complete):
            return _build_manifest(
                run_id=run_id,
                started=started,
                finished=datetime.now(timezone.utc),
                duration=time.monotonic() - clock,
                complete=complete,
                ingestion_date=ingestion_date,
                args=args,
                targeted=targeted,
                files=files,
                failures=failures,
                history=history,
            )

        # Manifesto preliminar: ver docstring do modulo.
        write_manifest(partition, snapshot_manifest(False))
        mark_success(partition, False, run_id)

        for province in provinces:
            for dataset in DATASETS:
                label = f"provincia={province} dataset={dataset}"
                source_path = available.get((dataset, province))
                if source_path is None:
                    failures.append(
                        {"province": province, "dataset": dataset, "error": "arquivo nao encontrado em --in"}
                    )
                    log(f"{label} FALHOU: nao encontrado em {args.in_dir}")
                    continue

                original_filename = os.path.basename(source_path)
                try:
                    target_path = dataset_path(partition, province, original_filename)
                except PartitionError as exc:
                    failures.append({"province": province, "dataset": dataset, "error": str(exc)})
                    log(f"{label} FALHOU: {exc}")
                    continue

                relative = os.path.relpath(target_path, args.out)
                reused = False
                blob = b""
                if os.path.exists(target_path) and not args.overwrite:
                    landed_blob = read_bytes(target_path)
                    source_blob = read_bytes(source_path)
                    if landed_blob == source_blob:
                        reused = True
                        blob = landed_blob
                    else:
                        failures.append(
                            {
                                "province": province,
                                "dataset": dataset,
                                "error": "arquivo ja landado diverge do arquivo atual em --in; use --overwrite",
                            }
                        )
                        log(f"{label} FALHOU: landado diverge do --in atual")
                        continue

                if reused:
                    file_digest, file_size = digest(blob), len(blob)
                else:
                    file_digest, file_size = copy_verbatim(source_path, target_path)

                stats = line_stats(target_path)
                files.append(
                    {
                        "path": relative,
                        "province": province,
                        "dataset": dataset,
                        "original_filename": original_filename,
                        "sha256": file_digest,
                        "bytes": file_size,
                        "line_count": stats["line_count"],
                        "dominant_line_width": dominant_width(stats),
                        "reused": reused,
                    }
                )
                log(
                    f"{label} {'reaproveitado' if reused else 'ok'} "
                    f"({stats['line_count']} linhas, largura dominante {dominant_width(stats)})"
                )

        complete = not failures
        manifest = snapshot_manifest(complete)
        write_manifest(partition, manifest)
        mark_success(partition, complete, run_id)

        log(
            f"== fim: {len(files)}/{targeted} arquivos landados, "
            f"{manifest['duration_seconds']}s, complete={complete}"
        )
        if failures:
            log(
                f"== {len(failures)} combinacao(oes) provincia/dataset falharam; "
                f"reexecute o mesmo comando para completar a particao"
            )
        return EXIT_PARTIAL if failures else EXIT_OK
