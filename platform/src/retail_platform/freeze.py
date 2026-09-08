"""THE CAPTURE SEAL: `make freeze` and `make freeze-check`.

THE PROBLEM THIS SOLVES, and it is not theoretical.

The last three documentation revisions of this project existed for the same reason: a
regeneration changed numbers that were already written, and nothing warned about it. The
discipline "don't touch RAW after closing" was honored by attention, never by verification —
and attention fails no build.

This turns the discipline into a test.

WHAT IS SEALED, AND WHAT IS DELIBERATELY IGNORED.

Each RAW `_manifest.json` declares the partition's files with `path`, `sha256`, `bytes` and
`records`. The seal covers EXACTLY that — the data. Deliberately left out:

    run_id, started_at_utc, finished_at_utc, duration_seconds, history

These are EXECUTION metadata. Including them would make a byte-identical re-land of the data
break the seal, and a seal that breaks on a no-op trains reviewers to ignore it — the same
defect the dashboard's CONTRACT had with its timestamp, and for the same reason: an alarm
that fires without cause is worse than no alarm.

WHY RAW IS SEALED AND NOT REPRODUCED. It is NOT reproducible, and promising that it were
would be false: the Mercadona API is live, the Callejero is a semiannual manual download, and
the MAPA URL points to "latest data". Everything DOWNSTREAM is deterministic given the same
RAW. `capture_id` is what names that condition: on a new machine the operator runs `make
freeze` and seals THEIR capture, and the test starts guarding theirs.

TWO ARTIFACTS, and the duplication has a reason:

    docs/FREEZE.md                          for a human to read
    platform/dbt/seeds/frozen_capture_seed.csv   for SQL to verify

The markdown isn't readable by SQL, and the CSV isn't readable by a human. Both come out of
the SAME collection, in the same command — there is no second place where the truth lives.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
DEFAULT_PAGE = os.path.join("docs", "FREEZE.md")
DEFAULT_SEED = os.path.join("platform", "dbt", "seeds", "frozen_capture_seed.csv")

MANIFEST_NAME = "_manifest.json"
CAMPOS = ("source", "partition_key", "files", "records", "bytes", "content_sha256")


class FreezeError(RuntimeError):
    """Failure sealing or checking the capture."""


def _sha256_do_conteudo(manifesto: dict) -> str:
    """sha256 of the SORTED list of (path, sha256, bytes, records) declared in the manifest.

    Sorted because the order in which the Source lists the files is an implementation
    detail — two runs over the same data can enumerate differently, and the seal cannot
    depend on that. Canonical via JSON with fixed separators: `json.dumps` with variable
    spacing would change the hash without a single byte of data having changed.
    """
    entradas = sorted(
        (str(f.get("path")), str(f.get("sha256")), int(f.get("bytes") or 0),
         int(f.get("records") or 0))
        for f in manifesto.get("files") or []
    )
    corpo = json.dumps(entradas, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(corpo.encode("utf-8")).hexdigest()


def collect(config=None) -> list[dict]:
    """Walks RAW and returns one row per sealed partition, in stable order."""
    from .config import from_env

    config = config or from_env()
    cliente = config.client()
    paginador = cliente.get_paginator("list_objects_v2")

    registros: list[dict] = []
    for pagina in paginador.paginate(Bucket=config.raw_bucket):
        for item in pagina.get("Contents") or []:
            chave = item["Key"]
            if not chave.endswith("/" + MANIFEST_NAME):
                continue
            corpo = cliente.get_object(Bucket=config.raw_bucket, Key=chave)["Body"].read()
            manifesto = json.loads(corpo)
            prefixo = chave[: -len("/" + MANIFEST_NAME)]
            fonte, _, particao = prefixo.partition("/")
            arquivos = manifesto.get("files") or []
            registros.append({
                "source": fonte,
                "partition_key": particao,
                "files": len(arquivos),
                "records": sum(int(f.get("records") or 0) for f in arquivos),
                "bytes": sum(int(f.get("bytes") or 0) for f in arquivos),
                "content_sha256": _sha256_do_conteudo(manifesto),
            })
    registros.sort(key=lambda r: (r["source"], r["partition_key"]))
    if not registros:
        raise FreezeError(
            f"no _manifest.json under s3://{config.raw_bucket}/. Nothing has landed — "
            "run `make daily` (or the land targets) before sealing."
        )
    return registros


def capture_id(registros: list[dict]) -> str:
    """Aggregate sha256 over the partition seals. ONE number names the whole capture.

    It exists so that a conversation about "the 4,939 products" can say WHICH capture
    those 4,939 belong to, without repeating an 81-row table.
    """
    corpo = json.dumps(
        [[r["source"], r["partition_key"], r["content_sha256"]] for r in registros],
        separators=(",", ":"), ensure_ascii=True,
    )
    return hashlib.sha256(corpo.encode("utf-8")).hexdigest()


def escrever_seed(registros: list[dict], destino: str = DEFAULT_SEED) -> str:
    caminho = destino if os.path.isabs(destino) else os.path.join(REPO, destino)
    buffer = io.StringIO()
    escritor = csv.DictWriter(buffer, fieldnames=CAMPOS, lineterminator="\n")
    escritor.writeheader()
    escritor.writerows({c: r[c] for c in CAMPOS} for r in registros)
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(buffer.getvalue())
    os.replace(temporario, caminho)
    return caminho


def ler_seed(origem: str = DEFAULT_SEED) -> list[dict]:
    caminho = origem if os.path.isabs(origem) else os.path.join(REPO, origem)
    if not os.path.exists(caminho):
        raise FreezeError(
            f"{origem} does not exist: the capture was never sealed. Run `make freeze`."
        )
    with open(caminho, encoding="utf-8") as arquivo:
        return [
            {"source": l["source"], "partition_key": l["partition_key"],
             "files": int(l["files"]), "records": int(l["records"]),
             "bytes": int(l["bytes"]), "content_sha256": l["content_sha256"]}
            for l in csv.DictReader(arquivo)
        ]


def comparar(selado: list[dict], atual: list[dict]) -> list[str]:
    """Differences between the seal and today's RAW. Empty list = the capture is intact."""
    por_chave = lambda lista: {(r["source"], r["partition_key"]): r for r in lista}  # noqa: E731
    a, b = por_chave(selado), por_chave(atual)
    problemas: list[str] = []
    for chave in sorted(set(a) - set(b)):
        problemas.append(f"partition SEALED but MISSING from RAW: {chave[0]}/{chave[1]}")
    for chave in sorted(set(b) - set(a)):
        problemas.append(f"NEW partition, outside the seal: {chave[0]}/{chave[1]}")
    for chave in sorted(set(a) & set(b)):
        if a[chave]["content_sha256"] != b[chave]["content_sha256"]:
            problemas.append(
                f"content CHANGED in {chave[0]}/{chave[1]}: "
                f"sealed={a[chave]['content_sha256'][:16]}... "
                f"current={b[chave]['content_sha256'][:16]}..."
            )
    return problemas


def render(registros: list[dict]) -> str:
    identidade = capture_id(registros)
    por_fonte: dict[str, list[dict]] = {}
    for r in registros:
        por_fonte.setdefault(r["source"], []).append(r)

    linhas = [
        "# The sealed capture",
        "",
        "**Generated by `make freeze`.** Do not edit by hand.",
        "",
        f"**`capture_id` = `{identidade}`**",
        "",
        f"Sealed on {datetime.now(timezone.utc).strftime('%Y-%m-%d')} · "
        f"{len(registros)} partitions · "
        f"{sum(r['records'] for r in registros):,} records · "
        f"{sum(r['bytes'] for r in registros) / (1024**3):.2f} GB".replace(",", "."),
        "",
        "## What this seal is, and what it is not",
        "",
        "**This project's RAW is not reproducible, and promising that it were would be",
        "false.** The Mercadona API is live, the Callejero is a semiannual manual download,",
        "and the MAPA URL points to \"latest data\". Running the extraction tomorrow produces",
        "another capture — and that is not a defect, it is the nature of public sources.",
        "",
        "What **is** guaranteed: everything downstream is deterministic **given the same",
        "RAW**. `capture_id` is the name for that condition. On a new machine, the operator",
        "runs `make freeze` and seals **their** capture; `make freeze-check` starts guarding",
        "theirs.",
        "",
        "**The seal covers the data, not the run.** Each `content_sha256` is the hash of the",
        "sorted list of `(path, sha256, bytes, records)` declared in the partition's",
        "manifest. Deliberately left out are `run_id`, `started_at_utc`, `finished_at_utc`,",
        "`duration_seconds` and `history`: including them would make a byte-identical",
        "re-land of the data break the seal, and an alarm that fires without cause is worse",
        "than no alarm — the same reason `CONTRACT.md` for the dashboard carries the source's",
        "sha256 and not the generation date.",
        "",
        "## How to check",
        "",
        "```bash",
        "make freeze-check   # rereads RAW and compares it with this seal; exits 1 on any difference",
        "```",
        "",
        "Changing a single byte of a sealed partition fails it. Adding a new partition does",
        "too — because a window that keeps growing after closing invalidates every number",
        "already published about it.",
        "",
        "## Summary by source",
        "",
        "| Source | Partitions | Records | GB |",
        "|---|---|---|---|",
    ]
    for fonte in sorted(por_fonte):
        grupo = por_fonte[fonte]
        linhas.append(
            f"| `{fonte}` | {len(grupo)} | "
            + f"{sum(r['records'] for r in grupo):,}".replace(",", ".")
            + f" | {sum(r['bytes'] for r in grupo) / (1024**3):.3f} |"
        )
    linhas += ["", "## Partitions", ""]
    for fonte in sorted(por_fonte):
        linhas += [f"### `{fonte}`", "",
                   "| Partition | Files | Records | Bytes | `content_sha256` |",
                   "|---|---|---|---|---|"]
        for r in por_fonte[fonte]:
            linhas.append(
                f"| `{r['partition_key']}` | {r['files']} | "
                + f"{r['records']:,}".replace(",", ".")
                + f" | {r['bytes']:,}".replace(",", ".")
                + f" | `{r['content_sha256'][:24]}…` |"
            )
        linhas.append("")
    return "\n".join(linhas) + "\n"


def escrever_pagina(registros: list[dict], destino: str = DEFAULT_PAGE) -> str:
    caminho = destino if os.path.isabs(destino) else os.path.join(REPO, destino)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(render(registros))
    os.replace(temporario, caminho)
    return caminho
