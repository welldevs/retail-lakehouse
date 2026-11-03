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

A WINDOW THAT GROWS IS NOT A CAPTURE THAT CHANGED (CR-020). Since the daily schedule runs
(CR-016), every 06:00 lands four Mercadona partitions dated AFTER the sealed window. Failing
on those made `freeze-check` red every day by design — an alarm that is always red trains
reviewers to ignore it (ARCHITECTURE debt #8). The check now separates three things:

    sealed partition changed or missing           -> REFUSED (the published numbers moved)
    new partition dated INSIDE the sealed window  -> REFUSED (a late landing for 10-28
                                                     would move them just as silently)
    new partition dated AFTER the sealed window   -> reported, not refused (growth)

The window is per source: the latest `ingestion_date` sealed for it. A new source, or a
partition key without a date, has no window to be after, so it is refused.

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
import re
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
DEFAULT_PAGE = os.path.join("docs", "FREEZE.md")
DEFAULT_SEED = os.path.join("platform", "dbt", "seeds", "frozen_capture_seed.csv")

MANIFEST_NAME = "_manifest.json"
DATE_IN_KEY = re.compile(r"ingestion_date=(\d{4}-\d{2}-\d{2})")
FIELDS = ("source", "partition_key", "files", "records", "bytes", "content_sha256")


class FreezeError(RuntimeError):
    """Failure sealing or checking the capture."""


def _content_sha256(manifest: dict) -> str:
    """sha256 of the SORTED list of (path, sha256, bytes, records) declared in the manifest.

    Sorted because the order in which the Source lists the files is an implementation
    detail — two runs over the same data can enumerate differently, and the seal cannot
    depend on that. Canonical via JSON with fixed separators: `json.dumps` with variable
    spacing would change the hash without a single byte of data having changed.
    """
    entries = sorted(
        (str(f.get("path")), str(f.get("sha256")), int(f.get("bytes") or 0),
         int(f.get("records") or 0))
        for f in manifest.get("files") or []
    )
    body = json.dumps(entries, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def collect(config=None) -> list[dict]:
    """Walks RAW and returns one row per sealed partition, in stable order."""
    from .config import from_env

    config = config or from_env()
    client = config.client()
    paginator = client.get_paginator("list_objects_v2")

    records: list[dict] = []
    for page in paginator.paginate(Bucket=config.raw_bucket):
        for item in page.get("Contents") or []:
            key = item["Key"]
            if not key.endswith("/" + MANIFEST_NAME):
                continue
            body = client.get_object(Bucket=config.raw_bucket, Key=key)["Body"].read()
            manifest = json.loads(body)
            prefix = key[: -len("/" + MANIFEST_NAME)]
            source, _, partition = prefix.partition("/")
            files = manifest.get("files") or []
            records.append({
                "source": source,
                "partition_key": partition,
                "files": len(files),
                "records": sum(int(f.get("records") or 0) for f in files),
                "bytes": sum(int(f.get("bytes") or 0) for f in files),
                "content_sha256": _content_sha256(manifest),
            })
    records.sort(key=lambda r: (r["source"], r["partition_key"]))
    if not records:
        raise FreezeError(
            f"no _manifest.json under s3://{config.raw_bucket}/. Nothing has landed — "
            "run `make daily` (or the land targets) before sealing."
        )
    return records


def capture_id(records: list[dict]) -> str:
    """Aggregate sha256 over the partition seals. ONE number names the whole capture.

    It exists so that a conversation about "the 4,939 products" can say WHICH capture
    those 4,939 belong to, without repeating an 81-row table.
    """
    body = json.dumps(
        [[r["source"], r["partition_key"], r["content_sha256"]] for r in records],
        separators=(",", ":"), ensure_ascii=True,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def escrever_seed(records: list[dict], target: str = DEFAULT_SEED) -> str:
    path = target if os.path.isabs(target) else os.path.join(REPO, target)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows({c: r[c] for c in FIELDS} for r in records)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        fh.write(buffer.getvalue())
    os.replace(tmp_path, path)
    return path


def ler_seed(origin: str = DEFAULT_SEED) -> list[dict]:
    path = origin if os.path.isabs(origin) else os.path.join(REPO, origin)
    if not os.path.exists(path):
        raise FreezeError(
            f"{origin} does not exist: the capture was never sealed. Run `make freeze`."
        )
    with open(path, encoding="utf-8") as fh:
        return [
            {"source": l["source"], "partition_key": l["partition_key"],
             "files": int(l["files"]), "records": int(l["records"]),
             "bytes": int(l["bytes"]), "content_sha256": l["content_sha256"]}
            for l in csv.DictReader(fh)
        ]


def _key_date(partition_key: str) -> str | None:
    match = DATE_IN_KEY.search(partition_key)
    return match.group(1) if match else None


def janela(sealed: list[dict]) -> dict[str, str]:
    """The sealed window per source: its latest sealed `ingestion_date`."""
    window: dict[str, str] = {}
    for r in sealed:
        day = _key_date(r["partition_key"])
        if day and day > window.get(r["source"], ""):
            window[r["source"]] = day
    return window


def _grew(key: tuple[str, str], window: dict[str, str]) -> bool:
    day = _key_date(key[1])
    return key[0] in window and day is not None and day > window[key[0]]


def comparar(sealed: list[dict], current: list[dict]) -> list[str]:
    """What moved the sealed capture. Empty list = intact (it may still have GROWN —
    see `crescimento`)."""
    by_key = lambda items: {(r["source"], r["partition_key"]): r for r in items}  # noqa: E731
    a, b = by_key(sealed), by_key(current)
    window = janela(sealed)
    problems: list[str] = []
    for key in sorted(set(a) - set(b)):
        problems.append(f"partition SEALED but MISSING from RAW: {key[0]}/{key[1]}")
    for key in sorted(set(b) - set(a)):
        if key[0] not in window or _key_date(key[1]) is None:
            problems.append(f"NEW partition with no sealed window to grow: {key[0]}/{key[1]}")
        elif not _grew(key, window):
            problems.append(f"NEW partition INSIDE the sealed window: {key[0]}/{key[1]}")
    for key in sorted(set(a) & set(b)):
        if a[key]["content_sha256"] != b[key]["content_sha256"]:
            problems.append(
                f"content CHANGED in {key[0]}/{key[1]}: "
                f"sealed={a[key]['content_sha256'][:16]}... "
                f"current={b[key]['content_sha256'][:16]}..."
            )
    return problems


def crescimento(sealed: list[dict], current: list[dict]) -> list[str]:
    """New partitions dated AFTER their source's sealed window: growth, not a change."""
    sealed_keys = {(r["source"], r["partition_key"]) for r in sealed}
    window = janela(sealed)
    return [f"{r['source']}/{r['partition_key']}" for r in current
            if (r["source"], r["partition_key"]) not in sealed_keys
            and _grew((r["source"], r["partition_key"]), window)]


def render(records: list[dict]) -> str:
    identity = capture_id(records)
    by_source: dict[str, list[dict]] = {}
    for r in records:
        by_source.setdefault(r["source"], []).append(r)

    lines = [
        "# The sealed capture",
        "",
        "**Generated by `make freeze`.** Do not edit by hand.",
        "",
        f"**`capture_id` = `{identity}`**",
        "",
        f"Sealed on {datetime.now(timezone.utc).strftime('%Y-%m-%d')} · "
        f"{len(records)} partitions · "
        f"{sum(r['records'] for r in records):,} records · "
        f"{sum(r['bytes'] for r in records) / (1024**3):.2f} GB",
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
        "make freeze-check   # rereads RAW and compares it with this seal; exits 1 if it moved",
        "```",
        "",
        "Changing a single byte of a sealed partition fails it, and so does a missing one or a",
        "new partition dated INSIDE the sealed window — a late landing would invalidate every",
        "number already published about it. A partition dated AFTER the window (the daily",
        "schedule's next collection) is reported as growth and does not fail it (CR-020).",
        "",
        "## Summary by source",
        "",
        "| Source | Partitions | Records | GB |",
        "|---|---|---|---|",
    ]
    for source in sorted(by_source):
        group = by_source[source]
        lines.append(
            f"| `{source}` | {len(group)} | "
            + f"{sum(r['records'] for r in group):,}"
            + f" | {sum(r['bytes'] for r in group) / (1024**3):.3f} |"
        )
    lines.append(
        f"| **total** | **{len(records)}** | "
        + f"**{sum(r['records'] for r in records):,}**"
        + f" | **{sum(r['bytes'] for r in records) / (1024**3):.2f}** |"
    )
    # The per-partition detail is not a table here: with the window growing, it was hundreds
    # of rows nobody reads, duplicating the seed. The seed is what freeze-check compares.
    lines += [
        "",
        f"The full per-partition breakdown ({len(records)} rows: partition key, file count, records,",
        "bytes, `content_sha256`) is what `make freeze-check` actually compares against a",
        "re-read of RAW. It is written machine-readable to",
        "`platform/dbt/seeds/frozen_capture_seed.csv` rather than duplicated here as a table",
        "for human reading.",
    ]
    return "\n".join(lines) + "\n"


def escrever_pagina(records: list[dict], target: str = DEFAULT_PAGE) -> str:
    path = target if os.path.isabs(target) else os.path.join(REPO, target)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        fh.write(render(records))
    os.replace(tmp_path, path)
    return path
