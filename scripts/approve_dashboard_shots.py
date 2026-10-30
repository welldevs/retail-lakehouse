#!/usr/bin/env python3
"""Record that every dashboard screenshot was READ before it is published.

    make dashboard-shots-approve NOTE="what you checked"

Approving is a statement, not a formality: run it only after opening every PNG listed in the
manifest and reading it like a business user (CR-018). It refuses when a listed file is
missing or a file in the folder is not listed, so a stale shot cannot ride along.
"""

from __future__ import annotations

import datetime
import json
import pathlib
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 2 or not argv[1].strip():
        print('usage: approve_dashboard_shots.py <dir> "<what you checked>"')
        return 2
    folder, note = pathlib.Path(argv[0]), argv[1].strip()
    manifest_path = folder / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    listed = {entry["file"] for entry in manifest["files"]}
    present = {p.name for p in folder.glob("*.png")}
    if listed != present:
        print(f"REFUSED: missing {sorted(listed - present)}, unlisted {sorted(present - listed)}")
        return 1
    manifest["reviewed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")
    manifest["reviewed_note"] = note
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"approved {len(listed)} screenshots of capture {manifest['capture_id'][:8]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
