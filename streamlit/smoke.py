#!/usr/bin/env python3
"""Runs a dashboard end to end against the live warehouse and demands zero exceptions.

    make dashboard-check          # both panels

WHY SEPARATE FROM `make test`. The offline suite checks what can be checked without a
network: catalog structure, bound parameters, and that docs/indicators/ matches the catalog.
What it cannot check is whether the queries RUN — that needs a live warehouse, and `make test`
staying offline is an invariant of this repository.

This closes that half honestly: it runs the real Streamlit script with `AppTest` and fails if
any indicator raises or the screen comes out empty. `curl` on `/health` would not do —
Streamlit serves the page skeleton with HTTP 200 even when the script dies on the first
`select`, because rendering happens in the client.

It also runs `connection.probe_isolation` once: the panel's role must read MART and be
REFUSED on GOLD and STAGE. A panel that claims a boundary without demonstrating it is asking
for trust.

MINIMUMS are proof that the screen rendered, not business numbers; pinning exact values would
break on every new indicator. `warning: 0` is by design: a technical caveat leaking back onto
a production panel shows up as an `st.warning`. The prices panel's Product tab shows no table
until someone searches, so it does not count toward its dataframe minimum.
"""

from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
PANELS = {
    "operations": ("operations_app.py", {"metric": 10, "tabs": 7, "dataframe": 16, "warning": 0}),
    "prices": ("prices_app.py", {"metric": 6, "tabs": 6, "dataframe": 6, "warning": 0}),
}


def check(name: str) -> list[str]:
    from streamlit.testing.v1 import AppTest

    script, minimums = PANELS[name]
    timeout = int(os.environ.get("RETAIL_DASHBOARD_SMOKE_TIMEOUT", "600"))
    app = AppTest.from_file(os.path.join(ROOT, script), default_timeout=timeout).run()

    problems = []
    for exc in app.exception:
        print(f"EXCEPTION: {str(exc.value)[:300]}")
    if app.exception:
        problems.append(f"{len(app.exception)} exception(s) while rendering")
    # A column the panel forgot to rename reaches the reader as the catalog's alias
    # (PRODUTOS_NO_DIA, MOEDA): Portuguese, upper case, and meaningless to a business user.
    raw = sorted({str(column) for frame in app.dataframe
                  for column in getattr(frame.value, "columns", [])
                  if re.fullmatch(r"[A-Z][A-Z0-9_]{2,}", str(column))})
    if raw:
        print(f"RAW COLUMN NAMES ON SCREEN: {', '.join(raw)}")
        problems.append(f"{name}: {len(raw)} column(s) not renamed")
    counts = {"metric": len(app.metric), "tabs": len(app.tabs),
              "dataframe": len(app.dataframe), "warning": len(app.warning)}
    print(f"== {name} ({script})")
    for element, minimum in minimums.items():
        got = counts[element]
        state = "ok" if got >= minimum else f"BELOW MINIMUM ({minimum})"
        if element == "warning":
            state = "ok" if got == 0 else "MUST BE ZERO"
        print(f"  {element:.<14} {got:>3}  {state}")
        if (element == "warning" and got) or (element != "warning" and got < minimum):
            problems.append(f"{name}: {element} = {got}")
    return problems


def check_isolation() -> list[str]:
    sys.path.insert(0, ROOT)
    from connection import open_session, probe_isolation

    conn = open_session()
    try:
        results = probe_isolation(conn)
    finally:
        conn.close()
    print("== isolation (panel role)")
    for target, ok, detail in results:
        print(f"  {target:.<28} {'ok' if ok else 'VIOLATION'}  {detail}")
    return [f"isolation: {target} — {detail}" for target, ok, detail in results if not ok]


def main(argv: list[str]) -> int:
    try:
        import streamlit.testing.v1  # noqa: F401
    except ImportError:
        print("ERROR: streamlit is not installed. Run `make dashboard-venv`.")
        return 1
    names = argv or list(PANELS)
    problems = [p for name in names for p in check(name)]
    problems += check_isolation()
    if problems:
        print(f"FAILED — {len(problems)} problem(s):")
        print("\n".join(f"  - {p}" for p in problems))
        return 1
    print(f"PASSED: {', '.join(names)} rendered with no exception, reading only MART.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
