#!/usr/bin/env python3
"""Runs the price-watch panel end to end against the live account and demands zero exception.

    make price-dashboard-check

Same reasoning as `smoke.py`: `curl` on a port doesn't prove anything here either — Streamlit
serves the page skeleton with HTTP 200 even when the script dies on the first `select`. This
uses `AppTest`, exactly like the operations dashboard's own check, against `price_app.py`
instead of `app.py`.

MINIMOS below were MEASURED on a fresh run with no product search typed (the default state):
the Product tab shows an `st.info()` and no table until someone types something, so its
dataframe does not count here — recalibrate if that tab's default behavior changes.
"""

from __future__ import annotations

import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP = os.path.join(RAIZ, "streamlit", "price_app.py")

MINIMOS = {"metric": 6, "tabs": 6, "dataframe": 6, "warning": 0}


def main() -> int:
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        print("ERRO: streamlit nao esta instalado. Rode `make dashboard-venv`.")
        return 1

    tempo = int(os.environ.get("RETAIL_DASHBOARD_SMOKE_TIMEOUT", "600"))
    app = AppTest.from_file(APP, default_timeout=tempo).run()

    problemas = []
    if app.exception:
        for excecao in app.exception:
            print(f"EXCECAO: {str(excecao.value)[:300]}")
        problemas.append(f"{len(app.exception)} excecao(oes) ao renderizar")

    contagens = {
        "metric": len(app.metric), "tabs": len(app.tabs),
        "dataframe": len(app.dataframe), "warning": len(app.warning),
    }
    for elemento, minimo in MINIMOS.items():
        obtido = contagens[elemento]
        estado = "ok" if obtido >= minimo else f"ABAIXO DO MINIMO ({minimo})"
        print(f"{elemento:.<14} {obtido:>3}  {estado}")
        if obtido < minimo:
            problemas.append(f"{elemento}: {obtido} < {minimo}")

    print()
    if app.metric:
        print("metricas do topo:")
        for metrica in app.metric[:6]:
            print(f"  {metrica.label:.<28} {metrica.value}")
        print()

    if problemas:
        print(f"REPROVADO — {len(problemas)} problema(s):")
        for problema in problemas:
            print(f"  - {problema}")
        return 1
    print(f"APROVADO: o painel renderizou sem excecao, com {contagens['tabs']} abas e "
          f"{contagens['dataframe']} tabelas lidas do MART.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
