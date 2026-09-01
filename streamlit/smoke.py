#!/usr/bin/env python3
"""Executa o painel de ponta a ponta contra a conta viva e exige zero excecao.

    make dashboard-check

POR QUE SEPARADO DE `make test`. A suite offline (`test_dashboard_indicators.py`) confere o
que da para conferir sem rede: estrutura, parametro ligado, e a sincronia entre o CONTRACT e
`indicators.py`. O que ela NAO pode conferir e se as 19 consultas rodam — isso exige conta, e
`make test` sem rede e invariante do repositorio.

Este script fecha a metade que falta, e fecha do jeito honesto: roda o script do Streamlit de
verdade, com `AppTest`, e reprova se qualquer indicador levantar excecao ou se a tela sair
vazia. `curl` num `/health` nao serve — o Streamlit serve o esqueleto com HTTP 200 mesmo
quando o script morre no primeiro `select`, porque a renderizacao e no cliente. Foi por isso
que este arquivo existe em vez de uma checagem de porta.
"""

from __future__ import annotations

import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP = os.path.join(RAIZ, "streamlit", "app.py")

# Minimos exigidos. Nao sao numeros de negocio — sao a prova de que a tela renderizou em vez
# de morrer no meio. Cravar o valor exato quebraria a cada indicador novo, sem ganho.
MINIMOS = {"metric": 5, "tabs": 7, "dataframe": 8, "warning": 4}


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
        for metrica in app.metric[:5]:
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
