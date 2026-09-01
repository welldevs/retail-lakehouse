"""Evidencia do Spark — INCLUSIVE a medicao que joga contra ele.

POR QUE ESTA PAGINA EXISTE, e por que ela publica um numero desfavoravel de proposito.

O ARCHITECTURE afirma que o Spark NAO foi adotado por desempenho. Uma afirmacao dessas, sem
medicao, e modestia retorica — a mesma doenca do numero copiado a mao, so que com o sinal
trocado. O documento de restricoes deste projeto proibe "alegar performance sem benchmark", e
a obrigacao e simetrica: alegar AUSENCIA de performance tambem exige medir.

Entao esta pagina roda o MESMO job de estoque nas duas implementacoes — Spark e Python puro —
sobre a MESMA entrada, e publica os dois tempos lado a lado, seja qual for o vencedor.

AS TRES COISAS QUE ELA REGISTRA, e nenhuma e "o Spark e rapido":

  1. INTEROP — quantos escritores distintos o catalogo Iceberg tem hoje, e quais. Era a
     propriedade que justificou o Iceberg desde a Fase 3 e que estava afirmada e nunca
     demonstrada, porque os dois escritores eram Python.
  2. FORMA — o resultado das duas implementacoes tem de ser IDENTICO. Se divergirem, a
     comparacao de tempo nao significa nada, porque medem coisas diferentes.
  3. TEMPO — os dois, com o overhead da JVM incluido e declarado.

E ela registra o GATILHO QUE NAO DISPAROU: o self-join de cesta, o candidato natural a
"volume que exige Spark", medido no DuckDB.

O QUE ESTE MODULO NAO FAZ: nao valida, nao conserta e nao decide. Ele observa e escreve. Se o
Spark nao estiver disponivel, a secao diz isso — evidencia parcial e util, evidencia
inventada nao e.
"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timezone

DEFAULT_OUT = os.path.join("docs", "spark-evidence", "README.md")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _tabela(colunas: list[str], linhas: list) -> list[str]:
    return [
        "| " + " | ".join(colunas) + " |",
        "|" + "|".join("---" for _ in colunas) + "|",
        *["| " + " | ".join("" if v is None else str(v) for v in linha) + " |"
          for linha in linhas],
    ]


# ======================================================================================
# 1. INTEROP: quem escreve no catalogo
# ======================================================================================

def _observar_catalogo(config=None) -> dict:
    """Quais tabelas o catalogo tem, e QUEM escreveu cada uma.

    `written_by` e o que torna "tres escritores" um fato consultavel em vez de uma frase de
    documentacao. Sem essa coluna, a interop seria afirmada de novo.
    """
    from .orders_projection import TABLE_NAME as PROJECAO
    from .stock_ledger import CONSUMPTION_TABLE, LEDGER_TABLE, catalog

    saida = {"tabelas": [], "escritores": {}}
    cat = catalog(config)
    for nome in (PROJECAO, CONSUMPTION_TABLE, LEDGER_TABLE):
        if not cat.table_exists(nome):
            saida["tabelas"].append({"nome": nome, "erro": "nao existe no catalogo"})
            continue
        tabela = cat.load_table(nome)
        arrow = tabela.scan().to_arrow()
        escritores = {}
        if "written_by" in arrow.column_names:
            for valor in arrow.column("written_by").to_pylist():
                escritores[valor] = escritores.get(valor, 0) + 1
                saida["escritores"][valor] = saida["escritores"].get(valor, 0) + 1
        saida["tabelas"].append({
            "nome": nome,
            "linhas": arrow.num_rows,
            "snapshots": len(list(tabela.metadata.snapshots)),
            "escritores": escritores,
            "metadata_location": tabela.metadata_location,
        })
    return saida


# ======================================================================================
# 2 e 3. A MESMA CONTA, NOS DOIS MOTORES
# ======================================================================================

def _ledger_em_python(config=None) -> dict:
    """O MESMO laco do job Spark, em Python puro sobre a mesma tabela Iceberg.

    NAO E UMA REIMPLEMENTACAO APROXIMADA — e o mesmo algoritmo: a funcao do `applyInPandas`
    e importada de `jobs/spark/stock_ledger.py`, entao nao ha como as duas divergirem por um
    detalhe de traducao. O que muda e SO quem itera sobre os grupos: aqui um `for` num
    processo, la o Spark distribuindo. Se fossem duas implementacoes diferentes, a comparacao
    de tempo mediria a habilidade de quem escreveu cada uma.
    """
    import sys

    import pandas as pd

    if REPO not in sys.path:
        sys.path.insert(0, REPO)
    from jobs.spark.stock_ledger import construir_ledger, ler_premissas  # noqa: E402

    from .stock_ledger import CONSUMPTION_TABLE, catalog

    premissas = ler_premissas(os.path.join(
        REPO, "platform", "dbt", "seeds", "stock_premises_seed.csv"))
    ledger = construir_ledger(premissas)

    inicio = time.time()
    consumo = catalog(config).load_table(CONSUMPTION_TABLE).scan().to_arrow().to_pandas()
    dias = pd.date_range(consumo["stock_date"].min(), consumo["stock_date"].max(), freq="D")
    n_dias = len(dias)

    # A MESMA DENSIFICACAO do job: uma linha por serie por dia, inclusive sem venda.
    series = consumo.groupby(["wh", "source_product_id"], as_index=False)["units_consumed"].sum()
    series["mean_daily_demand"] = series["units_consumed"] / float(n_dias)
    grade = series.merge(pd.DataFrame({"stock_date": dias.date}), how="cross")
    entrada = grade.merge(
        consumo, on=["wh", "source_product_id", "stock_date"], how="left",
        suffixes=("_total", ""))
    entrada["units_demanded"] = entrada["units_consumed"].fillna(0).astype("int64")

    linhas = []
    for chave, grupo in entrada.groupby(["wh", "source_product_id"], sort=False):
        linhas.append(ledger(chave, grupo))
    resultado = pd.concat(linhas, ignore_index=True)
    decorrido = time.time() - inicio

    return {
        "segundos": round(decorrido, 1),
        "linhas": int(len(resultado)),
        "series": int(len(series)),
        "demanda": int(resultado["units_demanded"].sum()),
        "atendido": int(resultado["units_fulfilled"].sum()),
        "ruptura": int(resultado["units_short"].sum()),
        "ordens": int((resultado["reorder_units"] > 0).sum()),
        "chegadas": int((resultado["units_received"] > 0).sum()),
    }


def _ledger_em_spark() -> dict:
    """Roda o job de verdade, pelo mesmo alvo que a operacao usaria, e le o que ele imprime.

    O TEMPO INCLUI A SUBIDA DA JVM e o `docker compose run`, e isso e declarado na pagina em
    vez de descontado. Descontar o custo de subir o motor seria medir um Spark que nao existe
    — quem roda o job paga esse custo.
    """
    comando = ["docker", "compose", "--env-file", ".env", "-f", "infra/docker-compose.yml",
               "--profile", "spark", "run", "--rm", "--no-deps", "spark",
               "python3", "jobs/spark/stock_ledger.py"]
    inicio = time.time()
    proc = subprocess.run(comando, cwd=REPO, capture_output=True, text=True)
    decorrido = time.time() - inicio
    if proc.returncode != 0:
        return {"erro": (proc.stdout + proc.stderr)[-400:]}

    metricas = {"segundos_com_jvm": round(decorrido, 1)}
    for linha in proc.stdout.splitlines():
        if "." in linha and " " in linha:
            chave, _, valor = linha.partition(" ")
            chave = chave.rstrip(".")
            if chave in ("linhas", "series", "demanda", "atendido", "ruptura",
                         "ordens", "chegadas", "segundos", "dias_com_ruptura",
                         "unidades_pedidas", "cobertura_media"):
                try:
                    metricas[chave] = float(valor) if "." in valor else int(valor)
                except ValueError:
                    pass
    return metricas


# ======================================================================================
# O GATILHO QUE NAO DISPAROU
# ======================================================================================

SELF_JOIN = """
    with cesta as (
        select order_id, fulfilled_source_product_id as produto
        from silver_order_line
        where line_status in ('fulfilled', 'substituted')
          and fulfilled_source_product_id is not null
    )
    select count(*) as pares, count(distinct a.produto || '|' || b.produto) as distintos
    from cesta a
    join cesta b on a.order_id = b.order_id and a.produto < b.produto
"""


def _observar_nao_gatilho(config=None) -> dict:
    """O self-join de cesta no DuckDB: o candidato natural a "volume que exige Spark".

    Ele esta aqui porque a decisao de adotar Spark tem de vir acompanhada da medicao que
    NAO a sustenta. Se um dia este numero virar minutos e gigabytes, o gatilho de volume
    tera disparado — e ai a justificativa do Spark muda, o que tambem e informacao.
    """
    import resource

    from .config import from_env
    from .query import connect_lakehouse

    config = config or from_env()
    conexao = connect_lakehouse(config)
    antes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    inicio = time.time()
    pares, distintos = conexao.execute(SELF_JOIN).fetchone()
    decorrido = time.time() - inicio
    depois = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "pares": pares,
        "distintos": distintos,
        "segundos": round(decorrido, 2),
        "pico_gb": round(depois / (1024 * 1024), 2),
        "delta_gb": round(max(depois - antes, 0) / (1024 * 1024), 2),
    }


# ======================================================================================
# coleta e render
# ======================================================================================

def collect(config=None, *, rodar_spark: bool = True) -> dict:
    dados = {"gerado_em": _utc_now()}
    for nome, funcao in (
        ("catalogo", lambda: _observar_catalogo(config)),
        ("nao_gatilho", lambda: _observar_nao_gatilho(config)),
        ("python", lambda: _ledger_em_python(config)),
    ):
        try:
            dados[nome] = funcao()
        except Exception as exc:  # a pagina declara a ausencia; nunca inventa o numero
            dados[nome] = {"erro": f"{type(exc).__name__}: {str(exc)[:300]}"}
    dados["spark"] = _ledger_em_spark() if rodar_spark else {"erro": "nao executado"}
    return dados


def _ausente(titulo: str, erro: str) -> list[str]:
    return [f"### {titulo}", "",
            f"**Não observado nesta execução.** `{erro}`", "",
            "A seção declara a ausência em vez de sumir: uma página que esconde o que não",
            "conseguiu medir é indistinguível de uma que mediu e não gostou.", ""]


def render(dados: dict) -> str:
    linhas = [
        "# Evidência do Spark — e a medição que joga contra ele",
        "",
        f"**Gerado por `make spark-evidence` em {dados['gerado_em']}.** Não editar à mão.",
        "",
        "Esta página existe para sustentar uma afirmação **negativa**: o Spark não foi",
        "adotado por desempenho. Dizer isso sem medir seria modéstia retórica — a mesma",
        "doença do número copiado à mão, com o sinal trocado. Então o mesmo job roda nos",
        "dois motores, sobre a mesma entrada, e os dois tempos ficam publicados.",
        "",
        "## Por que o Spark está neste projeto",
        "",
        "Duas razões, e desempenho não é nenhuma delas.",
        "",
        "**1. É o terceiro escritor do catálogo Iceberg, e o primeiro fora do Python.** O",
        "Iceberg foi justificado por *interop entre engines* desde a Fase 3, e essa metade da",
        "justificativa estava **afirmada e nunca demonstrada**: os dois escritores eram",
        "Python usando a mesma biblioteca. `make spike-spark-iceberg` foi o portão que testou",
        "isso antes de qualquer linha desta fase existir — com os dois desfechos declarados",
        "de antemão, incluindo o de apagar a cláusula de interop se ela não se sustentasse.",
        "",
        "**2. A forma do job não é SQL.** O saldo de estoque é uma soma corrida cujas",
        "*entradas são geradas por decisões tomadas a partir do próprio estado*: o saldo cai",
        "abaixo do ponto, uma ordem é emitida, ela chega dias depois e muda o saldo seguinte,",
        "que decide se há nova ordem. Window function lê a partition inteira mas não escreve",
        "de volta nela.",
        "",
    ]

    # ---- interop ---------------------------------------------------------------------
    catalogo = dados.get("catalogo", {})
    if "erro" in catalogo:
        linhas += _ausente("Escritores do catálogo", catalogo["erro"])
    else:
        linhas += [
            "## Os escritores do catálogo, hoje",
            "",
            "`written_by` é o que torna \"três escritores\" um **fato consultável** em vez de",
            "uma frase de documentação.",
            "",
        ]
        linhas += _tabela(
            ["Tabela", "Linhas", "Snapshots", "Escritores"],
            [[t["nome"], t.get("linhas", "—"), t.get("snapshots", "—"),
              ", ".join(f"`{k}` ({v})" for k, v in sorted(t.get("escritores", {}).items()))
              or t.get("erro", "—")]
             for t in catalogo["tabelas"]],
        )
        distintos = sorted(catalogo.get("escritores", {}))
        # A FRASE E DERIVADA DA LISTA, e nao cravada. A primeira versao dizia "`stream` e
        # `rebuild` sao Python" — e os escritores presentes eram `platform` e `rebuild`. Um
        # texto fixo ao lado de uma tabela gerada e a mesma doenca do numero copiado a mao,
        # numa pagina que existe justamente para nao ter nenhum.
        jvm = [e for e in distintos if e == "spark"]
        python = [e for e in distintos if e != "spark"]
        linhas += ["",
                   f"**Escritores distintos no catálogo: {len(distintos)}** — "
                   + ", ".join(f"`{e}`" for e in distintos) + ".",
                   "",
                   ", ".join(f"`{e}`" for e in python)
                   + (" é Python" if len(python) == 1 else " são Python")
                   + (f"; {', '.join(f'`{e}`' for e in jvm)} é a JVM." if jvm
                      else ". **Nenhum escritor fora do Python** — a interop continua "
                           "afirmada e não demonstrada."),
                   "",
                   "A propriedade que justificou o Iceberg desde a Fase 3 era *interop entre",
                   "engines*, e ela só deixa de ser afirmação quando esta lista tem um nome",
                   "que não é Python.",
                   ""]

    # ---- os dois motores --------------------------------------------------------------
    py, sp = dados.get("python", {}), dados.get("spark", {})
    linhas += ["## O mesmo job, nos dois motores", "",
               "A função do laço é **importada** de `jobs/spark/stock_ledger.py` pelos dois",
               "caminhos — não é uma reimplementação aproximada. O que muda é só quem itera",
               "sobre os grupos: um `for` num processo, ou o Spark distribuindo. Se fossem",
               "duas implementações diferentes, a comparação mediria a habilidade de quem",
               "escreveu cada uma.", ""]
    if "erro" in py or "erro" in sp:
        linhas += _ausente("Comparação", py.get("erro") or sp.get("erro"))
    else:
        iguais = all(py.get(k) == sp.get(k)
                     for k in ("linhas", "series", "demanda", "atendido", "ruptura",
                               "ordens", "chegadas"))
        linhas += _tabela(
            ["", "Python puro", "Spark"],
            [["linhas", py.get("linhas"), sp.get("linhas")],
             ["séries", py.get("series"), sp.get("series")],
             ["demanda", py.get("demanda"), sp.get("demanda")],
             ["atendido", py.get("atendido"), sp.get("atendido")],
             ["ruptura", py.get("ruptura"), sp.get("ruptura")],
             ["ordens emitidas", py.get("ordens"), sp.get("ordens")],
             ["chegadas", py.get("chegadas"), sp.get("chegadas")],
             ["**segundos**", f"**{py.get('segundos')}**",
              f"**{sp.get('segundos')}** (job) · "
              f"{sp.get('segundos_com_jvm')} com a JVM e o container"]],
        )
        linhas += ["",
                   f"**Os dois resultados são {'IDÊNTICOS' if iguais else 'DIFERENTES'}.** "
                   + ("Sem isso a comparação de tempo não significaria nada, porque os dois "
                      "mediriam coisas diferentes."
                      if iguais else
                      "**Isso é um defeito**: enquanto divergirem, nenhum dos dois tempos é "
                      "comparável, e a divergência é o que precisa ser investigado antes de "
                      "qualquer leitura desta página."),
                   "",
                   "O tempo do Spark **inclui a subida da JVM e o `docker compose run`**, e",
                   "isso não é descontado de propósito: quem roda o job paga esse custo. A",
                   "coluna \"job\" é o que o próprio job cronometra, para que a diferença",
                   "entre as duas fique visível em vez de escondida numa nota de rodapé.",
                   ""]
        if py.get("segundos") and sp.get("segundos"):
            mais_rapido = "Python puro" if py["segundos"] < sp["segundos"] else "Spark"
            razao = max(py["segundos"], sp["segundos"]) / max(
                min(py["segundos"], sp["segundos"]), 0.1)
            linhas += [
                f"**Neste volume, {mais_rapido} é ~{razao:.1f}x mais rápido.** Se o vencedor",
                "for o Python — que é o esperado nesta escala — o número fica publicado do",
                "mesmo jeito. Ele é a prova de que o Spark não está aqui por velocidade, e",
                "uma página que só publicasse resultados favoráveis não provaria nada.",
                "",
            ]

    # ---- o gatilho que nao disparou ----------------------------------------------------
    ng = dados.get("nao_gatilho", {})
    linhas += ["## O gatilho de volume, que NÃO disparou", ""]
    if "erro" in ng:
        linhas += _ausente("Self-join de cesta", ng["erro"])
    else:
        linhas += [
            "O candidato natural a \"volume que exige Spark\" neste projeto é o self-join de",
            "cesta — todo par de produtos comprados juntos, que é a base de qualquer análise",
            "de afinidade. Medido no DuckDB, num nó:",
            "",
        ]
        linhas += _tabela(
            ["Pares", "Pares distintos", "Segundos", "Pico de RSS (GB)"],
            [[f"{ng['pares']:,}".replace(",", "."),
              f"{ng['distintos']:,}".replace(",", "."),
              ng["segundos"], ng["pico_gb"]]],
        )
        linhas += ["",
                   "**O gatilho de volume não disparou, e está medido.** O ARCHITECTURE",
                   "declara o gatilho do Spark como *\"partição que o DuckDB não segura em",
                   "memória\"*; este número é o que diz que ele continua fechado. Se um dia",
                   "virar minutos e dezenas de gigabytes, a justificativa do Spark muda — e",
                   "isso também é informação.",
                   ""]

    linhas += [
        "## O que esta página NÃO prova",
        "",
        "- **Que o Spark escala aqui.** Ele roda `local[*]`: driver e executor no mesmo JVM.",
        "  Não há shuffle entre nós, não há cluster, e um cluster de mentira não provaria",
        "  nem escala nem interoperabilidade.",
        "- **Que o job precisa de Spark hoje.** Precisa de um motor que expresse",
        "  realimentação por série; o Python puro também expressa. O que o Spark acrescenta",
        "  é ser o escritor fora do Python e paralelizar por série quando as séries crescerem.",
        "- **Que o estoque é real.** O saldo é calculado a partir do consumo observado mais",
        "  uma política declarada em seed. Nenhuma fonte deste repositório mede estoque.",
        "",
    ]
    return "\n".join(linhas) + "\n"


def write(dados: dict, out: str = DEFAULT_OUT) -> str:
    destino = os.path.join(REPO, out) if not os.path.isabs(out) else out
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    temporario = destino + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(render(dados))
    os.replace(temporario, destino)
    return destino
