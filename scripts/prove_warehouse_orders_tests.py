#!/usr/bin/env python3
"""Prova que os testes do warehouse de Orders sabem REPROVAR.

    make warehouse-prove-tests

POR QUE ESTE SCRIPT EXISTE. "Verificação que nunca falhou não é verificação" é a regra do
repo, e o Marco 7 acabou de mostrar por quê: a primeira carga do STAGE colocou TODO
timestamp no ano 56.648.666, e os 166 nós do dbt construíram em VERDE. Contagem de linhas
batia, tipo nenhum mudava, grão continuava único, e um funil feito de `count_if(marco is
not null)` continua exato quando o instante está 56 milhões de anos deslocado. O defeito foi
achado por um humano lendo 80.000.060 minutos de separação num mart.

Um teste verde só significa alguma coisa depois de se ter visto ele vermelho. Este script
injeta, no dado REAL do warehouse, o defeito específico que cada teste diz pegar; exige que
o teste reprove; desfaz; e exige que ele volte a passar. Os cinco são exercidos no mesmo
dado que a operação usa — não numa fixture que já concorda com a expectativa.

SEGURO POR CONSTRUÇÃO: tudo que este script edita vive em GOLD e MART, que são
INTEIRAMENTE reconstruíveis por `dbt build` a partir do STAGE. Nada aqui toca o STAGE, o
RAW ou o Lakehouse. Ainda assim, cada injeção é desfeita pelo seu próprio par de comandos e
o script termina reconstruindo o que tocou — e uma sentinela confere, no fim, que as
contagens voltaram ao que eram.
"""

from __future__ import annotations

import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "platform", "src"))

from retail_platform.snowflake_load import connect, DEFAULT_DATABASE  # noqa: E402

DBT = os.path.join("platform", ".venv", "bin", "dbt")
PROJECT = ["--project-dir", "platform/dbt", "--profiles-dir", "platform/dbt",
           "--target", "snowflake"]

DB = os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_DATABASE)
GOLD = f"{DB}.GOLD"
MART = f"{DB}.MART"

# Cada caso é (teste, o defeito que ele diz pegar, injeção, reparo). A injeção é sempre a
# MENOR mudança possível — uma linha, uma coluna — porque um teste que só reprova diante de
# estrago grosseiro não protege de nada.
CASOS = [
    {
        "teste": "assert_every_order_has_a_customer_version",
        "pega": "o SCD2 resolvido para outra versão que não a vigente na data do pedido",
        "injetar": [
            f"update {GOLD}.FACT_ORDER set customer_version_from = dateadd(day, -30, customer_version_from) "
            f"where order_id = 'ord_mad1_20260827_000010'",
        ],
        "reparar": [
            f"update {GOLD}.FACT_ORDER set customer_version_from = customer_ingestion_date "
            f"where order_id = 'ord_mad1_20260827_000010'",
        ],
    },
    {
        "teste": "assert_fact_order_amount_equals_sum_of_items",
        "pega": "fanout numa das duas junções com DIM_PRODUCT, ou net_amount preenchido "
                "para pedido que nunca foi separado",
        "injetar": [
            f"update {GOLD}.FACT_ORDER set net_amount = net_amount + 0.01 "
            f"where order_id = 'ord_bcn1_20260825_000005' and net_amount is not null",
        ],
        "reparar": [
            f"update {GOLD}.FACT_ORDER set net_amount = net_amount - 0.01 "
            f"where order_id = 'ord_bcn1_20260825_000005' and net_amount is not null",
        ],
    },
    {
        "teste": "assert_order_item_price_matches_price_snapshot",
        "pega": "preço pago que não é o preço publicado naquele armazém naquele dia — "
                "o fecho cruzado entre a source sintética e a API real",
        "injetar": [
            f"update {GOLD}.FACT_ORDER_ITEM set unit_price_paid = unit_price_paid + 0.10 "
            f"where order_id = 'ord_svq1_20260826_000003' and line_no = 1 "
            f"and fulfilled_source_product_id is not null",
        ],
        "reparar": [
            f"update {GOLD}.FACT_ORDER_ITEM set unit_price_paid = unit_price_paid - 0.10 "
            f"where order_id = 'ord_svq1_20260826_000003' and line_no = 1 "
            f"and fulfilled_source_product_id is not null",
        ],
    },
    {
        "teste": "assert_order_funnel_totals_match_fact_order",
        "pega": "dia-armazém sumindo do mart, e entrega contada por STATUS em vez de por "
                "MARCO — os 61 pedidos devolvidos que também foram entregues",
        "injetar": [
            # As DUAS metades do que o teste promete: uma linha somem, outra passa a contar
            # entregas por status. Um total geral continuaria batendo nas duas.
            f"delete from {MART}.MART_ORDER_FUNNEL "
            f"where order_date = '2026-08-24' and wh = 'mad1'",
            f"update {MART}.MART_ORDER_FUNNEL set orders_delivered = orders_delivered - orders_returned "
            f"where order_date = '2026-08-25' and wh = 'bcn1'",
        ],
        "reparar": [],   # só um rebuild devolve a linha apagada
        "rebuild": "mart_order_funnel",
    },
    {
        "teste": "assert_order_milestones_are_plausible_against_the_order_date",
        "pega": "erro de UNIDADE no timestamp ao atravessar a fronteira — o defeito real "
                "que colocou os 6.400 pedidos no ano 56.648.666",
        "injetar": [
            # Reproduz o deslocamento de microssegundos-lidos-como-milissegundos num
            # pedido só. Marcos entre si continuam na ordem certa: é por isso que comparar
            # marcos uns com os outros não pegaria, e só ancorar em order_date pega.
            f"update {GOLD}.FACT_ORDER set "
            f"  placed_at = dateadd(year, 1000, placed_at), "
            f"  picked_at = dateadd(year, 1000, picked_at) "
            f"where order_id = 'ord_vlc1_20260824_000007'",
        ],
        "reparar": [
            f"update {GOLD}.FACT_ORDER set "
            f"  placed_at = dateadd(year, -1000, placed_at), "
            f"  picked_at = dateadd(year, -1000, picked_at) "
            f"where order_id = 'ord_vlc1_20260824_000007'",
        ],
    },
]

SENTINELA = [
    (f"{GOLD}.FACT_ORDER", 6400),
    (f"{GOLD}.FACT_ORDER_ITEM", 120693),
    (f"{MART}.MART_ORDER_FUNNEL", 16),
]


def executar(cursor, comandos):
    for sql in comandos:
        cursor.execute(sql)


def rodar_teste(nome: str) -> int:
    return subprocess.run(
        [DBT, "test", *PROJECT, "--select", nome],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode


def rebuild(alvo: str) -> None:
    subprocess.run([DBT, "build", *PROJECT, "--select", alvo],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


def main() -> int:
    conexao = connect()
    cursor = conexao.cursor()

    print("sentinela antes ..", end=" ")
    antes = {}
    for tabela, esperado in SENTINELA:
        cursor.execute(f"select count(*) from {tabela}")
        antes[tabela] = cursor.fetchone()[0]
        if antes[tabela] != esperado:
            print(f"\nRECUSADO: {tabela} tem {antes[tabela]} linhas, esperado {esperado}. "
                  f"A base já está fora do estado conhecido — rode `make warehouse-refresh` "
                  f"antes. Injetar defeito sobre base suja produz falso positivo.")
            return 1
    print("ok")
    print()

    falhas = []
    for caso in CASOS:
        nome = caso["teste"]
        print(f"{nome}")
        print(f"  pega ......... {caso['pega']}")

        if rodar_teste(nome) != 0:
            print("  RECUSADO: o teste já reprova ANTES da injeção. Um vermelho que já "
                  "era vermelho não prova nada.")
            falhas.append(nome)
            continue

        executar(cursor, caso["injetar"])
        codigo = rodar_teste(nome)
        print(f"  com o defeito  exit={codigo} "
              f"{'REPROVOU (esperado)' if codigo else 'PASSOU — o teste é cego'}")
        if codigo == 0:
            falhas.append(nome)

        executar(cursor, caso["reparar"])
        if caso.get("rebuild"):
            rebuild(caso["rebuild"])
        limpo = rodar_teste(nome)
        print(f"  restaurado ... exit={limpo} "
              f"{'passa de novo' if limpo == 0 else 'AINDA REPROVA — o reparo não fechou'}")
        if limpo != 0:
            falhas.append(nome)
        print()

    # O script mexeu no dado real. Terminar sem reconferir o CONJUNTO deixaria o warehouse
    # num estado que só as cinco checagens acima viram — e cada uma olhou uma coisa só.
    print("suite inteira do warehouse ..", end=" ", flush=True)
    completo = subprocess.run([DBT, "test", *PROJECT],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("verde" if completo.returncode == 0 else "REPROVOU")
    if completo.returncode != 0:
        falhas.append("suite completa apos os reparos")
    print()

    print("sentinela depois .", end=" ")
    for tabela, _ in SENTINELA:
        cursor.execute(f"select count(*) from {tabela}")
        depois = cursor.fetchone()[0]
        if depois != antes[tabela]:
            print(f"\nRECUSADO: {tabela} saiu de {antes[tabela]} para {depois}.")
            falhas.append(f"sentinela:{tabela}")
    conexao.close()
    if not falhas:
        print("ok")

    print()
    if falhas:
        print(f"REPROVADO — {len(set(falhas))} teste(s) não provaram saber reprovar:")
        for nome in sorted(set(falhas)):
            print(f"  - {nome}")
        return 1
    print(f"APROVADO: os {len(CASOS)} testes do warehouse de Orders reprovam diante do "
          f"defeito que cada um diz pegar, e voltam a passar depois do reparo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
