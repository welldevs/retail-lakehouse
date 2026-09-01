#!/usr/bin/env python3
"""O SALDO DE ESTOQUE COM REPOSICAO — o job cuja FORMA o SQL nao expressa.

    make stock-ledger

POR QUE ESTE JOB EXISTE EM SPARK, E POR QUE NAO E POR VOLUME.

O gatilho de volume NAO disparou neste projeto, e isso esta medido: o candidato natural —
o self-join de cesta, 37,9 milhoes de pares — roda em 1,45 s e 2,31 GB num no de DuckDB.
Inflar dado para justificar um motor seria o defeito que este repositorio passou seis fases
cacando. Entao o Spark entra por outra porta, e ela ja estava aberta.

DUAS RAZOES, E DESEMPENHO NAO E NENHUMA DELAS.

PRIMEIRA, a interop. O ARCHITECTURE justificou o Iceberg por "interop entre engines", e os
dois escritores eram Python usando a mesma biblioteca — propriedade AFIRMADA e nunca
demonstrada. Este job e o terceiro escritor do catalogo e o primeiro fora do Python.

SEGUNDA, a forma. Uma soma corrida e SQL trivial:

    sum(consumo) over (partition by serie order by dia)

Esta nao e uma soma corrida. E uma soma corrida cujas ENTRADAS SAO GERADAS POR DECISOES
TOMADAS A PARTIR DO PROPRIO ESTADO:

    saldo cai abaixo do ponto  ->  emite ordem de compra
                               ->  a ordem chega em `supplier_lead_days` dias
                               ->  a chegada MUDA o saldo dos dias seguintes
                               ->  que decide se ha nova ordem

Window function nao expressa realimentacao: ela le a partition inteira, mas nao pode
escrever de volta nela. Isso foi MEDIDO, nao afirmado — `make spike-spark-iceberg`, pergunta
S7b, roda a mesma entrada pelas duas formas: a soma corrida em SQL diverge em 14 dos 30 dias
do caso de teste e chega a -70 de saldo, porque nao ha como injetar a chegada que a propria
decisao dela gerou.

`applyInPandas` expressa, e paraleliza por serie: cada (armazem, produto) e um laco
independente, e sao ~17 mil deles.

O QUE ESTE JOB NAO E. Nao e o caminho padrao do projeto. `make silver`, `make warehouse`, o
painel e as cinco DAGs rodam sem ele — o portao de `silver_gate.py` tira do build o que
depende de uma tabela que nao existe. Rodar sem Spark e um caso testado, nao um acidente.

ENTRADA   operations.stock_consumption   escrita pela plataforma (pyiceberg) a partir do
                                         consumo OBSERVADO em silver_order_line
          stock_premises_seed.csv        a politica declarada: quantos dias cobrir
SAIDA     operations.stock_ledger        grao (armazem, produto, dia)
"""

from __future__ import annotations

import csv
import math
import os
import sys
import time

sys.path.insert(0, "/repo")

from jobs.spark.session import CATALOGO, sessao  # noqa: E402

NAMESPACE = os.environ.get("STOCK_NAMESPACE", "operations")
CONSUMO = f"{CATALOGO}.{NAMESPACE}.stock_consumption"
LEDGER = f"{CATALOGO}.{NAMESPACE}.stock_ledger"
PREMISSAS = os.environ.get("STOCK_PREMISES_CSV",
                           "/repo/platform/dbt/seeds/stock_premises_seed.csv")

ESQUEMA = """
    wh string, source_product_id string, stock_date date,
    opening_balance bigint, units_received bigint, units_demanded bigint,
    units_fulfilled bigint, units_short bigint, closing_balance bigint,
    reorder_units bigint, reorder_eta date,
    mean_daily_demand double, days_of_cover double,
    written_by string
"""


def ler_premissas(caminho: str) -> dict:
    """As premissas vem do MESMO seed que o dbt carrega, lido como arquivo.

    Nao ha copia: o container monta o repositorio em `/repo` e le o CSV que o
    `stock_premises` projeta para o Silver. Passa-las por --conf criaria o segundo lugar
    onde a politica mora, e ele divergiria na primeira edicao do seed.
    """
    with open(caminho, encoding="utf-8") as arquivo:
        linhas = {linha["premise_key"]: float(linha["value"])
                  for linha in csv.DictReader(arquivo)}
    faltando = {"opening_days_of_demand", "reorder_point_days",
                "reorder_target_days", "supplier_lead_days"} - set(linhas)
    if faltando:
        raise SystemExit(f"{caminho} nao declara: {sorted(faltando)}")
    return linhas


def unidades(dias: float, demanda_media: float) -> int:
    """Dias de cobertura -> unidades. `floor(x + 0.5)` e nao `round`.

    O `round` do Python arredonda 0,5 para o PAR mais proximo — 0,5 vira 0 e 1,5 vira 2.
    Num produto de giro baixo, onde a demanda media e uma fracao, isso faz duas series com
    a mesma politica receberem estoques diferentes por um detalhe do IEEE 754. O ledger
    ficaria correto e inexplicavel.
    """
    return int(math.floor(dias * demanda_media + 0.5))


def construir_ledger(premissas: dict):
    """Devolve a funcao que o `applyInPandas` aplica a cada serie.

    Fechada sobre as premissas de proposito: elas viajam para o executor como parte da
    funcao, e nao como broadcast a ser gerenciado.
    """
    import datetime as dt

    import pandas as pd

    cobertura = premissas["opening_days_of_demand"]
    ponto_dias = premissas["reorder_point_days"]
    alvo_dias = premissas["reorder_target_days"]
    prazo = int(premissas["supplier_lead_days"])

    def ledger(chave, pdf: "pd.DataFrame") -> "pd.DataFrame":
        wh, produto = chave
        pdf = pdf.sort_values("stock_date")
        demanda_media = float(pdf["mean_daily_demand"].iloc[0])

        saldo = unidades(cobertura, demanda_media)
        ponto = unidades(ponto_dias, demanda_media)
        alvo = unidades(alvo_dias, demanda_media)

        chegadas: dict[dt.date, int] = {}
        em_aberto = False
        linhas = []

        for _, linha in pdf.iterrows():
            dia = linha["stock_date"]
            if hasattr(dia, "date"):
                dia = dia.date()

            abertura = saldo

            # 1. CHEGA O QUE FOI PEDIDO ANTES. A ordem de hoje foi decidida ha `prazo`
            #    dias, pelo saldo daquele dia — e e isto que window function nao faz.
            recebido = chegadas.pop(dia, 0)
            if recebido:
                em_aberto = False
            saldo += recebido

            # 2. ATENDE O QUE DA. Nao se vende o que nao ha: o que falta e RUPTURA, e ela
            #    e registrada em vez de virar saldo negativo. Saldo negativo fecharia a
            #    soma e mentiria sobre a prateleira.
            demanda = int(linha["units_demanded"] or 0)
            atendido = min(demanda, saldo)
            faltou = demanda - atendido
            saldo -= atendido

            # 3. DECIDE REPOR. Uma ordem em aberto por vez — politica min-max classica.
            #    Sem essa trava, um produto em ruptura emitiria uma ordem por dia enquanto
            #    a primeira ainda estivesse a caminho, e a chegada em cascata produziria um
            #    pico de estoque que nenhuma operacao real teria.
            pedido, chegada = 0, None
            if saldo < ponto and not em_aberto:
                pedido = max(alvo - saldo, 0)
                if pedido:
                    chegada = dia + dt.timedelta(days=prazo)
                    chegadas[chegada] = chegadas.get(chegada, 0) + pedido
                    em_aberto = True

            linhas.append((
                wh, produto, dia,
                abertura, recebido, demanda, atendido, faltou, saldo,
                pedido, chegada,
                demanda_media,
                (saldo / demanda_media) if demanda_media > 0 else None,
                "spark",
            ))

        return pd.DataFrame(linhas, columns=[
            "wh", "source_product_id", "stock_date",
            "opening_balance", "units_received", "units_demanded",
            "units_fulfilled", "units_short", "closing_balance",
            "reorder_units", "reorder_eta",
            "mean_daily_demand", "days_of_cover", "written_by",
        ])

    return ledger


def main() -> int:
    from pyspark.sql import functions as F

    premissas = ler_premissas(PREMISSAS)
    print("premissas (dias) ..", {k: v for k, v in sorted(premissas.items())})

    spark = sessao("stock-ledger")
    spark.sparkContext.setLogLevel("ERROR")
    inicio = time.time()

    consumo = spark.table(CONSUMO)
    janela = consumo.agg(F.min("stock_date").alias("de"),
                         F.max("stock_date").alias("ate")).collect()[0]
    if janela["de"] is None:
        raise SystemExit(
            f"{CONSUMO} esta vazia. Rode `make stock-consumption` depois de `make silver`.")
    print(f"janela ............ {janela['de']} .. {janela['ate']}")

    # ---- DENSIFICAR: uma linha por serie POR DIA, mesmo sem consumo ------------------
    #
    # Sem isto o ledger so teria linha nos dias em que houve venda, e o saldo "pularia" os
    # dias parados — que sao exatamente os dias em que uma reposicao chega. Um produto que
    # nao vende ha tres dias tem saldo nesses tres dias, e a cobertura em dias so significa
    # alguma coisa se a serie for continua.
    dias = spark.sql(
        f"select explode(sequence(date '{janela['de']}', date '{janela['ate']}',"
        f" interval 1 day)) as stock_date"
    )
    series = consumo.groupBy("wh", "source_product_id").agg(
        F.sum("units_consumed").alias("total"),
        F.countDistinct("stock_date").alias("dias_com_venda"),
    )
    n_dias = spark.sql(
        f"select datediff(date '{janela['ate']}', date '{janela['de']}') + 1 as n"
    ).collect()[0]["n"]

    # A DEMANDA MEDIA E OBSERVADA, e divide pelos dias da JANELA — nao pelos dias em que
    # houve venda. Dividir pelos dias com venda mediria "quanto sai quando sai", que e outra
    # pergunta: um produto vendido uma vez em nove dias teria a mesma media diaria de um
    # vendido todo dia, e receberia o mesmo estoque.
    series = series.withColumn("mean_daily_demand", F.col("total") / F.lit(float(n_dias)))

    grade = series.crossJoin(dias)
    entrada = (
        grade.join(consumo, ["wh", "source_product_id", "stock_date"], "left")
             .select("wh", "source_product_id", "stock_date", "mean_daily_demand",
                     F.coalesce(F.col("units_consumed"), F.lit(0))
                      .cast("bigint").alias("units_demanded"))
    )

    ledger = (
        entrada.groupBy("wh", "source_product_id")
               .applyInPandas(construir_ledger(premissas), schema=ESQUEMA)
    )

    (ledger.writeTo(LEDGER)
           .tableProperty("write.format.default", "parquet")
           .createOrReplace())

    decorrido = time.time() - inicio
    resumo = spark.sql(f"""
        select count(*)                                      as linhas,
               count(distinct wh || '|' || source_product_id) as series,
               sum(units_demanded)                            as demanda,
               sum(units_fulfilled)                           as atendido,
               sum(units_short)                               as ruptura,
               count_if(units_short > 0)                      as dias_com_ruptura,
               count_if(reorder_units > 0)                    as ordens,
               sum(reorder_units)                             as unidades_pedidas,
               count_if(units_received > 0)                   as chegadas,
               round(avg(days_of_cover), 2)                   as cobertura_media
        from {LEDGER}
    """).collect()[0]

    print()
    for chave in resumo.asDict():
        print(f"{chave:.<20} {resumo[chave]}")
    print(f"{'segundos':.<20} {decorrido:.1f}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
