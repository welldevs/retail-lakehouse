#!/usr/bin/env python3
"""EXPERIMENTO FECHADO: o Spark le e escreve o catalogo Iceberg que o pyiceberg criou?

    make spike-spark-iceberg

POR QUE ISTO EXISTE ANTES DA FASE 7, E NAO DENTRO DELA — E POR QUE E UM PORTAO SOBRE O
ICEBERG, NAO SOBRE O SPARK.

O ARCHITECTURE justifica o Iceberg com duas propriedades. A primeira — commit atomico com
concorrencia otimista entre dois escritores — esta PROVADA por
`scripts/prove_iceberg_projection.py`. A segunda — *interop entre engines* — esta AFIRMADA e
nunca foi demonstrada, porque os dois escritores sao Python e usam a mesma biblioteca. Uma
propriedade afirmada e nao testada e exatamente o que este projeto passou seis fases
recusando em outros lugares.

O risco concreto: o catalogo e um `SqlCatalog` do pyiceberg, e o Spark precisa le-lo como
`org.apache.iceberg.jdbc.JdbcCatalog`. Sao duas implementacoes independentes de uma mesma
convencao de tabela. Elas podem simplesmente nao conversar.

OS DOIS DESFECHOS SAO ACEITAVEIS, e por isso isto e um experimento e nao uma tarefa:

  APROVADO  a interop deixa de ser afirmacao. O Spark entra como terceiro escritor.
  REPROVADO o Spark NAO entra, E a clausula de interop e APAGADA da justificativa do
            Iceberg no ARCHITECTURE. O que sobra continua provado e continua bastando.

COMO ELE RODA. Um arquivo, dois lados, porque a pergunta e sobre a fronteira entre eles:

    host       venv da plataforma, fala pyiceberg   ->  orquestra e confere
    container  imagem do Spark, fala JdbcCatalog    ->  `--side spark`

O lado do Spark imprime linhas `RESULTADO<TAB>ok<TAB>pergunta<TAB>detalhe`, que o host
recolhe e mistura as suas. Sem isso o veredito moraria em dois lugares.

O QUE ELE NAO TOCA. Namespace proprio (`spike_spark`), prefixo proprio no warehouse. A
tabela viva `projection.live_order_state` e apenas LIDA — e a ultima pergunta confere que
ela continua intacta e que o Spark nao alterou o schema do catalogo.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import traceback

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NAMESPACE = "spike_spark"
TABLE = f"{NAMESPACE}.engine_interop"
CATALOGO = os.environ.get("ICEBERG_CATALOG_NAME", "retail")
TABELA_VIVA = os.environ.get("ICEBERG_NAMESPACE", "projection") + ".live_order_state"

resultados: list[tuple[str, bool, str]] = []


def responde(pergunta: str, ok: bool, detalhe: str = "") -> None:
    resultados.append((pergunta, ok, detalhe))
    print(f"  [{'SIM  ' if ok else 'NAO  '}] {pergunta}", flush=True)
    for linha in (detalhe or "").split("\n"):
        if linha:
            print(f"          {linha}", flush=True)


def falhou(pergunta: str, exc: Exception) -> None:
    responde(pergunta, False, f"{type(exc).__name__}: {str(exc)[:400]}")


# ======================================================================================
# LADO SPARK — roda DENTRO do container, com a imagem `retail-lakehouse/spark`.
# ======================================================================================

def sessao_spark():
    """A sessao vem de `jobs/spark/session.py`, e NAO de uma copia local.

    A tentacao era congelar a configuracao aqui, para que a evidencia registrada
    descrevesse exatamente o que foi medido. Seria pior: no dia em que a configuracao de
    producao mudasse, este experimento continuaria APROVANDO uma configuracao que ninguem
    usa. Importando de la, `make spike-spark-iceberg` vira conferencia viva do que o job
    de estoque realmente usa.
    """
    from jobs.spark.session import sessao

    return sessao("spike-spark-iceberg")


def emite(ok: bool, pergunta: str, detalhe: str = "") -> None:
    """Uma linha que o host sabe ler. O veredito mora num lugar so, e ele e do host."""
    limpo = (detalhe or "").replace("\n", " | ").replace("\t", " ")
    print(f"RESULTADO\t{'1' if ok else '0'}\t{pergunta}\t{limpo}", flush=True)


def lado_spark(fase: str) -> int:
    spark = sessao_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        return FASES_SPARK[fase](spark)
    finally:
        spark.stop()


def spark_fase_leitura(spark) -> int:
    """S1-S3: o Spark ENXERGA e LE o que o pyiceberg escreveu."""
    import pyspark

    print(f"pyspark {pyspark.__version__} · catalogo `{CATALOGO}` via JdbcCatalog",
          flush=True)

    # S1. o catalogo responde, e os namespaces sao os que o pyiceberg criou.
    try:
        nomes = [r[0] for r in spark.sql(f"show namespaces in {CATALOGO}").collect()]
        emite(len(nomes) > 0, "S1. o Spark abre o catalogo SQL do pyiceberg como JdbcCatalog",
              f"namespaces: {sorted(nomes)}")
    except Exception as exc:
        emite(False, "S1. o Spark abre o catalogo SQL do pyiceberg como JdbcCatalog",
              f"{type(exc).__name__}: {exc}")
        return 1

    # S2. a tabela escrita pelo Python aparece na listagem.
    ns, tabela = TABELA_VIVA.split(".")
    try:
        nomes = [r[1] for r in spark.sql(f"show tables in {CATALOGO}.{ns}").collect()]
        emite(tabela in nomes, f"S2. o Spark ENXERGA `{TABELA_VIVA}`, escrita pelo pyiceberg",
              f"tabelas em {ns}: {sorted(nomes)}")
    except Exception as exc:
        emite(False, f"S2. o Spark ENXERGA `{TABELA_VIVA}`, escrita pelo pyiceberg",
              f"{type(exc).__name__}: {exc}")
        return 1

    # S3. e le o CONTEUDO — nao so o metadado. A contagem o host confere contra o pyiceberg.
    try:
        linhas = spark.table(f"{CATALOGO}.{TABELA_VIVA}").count()
        distintos = spark.sql(
            f"select count(distinct written_by) n from {CATALOGO}.{TABELA_VIVA}"
        ).collect()[0]["n"]
        emite(linhas > 0, f"S3. o Spark LE o conteudo de `{TABELA_VIVA}`",
              f"CONTAGEM={linhas} escritores_distintos={distintos}")
    except Exception as exc:
        emite(False, f"S3. o Spark LE o conteudo de `{TABELA_VIVA}`",
              f"{type(exc).__name__}: {exc}")
        return 1
    return 0


def spark_fase_escrita(spark) -> int:
    """S4-S5: o Spark CRIA e escreve uma tabela nova no mesmo catalogo."""
    spark.sql(f"create namespace if not exists {CATALOGO}.{NAMESPACE}")
    alvo = f"{CATALOGO}.{TABLE}"
    spark.sql(f"drop table if exists {alvo}")

    try:
        # `written_by` desde o primeiro CREATE: proveniencia nao e coluna que se acrescenta
        # depois, porque as linhas ja escritas nunca ganham o valor que faltava.
        spark.sql(f"""
            create table {alvo} (
                chave        string  not null,
                escritor     string  not null,
                sequencia    int     not null,
                written_by   string  not null
            ) using iceberg
        """)
        spark.sql(f"""
            insert into {alvo} values
                ('k_spark_01', 'spark', 1, 'spark'),
                ('k_spark_02', 'spark', 1, 'spark'),
                ('k_spark_03', 'spark', 1, 'spark')
        """)
        n = spark.table(alvo).count()
        emite(n == 3, f"S4. o Spark CRIA `{TABLE}` no catalogo e grava nela", f"{n} linhas")
    except Exception as exc:
        emite(False, f"S4. o Spark CRIA `{TABLE}` no catalogo e grava nela",
              f"{type(exc).__name__}: {exc}")
        return 1

    # S5. MERGE INTO: o upsert por chave que o ledger vai precisar, do lado do Spark.
    try:
        spark.sql(f"""
            merge into {alvo} t
            using (select 'k_spark_01' chave, 'spark' escritor, 9 sequencia,
                          'spark' written_by
                   union all
                   select 'k_spark_99', 'spark', 1, 'spark') s
            on t.chave = s.chave
            when matched then update set t.sequencia = s.sequencia
            when not matched then insert *
        """)
        estado = {r["chave"]: r["sequencia"] for r in spark.table(alvo).collect()}
        ok = estado.get("k_spark_01") == 9 and estado.get("k_spark_99") == 1
        emite(ok, "S5. MERGE INTO por chave funciona (atualiza 1, insere 1)",
              f"linhas={len(estado)} k_spark_01.sequencia={estado.get('k_spark_01')}")
    except Exception as exc:
        emite(False, "S5. MERGE INTO por chave funciona (atualiza 1, insere 1)",
              f"{type(exc).__name__}: {exc}")
    return 0


def spark_fase_concorrente(spark) -> int:
    """S6: escreve devagar, avisando, para que o host escreva POR CIMA ao mesmo tempo."""
    alvo = f"{CATALOGO}.{TABLE}"
    print("PRONTO", flush=True)  # o host espera esta linha para comecar a escrever
    erros = 0
    for k in range(5):
        for tentativa in range(20):
            try:
                spark.sql(
                    f"insert into {alvo} values "
                    f"('k_conc_spark_{k:02d}', 'spark', 1, 'spark')"
                )
                break
            except Exception:
                erros += 1
                time.sleep(0.3)
        else:
            emite(False, "S6. as 5 escritas do Spark sob concorrencia chegaram",
                  f"desistiu em k={k}")
            return 1
        time.sleep(0.6)
    emite(True, "S6. as 5 escritas do Spark sob concorrencia chegaram",
          f"{erros} conflito(s) resolvidos por retry do lado do Spark")
    return 0


def spark_fase_forma(spark) -> int:
    """S7: `applyInPandas` — a FORMA que justifica o Spark, e a prova de que SQL nao a tem.

    Uma soma corrida e SQL trivial. Esta nao e uma soma corrida: e uma soma corrida cujas
    ENTRADAS sao geradas por decisoes tomadas a partir do proprio estado. O saldo de amanha
    depende de uma ordem emitida por causa do saldo de hoje, que chega `prazo` dias depois.

    S7b nao AFIRMA que a window function nao expressa isso — MEDE. A mesma entrada, com a
    soma corrida que o SQL sabe fazer, e a divergencia que sobra.

    O caso e minusculo de proposito: aqui se mede se o engine suporta a forma, nao quanto
    tempo leva. O tempo, com o contraditorio, vai para `make spark-evidence`.
    """
    import pandas as pd
    from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                                   StructType)

    DIAS, SALDO_INICIAL, PRAZO, PONTO, ALVO = 10, 100.0, 2, 40.0, 120.0

    esquema = StructType([
        StructField("serie", StringType()),
        StructField("dia", IntegerType()),
        StructField("consumo", DoubleType()),
        StructField("saldo", DoubleType()),
        StructField("recebido", DoubleType()),
        StructField("pedido", DoubleType()),
        StructField("chegada_prevista", IntegerType()),
    ])

    def ledger(chave, pdf: pd.DataFrame) -> pd.DataFrame:
        """O laco: saldo -> decisao -> chegada futura -> saldo. Um grupo por vez."""
        pdf = pdf.sort_values("dia")
        saldo = SALDO_INICIAL
        chegadas: dict[int, float] = {}
        linhas = []
        for _, linha in pdf.iterrows():
            dia = int(linha["dia"])
            recebido = chegadas.pop(dia, 0.0)
            saldo += recebido
            consumo = min(float(linha["consumo"]), saldo)   # nao se vende o que nao ha
            saldo -= consumo
            pedido, chegada = 0.0, -1
            if saldo < PONTO and not chegadas:              # uma ordem em aberto por vez
                pedido = ALVO - saldo
                chegada = dia + PRAZO
                chegadas[chegada] = pedido                  # <- a decisao muda o FUTURO
            linhas.append((chave[0], dia, consumo, saldo, recebido, pedido, chegada))
        return pd.DataFrame(linhas, columns=[c.name for c in esquema.fields])

    try:
        entrada = spark.createDataFrame(
            [(f"s{s}", d, 15.0 + s) for s in range(3) for d in range(DIAS)],
            "serie string, dia int, consumo double",
        )
        linhas = entrada.groupBy("serie").applyInPandas(ledger, schema=esquema).collect()

        pedidos = [r for r in linhas if r["pedido"] > 0]
        chegadas = [r for r in linhas if r["recebido"] > 0]
        em_transito = [r for r in pedidos if r["chegada_prevista"] > DIAS - 1]
        negativos = [r for r in linhas if r["saldo"] < 0]

        # A LEI DE CONSERVACAO, e a razao de a primeira versao desta pergunta ter reprovado
        # por MINHA culpa. Eu tinha exigido `chegadas == pedidos`, e medi 5 pedidos para 3
        # chegadas. Nao era defeito: um pedido emitido no dia 8 com prazo 2 chega no dia 10,
        # que esta FORA da janela. Pedido em transito no fim do periodo e propriedade real
        # de qualquer ledger — exigir que sumisse seria exigir que o modelo mentisse.
        conserva_ordens = len(chegadas) + len(em_transito) == len(pedidos)

        # A segunda invariante, essa sobre QUANTIDADE: o saldo final de cada serie tem de
        # ser exatamente o inicial mais o que entrou menos o que saiu. Ela pega erro que a
        # contagem de eventos nao pega — uma chegada aplicada duas vezes passa na primeira.
        conserva_saldo = True
        for serie in {r["serie"] for r in linhas}:
            da_serie = sorted([r for r in linhas if r["serie"] == serie],
                              key=lambda r: r["dia"])
            esperado = (SALDO_INICIAL
                        + sum(r["recebido"] for r in da_serie)
                        - sum(r["consumo"] for r in da_serie))
            if abs(da_serie[-1]["saldo"] - esperado) > 1e-9:
                conserva_saldo = False

        ok = (len(linhas) == 3 * DIAS and pedidos and conserva_ordens and conserva_saldo
              and not negativos)
        emite(ok, "S7a. `applyInPandas` expressa a realimentacao (saldo -> pedido -> chegada)",
              f"{len(linhas)} linhas · {len(pedidos)} pedido(s) · {len(chegadas)} chegada(s) "
              f"· {len(em_transito)} em transito no fim da janela · "
              f"{len(negativos)} saldo(s) negativo(s) · "
              f"conserva_ordens={conserva_ordens} conserva_saldo={conserva_saldo}")
    except Exception as exc:
        emite(False, "S7a. `applyInPandas` expressa a realimentacao (saldo -> pedido -> chegada)",
              f"{type(exc).__name__}: {exc}")
        return 1

    # ---- S7b: a mesma entrada pela soma corrida que o SQL sabe fazer -----------------
    try:
        entrada.createOrReplaceTempView("consumo_diario")
        sql = spark.sql(f"""
            select serie, dia,
                   {SALDO_INICIAL} - sum(consumo) over (
                       partition by serie order by dia
                       rows between unbounded preceding and current row) as saldo_sql
            from consumo_diario
        """).collect()

        saldo_ledger = {(r["serie"], r["dia"]): r["saldo"] for r in linhas}
        divergentes = [r for r in sql
                       if abs(r["saldo_sql"] - saldo_ledger[(r["serie"], r["dia"])]) > 1e-9]
        negativos_sql = [r for r in sql if r["saldo_sql"] < 0]

        # A window function TEM de divergir. Se ela concordasse, o job nao precisaria
        # existir — e esta pergunta e o que impede a justificativa do Spark de ser um
        # paragrafo que ninguem conferiu.
        emite(bool(divergentes) and bool(negativos_sql),
              "S7b. a soma corrida em SQL NAO reproduz o ledger (por isso o job existe)",
              f"{len(divergentes)}/{len(sql)} dias divergem · o SQL chega a "
              f"{min(r['saldo_sql'] for r in sql):.1f} de saldo em {len(negativos_sql)} dias, "
              f"porque nao ha como uma window function injetar a chegada que a propria "
              f"decisao dela gerou")
    except Exception as exc:
        emite(False, "S7b. a soma corrida em SQL NAO reproduz o ledger (por isso o job existe)",
              f"{type(exc).__name__}: {exc}")
    return 0


def spark_fase_limpeza(spark) -> int:
    alvo = f"{CATALOGO}.{TABLE}"
    try:
        spark.sql(f"drop table if exists {alvo} purge")
        spark.sql(f"drop namespace if exists {CATALOGO}.{NAMESPACE}")
        emite(True, "S8. o Spark remove o que criou", "tabela e namespace do spike")
    except Exception as exc:
        emite(False, "S8. o Spark remove o que criou", f"{type(exc).__name__}: {exc}")
    return 0


FASES_SPARK = {
    "leitura": spark_fase_leitura,
    "escrita": spark_fase_escrita,
    "concorrente": spark_fase_concorrente,
    "forma": spark_fase_forma,
    "limpeza": spark_fase_limpeza,
}


# ======================================================================================
# LADO HOST — orquestra o container e confere pelo pyiceberg.
# ======================================================================================

def compose(*args: str) -> list[str]:
    return ["docker", "compose", "--env-file", ".env", "-f", "infra/docker-compose.yml",
            "--profile", "spark", *args]


def roda_no_spark(fase: str, *, marcador: str = None):
    """Roda uma fase no container. Devolve (retorno, linhas RESULTADO, processo|None).

    Com `marcador`, volta assim que o container imprimir aquela linha, com o processo
    AINDA VIVO — e o que permite escrever concorrentemente com ele pelo pyiceberg.
    """
    cmd = compose("run", "--rm", "--no-deps", "spark",
                  "python3", "scripts/spike_spark_iceberg.py", "--side", "spark",
                  "--fase", fase)
    proc = subprocess.Popen(cmd, cwd=REPO, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    colhidas = []
    for linha in proc.stdout:
        if linha.startswith("RESULTADO\t"):
            colhidas.append(linha.rstrip("\n").split("\t"))
        else:
            texto = linha.rstrip()
            if texto and not texto.startswith(("WARNING", "NOTE:", "Using ")):
                print(f"      spark| {texto}", flush=True)
        if marcador and linha.strip() == marcador:
            return None, colhidas, proc
    proc.wait()
    return proc.returncode, colhidas, None


def registra(colhidas) -> None:
    for _, ok, pergunta, detalhe in colhidas:
        responde(pergunta, ok == "1", detalhe)


def detalhe_de(colhidas, prefixo: str) -> str:
    for _, _, pergunta, detalhe in colhidas:
        if pergunta.startswith(prefixo):
            return detalhe
    return ""


def ddl_do_catalogo() -> str:
    """As colunas de `iceberg_tables`, para provar que o Spark nao as alterou.

    O JdbcCatalog Java sabe migrar o schema do catalogo (a versao V1 acrescenta uma coluna
    para suportar views). Se ele fizer isso no catalogo VIVO durante o spike, um
    experimento que deveria ser isolado teria mudado a producao em silencio — que e
    exatamente a classe de coisa que este projeto trata como defeito.
    """
    saida = subprocess.run(
        ["docker", "exec", "retail-oltp-postgres", "psql", "-U", "oltp",
         "-d", os.environ.get("ICEBERG_CATALOG_DB", "iceberg_catalog"), "-t", "-A", "-c",
         "select table_name || '.' || column_name || ':' || data_type "
         "from information_schema.columns "
         "where table_name in ('iceberg_tables','iceberg_namespace_properties') "
         "order by 1"],
        capture_output=True, text=True, check=True)
    return saida.stdout.strip()


def main_host() -> int:
    sys.path.insert(0, os.path.join(REPO, "platform", "src"))
    from retail_platform import orders_projection as proj

    print("EXPERIMENTO: o Spark le e escreve o catalogo Iceberg do pyiceberg?")
    print(f"catalogo ......... {CATALOGO}")
    print(f"tabela viva ...... {TABELA_VIVA}  (somente leitura)")
    print(f"tabela do spike .. {TABLE}")
    print()

    # ---- H0: pre-requisito, com a mensagem que diz o que fazer -----------------------
    try:
        cat = proj.catalog()
        viva = cat.load_table(TABELA_VIVA)
        linhas_antes = viva.scan().to_arrow().num_rows
        ddl_antes = ddl_do_catalogo()
    except Exception as exc:
        print(f"ERRO: o catalogo Iceberg nao respondeu ({type(exc).__name__}: {exc})")
        print("      O plano de stream nao sobe com `make up`. Rode `make stream-up` e,")
        print("      se a projecao nunca foi construida, `make orders-projection-init`.")
        return 2
    responde("H0. o pyiceberg le a tabela viva ANTES do experimento", linhas_antes > 0,
             f"{linhas_antes} linhas · metadado {os.path.basename(viva.metadata_location)}")

    # ---- S1-S3: o Spark enxerga e le -------------------------------------------------
    print()
    print("S1-S3. o Spark abre o catalogo, enxerga a tabela do Python e le o conteudo")
    rc, colhidas, _ = roda_no_spark("leitura")
    registra(colhidas)
    if rc != 0:
        return veredito(linhas_antes, ddl_antes)

    # H1. a contagem do Spark bate com a do pyiceberg. Ler "alguma coisa" nao e interop.
    lido = detalhe_de(colhidas, "S3.")
    contagem_spark = None
    for pedaco in lido.split():
        if pedaco.startswith("CONTAGEM="):
            contagem_spark = int(pedaco.split("=", 1)[1])
    responde("H1. a contagem do Spark BATE com a do pyiceberg na mesma tabela",
             contagem_spark == linhas_antes,
             f"spark={contagem_spark} pyiceberg={linhas_antes}")

    # ---- S4-S5: o Spark escreve ------------------------------------------------------
    print()
    print("S4-S5. o Spark cria uma tabela no mesmo catalogo e escreve nela")
    rc, colhidas, _ = roda_no_spark("escrita")
    registra(colhidas)
    if rc != 0:
        return veredito(linhas_antes, ddl_antes)

    # ---- H2: a volta. o pyiceberg le o que o Spark escreveu --------------------------
    print()
    print("H2. o pyiceberg ENXERGA e LE a tabela que o Spark criou (a volta do trajeto)")
    try:
        cat = proj.catalog()  # recarregado: o namespace nasceu depois da primeira abertura
        spark_tbl = cat.load_table(TABLE)
        linhas = spark_tbl.scan().to_arrow()
        chaves = {r["chave"] for r in linhas.to_pylist()}
        responde("H2. o pyiceberg le a tabela criada pelo Spark",
                 {"k_spark_01", "k_spark_99"} <= chaves,
                 f"{linhas.num_rows} linhas · colunas {linhas.column_names}")
    except Exception as exc:
        falhou("H2. o pyiceberg le a tabela criada pelo Spark", exc)
        return veredito(linhas_antes, ddl_antes)

    # ---- H3: conflito otimista CRUZADO, com recuperacao completa ---------------------
    #
    # Nao basta observar que um perde. Perder nao e a propriedade; a propriedade e
    # detectar -> recarregar -> reaplicar -> commitar, com o commit velho NUNCA
    # sobrescrevendo o novo. E o ciclo que qualquer escritor concorrente vai precisar.
    print()
    print("H3. conflito CRUZADO: o pyiceberg com snapshot velho depois de um commit do Spark")
    try:
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        esquema = spark_tbl.schema().as_arrow()

        def lote(chave):
            return pa.Table.from_pylist(
                [{"chave": chave, "escritor": "pyiceberg", "sequencia": 1,
                  "written_by": "pyiceberg"}], schema=esquema)

        velha = cat.load_table(TABLE)          # referencia carregada ANTES do Spark commitar
        rc, extra, _ = roda_no_spark("escrita")  # o Spark recria e commita: snapshot novo
        del extra

        recusado, motivo = False, ""
        try:
            velha.append(lote("k_cruzado"))
        except CommitFailedException as conflito:
            recusado, motivo = True, str(conflito)[:150]
        except Exception as conflito:
            recusado, motivo = True, f"{type(conflito).__name__}: {str(conflito)[:150]}"
        responde("H3a. o pyiceberg com snapshot velho e RECUSADO, nao perdido",
                 recusado, motivo or "commitou em silencio — isto seria lost update")

        nova = cat.load_table(TABLE)           # recarregar
        nova.append(lote("k_cruzado"))          # reaplicar
        final = cat.load_table(TABLE).scan().to_arrow()
        chaves = {r["chave"] for r in final.to_pylist()}
        responde("H3b. depois de recarregar, o retry do pyiceberg commita",
                 "k_cruzado" in chaves)
        responde("H3c. a escrita do Spark NAO foi sobrescrita pelo commit mais velho",
                 {"k_spark_01", "k_spark_02", "k_spark_03", "k_cruzado"} <= chaves,
                 f"{final.num_rows} linhas: {sorted(chaves)}")
    except Exception as exc:
        falhou("H3a. o pyiceberg com snapshot velho e RECUSADO, nao perdido", exc)

    # ---- S6 + H4: escritas REALMENTE simultaneas, um engine de cada lado -------------
    print()
    print("S6/H4. Spark e pyiceberg escrevendo na MESMA tabela ao mesmo tempo")
    try:
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        _, _, proc = roda_no_spark("concorrente", marcador="PRONTO")
        conflitos = 0
        for k in range(5):
            for tentativa in range(30):
                try:
                    t = cat.load_table(TABLE)
                    t.append(pa.Table.from_pylist(
                        [{"chave": f"k_conc_py_{k:02d}", "escritor": "pyiceberg",
                          "sequencia": 1, "written_by": "pyiceberg"}], schema=esquema))
                    break
                except CommitFailedException:
                    conflitos += 1
                    time.sleep(0.2)
            else:
                raise RuntimeError(f"pyiceberg desistiu em k={k}")
            time.sleep(0.5)

        for linha in proc.stdout:
            if linha.startswith("RESULTADO\t"):
                registra([linha.rstrip("\n").split("\t")])
        proc.wait()

        final = cat.load_table(TABLE).scan().to_arrow()
        chaves = {r["chave"] for r in final.to_pylist()}
        esperadas = ({f"k_conc_spark_{k:02d}" for k in range(5)}
                     | {f"k_conc_py_{k:02d}" for k in range(5)})
        faltando = esperadas - chaves
        # HONESTIDADE SOBRE O QUE ESTA PERGUNTA PROVA. Ela prova AUSENCIA DE LOST UPDATE
        # sob escrita simultanea. Ela NAO prova que houve colisao: se os commits nao se
        # cruzarem no relogio, o laco de retry nem dispara — e um `0 conflito(s)` seria
        # lido como "a concorrencia funciona" quando na verdade nao foi exercida. Quem
        # exercita o caminho do conflito de forma DETERMINISTICA e H3a, com a referencia
        # velha forcada. As duas perguntas existem porque uma nao substitui a outra.
        exercitou = (" · o retry FOI exercido" if conflitos
                     else " · nenhuma colisao no relogio desta execucao; quem prova o "
                          "caminho do conflito e H3a, que e determinista")
        responde("H4. as 10 escritas simultaneas dos DOIS engines estao todas presentes",
                 not faltando,
                 f"{final.num_rows} linhas · {conflitos} conflito(s) do lado do pyiceberg"
                 + exercitou
                 + (f" · FALTANDO {sorted(faltando)}" if faltando else ""))
    except Exception as exc:
        falhou("H4. as 10 escritas simultaneas dos DOIS engines estao todas presentes", exc)

    # ---- S7: a forma -----------------------------------------------------------------
    print()
    print("S7. a forma do job: realimentacao por serie")
    _, colhidas, _ = roda_no_spark("forma")
    registra(colhidas)

    # ---- limpeza ---------------------------------------------------------------------
    print()
    _, colhidas, _ = roda_no_spark("limpeza")
    registra(colhidas)

    return veredito(linhas_antes, ddl_antes)


def veredito(linhas_antes: int, ddl_antes: str) -> int:
    # ---- H5/H6: o experimento nao pode ter mexido no que estava vivo -----------------
    print()
    print("H5/H6. o experimento deixou a producao como encontrou?")
    try:
        sys.path.insert(0, os.path.join(REPO, "platform", "src"))
        from retail_platform import orders_projection as proj

        depois = proj.catalog().load_table(TABELA_VIVA).scan().to_arrow().num_rows
        responde("H5. a tabela viva continua com as mesmas linhas", depois == linhas_antes,
                 f"antes={linhas_antes} depois={depois}")
    except Exception as exc:
        falhou("H5. a tabela viva continua com as mesmas linhas", exc)
    try:
        igual = ddl_do_catalogo() == ddl_antes
        responde("H6. o Spark NAO alterou o schema das tabelas do catalogo", igual,
                 "iceberg_tables e iceberg_namespace_properties intactas" if igual
                 else "O JdbcCatalog migrou o schema do catalogo VIVO — investigar antes\n"
                      "de qualquer outra coisa. Ver `jdbc.schema-version`.")
    except Exception as exc:
        falhou("H6. o Spark NAO alterou o schema das tabelas do catalogo", exc)

    print()
    print("=" * 80)
    for pergunta, ok, _ in resultados:
        print(f"  {'SIM' if ok else 'NAO'}  {pergunta}")
    print()

    # As perguntas que decidem. As demais informam; estas mandam.
    essenciais = ("S1.", "S2.", "S3.", "H1.", "S4.", "H2.", "H3a", "H3b", "H3c", "H4.",
                  "S7a", "S7b", "H5.", "H6.")
    faltando = [p for p in essenciais
                if not any(q.startswith(p) and ok for q, ok, _ in resultados)]
    if faltando:
        print("VEREDITO: o experimento REPROVOU.")
        for p in faltando:
            print(f"  falta: {p}")
        print()
        print("O QUE ISSO DECIDE, e nao e so sobre o Spark: a clausula de *interop entre")
        print("engines* da justificativa do Iceberg foi TESTADA e nao se sustenta. Ela sai")
        print("do ARCHITECTURE. O que continua provado — commit atomico e concorrencia")
        print("otimista entre os dois escritores Python — continua la, e continua bastando.")
        return 1

    print("VEREDITO: APROVADO. O Spark le o catalogo escrito pelo pyiceberg, escreve nele,")
    print("e os dois engines resolvem conflito otimista sem perder escrita. A interop que")
    print("justificou o Iceberg deixa de ser afirmacao. `applyInPandas` expressa a")
    print("realimentacao que o SQL nao expressa — que e a forma do job de estoque.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--side", choices=["host", "spark"], default="host",
                        help="`spark` roda DENTRO do container; o padrao orquestra.")
    parser.add_argument("--fase", choices=sorted(FASES_SPARK), help="so com --side spark")
    args = parser.parse_args()

    if args.side == "spark":
        if not args.fase:
            parser.error("--side spark exige --fase")
        try:
            return lado_spark(args.fase)
        except Exception:
            traceback.print_exc()
            emite(False, f"S?. fase `{args.fase}` levantou excecao", "ver traceback acima")
            return 1
    return main_host()


if __name__ == "__main__":
    raise SystemExit(main())
