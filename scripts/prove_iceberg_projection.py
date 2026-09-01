#!/usr/bin/env python3
"""Prova da projeção concorrente: dois escritores, um leitor, fusão monotônica.

O QUE ESTE MARCO PRECISA PROVAR, e por que cada peça é necessária.

O gatilho do Iceberg era *"um segundo engine precisar escrever a mesma tabela"*. Ter dois
processos que escrevem não prova nada sozinho — o que precisa ficar demonstrado é o
comportamento sob conflito, que é onde um armazenamento sem controle de concorrência falha em
silêncio:

  1. CONFLITO É RECUSADO, não absorvido. O escritor que parte de um snapshot velho leva
     `CommitFailedException`. Se passasse calado, seria lost update — e aí o Iceberg não
     estaria comprando nada.

  2. RETRY NÃO BASTA: A FUSÃO PRECISA SER MONOTÔNICA. Recarregar e tentar de novo resolve o
     conflito de COMMIT e ainda assim perde dado — se o outro escritor já gravou o pedido no
     `sequence_no` 7 e a nossa tentativa carrega o 5, o retry cego escreve o 5 por cima. O
     commit passa, a tabela regride, e nada reprova. Esta é a prova que mais importa.

  3. O LEITOR VÊ SNAPSHOT ESTÁVEL. Uma leitura em curso não enxerga escrita pela metade.

  4. OS DOIS ESCRITORES DE VERDADE, na tabela de verdade, com dado de verdade — e a
     reconciliação contra `silver_order` no fim.

  5. TIME TRAVEL na projeção.

  6. `orders-reconcile` REPROVA quando há divergência. Verificação que nunca falhou não é
     verificação.

    make orders-prove-projection
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.config import from_env  # noqa: E402
from retail_platform.orders_projection import (  # noqa: E402
    COLUMNS,
    NAMESPACE,
    TABLE_NAME,
    WRITER_REBUILD,
    WRITER_STREAM,
    IcebergProjection,
    arrow_schema,
    catalog,
    ensure_table,
    reconcile,
)

PROBE = f"{NAMESPACE}.concurrency_probe"
falhas = []


def check(label, ok, detalhe=""):
    print(f"  [{'OK   ' if ok else 'FALHA'}] {label}{(' — ' + detalhe) if detalhe else ''}")
    if not ok:
        falhas.append(label)


def estado(order_id, seq, status="PLACED", writer=WRITER_STREAM):
    agora = datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc)
    return {
        "order_id": order_id, "wh": "mad1", "order_date": agora.date(),
        "customer_id": "cust_mad1_000001", "status": status, "last_sequence_no": seq,
        "events_applied": seq, "placed_at": agora, "updated_at": agora,
        "confirmed_at": None, "picking_started_at": None, "picked_at": None,
        "dispatched_at": None, "terminal_at": None, "line_count": 3,
        "picked_line_count": None, "substituted_lines": 0, "removed_lines": 0,
        "gross_amount": "10.00", "net_amount": None, "returned_amount": None,
        "delivered_within_slot": None, "picking_minutes": None, "sla_breached": None,
        "written_by": writer,
    }


def main() -> int:
    config = from_env()
    cat = catalog(config)

    # SENTINELA. As partes 1 a 4 mexem SÓ na tabela de sonda; se a de produção mudar de
    # tamanho, alguma escrita foi para o lugar errado. Sem esta linha, foi assim que um
    # `load_table(TABLE_NAME)` cravado num refresh gravou quatro linhas sintéticas na
    # projeção real — e só a reconciliação, três passos adiante, apontou. Uma prova que usa
    # uma sonda tem de vigiar o alvo que ela NÃO deveria tocar.
    real_antes = ensure_table(cat).scan(selected_fields=("order_id",)).to_arrow().num_rows

    # ---- 1. mecânica do conflito, numa tabela de sonda -------------------------------
    print("1. mecânica do conflito, numa tabela isolada")
    if cat.table_exists(PROBE):
        cat.drop_table(PROBE)
    cat.create_table(PROBE, schema=arrow_schema())

    import pyarrow as pa
    from pyiceberg.exceptions import CommitFailedException

    from retail_platform.orders_projection import _normalize

    def escreve_cru(tabela, linhas):
        tabela.upsert(
            pa.Table.from_pylist([_normalize(l, l["written_by"]) for l in linhas],
                                 schema=arrow_schema()),
            join_cols=["order_id"],
        )

    a = cat.load_table(PROBE)
    b = cat.load_table(PROBE)          # referência carregada ANTES do commit de `a`
    escreve_cru(a, [estado("ord_a", 1)])
    recusado = False
    try:
        escreve_cru(b, [estado("ord_b", 1)])
    except CommitFailedException as exc:
        recusado = True
        motivo = str(exc)[:90]
    check("o escritor com snapshot velho é RECUSADO, não perdido", recusado,
          motivo if recusado else "commitou em silêncio — seria lost update")

    # ---- 2. A PROVA QUE MAIS IMPORTA: fusão monotônica --------------------------------
    print()
    print("2. fusão monotônica — retry não pode escrever um estado ANTIGO por cima")
    tabela = cat.load_table(PROBE)
    novo = IcebergProjection(tabela, writer=WRITER_STREAM, cat=cat)
    novo.save([estado("ord_corrida", 7, status="DELIVERED")])
    novo.commit()

    velho = IcebergProjection(cat.load_table(PROBE), writer=WRITER_REBUILD, cat=cat)
    velho.save([estado("ord_corrida", 5, status="PICKING", writer=WRITER_REBUILD)])
    stats = velho.commit()

    linhas = {r["order_id"]: r for r in
              cat.load_table(PROBE).scan().to_arrow().to_pylist()}
    atual = linhas.get("ord_corrida", {})
    check("o estado antigo foi descartado como obsoleto", stats.rows_dropped_as_stale == 1,
          f"descartadas={stats.rows_dropped_as_stale} escritas={stats.rows_written}")
    check("a tabela NÃO regrediu", atual.get("last_sequence_no") == 7
          and atual.get("status") == "DELIVERED",
          f"seq={atual.get('last_sequence_no')} status={atual.get('status')}")

    # e o caminho normal continua funcionando: o que avança, escreve
    mais_novo = IcebergProjection(cat.load_table(PROBE), writer=WRITER_REBUILD, cat=cat)
    mais_novo.save([estado("ord_corrida", 9, status="RETURNED", writer=WRITER_REBUILD)])
    stats2 = mais_novo.commit()
    depois = {r["order_id"]: r for r in
              cat.load_table(PROBE).scan().to_arrow().to_pylist()}["ord_corrida"]
    check("o que AVANÇA continua escrevendo", stats2.rows_written == 1
          and depois["last_sequence_no"] == 9 and depois["written_by"] == WRITER_REBUILD)

    # ---- 3. dois escritores concorrentes, com leitor ativo ----------------------------
    print()
    print("3. dois escritores concorrentes na sonda, com um leitor em laço")
    conflitos = {"n": 0}
    leituras = {"n": 0, "invalidas": 0}
    parar = threading.Event()

    def leitor():
        while not parar.is_set():
            linhas = cat.load_table(PROBE).scan(
                selected_fields=("order_id", "last_sequence_no")).to_arrow().to_pylist()
            leituras["n"] += 1
            # Um snapshot válido nunca tem chave repetida nem sequência nula: leitura
            # parcial de um upsert copy-on-write apareceria como um dos dois.
            ids = [l["order_id"] for l in linhas]
            if len(ids) != len(set(ids)) or any(l["last_sequence_no"] is None for l in linhas):
                leituras["invalidas"] += 1
            time.sleep(0.02)

    def escritor(nome, faixa):
        for i in faixa:
            projecao = IcebergProjection(cat.load_table(PROBE), writer=nome, cat=cat)
            projecao.save([estado(f"ord_{nome}_{i:02d}", 1, writer=nome)])
            stats = projecao.commit()
            conflitos["n"] += stats.conflicts

    t_leitor = threading.Thread(target=leitor, daemon=True)
    t_leitor.start()
    threads = [
        threading.Thread(target=escritor, args=(WRITER_STREAM, range(8))),
        threading.Thread(target=escritor, args=(WRITER_REBUILD, range(8))),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    parar.set()
    t_leitor.join(timeout=5)

    final = {r["order_id"] for r in cat.load_table(PROBE).scan().to_arrow().to_pylist()}
    esperados = {f"ord_{n}_{i:02d}" for n in (WRITER_STREAM, WRITER_REBUILD)
                 for i in range(8)}
    check("as 16 escritas concorrentes chegaram todas", esperados <= final,
          f"{len(esperados & final)}/16, {conflitos['n']} conflito(s) resolvidos")
    check("o leitor nunca viu um snapshot inconsistente", leituras["invalidas"] == 0,
          f"{leituras['n']} leituras durante a escrita")
    check("houve contenção de verdade (senão a prova não provou nada)",
          conflitos["n"] > 0, f"{conflitos['n']} conflitos")

    # ---- 4. time travel ---------------------------------------------------------------
    print()
    print("4. time travel na projeção")
    sonda = cat.load_table(PROBE)
    snapshots = list(sonda.metadata.snapshots)
    primeiro = sonda.scan(snapshot_id=snapshots[0].snapshot_id).to_arrow().num_rows
    agora = sonda.scan().to_arrow().num_rows
    check("o primeiro snapshot ainda é legível e é menor", primeiro < agora,
          f"{primeiro} -> {agora} linhas em {len(snapshots)} snapshots")

    cat.drop_table(PROBE)
    print("      sonda removida.")

    real_depois = ensure_table(cat).scan(selected_fields=("order_id",)).to_arrow().num_rows
    check("a tabela de PRODUÇÃO não foi tocada pelas provas da sonda",
          real_depois == real_antes, f"{real_antes} -> {real_depois} linhas")

    # ---- 5. o DuckDB lê a tabela REAL, pelo caminho do catálogo -----------------------
    print()
    print("5. o DuckDB lê a tabela real enquanto ela é escrita")
    import duckdb

    tabela_real = ensure_table(cat)
    esperado = tabela_real.scan(selected_fields=("order_id",)).to_arrow().num_rows
    metadado = tabela_real.metadata_location

    con = duckdb.connect()
    con.execute("install iceberg"); con.execute("load iceberg")
    con.execute("install httpfs"); con.execute("load httpfs")
    host = config.endpoint.split("://", 1)[-1]
    con.execute(f"""
        create or replace secret minio (
            type s3, key_id '{config.access_key}', secret '{config.secret_key}',
            region '{config.region}', endpoint '{host}',
            use_ssl {'true' if config.endpoint.startswith('https') else 'false'},
            url_style 'path')
    """)
    lidas = con.execute(f"select count(*) from iceberg_scan('{metadado}')").fetchone()[0]
    check("o DuckDB lê pelo metadata_location do catálogo", lidas == esperado,
          f"{lidas} linhas")

    # E o leitor preso ao metadado antigo continua vendo o estado antigo, mesmo depois de
    # uma escrita nova — isolamento de snapshot atravessando dois motores.
    projecao = IcebergProjection(cat.load_table(TABLE_NAME), writer=WRITER_STREAM, cat=cat)
    alvo = tabela_real.scan(selected_fields=("order_id", "last_sequence_no")
                            ).to_arrow().to_pylist()[0]
    projecao.save([estado(alvo["order_id"], alvo["last_sequence_no"] + 100,
                          status="RETURNED")])
    projecao.commit()
    ainda = con.execute(
        f"select last_sequence_no from iceberg_scan('{metadado}') where order_id = "
        f"'{alvo['order_id']}'").fetchone()[0]
    novo_metadado = cat.load_table(TABLE_NAME).metadata_location
    agora_val = con.execute(
        f"select last_sequence_no from iceberg_scan('{novo_metadado}') where order_id = "
        f"'{alvo['order_id']}'").fetchone()[0]
    check("o metadado antigo ainda mostra o estado antigo", ainda == alvo["last_sequence_no"],
          f"antigo={ainda} novo={agora_val}")
    check("o metadado novo mostra a escrita", agora_val == alvo["last_sequence_no"] + 100)

    # desfaz a escrita de teste, para a reconciliação abaixo comparar o estado de verdade
    restaura = IcebergProjection(cat.load_table(TABLE_NAME), writer=WRITER_STREAM, cat=cat)
    linha_original = {c: alvo.get(c) for c in COLUMNS}
    original_completa = [r for r in tabela_real.scan().to_arrow().to_pylist()
                         if r["order_id"] == alvo["order_id"]][0]
    original_completa["last_sequence_no"] = alvo["last_sequence_no"] + 1000
    restaura.save([original_completa])
    restaura.commit()
    # e agora de volta ao valor certo, com o guard desativado por um upsert direto
    original_completa["last_sequence_no"] = alvo["last_sequence_no"]
    escreve_cru(cat.load_table(TABLE_NAME), [original_completa])
    del linha_original
    print("      escrita de teste desfeita.")

    # ---- 6. reconciliação: passa, e REPROVA quando forçada ---------------------------
    print()
    print("6. reconciliação dos três folds")
    relatorio = reconcile(config, cat=cat)
    check("Iceberg, Silver e OLTP concordam", relatorio["ok"],
          f"{relatorio['compared']} pedidos comparados, "
          f"{relatorio['divergence_count']} divergência(s)")

    print()
    print("   injetando uma divergência de propósito...")
    if not relatorio["ok"]:
        # SE A BASE JÁ ESTÁ DIVERGENTE, a injeção não prova nada: `not ok` seria verdade de
        # qualquer jeito. Foi exatamente esse falso-positivo que a primeira versão desta
        # prova produziu — ela "passou" porque a base estava suja, não porque detectou.
        check("a base está limpa antes da injeção (senão a injeção não prova nada)", False,
              "reconciliação já falhava; injeção pulada")
    else:
        vitima = [r for r in cat.load_table(TABLE_NAME).scan().to_arrow().to_pylist()][0]
        guardado = dict(vitima)
        adulterada = dict(vitima)
        adulterada["status"] = "CANCELLED" if vitima["status"] != "CANCELLED" else "DELIVERED"
        escreve_cru(cat.load_table(TABLE_NAME), [adulterada])
        depois_injecao = reconcile(config, cat=cat)
        # Não basta "reprovou": tem de ter reprovado POR CAUSA da linha que foi adulterada.
        apontou = any(d["order_id"] == vitima["order_id"] and d["coluna"] == "status"
                      for d in depois_injecao["divergences"])
        check("a reconciliação REPROVA a divergência injetada",
              not depois_injecao["ok"] and apontou,
              f"apontou {vitima['order_id']}.status" if apontou
              else f"reprovou, mas por outro motivo: {depois_injecao['divergences'][:1]}")
        escreve_cru(cat.load_table(TABLE_NAME), [guardado])
        restaurado = reconcile(config, cat=cat)
        check("e volta a passar depois de restaurar", restaurado["ok"],
              f"{restaurado['divergence_count']} divergência(s)")

    # ---- 7. os dois escritores marcaram presença -------------------------------------
    print()
    print("7. proveniência: os dois escritores escreveram mesmo?")
    projecao = IcebergProjection(cat.load_table(TABLE_NAME), cat=cat)
    quem = projecao.writers_seen()
    print(f"      {quem}")
    check("a tabela registra qual escritor tocou cada linha", bool(quem),
          ", ".join(f"{k}={v}" for k, v in sorted(quem.items())))

    print()
    print("=" * 78)
    if falhas:
        print(f"REPROVADO: {len(falhas)} conferência(s) falharam:")
        for nome in falhas:
            print(f"  - {nome}")
        return 1
    print("APROVADO: conflito é recusado, a fusão é monotônica, o leitor vê snapshot")
    print("estável, o DuckDB lê pelo catálogo e os três folds concordam.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
