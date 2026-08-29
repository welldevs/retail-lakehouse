#!/usr/bin/env python3
"""Prova de atomicidade do outbox, contra um Postgres de verdade.

POR QUE ESTE ARQUIVO EXISTE, E POR QUE ELE NAO E UM TESTE DE `make test`.

`make test` e sem rede — e invariante do repositorio. O duplo em `platform/tests/fake_pg.py`
prova, offline, que `apply_event` emite a linha do outbox e as mudancas de estado entre o
mesmo inicio e o mesmo commit. Isso pega o defeito que se comete de verdade (um `commit()`
a mais no meio), mas NAO prova que `rollback` desfaz alguma coisa: um duplo que desfaz so
demonstra que o duplo desfaz.

Atomicidade e propriedade do MOTOR. So se prova contra o motor. E a forma correta de
prova-la e a mesma que o repositorio ja usa para `verify-landing` e para cada teste dbt:
FAZER FALHAR DE PROPOSITO e conferir que o estrago nao sobreviveu.

A injecao nao mexe no codigo da plataforma — mexe no BANCO. Um trigger que levanta excecao
no insert do outbox e uma falha que o applier nao pode prever nem tratar, que e exatamente
o tipo de falha contra a qual a transacao existe.

    make orders-prove-atomicity
"""

from __future__ import annotations

import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.orders_oltp import (  # noqa: E402
    OrdersOltpError,
    apply_partition,
    connect,
    create_schema,
    drop_schema,
    read_log,
    verify_outbox,
)

BOOM = """
create or replace function retail_injected_boom() returns trigger
language plpgsql as $$ begin raise exception 'falha injetada em %', TG_TABLE_NAME; end $$;
create trigger retail_injected_boom before insert on {table}
for each row execute function retail_injected_boom();
"""

DROP_BOOM = "drop trigger if exists retail_injected_boom on {table};"

failures = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "OK   " if condition else "FALHA"
    print(f"  [{mark}] {label}{(' — ' + detail) if detail else ''}")
    if not condition:
        failures.append(label)


def counts(connection) -> tuple[int, int, int]:
    with connection.cursor() as cur:
        cur.execute("select count(*) from orders")
        orders = cur.fetchone()[0]
        cur.execute("select count(*) from order_line")
        lines = cur.fetchone()[0]
        cur.execute("select count(*) from outbox")
        outbox = cur.fetchone()[0]
    return orders, lines, outbox


def inject(connection, table: str) -> None:
    with connection.cursor() as cur:
        cur.execute(BOOM.format(table=table))
    connection.commit()


def remove(connection, table: str) -> None:
    with connection.cursor() as cur:
        cur.execute(DROP_BOOM.format(table=table))
    connection.commit()


def main(partition: str) -> int:
    events = len(read_log(partition))
    print(f"particao ......... {partition}")
    print(f"eventos no log ... {events}")
    print()

    with connect() as connection:
        print("preparando um OLTP vazio...")
        drop_schema(connection)
        create_schema(connection)
        check("o OLTP comeca vazio", counts(connection) == (0, 0, 0))
        print()

        # ---- 1. a falha esta no OUTBOX; o ESTADO nao pode sobreviver ----------------
        print("1. trigger que faz o insert no OUTBOX explodir")
        inject(connection, "outbox")
        try:
            apply_partition(connection, partition)
            check("orders-apply reprovou", False, "aplicou sem erro, o que e o defeito")
        except Exception as exc:
            check("orders-apply reprovou", True, f"{type(exc).__name__}")
        remove(connection, "outbox")
        orders, lines, outbox = counts(connection)
        check("nenhum pedido sobreviveu", orders == 0, f"orders={orders}")
        check("nenhuma linha sobreviveu", lines == 0, f"order_line={lines}")
        check("nenhuma linha de outbox", outbox == 0, f"outbox={outbox}")
        print()

        # ---- 2. a falha esta no ESTADO; o OUTBOX nao pode sobreviver ----------------
        # A direcao inversa importa tanto quanto a primeira, e e a que se esquece: um
        # outbox que sobrevivesse a um estado que nao mudou PUBLICARIA UM EVENTO QUE NUNCA
        # ACONTECEU. Provar so um dos lados provaria metade do padrao.
        print("2. trigger que faz o insert em ORDERS explodir")
        inject(connection, "orders")
        try:
            apply_partition(connection, partition)
            check("orders-apply reprovou", False, "aplicou sem erro, o que e o defeito")
        except Exception as exc:
            check("orders-apply reprovou", True, f"{type(exc).__name__}")
        remove(connection, "orders")
        orders, lines, outbox = counts(connection)
        check("nenhum pedido sobreviveu", orders == 0, f"orders={orders}")
        check("NENHUMA linha de outbox sobreviveu", outbox == 0, f"outbox={outbox}")
        print()

        # ---- 3. sem injecao: a particao inteira entra --------------------------------
        print("3. sem injecao — a particao inteira")
        result = apply_partition(connection, partition)
        orders, lines, outbox = counts(connection)
        check("todo evento do log virou linha de outbox", outbox == events,
              f"outbox={outbox} log={events}")
        check("exatamente 1 linha de outbox por evento", result.applied == events,
              f"aplicados={result.applied}")
        check("nada foi pulado nesta primeira aplicacao", result.skipped == 0)
        check("os pedidos existem", orders == result.orders, f"orders={orders}")
        proof = verify_outbox(connection, partition)
        check("o outbox reconstitui o log byte a byte", proof["matches"],
              f"sha256 {proof['outbox_sha256'][:16]}... vs {proof['declared_sha256'][:16]}...")
        print()

        # ---- 4. replay: idempotencia por event_id unico ------------------------------
        print("4. replay da MESMA particao")
        again = apply_partition(connection, partition)
        orders2, lines2, outbox2 = counts(connection)
        check("nada foi aplicado de novo", again.applied == 0, f"aplicados={again.applied}")
        check("tudo foi reconhecido como ja aplicado", again.skipped == events)
        check("o outbox nao cresceu", outbox2 == outbox, f"{outbox} -> {outbox2}")
        check("o estado nao mudou", (orders2, lines2) == (orders, lines))
        check("o log reconstituido continua identico",
              verify_outbox(connection, partition)["matches"])
        print()

        # ---- 5. a guarda de ordem: o OLTP RECUSA evento fora de ordem ----------------
        # E esta propriedade que da sentido a `key = order_id` no Kafka do Marco 5. Se o
        # OLTP aceitasse evento fora de ordem, preservar ordem por pedido no broker seria
        # enfeite; e porque ele RECUSA que a chave de particao vira exigencia.
        print("5. guarda de ordem — evento fora de sequencia num pedido novo")
        log = read_log(partition)
        futuro = next(
            ((e, line) for e, line in log if e["sequence_no"] == 2), None
        )
        if futuro is None:
            check("ha um evento seq=2 no log para injetar", False)
        else:
            evento, linha = futuro
            forjado = dict(evento)
            forjado["order_id"] = "ord_inexistente_00000000_999999"
            forjado["event_id"] = "f" * 32
            import json as _json
            linha_forjada = _json.dumps(forjado, ensure_ascii=False, sort_keys=True)
            from retail_platform.orders_oltp import apply_event
            try:
                apply_event(connection, forjado, linha_forjada)
                check("o OLTP recusou o evento orfao", False, "aceitou, o que e o defeito")
            except OrdersOltpError as exc:
                check("o OLTP recusou o evento orfao", "fora de ordem" in str(exc),
                      str(exc)[:80])
            orders3, _, outbox3 = counts(connection)
            check("a recusa nao deixou linha de outbox para tras", outbox3 == outbox,
                  f"{outbox} -> {outbox3}")
        print()

    print("=" * 78)
    if failures:
        print(f"REPROVADO: {len(failures)} conferencia(s) falharam:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("APROVADO: estado e evento sao atomicos nas duas direcoes, o outbox reconstitui")
    print("o log byte a byte, o replay e idempotente e evento fora de ordem e recusado.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("uso: prove_oltp_atomicity.py <particao-de-pedidos>")
        raise SystemExit(2)
    try:
        raise SystemExit(main(sys.argv[1]))
    except OrdersOltpError as exc:
        print(f"ERRO: {exc}")
        raise SystemExit(2)
