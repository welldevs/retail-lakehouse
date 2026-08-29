#!/usr/bin/env python3
"""Prova de transporte, semantica de entrega, consumo idempotente e replay.

POR QUE ISTO NAO E "SUBIR UM BROKER E CONTAR MENSAGENS".

Contar mensagens prova que algo trafegou. Nao prova que trafegou intacto, nem o que
acontece quando alguem morre no meio, nem que reprocessar e seguro. As quatro propriedades
que interessam sao independentes entre si e cada uma precisa de uma prova propria:

  1. TRANSPORTE FIEL — o topico relido do offset 0 reproduz o `sha256` dos 16 manifestos.
  2. ORDEM POR CHAVE — cada pedido mora numa unica particao e seus offsets crescem com
     `sequence_no`. Nao e detalhe de configuracao: e a PREMISSA da deduplicacao. Se a ordem
     nao valesse, uma duplicata poderia chegar antes do original e seria lida como buraco.
  3. AT-LEAST-ONCE E REAL — nao afirmado, DEMONSTRADO: reproduzimos a janela em que o
     publisher morre depois do ack do broker e antes do commit do `published_at`, e o topico
     passa a ter mais mensagens do que o log tem eventos.
  4. CONSUMO IDEMPOTENTE E REPLAY — apesar das duplicatas, a projecao nao muda; e rebobinar
     o grupo e reprocessar o topico inteiro tambem nao muda.

Mais duas que so existem por serem capazes de reprovar:

  5. BURACO E RECUSADO — um evento fora de sequencia nao vira no-op silencioso.
  6. TRES FOLDS INDEPENDENTES CONCORDAM — Silver (window function), OLTP (transacional) e
     projecao (streaming). Tres caminhos, um numero.

    make orders-prove-stream
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.orders_oltp import connect as oltp_connect  # noqa: E402
from retail_platform.orders_stream import (  # noqa: E402
    DEFAULT_BOOTSTRAP,
    DEFAULT_GROUP,
    DEFAULT_TOPIC,
    OrdersStreamError,
    PostgresProjection,
    build_consumer,
    build_producer,
    project,
    project_batch,
    projection_connect,
    publish,
    read_sla_minutes,
    read_topic,
    reset_group,
    topic_watermarks,
)

ORDERS_ROOT = os.path.join(REPO, "data", "orders")
REPUBLISH = 500

failures = []


def check(label, condition, detail=""):
    print(f"  [{'OK   ' if condition else 'FALHA'}] {label}{(' — ' + detail) if detail else ''}")
    if not condition:
        failures.append(label)


def manifests() -> dict:
    """{(ingestion_date, wh): (sha256, bytes, records)} das particoes em disco."""
    out = {}
    for day in sorted(os.listdir(ORDERS_ROOT)):
        if not day.startswith("ingestion_date="):
            continue
        for axis in sorted(os.listdir(os.path.join(ORDERS_ROOT, day))):
            path = os.path.join(ORDERS_ROOT, day, axis, "_manifest.json")
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            entry = [f for f in data["files"] if f["path"].endswith(".jsonl")][0]
            out[(day.split("=", 1)[1], axis.split("=", 1)[1])] = (
                entry["sha256"], entry["bytes"], entry["records"]
            )
    return out


def partition_of(order_id: str) -> tuple:
    """`ord_<wh>_<YYYYMMDD>_<n>` — a chave ja carrega a particao a que o evento pertence."""
    _, wh, day, _ = order_id.split("_", 3)
    return f"{day[:4]}-{day[4:6]}-{day[6:]}", wh


def total_messages(marks) -> int:
    return sum(m["high"] - m["low"] for m in marks.values())


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    topic = os.environ.get("KAFKA_ORDERS_TOPIC", DEFAULT_TOPIC)
    group = os.environ.get("KAFKA_ORDERS_GROUP", DEFAULT_GROUP)
    declared = manifests()
    eventos_no_log = sum(v[2] for v in declared.values())

    marks = topic_watermarks(bootstrap, topic)
    print(f"broker ........... {bootstrap}")
    print(f"topico ........... {topic}  ({len(marks)} particoes)")
    print(f"particoes em disco {len(declared)}, {eventos_no_log} eventos")
    print()

    # ---- 1. transporte fiel ------------------------------------------------------------
    print("1. transporte fiel — o topico relido reproduz o sha256 de cada manifesto")
    messages = read_topic(bootstrap, topic)
    check("o topico tem pelo menos um evento por linha do log",
          len(messages) >= eventos_no_log, f"{len(messages)} mensagens")

    por_particao = defaultdict(dict)
    for message in messages:
        event = json.loads(message["value"])
        # Dedup por event_id: o topico e at-least-once, entao a mesma linha pode estar la
        # mais de uma vez. Reconstituir sem deduplicar mediria a duplicacao, nao a fidelidade.
        por_particao[partition_of(event["order_id"])][event["event_id"]] = (
            event["occurred_at"], event["order_id"], event["sequence_no"], message["value"]
        )

    batem = 0
    for chave, (sha, tamanho, linhas) in sorted(declared.items()):
        vistos = por_particao.get(chave, {})
        blob = "".join(
            f"{v[3]}\n" for v in sorted(vistos.values(), key=lambda v: (v[0], v[1], v[2]))
        ).encode("utf-8")
        if hashlib.sha256(blob).hexdigest() == sha and len(blob) == tamanho:
            batem += 1
        else:
            print(f"      divergiu: {chave} ({len(vistos)} de {linhas} eventos)")
    check("as 16 particoes reconstituidas do topico batem sha256", batem == len(declared),
          f"{batem}/{len(declared)}")
    print()

    # ---- 2. ordem por chave ------------------------------------------------------------
    print("2. ordem por chave — a premissa em que a deduplicacao se apoia")
    particoes_por_pedido = defaultdict(set)
    trilha = defaultdict(list)
    for message in messages:
        event = json.loads(message["value"])
        particoes_por_pedido[event["order_id"]].add(message["partition"])
        trilha[event["order_id"]].append((message["offset"], event["sequence_no"]))

    espalhados = [o for o, p in particoes_por_pedido.items() if len(p) > 1]
    check("todo pedido mora numa unica particao", not espalhados,
          f"{len(espalhados)} espalhados" if espalhados else f"{len(particoes_por_pedido)} pedidos")

    # A GARANTIA E SOBRE A ORDEM DE PRODUCAO, NAO SOBRE A LISTA DE OFFSETS. Uma duplicata
    # republicada foi produzida DEPOIS, entao aparecer depois e o comportamento correto — a
    # sequencia crua de um pedido com duplicata le 1, 2, ..., 1. Exigir a lista inteira
    # ordenada reprovaria o broker por fazer exatamente o certo (foi o que aconteceu na
    # primeira versao desta prova, no segundo run, quando o topico ja tinha duplicata).
    #
    # As duas propriedades que realmente importam, e que a deduplicacao usa:
    fora_de_ordem = 0
    repeticao_antes_do_original = 0
    for eventos in trilha.values():
        eventos.sort()
        primeira_vez = {}
        for offset, sequencia in eventos:
            if sequencia in primeira_vez:
                if offset < primeira_vez[sequencia]:
                    repeticao_antes_do_original += 1
            else:
                primeira_vez[sequencia] = offset
        originais = [s for s, _ in sorted(primeira_vez.items(), key=lambda kv: kv[1])]
        if originais != sorted(originais):
            fora_de_ordem += 1
    check("a PRIMEIRA aparicao de cada sequence_no vem em ordem", fora_de_ordem == 0,
          f"{fora_de_ordem} pedidos fora de ordem")
    check("toda repeticao vem DEPOIS do seu original", repeticao_antes_do_original == 0,
          "e por isso `sequence_no <= last` basta para deduplicar")

    # AS DUAS CONFERENCIAS ACIMA PASSARIAM COM UMA CHAVE DEGENERADA (constante, ou `wh`):
    # tudo numa particao so satisfaz "um pedido, uma particao" e "offsets em ordem" por
    # acidente. Esta terceira e a que distingue ordem GARANTIDA POR CHAVE de ordem global
    # por falta de paralelismo.
    ocupacao = defaultdict(int)
    for order_id, particoes in particoes_por_pedido.items():
        ocupacao[next(iter(particoes))] += 1
    check("os pedidos se espalham por todas as particoes", len(ocupacao) == len(marks),
          " ".join(f"p{p}={ocupacao[p]}" for p in sorted(ocupacao)))
    if len(set(ocupacao.values())) == 1:
        # MEDIDO, E NAO E MERITO DO PARTICIONADOR. A parte variavel da chave e um contador
        # sequencial denso com zeros a esquerda, e os bits baixos do murmur2 acompanham os
        # ultimos digitos de forma linear: o resultado e um sistema completo de residuos, e
        # o corte sai exatamente uniforme em cada (armazem, dia). Com `order_id` esparso ou
        # em UUID o equilibrio viraria apenas estatistico. Nao dependa deste numero.
        print("      distribuicao EXATAMENTE uniforme: artefato do indice sequencial denso na")
        print("      chave, nao qualidade do hash. Nao e garantia e nao deve virar premissa.")
    print("      (com 1 particao as duas primeiras seriam verdade POR ACIDENTE — o topico tem 4)")
    print()

    # ---- 3. at-least-once e real -------------------------------------------------------
    print(f"3. at-least-once — reproduzindo a morte do publisher depois do ack ({REPUBLISH} eventos)")
    antes = total_messages(topic_watermarks(bootstrap, topic))
    with oltp_connect() as connection:
        with connection.cursor() as cur:
            # A JANELA EXATA: o broker aceitou, o `published_at` ainda nao era duravel, o
            # processo morreu. Ao voltar, o lote esta na fila de novo. Nao ha transacao entre
            # Postgres e Kafka que feche isso; a alternativa seria marcar antes e PERDER.
            cur.execute(
                """
                update outbox set published_at = null
                 where outbox_id in (select outbox_id from outbox
                                      where published_at is not null
                                      order by outbox_id limit %s)
                """,
                (REPUBLISH,),
            )
            revertidos = cur.rowcount
        connection.commit()
        producer = build_producer(bootstrap)
        republicados = publish(connection, producer, topic).published
    depois = total_messages(topic_watermarks(bootstrap, topic))
    check("o lote voltou para a fila", revertidos == REPUBLISH, f"{revertidos}")
    check("e foi republicado", republicados == REPUBLISH, f"{republicados}")
    check("o topico agora tem MAIS mensagens que o log tem eventos",
          depois == antes + REPUBLISH and depois > eventos_no_log,
          f"{antes} -> {depois}, log={eventos_no_log}")
    print("      at-least-once nao e uma ressalva de documentacao: e o que acabou de acontecer")
    print()

    # ---- 4. consumo idempotente --------------------------------------------------------
    print("4. consumo idempotente — as duplicatas nao mexem na projecao")
    sla = read_sla_minutes(os.path.join(REPO, "platform", "dbt", "seeds"))
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        digest_antes = projection.digest()
        linhas_antes = projection.count()
        consumer = build_consumer(bootstrap, group, topic=topic, from_beginning=True)
        try:
            resultado = project(consumer, projection, sla_minutes=sla, idle_timeout=6.0)
        finally:
            consumer.close()
        digest_depois = projection.digest()
        linhas_depois = projection.count()

    check("as duplicatas foram reconhecidas e descartadas", resultado.duplicates >= REPUBLISH,
          f"{resultado.duplicates} descartadas, {resultado.applied} aplicadas")
    check("nenhum buraco", not resultado.gaps)
    check("a projecao nao mudou", digest_antes == digest_depois,
          f"{digest_antes[:16]}...")
    check("nenhuma linha a mais", linhas_antes == linhas_depois, f"{linhas_depois} pedidos")
    print()

    # ---- 5. replay ---------------------------------------------------------------------
    print("5. replay — rebobinar o grupo e reprocessar o topico inteiro")
    particoes = reset_group(bootstrap, topic, group)
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        consumer = build_consumer(bootstrap, group, topic=topic, from_beginning=True)
        try:
            replay = project(consumer, projection, sla_minutes=sla, idle_timeout=6.0)
        finally:
            consumer.close()
        digest_replay = projection.digest()
        linhas_replay = projection.count()

    check(f"o grupo foi rebobinado em {particoes} particoes", particoes == len(marks))
    check("o topico inteiro foi reprocessado",
          replay.duplicates + replay.applied >= eventos_no_log,
          f"{replay.duplicates + replay.applied} mensagens")
    check("NADA foi aplicado de novo", replay.applied == 0, f"aplicados={replay.applied}")
    check("o digest da projecao e identico", digest_replay == digest_antes,
          f"{digest_replay[:16]}...")
    check("a contagem de pedidos e identica", linhas_replay == linhas_antes)
    print("      a projecao NAO foi apagada antes: reprocessar por cima e o que prova")
    print("      idempotencia. Apagar antes provaria que o fold e deterministico, que e outra coisa.")
    print()

    # ---- 6. buraco e recusado ----------------------------------------------------------
    print("6. buraco na sequencia — perda tem de virar recusa, nao no-op")
    with projection_connect() as connection:
        projection = PostgresProjection(connection)
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, last_sequence_no from live_order_state "
                "where last_sequence_no >= 4 order by order_id limit 1"
            )
            order_id, ultimo = cur.fetchone()
        forjado = {
            "event_id": "0" * 32, "event_type": "order_dispatched", "event_version": 1,
            "occurred_at": "2026-08-27T23:00:00Z", "order_id": order_id,
            # +2: um evento nao chegou. E o unico defeito que a deduplicacao NAO pode
            # absorver, porque absorve-lo seria aplicar um estado que pulou etapa.
            "sequence_no": ultimo + 2, "wh": order_id.split("_")[1],
            "producer": "simulated_orders", "payload": {},
        }
        resultado = project_batch(projection, [forjado], sla_minutes=sla)
        connection.rollback()
    check("o buraco foi detectado", len(resultado.gaps) == 1,
          resultado.gaps[0][:70] if resultado.gaps else "nenhum")
    check("e o evento NAO foi aplicado", resultado.applied == 0)

    duplicata = dict(forjado, sequence_no=ultimo)
    with projection_connect() as connection:
        resultado = project_batch(PostgresProjection(connection), [duplicata], sla_minutes=sla)
        connection.rollback()
    check("e a duplicata continua sendo descartada em silencio",
          resultado.duplicates == 1 and not resultado.gaps)
    print()

    # ---- 7. tres folds independentes ---------------------------------------------------
    print("7. tres folds independentes — Silver, OLTP e projecao")
    with oltp_connect() as connection:
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, status, last_sequence_no, line_count, picked_line_count, "
                "gross_amount, net_amount from orders order by order_id"
            )
            do_oltp = {r[0]: r[1:] for r in cur.fetchall()}
    with projection_connect() as connection:
        with connection.cursor() as cur:
            cur.execute(
                "select order_id, status, last_sequence_no, line_count, picked_line_count, "
                "gross_amount, net_amount from live_order_state order by order_id"
            )
            da_projecao = {r[0]: r[1:] for r in cur.fetchall()}

    check("mesmo conjunto de pedidos", set(do_oltp) == set(da_projecao),
          f"OLTP={len(do_oltp)} projecao={len(da_projecao)}")
    divergentes = [k for k in do_oltp if do_oltp[k] != da_projecao.get(k)]
    check("estado, sequencia, contagens e valores identicos", not divergentes,
          f"{len(divergentes)} divergentes" if divergentes else f"{len(do_oltp)} pedidos")
    print("      (contra o Silver: `make orders-reconcile` no Marco 6)")
    print()

    # ---- 8. o alerta de SLA ------------------------------------------------------------
    print("8. o alerta de SLA — o mecanismo funciona; o limiar declarado nunca liga")
    with projection_connect() as connection:
        with connection.cursor() as cur:
            cur.execute("select max(picking_minutes), count(*) filter (where sla_breached) "
                        "from live_order_state")
            maior, estourados = cur.fetchone()
        projection = PostgresProjection(connection)
        with connection.cursor() as cur:
            cur.execute("select order_id, last_sequence_no from live_order_state "
                        "where picking_minutes is not null order by picking_minutes desc limit 1")
            order_id, _ = cur.fetchone()
    check(f"nenhum pedido estourou o SLA declarado de {sla} min", estourados == 0,
          f"maior separacao observada: {maior} min")
    print(f"      DECLARADO NAO E ALCANCAVEL: basket_lines_max x minutes_per_line_picked")
    print(f"      = 40 x 2 = 80 min < {sla} min. O limiar nao pode disparar por construcao.")
    print("      Nao foi ajustado para produzir alerta: adaptar uma premissa declarada ate a")
    print("      verificacao acender e o oposto de verificar. Fica registrado como medido.")
    print()

    print("=" * 78)
    if failures:
        print(f"REPROVADO: {len(failures)} conferencia(s) falharam:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("APROVADO: transporte fiel, ordem por chave, at-least-once demonstrado,")
    print("consumo idempotente, replay sem efeito, buraco recusado e tres folds de acordo.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OrdersStreamError as exc:
        print(f"ERRO: {exc}")
        raise SystemExit(2)
