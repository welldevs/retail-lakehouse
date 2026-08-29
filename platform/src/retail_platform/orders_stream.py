"""Transporte, replay e consumo idempotente dos eventos de pedido.

O QUE ESTE MARCO PROVA, E O QUE ELE SE RECUSA A AFIRMAR.

Instalar um broker e publicar mensagens nao prova nada — o proprio ARCHITECTURE.md ja
recusou essa versao por escrito. O que precisa ficar provado, e o que este modulo existe
para tornar demonstravel, sao quatro coisas distintas:

  1. TRANSPORTE FIEL. Todo evento do outbox chega ao topico, sem alteracao. Provado
     relendo o topico do offset 0 e reproduzindo o `sha256` dos 16 manifestos.

  2. SEMANTICA DE ENTREGA, DITA COM PRECISAO. Este pipeline e AT-LEAST-ONCE do produtor ao
     broker, e nao exactly-once. A razao e estrutural e nao tem conserto barato: marcar
     `published_at` no Postgres e receber o ack do Kafka sao duas escritas em dois sistemas,
     e nao existe transacao entre eles. A escolha esta em qual lado errar:

         publicar -> ack -> marcar     morrer no meio REPUBLICA   (at-least-once)
         marcar   -> publicar          morrer no meio PERDE       (at-most-once)

     Perder evento e irreversivel; duplicar e absorvivel a jusante. Por isso a ordem e a
     primeira, e por isso o consumidor tem de ser idempotente — nao por elegancia, por
     obrigacao.

     `enable.idempotence=true` no produtor NAO resolve isso, e conflatar as duas coisas e o
     erro mais comum aqui: ele elimina duplicata gerada por RETRY DENTRO DA SESSAO do
     produtor. Duplicata gerada por o processo morrer antes do commit do outbox esta fora do
     alcance dele.

  3. CONSUMO IDEMPOTENTE. O consumidor deduplica por `(order_id, sequence_no)` contra o
     estado que ele ja tem — nao por um conjunto de `event_id` que cresce sem limite. Isso
     e possivel PORQUE a ordem por chave e garantida: uma duplicata sempre chega depois do
     original, com `sequence_no <= last_sequence_no`. E o que torna `key = order_id` uma
     peca de carga, e nao decoracao.

     A mesma comparacao pega o caso oposto: `sequence_no > last + 1` e BURACO, e buraco e
     PERDA. O consumidor recusa em voz alta em vez de aplicar um estado que pulou etapa.

  4. REPLAY. Rebobinar o grupo para o inicio e reprocessar o topico inteiro deixa a projecao
     identica — mesmo digest. E a mesma propriedade de aditividade da Source, um nivel
     adiante e num meio que nao e disco.

O OFFSET E COMMITADO DEPOIS DA ESCRITA, nunca antes, e `enable.auto.commit` e false. Commit
automatico entregaria at-most-once sem ninguem escolher isso: o offset avanca no timer, e um
evento entre o avanco e a escrita simplesmente nao aconteceu.

A PROJECAO VIVE EM OUTRO BANCO. `live_order_state` e um READ MODEL a jusante, e nao pode
compartilhar transacao com o OLTP que e a fonte da verdade — se pudesse, a tentacao de
escrever os dois juntos apagaria a distincao entre o sistema que decide e o que observa.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_ORDERS_TOPIC", "retail.orders.events.v1")
DEFAULT_GROUP = os.environ.get("KAFKA_ORDERS_GROUP", "orders-projector")
DEFAULT_PROJECTION_DSN = os.environ.get(
    "PROJECTION_DSN", "postgresql://oltp:oltp@localhost:5433/projection"
)
DEFAULT_SEEDS_DIR = "platform/dbt/seeds"
PREMISES_SEED = "order_premises_seed.csv"


class OrdersStreamError(Exception):
    """Transporte, projecao ou semantica de entrega violada."""


# --------------------------------------------------------------------------------------
# O fold, puro: evento + estado anterior -> estado novo
# --------------------------------------------------------------------------------------
#
# TERCEIRA IMPLEMENTACAO INDEPENDENTE DO MESMO FOLD. A primeira e `silver_order.sql` (window
# functions sobre o log inteiro); a segunda e `orders_oltp.plan()` (incremental, em SQL,
# dentro de transacao); esta e incremental, em memoria, alimentada pelo broker.
#
# Tres caminhos independentes para o mesmo numero e o que faz `orders-reconcile` significar
# alguma coisa. O Marco 4 ja mostrou o retorno disso: dois folds discordando acharam dois
# defeitos que nenhum teste pegava, porque cada um era internamente coerente.

APPLY = "apply"
DUPLICATE = "duplicate"
GAP = "gap"

TERMINAL_STATES = (
    "PAYMENT_FAILED", "CANCELLED", "DELIVERY_FAILED", "RETURNED", "DELIVERED",
)

STATE_AFTER = {
    "order_placed": "PLACED",
    "order_payment_authorized": "CONFIRMED",
    "order_payment_failed": "PAYMENT_FAILED",
    "order_cancelled": "CANCELLED",
    "order_picking_started": "PICKING",
    "order_line_substituted": "PICKING",
    "order_line_removed": "PICKING",
    "order_picked": "PICKED",
    "order_dispatched": "IN_TRANSIT",
    "order_delivered": "DELIVERED",
    "order_delivery_failed": "DELIVERY_FAILED",
    "order_returned": "RETURNED",
}


def decide(last_sequence_no, event) -> str:
    """Aplicar, descartar como duplicata, ou recusar como buraco. Funcao pura.

    A DEDUPLICACAO NAO USA CONJUNTO DE `event_id`. Um conjunto cresce sem limite e obriga a
    inventar uma politica de expiracao — e toda politica de expiracao e uma janela em que a
    duplicata volta a passar. Comparar `sequence_no` com o estado que ja existe e LIMITADO
    por construcao (um inteiro por pedido) e nao tem janela.

    Isso so funciona porque a ordem por chave e garantida pelo broker. Se nao fosse, uma
    duplicata poderia chegar ANTES do original e seria classificada como buraco. E por isso
    que `key = order_id` nao e detalhe de configuracao: e a premissa desta funcao.
    """
    sequence_no = event["sequence_no"]
    if last_sequence_no is None:
        last_sequence_no = 0
    if sequence_no <= last_sequence_no:
        return DUPLICATE
    if sequence_no > last_sequence_no + 1:
        return GAP
    return APPLY


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fold_event(state, event, *, sla_minutes: int):
    """Estado novo do pedido depois deste evento. Nao muta `state`.

    Nao le banco, nao tem relogio, nao sorteia: dois consumidores com a mesma sequencia de
    eventos chegam ao mesmo estado. E o que permite o replay ser uma verificacao e nao uma
    esperanca.
    """
    kind = event["event_type"]
    if kind not in STATE_AFTER:
        raise OrdersStreamError(
            f"{event['order_id']} seq={event['sequence_no']}: event_type {kind!r} fora do "
            f"vocabulario que esta projecao conhece"
        )
    payload = event["payload"]
    occurred_at = _ts(event["occurred_at"])

    if kind == "order_placed":
        if state is not None:
            raise OrdersStreamError(f"{event['order_id']}: order_placed num pedido existente")
        state = {
            "order_id": event["order_id"],
            "wh": event["wh"],
            "order_date": occurred_at.date(),
            "customer_id": payload["customer_id"],
            "placed_at": occurred_at,
            "line_count": payload["line_count"],
            "gross_amount": payload["gross_amount"],
            "net_amount": None,
            "picked_line_count": None,
            "returned_amount": None,
            "delivered_within_slot": None,
            "substituted_lines": 0,
            "removed_lines": 0,
            "confirmed_at": None,
            "picking_started_at": None,
            "picked_at": None,
            "dispatched_at": None,
            "terminal_at": None,
            "picking_minutes": None,
            "sla_breached": None,
            "events_applied": 0,
        }
    else:
        if state is None:
            raise OrdersStreamError(
                f"{event['order_id']} seq={event['sequence_no']} {kind}: evento de um pedido "
                f"que a projecao nunca viu — order_placed nao chegou"
            )
        state = dict(state)

    state["status"] = STATE_AFTER[kind]
    state["last_sequence_no"] = event["sequence_no"]
    state["updated_at"] = occurred_at
    state["events_applied"] = state["events_applied"] + 1

    if kind == "order_payment_authorized":
        state["confirmed_at"] = occurred_at
    elif kind == "order_picking_started":
        state["picking_started_at"] = occurred_at
    elif kind == "order_line_substituted":
        state["substituted_lines"] += 1
    elif kind == "order_line_removed":
        state["removed_lines"] += 1
    elif kind == "order_picked":
        state["picked_at"] = occurred_at
        state["net_amount"] = payload["picked_amount"]
        state["picked_line_count"] = payload["picked_line_count"]
        # O UNICO CALCULO QUE ESTA PROJECAO FAZ E O LOTE NAO FARIA MAIS CEDO. Aqui ele fica
        # disponivel no instante em que o evento chega; no lote, so depois de a particao do
        # dia fechar. E a diferenca de latencia que justifica um consumidor — o mecanismo
        # existe e e mensuravel. Se ALGUEM PRECISA dessa latencia e outra pergunta, e ela
        # continua sem resposta neste repositorio: ver ARCHITECTURE.md.
        if state["picking_started_at"] is not None:
            minutes = (occurred_at - state["picking_started_at"]).total_seconds() / 60.0
            state["picking_minutes"] = round(minutes, 2)
            state["sla_breached"] = minutes > sla_minutes
    elif kind == "order_dispatched":
        state["dispatched_at"] = occurred_at
    elif kind == "order_delivered":
        state["delivered_within_slot"] = payload["delivered_within_slot"]
    elif kind == "order_returned":
        state["returned_amount"] = payload["returned_amount"]
    elif kind in ("order_cancelled", "order_payment_failed"):
        # Morreu antes da separacao: `net_amount` fica indeterminado, exatamente como no
        # OLTP e no Silver. Preencher com o valor colocado faria a coluna responder outra
        # pergunta com o mesmo nome — o defeito que o Marco 4 achou.
        pass

    if state["status"] in TERMINAL_STATES:
        state["terminal_at"] = occurred_at
    return state


def read_sla_minutes(seeds_dir: str = DEFAULT_SEEDS_DIR) -> int:
    """O limiar vem do seed de premissas, nao de uma constante deste modulo.

    Mesma disciplina da var `currency`: a premissa viaja junto do numero. Uma constante aqui
    poderia divergir do que o gerador usou, e o alerta passaria a medir outra coisa.
    """
    path = os.path.join(seeds_dir, PREMISES_SEED)
    try:
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row["premise_key"] == "sla_minutes_picking":
                    return int(float(row["value"]))
    except OSError as exc:
        raise OrdersStreamError(f"nao foi possivel ler {path}: {exc}") from exc
    raise OrdersStreamError(f"{path} nao declara sla_minutes_picking")


# --------------------------------------------------------------------------------------
# A projecao: read model, com a costura que o Marco 6 vai trocar por Iceberg
# --------------------------------------------------------------------------------------

PROJECTION_DDL = """
create table if not exists live_order_state (
    order_id             text primary key,
    wh                   text not null,
    order_date           date not null,
    customer_id          text not null,
    status               text not null,
    last_sequence_no     integer not null,
    events_applied       integer not null,
    placed_at            timestamptz not null,
    updated_at           timestamptz not null,
    confirmed_at         timestamptz,
    picking_started_at   timestamptz,
    picked_at            timestamptz,
    dispatched_at        timestamptz,
    terminal_at          timestamptz,
    line_count           integer not null,
    picked_line_count    integer,
    substituted_lines    integer not null,
    removed_lines        integer not null,
    gross_amount         numeric(12,2) not null,
    net_amount           numeric(12,2),
    returned_amount      numeric(12,2),
    delivered_within_slot boolean,
    picking_minutes      numeric(10,2),
    sla_breached         boolean
);
"""

_COLUMNS = (
    "order_id", "wh", "order_date", "customer_id", "status", "last_sequence_no",
    "events_applied", "placed_at", "updated_at", "confirmed_at", "picking_started_at",
    "picked_at", "dispatched_at", "terminal_at", "line_count", "picked_line_count",
    "substituted_lines", "removed_lines", "gross_amount", "net_amount", "returned_amount",
    "delivered_within_slot", "picking_minutes", "sla_breached",
)

_UPSERT = """
insert into live_order_state ({columns}) values ({placeholders})
on conflict (order_id) do update set {updates}
""".format(
    columns=", ".join(_COLUMNS),
    placeholders=", ".join(["%s"] * len(_COLUMNS)),
    updates=", ".join(f"{c} = excluded.{c}" for c in _COLUMNS if c != "order_id"),
)


class PostgresProjection:
    """Sink da projecao. Interface deliberadamente pequena — e a costura do Marco 6.

    `load` / `save` / `digest` e tudo que o consumidor usa. Trocar o armazenamento por uma
    tabela Iceberg com dois escritores nao muda uma linha do consumidor, e essa e a forma de
    o Marco 6 provar concorrencia sem reescrever semantica de entrega.
    """

    def __init__(self, connection):
        self._connection = connection

    def load(self, order_ids) -> dict:
        if not order_ids:
            return {}
        with self._connection.cursor() as cur:
            cur.execute(
                f"select {', '.join(_COLUMNS)} from live_order_state where order_id = any(%s)",
                (list(order_ids),),
            )
            return {row[0]: dict(zip(_COLUMNS, row)) for row in cur.fetchall()}

    def save(self, states) -> None:
        if not states:
            return
        with self._connection.cursor() as cur:
            for state in states:
                cur.execute(_UPSERT, tuple(state.get(c) for c in _COLUMNS))

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def count(self) -> int:
        with self._connection.cursor() as cur:
            cur.execute("select count(*) from live_order_state")
            return cur.fetchone()[0]

    def digest(self) -> str:
        """Impressao digital da projecao inteira, para comparar antes e depois do replay.

        Ordenada por `order_id` e serializada de forma canonica: o mesmo conteudo tem de dar
        o mesmo digest independentemente da ordem em que o scan devolveu as linhas.
        """
        with self._connection.cursor() as cur:
            cur.execute(
                f"select {', '.join(_COLUMNS)} from live_order_state order by order_id"
            )
            sha = hashlib.sha256()
            for row in cur.fetchall():
                sha.update(
                    json.dumps([str(v) for v in row], ensure_ascii=False).encode("utf-8")
                )
                sha.update(b"\n")
        return sha.hexdigest()


# --------------------------------------------------------------------------------------
# Produtor: drena o outbox
# --------------------------------------------------------------------------------------

@dataclass
class PublishResult:
    published: int = 0
    failed: int = 0
    batches: int = 0
    errors: list = field(default_factory=list)


def publish_batch(connection, producer, topic: str, batch_size: int) -> int:
    """Drena UM lote do outbox. Devolve quantas linhas foram publicadas e marcadas.

    `for update skip locked` e o que permite mais de um publisher sem coordenacao: cada um
    pega um lote diferente e nenhum espera pelo outro. `order by outbox_id` mantem a ordem
    de insercao dentro do lote, que por chave e a ordem dos eventos.

    A JANELA DE DUPLICACAO ESTA AQUI, e e explicita: entre o `flush()` (o broker ja aceitou)
    e o `commit()` (o `published_at` ja e durável) o processo pode morrer. Se morrer, o lote
    volta a fila e e republicado. Nao ha transacao entre Postgres e Kafka que feche isso — a
    alternativa seria marcar antes e PERDER, que e pior. Ver o cabecalho do modulo.
    """
    acked: dict = {}
    failures: list = []

    def callback_for(outbox_id):
        """Um callback por mensagem, fechando sobre o `outbox_id`.

        `produce()` do confluent-kafka nao carrega payload opaco de usuario, e correlacionar
        o ack pelo conteudo da mensagem seria fragil. O fechamento e o que garante que a
        linha marcada e a linha que o broker aceitou.
        """
        def on_delivery(err, _message):
            if err is not None:
                failures.append((outbox_id, str(err)))
            else:
                acked[outbox_id] = True
        return on_delivery

    with connection.cursor() as cur:
        cur.execute(
            """
            select outbox_id, order_id, event_json
              from outbox
             where published_at is null
             order by outbox_id
             for update skip locked
             limit %s
            """,
            (batch_size,),
        )
        rows = cur.fetchall()
        if not rows:
            connection.rollback()
            return 0

        for outbox_id, order_id, event_json in rows:
            producer.produce(
                topic,
                key=order_id.encode("utf-8"),
                value=event_json.encode("utf-8"),
                on_delivery=callback_for(outbox_id),
            )
        producer.flush()

        if failures:
            # Nada e marcado: o lote inteiro volta a fila. Marcar so os que passaram
            # deixaria um buraco no meio de uma chave, e buraco e o unico defeito que o
            # consumidor NAO consegue absorver.
            connection.rollback()
            raise OrdersStreamError(
                f"{len(failures)} de {len(rows)} mensagens recusadas pelo broker; "
                f"nenhuma marcada como publicada. Primeira: {failures[0][1]}"
            )

        cur.execute(
            "update outbox set published_at = now() where outbox_id = any(%s)",
            (list(acked),),
        )
        marked = cur.rowcount
    connection.commit()
    return marked


def publish(connection, producer, topic: str, *, batch_size: int = 1000,
            max_batches: int | None = None) -> PublishResult:
    result = PublishResult()
    while max_batches is None or result.batches < max_batches:
        published = publish_batch(connection, producer, topic, batch_size)
        if published == 0:
            break
        result.batches += 1
        result.published += published
    return result


def build_producer(bootstrap: str = DEFAULT_BOOTSTRAP):
    """Produtor idempotente. Importa confluent_kafka sob demanda.

    `enable.idempotence` elimina duplicata de RETRY DENTRO DA SESSAO e preserva a ordem por
    particao mesmo com varias requisicoes em voo. Ele NAO cobre o processo morrer antes do
    commit do outbox — essa duplicata e do desenho, nao do transporte, e e o consumidor que
    a absorve.

    `acks=all` e consequencia de `enable.idempotence` e nao esta escrito aqui de proposito:
    duplicar configuracao implicita e como ela passa a divergir da biblioteca.
    """
    try:
        from confluent_kafka import Producer
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise OrdersStreamError(
            "confluent-kafka nao esta instalado neste venv. Rode `make venv`."
        ) from exc
    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 20,
        "client.id": "orders-publish",
    })


# --------------------------------------------------------------------------------------
# Consumidor: projeta, deduplica, recusa buraco
# --------------------------------------------------------------------------------------

@dataclass
class ProjectResult:
    applied: int = 0
    duplicates: int = 0
    orders: int = 0
    batches: int = 0
    gaps: list = field(default_factory=list)
    sla_breaches: int = 0


def project_batch(projection, events, *, sla_minutes: int) -> ProjectResult:
    """Aplica um lote ao read model. Puro em relacao ao broker: nao sabe o que e offset.

    Separado de `project()` de proposito — e aqui que mora a semantica de deduplicacao, e ela
    tem de ser exercivel sem broker nenhum.
    """
    result = ProjectResult()
    states = projection.load({event["order_id"] for event in events})
    touched: dict = {}

    for event in events:
        order_id = event["order_id"]
        current = touched.get(order_id, states.get(order_id))
        verdict = decide(None if current is None else current["last_sequence_no"], event)
        if verdict == DUPLICATE:
            result.duplicates += 1
            continue
        if verdict == GAP:
            result.gaps.append(
                f"{order_id}: chegou seq={event['sequence_no']} com "
                f"last_sequence_no={None if current is None else current['last_sequence_no']}"
            )
            continue
        touched[order_id] = fold_event(current, event, sla_minutes=sla_minutes)
        result.applied += 1

    projection.save(list(touched.values()))
    result.orders = len(touched)
    result.sla_breaches = sum(1 for s in touched.values() if s.get("sla_breached"))
    return result


def project(consumer, projection, *, sla_minutes: int, batch_size: int = 500,
            idle_timeout: float = 5.0, max_messages: int | None = None,
            strict: bool = True) -> ProjectResult:
    """Consome ate o topico secar (ou ate `max_messages`), projetando em lotes.

    O OFFSET E COMMITADO DEPOIS DA ESCRITA. Se o processo morrer entre a escrita e o commit,
    o lote e reentregue e a deduplicacao o descarta — at-least-once na entrega, efeito
    exactly-once na projecao. Commitar antes trocaria isso por at-most-once sem ninguem
    escolher, que e o que `enable.auto.commit` faz por padrao.
    """
    total = ProjectResult()
    pending: list = []
    idle_since = None

    while True:
        message = consumer.poll(1.0)
        if message is None:
            if pending:
                _flush(projection, consumer, pending, sla_minutes, total, strict)
                pending = []
                idle_since = None
                continue
            idle_since = idle_since or time.monotonic()
            if time.monotonic() - idle_since >= idle_timeout:
                break
            continue
        if message.error():
            raise OrdersStreamError(f"erro do broker: {message.error()}")

        idle_since = None
        try:
            pending.append(json.loads(message.value().decode("utf-8")))
        except ValueError as exc:
            raise OrdersStreamError(
                f"mensagem ilegivel em {message.topic()}[{message.partition()}]"
                f"@{message.offset()}: {exc}"
            ) from exc

        if len(pending) >= batch_size:
            _flush(projection, consumer, pending, sla_minutes, total, strict)
            pending = []
        if max_messages is not None and total.applied + total.duplicates >= max_messages:
            break

    if pending:
        _flush(projection, consumer, pending, sla_minutes, total, strict)
    return total


def _flush(projection, consumer, events, sla_minutes, total, strict) -> None:
    try:
        batch = project_batch(projection, events, sla_minutes=sla_minutes)
    except Exception:
        projection.rollback()
        raise
    projection.commit()
    # SO DEPOIS DE A ESCRITA ESTAR DURAVEL. Esta ordem e a diferenca entre at-least-once e
    # at-most-once, e ela nao aparece em nenhum teste que so conte mensagens.
    consumer.commit(asynchronous=False)

    total.applied += batch.applied
    total.duplicates += batch.duplicates
    total.orders += batch.orders
    total.sla_breaches += batch.sla_breaches
    total.gaps.extend(batch.gaps)
    total.batches += 1
    if batch.gaps and strict:
        raise OrdersStreamError(
            "BURACO na sequencia de um pedido: o transporte perdeu evento. "
            + "; ".join(batch.gaps[:3])
        )


def build_consumer(bootstrap: str = DEFAULT_BOOTSTRAP, group: str = DEFAULT_GROUP,
                   *, topic: str = DEFAULT_TOPIC, from_beginning: bool = False):
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError(
            "confluent-kafka nao esta instalado neste venv. Rode `make venv`."
        ) from exc
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        # NUNCA true. Ver o cabecalho do modulo: o commit automatico anda no timer, nao na
        # escrita, e entrega at-most-once sem ninguem ter escolhido isso.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "session.timeout.ms": 45000,
        "client.id": "orders-project",
    })
    consumer.subscribe([topic])
    return consumer


# --------------------------------------------------------------------------------------
# Leitura direta do topico, para as provas
# --------------------------------------------------------------------------------------

def read_topic(bootstrap: str, topic: str, *, idle_timeout: float = 6.0) -> list:
    """Le o topico inteiro do offset 0, sem grupo persistente. So para conferencia.

    Usa um `group.id` derivado do relogio e NAO commita nada: uma conferencia que mexesse no
    offset do consumidor de producao mudaria o que ela veio medir.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("confluent-kafka nao esta instalado neste venv.") from exc

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"inspect-{stamp}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        meta = consumer.list_topics(topic, timeout=15)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise OrdersStreamError(f"topico {topic!r} nao existe no broker {bootstrap}")
        partitions = sorted(meta.topics[topic].partitions)
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])

        messages = []
        idle_since = None
        while True:
            message = consumer.poll(1.0)
            if message is None:
                idle_since = idle_since or time.monotonic()
                if time.monotonic() - idle_since >= idle_timeout:
                    break
                continue
            if message.error():
                raise OrdersStreamError(f"erro do broker: {message.error()}")
            idle_since = None
            messages.append({
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
                "value": message.value().decode("utf-8"),
            })
        return messages
    finally:
        consumer.close()


def topic_watermarks(bootstrap: str, topic: str) -> dict:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("confluent-kafka nao esta instalado neste venv.") from exc
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "watermarks"})
    try:
        meta = consumer.list_topics(topic, timeout=15)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise OrdersStreamError(f"topico {topic!r} nao existe no broker {bootstrap}")
        out = {}
        for partition in sorted(meta.topics[topic].partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            out[partition] = {"low": low, "high": high}
        return out
    finally:
        consumer.close()


def group_lag(bootstrap: str, topic: str, group: str) -> dict:
    """Lag por particao: `high watermark - offset commitado`.

    Le o offset commitado com um consumidor do MESMO grupo mas sem `subscribe`, para nao
    disparar rebalance no consumidor de producao que estiver rodando.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition, OFFSET_INVALID
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("confluent-kafka nao esta instalado neste venv.") from exc
    marks = topic_watermarks(bootstrap, topic)
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "enable.auto.commit": False})
    try:
        wanted = [TopicPartition(topic, p) for p in sorted(marks)]
        committed = consumer.committed(wanted, timeout=15)
        out = {}
        for entry in committed:
            high = marks[entry.partition]["high"]
            offset = None if entry.offset in (OFFSET_INVALID, -1001) else entry.offset
            out[entry.partition] = {
                "committed": offset,
                "high": high,
                "lag": high - (offset if offset is not None else marks[entry.partition]["low"]),
            }
        return out
    finally:
        consumer.close()


def reset_group(bootstrap: str, topic: str, group: str) -> int:
    """Rebobina o grupo para o inicio do topico. E o verbo do replay.

    Nao apaga a projecao: o ponto do replay e justamente reprocessar tudo POR CIMA do estado
    existente e o resultado nao mudar. Apagar antes provaria outra coisa (que o fold e
    deterministico), nao esta (que o consumo e idempotente).
    """
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("confluent-kafka nao esta instalado neste venv.") from exc
    marks = topic_watermarks(bootstrap, topic)
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "enable.auto.commit": False})
    try:
        offsets = [TopicPartition(topic, p, marks[p]["low"]) for p in sorted(marks)]
        consumer.commit(offsets=offsets, asynchronous=False)
        return len(offsets)
    finally:
        consumer.close()


# --------------------------------------------------------------------------------------
# Conexao com o banco da projecao
# --------------------------------------------------------------------------------------

def ensure_database(dsn: str) -> bool:
    """Cria o banco da projecao se ele nao existir. Devolve True se criou.

    `create database` nao roda dentro de transacao, entao esta e a unica conexao da
    plataforma que abre em autocommit — e ela nao escreve mais nada.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("psycopg nao esta instalado neste venv.") from exc
    from urllib.parse import urlparse

    parsed = urlparse(dsn)
    name = parsed.path.lstrip("/")
    if not name:
        raise OrdersStreamError(f"DSN sem nome de banco: {dsn}")
    admin = dsn.replace(f"/{name}", "/postgres", 1)
    with psycopg.connect(admin, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (name,))
            if cur.fetchone():
                return False
            cur.execute(f'create database "{name}"')
    return True


def projection_connect(dsn: str = None):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise OrdersStreamError("psycopg nao esta instalado neste venv.") from exc
    try:
        return psycopg.connect(dsn or DEFAULT_PROJECTION_DSN)
    except psycopg.Error as exc:
        raise OrdersStreamError(
            f"nao foi possivel conectar na projecao ({dsn or DEFAULT_PROJECTION_DSN}): {exc}\n"
            "Rode `make stream-up`."
        ) from exc


def create_projection_schema(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(PROJECTION_DDL)
    connection.commit()
