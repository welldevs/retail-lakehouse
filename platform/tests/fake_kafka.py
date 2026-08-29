"""Duplos de produtor e consumidor Kafka que GRAVAM A ORDEM DAS OPERACOES.

O QUE ELES PROVAM. A semantica de entrega nao e uma propriedade do broker: e uma
propriedade da ORDEM em que este codigo faz as coisas. Duas escolhas decidem tudo, e as
duas sao invisiveis para qualquer teste que so conte mensagens:

    produzir -> ack -> marcar published_at        at-least-once   (correto aqui)
    marcar   -> produzir                          at-most-once    (perde)

    escrever a projecao -> commitar o offset      at-least-once   (correto aqui)
    commitar o offset   -> escrever a projecao    at-most-once    (perde)

Trocar qualquer uma das duas continua compilando, continua passando em teste de contagem, e
so aparece como dado faltando semanas depois. Um duplo que grava o diario pega as duas,
offline, e reprova a inversao.

O QUE ELES NAO PROVAM. Que o broker preserva ordem por chave, que `enable.idempotence`
deduplica retry, ou que um grupo rebalanceia sem perder. Isso e do broker, e so se prova
contra um broker: `make orders-prove-stream`. Mesmo par de fake_pg.py com
`make orders-prove-atomicity`.
"""

from __future__ import annotations


class FakeKafkaError(Exception):
    """O que os duplos levantam no lugar de confluent_kafka.KafkaException."""


class FakeMessage:
    def __init__(self, value, *, key=None, partition=0, offset=0,
                 topic="retail.orders.events.v1", error=None):
        self._value = value if isinstance(value, bytes) else value.encode("utf-8")
        self._key = key
        self._partition = partition
        self._offset = offset
        self._topic = topic
        self._error = error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class RecordingProducer:
    """Produtor que guarda o que foi produzido e quando o flush aconteceu.

    `fail_on(key, value)` injeta recusa do broker: o callback recebe erro, e o codigo sob
    teste tem de nao marcar NADA como publicado.
    """

    def __init__(self, *, fail_on=None):
        self.journal: list[tuple] = []
        self.produced: list[dict] = []
        self._pending: list = []
        self.fail_on = fail_on
        self.flushes = 0

    def produce(self, topic, key=None, value=None, on_delivery=None, **_ignored):
        record = {"topic": topic, "key": key, "value": value}
        self.produced.append(record)
        self.journal.append(("produce", key.decode("utf-8") if key else None))
        self._pending.append((record, on_delivery))

    def flush(self, *_args, **_kwargs):
        self.flushes += 1
        self.journal.append(("flush",))
        pending, self._pending = self._pending, []
        for record, callback in pending:
            if callback is None:
                continue
            failure = None
            if self.fail_on is not None and self.fail_on(record["key"], record["value"]):
                failure = "Broker: injecao de teste"
            callback(failure, FakeMessage(record["value"], key=record["key"]))
        return 0

    @property
    def keys(self) -> list:
        return [r["key"].decode("utf-8") for r in self.produced if r["key"]]

    @property
    def values(self) -> list:
        return [r["value"].decode("utf-8") for r in self.produced]


class RecordingConsumer:
    """Consumidor com roteiro. `journal` grava poll/commit/close na ordem em que ocorrem.

    `poll` devolve as mensagens do roteiro e depois None para sempre, que e como o codigo
    sob teste descobre que o topico secou.
    """

    def __init__(self, messages=(), *, commit_fails=False, journal=None):
        self._messages = list(messages)
        # Diario COMPARTILHAVEL com a projecao. A ordem entre "escreveu" e "commitou o
        # offset" e a propriedade sob teste, e ela nao existe em dois diarios separados —
        # reconstruir a intercalacao depois seria inventar a evidencia.
        self.journal: list[tuple] = journal if journal is not None else []
        self.commits = 0
        self.closed = False
        self.commit_fails = commit_fails

    def poll(self, _timeout=None):
        if self._messages:
            message = self._messages.pop(0)
            self.journal.append(("consumidor:poll", message.offset()))
            return message
        self.journal.append(("consumidor:poll", None))
        return None

    def commit(self, *_args, **kwargs):
        if self.commit_fails:
            raise FakeKafkaError("commit de offset recusado")
        # `asynchronous=False` e obrigatorio: um commit assincrono voltaria antes de o
        # offset estar durável, e a garantia dependeria de sorte no tempo.
        self.journal.append(("consumidor:commit-offset", kwargs.get("asynchronous")))
        self.commits += 1

    def close(self):
        self.journal.append(("consumidor:close",))
        self.closed = True

    def subscribe(self, topics):
        self.journal.append(("consumidor:subscribe", tuple(topics)))


class RecordingProjection:
    """Sink da projecao que grava save/commit/rollback, com estado em memoria.

    Implementa a mesma interface pequena de `PostgresProjection` — que e a costura que o
    Marco 6 troca por Iceberg. Se esta classe deixar de servir, a costura vazou.
    """

    def __init__(self, *, initial=None, fail_on_save=False, journal=None):
        self.state: dict = dict(initial or {})
        self.journal: list[tuple] = journal if journal is not None else []
        self.fail_on_save = fail_on_save

    def load(self, order_ids) -> dict:
        self.journal.append(("projecao:load", tuple(sorted(order_ids))))
        return {k: dict(v) for k, v in self.state.items() if k in order_ids}

    def save(self, states) -> None:
        if self.fail_on_save:
            raise FakeKafkaError("escrita da projecao recusada")
        self.journal.append(("projecao:save", tuple(sorted(s["order_id"] for s in states))))
        for entry in states:
            self.state[entry["order_id"]] = dict(entry)

    def commit(self) -> None:
        self.journal.append(("projecao:commit",))

    def rollback(self) -> None:
        self.journal.append(("projecao:rollback",))

    def count(self) -> int:
        return len(self.state)

    def digest(self) -> str:
        import hashlib
        import json

        sha = hashlib.sha256()
        for key in sorted(self.state):
            sha.update(json.dumps(
                {k: str(v) for k, v in sorted(self.state[key].items())},
                ensure_ascii=False,
            ).encode("utf-8"))
        return sha.hexdigest()


def steps(journal) -> list:
    """So as etiquetas do diario compartilhado, na ordem — o que o teste assere."""
    return [entry[0] for entry in journal]
