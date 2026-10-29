"""Kafka producer and consumer doubles that RECORD THE ORDER OF OPERATIONS.

WHAT THEY PROVE. Delivery semantics are not a property of the broker: they are a property
of the ORDER in which this code does things. Two choices decide everything, and both are
invisible to any test that only counts messages:

    produce -> ack -> mark published_at        at-least-once   (correct here)
    mark    -> produce                         at-most-once    (loses)

    write the projection -> commit the offset  at-least-once   (correct here)
    commit the offset    -> write the projection at-most-once  (loses)

Swapping either one still compiles, still passes count tests, and only shows up as missing
data weeks later. A double that records the journal catches both, offline, and fails the
inversion.

WHAT THEY DO NOT PROVE. That the broker preserves per-key order, that `enable.idempotence`
deduplicates retries, or that a group rebalances without loss. That belongs to the broker,
and can only be proven against one: `make orders-prove-stream`. Same pairing as fake_pg.py
with `make orders-prove-atomicity`.
"""

from __future__ import annotations


class FakeKafkaError(Exception):
    """What the doubles raise in place of confluent_kafka.KafkaException."""


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
    """Producer that keeps what was produced and when the flush happened.

    `fail_on(key, value)` injects a broker rejection: the callback receives an error, and the
    code under test must mark NOTHING as published.
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
                failure = "Broker: test injection"
            callback(failure, FakeMessage(record["value"], key=record["key"]))
        return 0

    @property
    def keys(self) -> list:
        return [r["key"].decode("utf-8") for r in self.produced if r["key"]]

    @property
    def values(self) -> list:
        return [r["value"].decode("utf-8") for r in self.produced]


class RecordingConsumer:
    """Scripted consumer. `journal` records poll/commit/close in the order they occur.

    `poll` returns the scripted messages and then None forever, which is how the code under
    test discovers the topic has run dry.
    """

    def __init__(self, messages=(), *, commit_fails=False, journal=None):
        self._messages = list(messages)
        # Journal SHAREABLE with the projection. The order between "wrote" and "committed
        # the offset" is the property under test, and it does not exist in two separate
        # journals — reconstructing the interleaving afterwards would be inventing evidence.
        self.journal: list[tuple] = journal if journal is not None else []
        self.commits = 0
        self.closed = False
        self.commit_fails = commit_fails

    def poll(self, _timeout=None):
        if self._messages:
            message = self._messages.pop(0)
            self.journal.append(("consumer:poll", message.offset()))
            return message
        self.journal.append(("consumer:poll", None))
        return None

    def commit(self, *_args, **kwargs):
        if self.commit_fails:
            raise FakeKafkaError("offset commit rejected")
        # `asynchronous=False` is mandatory: an asynchronous commit would return before the
        # offset is durable, and the guarantee would depend on timing luck.
        self.journal.append(("consumer:commit-offset", kwargs.get("asynchronous")))
        self.commits += 1

    def close(self):
        self.journal.append(("consumer:close",))
        self.closed = True

    def subscribe(self, topics):
        self.journal.append(("consumer:subscribe", tuple(topics)))


class RecordingProjection:
    """Projection sink that records save/commit/rollback, with in-memory state.

    Implements the same small interface as `PostgresProjection` — which is the seam
    Milestone 6 swaps for Iceberg. If this class stops fitting, the seam has leaked.
    """

    def __init__(self, *, initial=None, fail_on_save=False, journal=None):
        self.state: dict = dict(initial or {})
        self.journal: list[tuple] = journal if journal is not None else []
        self.fail_on_save = fail_on_save

    def load(self, order_ids) -> dict:
        self.journal.append(("projection:load", tuple(sorted(order_ids))))
        return {k: dict(v) for k, v in self.state.items() if k in order_ids}

    def save(self, states) -> None:
        if self.fail_on_save:
            raise FakeKafkaError("projection write rejected")
        self.journal.append(("projection:save", tuple(sorted(s["order_id"] for s in states))))
        for entry in states:
            self.state[entry["order_id"]] = dict(entry)

    def commit(self) -> None:
        self.journal.append(("projection:commit",))

    def rollback(self) -> None:
        self.journal.append(("projection:rollback",))

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
    """Only the labels of the shared journal, in order — what the test asserts."""
    return [entry[0] for entry in journal]
