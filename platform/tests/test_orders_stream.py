"""Transporte, semantica de entrega, consumo idempotente e replay — a metade offline.

TRES CAMADAS, deliberadamente separadas, no mesmo padrao de test_orders_oltp.py:

  1. `decide` e `fold_event` sao PURAS. Toda a semantica de deduplicacao e todo o fold do
     read model moram nelas, e nao precisam de broker, banco nem duplo.
  2. A ORDEM DAS OPERACOES, contra duplos que gravam um diario COMPARTILHADO. E aqui que
     "o offset e commitado depois da escrita" deixa de ser um comentario e vira asercao.
  3. O produtor drenando o outbox, contra o duplo de Postgres do Marco 4.

O que NAO esta aqui, e nao poderia estar: que o broker preserva ordem por chave, que
duplicata republicada chega depois do original, que rebalance nao perde. Isso e do broker e
so se prova contra um: `make orders-prove-stream`.
"""

from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timezone

from retail_platform.orders_stream import (
    APPLY,
    DUPLICATE,
    GAP,
    OrdersStreamError,
    ProjectResult,
    decide,
    fold_event,
    project,
    project_batch,
    publish,
    publish_batch,
    read_sla_minutes,
)

from .fake_kafka import (
    FakeKafkaError,
    FakeMessage,
    RecordingConsumer,
    RecordingProducer,
    RecordingProjection,
    steps,
)
from .fake_pg import FakeConnection

SLA = 90


def event(kind, *, order_id="ord_mad1_20260827_000001", seq=1, at="2026-08-27T07:00:00Z",
          payload=None, wh="mad1"):
    return {
        "event_id": f"{order_id}|{seq}",
        "event_type": kind,
        "event_version": 1,
        "occurred_at": at,
        "order_id": order_id,
        "sequence_no": seq,
        "wh": wh,
        "producer": "simulated_orders",
        "payload": payload if payload is not None else {},
    }


PLACED = {
    "customer_id": "cust_mad1_000001",
    "customer_ingestion_date": "2026-08-27",
    "delivery_slot_start": "2026-08-27T23:00:00Z",
    "delivery_slot_end": "2026-08-28T01:00:00Z",
    "gross_amount": "10.00",
    "line_count": 2,
    "lines": [],
}


def placed(order_id="ord_mad1_20260827_000001"):
    return event("order_placed", order_id=order_id, payload=PLACED)


class TestDecideIsPure(unittest.TestCase):
    """A deduplicacao inteira mora numa funcao de tres linhas, e e testavel sem nada."""

    def test_the_next_event_applies(self):
        self.assertEqual(decide(3, event("order_dispatched", seq=4)), APPLY)

    def test_a_first_event_applies_against_an_unknown_order(self):
        self.assertEqual(decide(None, placed()), APPLY)

    def test_an_already_seen_sequence_is_a_duplicate(self):
        for last, seq in ((5, 5), (5, 4), (5, 1)):
            self.assertEqual(decide(last, event("order_dispatched", seq=seq)), DUPLICATE)

    def test_a_skipped_sequence_is_a_gap_not_a_duplicate(self):
        """Buraco e PERDA. Confundi-lo com duplicata faria o consumidor descartar em silencio
        exatamente o caso que ele existe para denunciar."""
        self.assertEqual(decide(5, event("order_dispatched", seq=7)), GAP)

    def test_dedup_needs_no_growing_set_of_event_ids(self):
        """A dedup e LIMITADA por construcao: um inteiro por pedido, nao um conjunto que
        cresce sem limite e obriga a inventar politica de expiracao — e toda politica de
        expiracao e uma janela em que a duplicata volta a passar.

        Isso so vale porque a ordem por chave e garantida: uma duplicata sempre chega DEPOIS
        do original. Se nao chegasse, ela seria classificada como buraco. A premissa e
        provada contra o broker de verdade, nao aqui."""
        import ast
        import inspect

        from retail_platform import orders_stream

        # SO O CORPO, sem a docstring — que fala de `event_id` justamente para explicar por
        # que ele NAO e usado. Conferir o texto inteiro reprovaria o comentario, nao o codigo.
        tree = ast.parse(inspect.getsource(orders_stream.decide).lstrip())
        corpo = tree.body[0].body
        if isinstance(corpo[0], ast.Expr) and isinstance(corpo[0].value, ast.Constant):
            corpo = corpo[1:]
        codigo = "\n".join(ast.unparse(node) for node in corpo)
        self.assertNotIn("event_id", codigo)
        self.assertIn("sequence_no", codigo)


class TestFoldIsPure(unittest.TestCase):
    def test_placing_an_order_starts_the_state(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        self.assertEqual(state["status"], "PLACED")
        self.assertEqual(state["last_sequence_no"], 1)
        self.assertEqual(state["events_applied"], 1)
        # Indeterminado ate a separacao — a mesma disciplina do OLTP e do Silver. Preencher
        # com o valor colocado faria a coluna responder outra pergunta com o mesmo nome.
        self.assertIsNone(state["net_amount"])

    def test_it_does_not_mutate_the_previous_state(self):
        """O estado anterior pode ser o que o `load` trouxe do banco. Muta-lo faria um
        rollback deixar memoria e disco discordando."""
        state = fold_event(None, placed(), sla_minutes=SLA)
        antes = dict(state)
        fold_event(state, event("order_payment_authorized", seq=2,
                                payload={"payment_method": "card"}), sla_minutes=SLA)
        self.assertEqual(state, antes)

    def test_every_event_type_of_the_contract_is_folded(self):
        payloads = {
            "order_line_substituted": {"line_no": 1},
            "order_line_removed": {"line_no": 1},
            "order_picked": {"picked_line_count": 2, "picked_amount": "10.00"},
            "order_delivered": {"delivered_within_slot": True},
            "order_returned": {"returned_amount": "0.70", "returned_line_count": 1},
        }
        from retail_platform.orders_stream import STATE_AFTER

        for kind in STATE_AFTER:
            if kind == "order_placed":
                continue
            state = fold_event(None, placed(), sla_minutes=SLA)
            got = fold_event(state, event(kind, seq=2, payload=payloads.get(kind, {})),
                             sla_minutes=SLA)
            self.assertEqual(got["status"], STATE_AFTER[kind])

    def test_an_unknown_event_type_is_refused_not_ignored(self):
        with self.assertRaises(OrdersStreamError) as ctx:
            fold_event(fold_event(None, placed(), sla_minutes=SLA),
                       event("order_teleported", seq=2), sla_minutes=SLA)
        self.assertIn("vocabulario", str(ctx.exception))

    def test_an_orphan_event_is_refused(self):
        with self.assertRaises(OrdersStreamError) as ctx:
            fold_event(None, event("order_dispatched", seq=2), sla_minutes=SLA)
        self.assertIn("nunca viu", str(ctx.exception))

    def test_line_events_count_instead_of_recomputing(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        state = fold_event(state, event("order_picking_started", seq=2), sla_minutes=SLA)
        state = fold_event(state, event("order_line_substituted", seq=3,
                                        payload={"line_no": 1}), sla_minutes=SLA)
        state = fold_event(state, event("order_line_removed", seq=4,
                                        payload={"line_no": 2}), sla_minutes=SLA)
        self.assertEqual((state["substituted_lines"], state["removed_lines"]), (1, 1))

    def test_the_sla_is_measured_between_the_two_picking_milestones(self):
        state = fold_event(None, placed(), sla_minutes=30)
        state = fold_event(state, event("order_picking_started", seq=2,
                                        at="2026-08-27T08:00:00Z"), sla_minutes=30)
        state = fold_event(state, event("order_picked", seq=3, at="2026-08-27T09:00:00Z",
                                        payload={"picked_line_count": 2,
                                                 "picked_amount": "10.00"}), sla_minutes=30)
        self.assertEqual(state["picking_minutes"], 60.0)
        self.assertTrue(state["sla_breached"])

    def test_the_declared_sla_comes_from_the_seed_not_from_a_constant(self):
        """Mesma disciplina da var `currency`: a premissa viaja junto do numero. Uma
        constante neste modulo poderia divergir do que o gerador declarou.

        ESTE TESTE JA FOI A SEGUNDA COPIA QUE ELE EXISTE PARA IMPEDIR. Ate a Fase 7 ele
        afirmava `assertEqual(read_sla_minutes(seeds), 90)` — um 90 cravado, dentro do teste
        cuja tese e que o 90 nao pode ser cravado em lugar nenhum. Quando o seed caiu para 60,
        por o limiar antigo estar acima do teto aritmetico da separacao, foi este teste que
        reprovou, e a reprovacao estava CERTA pelo motivo errado: nada havia quebrado no
        codigo, so a copia tinha envelhecido.
        """
        # A suite roda com CWD em `platform/`; o default do modulo e relativo a RAIZ do
        # repositorio, que e de onde o Makefile invoca o CLI. Resolver a partir do arquivo de
        # teste confere o CONTEUDO do seed sem depender de onde o unittest foi chamado.
        seeds = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "platform", "dbt", "seeds")

        # O VALOR VEM DO SEED, POR UM CAMINHO INDEPENDENTE. Ler o CSV com `csv` puro nao e
        # duplicar a leitura: `read_sla_minutes` e quem tem de achar a linha certa, converter
        # e falhar bem, e e isso que esta sob teste. Comparar contra o proprio arquivo afere
        # o TRAJETO — que era a tese — sem afirmar nada sobre o numero, que e premissa e pode
        # mudar por decisao de dominio a qualquer momento.
        import csv
        with open(os.path.join(seeds, "order_premises_seed.csv"), encoding="utf-8") as f:
            declarado = {linha["premise_key"]: linha["value"] for linha in csv.DictReader(f)}
        self.assertEqual(read_sla_minutes(seeds), int(float(declarado["sla_minutes_picking"])))

        # E o default continua sendo o mesmo diretorio que as outras pontes da plataforma.
        from retail_platform.oltp_reference import DEFAULT_SEEDS_DIR as PONTE
        from retail_platform.orders_stream import DEFAULT_SEEDS_DIR as STREAM
        self.assertEqual(STREAM, PONTE)

    def test_timestamps_are_parsed_in_python(self):
        state = fold_event(None, placed(), sla_minutes=SLA)
        self.assertEqual(state["placed_at"],
                         datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc))


class TestProjectBatch(unittest.TestCase):
    def test_duplicates_inside_one_batch_are_absorbed(self):
        projection = RecordingProjection()
        result = project_batch(projection, [placed(), placed()], sla_minutes=SLA)
        self.assertEqual((result.applied, result.duplicates), (1, 1))
        self.assertEqual(projection.count(), 1)

    def test_a_gap_is_reported_and_the_event_is_not_applied(self):
        projection = RecordingProjection()
        result = project_batch(
            projection,
            [placed(), event("order_dispatched", seq=4)],
            sla_minutes=SLA,
        )
        self.assertEqual(result.applied, 1)
        self.assertEqual(len(result.gaps), 1)
        self.assertIn("seq=4", result.gaps[0])

    def test_state_already_in_the_projection_is_respected(self):
        """A dedup compara com o que ja esta gravado, e nao so com o que veio no lote — senao
        um replay em lotes novos reaplicaria tudo."""
        projection = RecordingProjection()
        project_batch(projection, [placed()], sla_minutes=SLA)
        result = project_batch(projection, [placed()], sla_minutes=SLA)
        self.assertEqual((result.applied, result.duplicates), (0, 1))

    def test_it_loads_only_the_orders_of_the_batch(self):
        projection = RecordingProjection()
        project_batch(projection, [placed("ord_a"), placed("ord_b")], sla_minutes=SLA)
        self.assertEqual(projection.journal[0], ("projecao:load", ("ord_a", "ord_b")))


class TestDeliveryOrder(unittest.TestCase):
    """A propriedade que nenhum teste de contagem enxerga: a ORDEM entre escrita e commit."""

    def _messages(self, events):
        return [FakeMessage(json.dumps(e), offset=i) for i, e in enumerate(events)]

    def test_the_offset_is_committed_after_the_write_never_before(self):
        """ESTE e o teste que pega a inversao. Commitar o offset antes de a projecao estar
        durável entrega AT-MOST-ONCE sem ninguem escolher isso: uma queda entre o commit e a
        escrita apaga o evento do mundo. E o padrao de `enable.auto.commit`, que por isso e
        false neste consumidor."""
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        projection = RecordingProjection(journal=journal)
        project(consumer, projection, sla_minutes=SLA, idle_timeout=0.0)

        etapas = [e for e in steps(journal) if e != "consumidor:poll"]
        self.assertEqual(
            etapas,
            ["projecao:load", "projecao:save", "projecao:commit", "consumidor:commit-offset"],
        )

    def test_the_offset_commit_is_synchronous(self):
        """Um commit assincrono voltaria antes de o offset estar durável, e a garantia
        passaria a depender de sorte no tempo."""
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                idle_timeout=0.0)
        commit = [e for e in journal if e[0] == "consumidor:commit-offset"][0]
        self.assertIs(commit[1], False)

    def test_a_failed_write_rolls_back_and_never_commits_the_offset(self):
        journal = []
        consumer = RecordingConsumer(self._messages([placed()]), journal=journal)
        projection = RecordingProjection(journal=journal, fail_on_save=True)
        with self.assertRaises(FakeKafkaError):
            project(consumer, projection, sla_minutes=SLA, idle_timeout=0.0)
        self.assertIn("projecao:rollback", steps(journal))
        self.assertNotIn("consumidor:commit-offset", steps(journal))
        self.assertEqual(consumer.commits, 0)

    def test_a_gap_stops_the_consumer_instead_of_advancing_the_offset(self):
        """Buraco e perda de dado. Avancar o offset por cima de um buraco tornaria a perda
        permanente e invisivel — o offset seguiria em frente e ninguem reprocessaria."""
        journal = []
        consumer = RecordingConsumer(
            self._messages([placed(), event("order_dispatched", seq=5)]), journal=journal
        )
        with self.assertRaises(OrdersStreamError) as ctx:
            project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                    idle_timeout=0.0)
        self.assertIn("BURACO", str(ctx.exception))

    def test_an_unreadable_message_is_refused_not_skipped(self):
        journal = []
        consumer = RecordingConsumer([FakeMessage(b"{nao e json")], journal=journal)
        with self.assertRaises(OrdersStreamError) as ctx:
            project(consumer, RecordingProjection(journal=journal), sla_minutes=SLA,
                    idle_timeout=0.0)
        self.assertIn("ilegivel", str(ctx.exception))

    def test_replaying_the_same_messages_changes_nothing(self):
        journal = []
        eventos = [placed(), event("order_payment_authorized", seq=2,
                                   payload={"payment_method": "card"})]
        projection = RecordingProjection(journal=journal)
        project(RecordingConsumer(self._messages(eventos), journal=journal), projection,
                sla_minutes=SLA, idle_timeout=0.0)
        antes = projection.digest()
        result = project(RecordingConsumer(self._messages(eventos), journal=journal),
                         projection, sla_minutes=SLA, idle_timeout=0.0)
        self.assertEqual(projection.digest(), antes)
        self.assertEqual(result.applied, 0)
        self.assertEqual(result.duplicates, 2)


class TestPublisher(unittest.TestCase):
    """O produtor drenando o outbox, contra o duplo de Postgres do Marco 4."""

    def _connection(self, rows):
        return FakeConnection(results={"select": [(r,) for r in rows] if rows else []})

    def _rows(self, n=3):
        return [(100 + i, f"ord_mad1_20260827_00000{i}",
                 json.dumps(placed(f"ord_mad1_20260827_00000{i}"), sort_keys=True))
                for i in range(n)]

    def test_the_key_is_the_order_id_which_is_what_makes_ordering_usable(self):
        """A chave nao e detalhe de configuracao: e a premissa da deduplicacao. Com chave
        constante tudo cairia numa particao e a ordem seria global POR ACIDENTE; com chave
        por armazem, dois pedidos do mesmo armazem competiriam sem necessidade."""
        rows = self._rows()
        connection = FakeConnection(results={"select": [rows]})
        producer = RecordingProducer()
        publish_batch(connection, producer, "t", 10)
        self.assertEqual(producer.keys, [r[1] for r in rows])

    def test_nothing_is_marked_before_the_broker_acknowledges(self):
        """Marcar antes do ack seria AT-MOST-ONCE: uma queda entre a marca e o ack apagaria
        o evento. A ordem produce -> flush -> update e a escolha que troca perda por
        duplicata, e duplicata o consumidor absorve."""
        connection = FakeConnection(results={"select": [self._rows()]})
        producer = RecordingProducer()
        publish_batch(connection, producer, "t", 10)

        etapas = [e[1] if e[0] == "execute" else e[0] for e in connection.journal]
        self.assertEqual(etapas, ["select", "update:outbox", "commit"])
        self.assertEqual(producer.journal[-1], ("flush",))
        # E o flush aconteceu antes do update.
        self.assertEqual(producer.flushes, 1)

    def test_a_broker_refusal_marks_nothing_at_all(self):
        """Marcar so os que passaram deixaria um BURACO no meio de uma chave, e buraco e o
        unico defeito que o consumidor nao consegue absorver."""
        connection = FakeConnection(results={"select": [self._rows()]})
        producer = RecordingProducer(fail_on=lambda key, _v: key.endswith(b"1"))
        with self.assertRaises(OrdersStreamError) as ctx:
            publish_batch(connection, producer, "t", 10)
        self.assertIn("recusadas pelo broker", str(ctx.exception))
        self.assertIn("rollback", [e[0] for e in connection.journal])
        self.assertNotIn("update:outbox",
                         [e[1] for e in connection.journal if e[0] == "execute"])

    def test_an_empty_outbox_produces_nothing_and_holds_no_transaction(self):
        """Uma transacao deixada aberta num laco de `--follow` e como um `idle in
        transaction` nasce e segura vacuum por horas."""
        connection = FakeConnection(results={"select": [[]]})
        producer = RecordingProducer()
        self.assertEqual(publish_batch(connection, producer, "t", 10), 0)
        self.assertEqual(producer.produced, [])
        self.assertEqual(connection.journal[-1], ("rollback",))

    def test_publish_drains_until_the_outbox_is_empty(self):
        connection = FakeConnection(
            results={"select": [self._rows(2), self._rows(1), []]},
            rowcounts={"update:outbox": lambda params: len(params[0])},
        )
        result = publish(connection, RecordingProducer(), "t", batch_size=2)
        self.assertEqual(result.published, 3)
        self.assertEqual(result.batches, 2)


if __name__ == "__main__":
    unittest.main()
