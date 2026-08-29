"""O OLTP de pedidos: plano de escrita, fronteira da transacao e conferencia do outbox.

Tres camadas de teste, deliberadamente separadas:

  1. `plan()` e uma funcao PURA — evento entra, lista de escritas sai. Testada sem banco,
     sem duplo, sem nada. E onde mora toda a regra, e e por isso que ela nao le o banco.
  2. A fronteira da transacao, contra `fake_pg.FakeConnection`, que grava a ORDEM das
     chamadas. Prova que nao existe commit entre o outbox e a mudanca de estado.
  3. A conferencia do outbox (`verify_outbox`), que e a prova de fidelidade byte a byte.

O que NAO esta aqui, e nao poderia estar: que `rollback` desfaz. Isso e
`make orders-prove-atomicity`, contra um Postgres de verdade. Ver fake_pg.py.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from retail_platform.manifest import ManifestError
from retail_platform.orders_oltp import (
    STATE_AFTER,
    STATE_BEFORE,
    Check,
    OrdersOltpError,
    Statement,
    apply_event,
    apply_partition,
    outbox_statement,
    plan,
    read_log,
    verify_outbox,
)

from .fake_pg import FakeConnection, FakePgError

CANONICAL = dict(ensure_ascii=False, sort_keys=True)


def event(kind, *, order_id="ord_mad1_20260827_000001", seq=1, at="2026-08-27T07:00:00Z",
          payload=None, wh="mad1"):
    return {
        "event_id": hashlib.sha256(f"{order_id}|{seq}".encode()).hexdigest()[:32],
        "event_type": kind,
        "event_version": 1,
        "occurred_at": at,
        "order_id": order_id,
        "sequence_no": seq,
        "wh": wh,
        "producer": "simulated_orders",
        "payload": payload if payload is not None else {},
    }


PLACED_PAYLOAD = {
    "customer_id": "cust_mad1_000001",
    "customer_ingestion_date": "2026-08-27",
    "delivery_slot_start": "2026-08-27T23:00:00Z",
    "delivery_slot_end": "2026-08-28T01:00:00Z",
    "gross_amount": "10.00",
    "line_count": 2,
    "lines": [
        {"line_no": 1, "source_product_id": "1", "quantity": 2, "unit_price": "2.00",
         "category_id": 10, "subgroup_id": 100},
        {"line_no": 2, "source_product_id": "2", "quantity": 3, "unit_price": "2.00",
         "category_id": 11, "subgroup_id": 101},
    ],
}


def canonical_line(payload) -> str:
    return json.dumps(payload, **CANONICAL)


class TestPlanIsPure(unittest.TestCase):
    """A regra inteira mora aqui, e nao precisa de banco para ser exercida."""

    def test_placed_inserts_the_order_and_every_line(self):
        ops = plan(event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(len(ops), 3)  # 1 pedido + 2 linhas
        self.assertTrue(all(isinstance(op, Statement) for op in ops))
        self.assertTrue(all(op.expect_rows == 1 for op in ops))
        # line_amount = quantidade * preco unitario, em Decimal — nunca float.
        self.assertEqual(ops[1].params[-1], Decimal("4.00"))
        self.assertEqual(ops[2].params[-1], Decimal("6.00"))
        self.assertIsInstance(ops[1].params[-1], Decimal)

    def test_placed_leaves_net_amount_undetermined(self):
        """`net_amount` responde "quanto foi separado" — e um pedido recem-colocado nao
        respondeu isso ainda. Inicializar com o valor colocado faria a MESMA coluna dizer
        "quanto o pedido vale" no OLTP e "quanto foi separado" no Silver, e a reconciliacao
        do Marco 6 compararia duas perguntas diferentes achando que compara duas respostas.

        Medido quando isto estava errado: 298 dos 6.400 pedidos divergiam entre os dois
        folds — exatamente os 196 cancelados e os 102 com pagamento recusado, que morrem
        antes da separacao."""
        ops = plan(event("order_placed", payload=PLACED_PAYLOAD))
        columns = " ".join(ops[0].sql.split())
        self.assertNotIn("net_amount", columns)
        self.assertIn("gross_amount", columns)

    def test_every_event_type_of_the_contract_has_a_plan(self):
        """Vocabulario fechado. Um tipo novo na Source sem tratamento aqui reprova em voz
        alta, e nao aplica o evento como se fosse um no-op."""
        for kind in STATE_AFTER:
            payloads = {
                "order_line_substituted": {"line_no": 1, "quantity": 2,
                                           "source_product_id": "1",
                                           "substitute_source_product_id": "9",
                                           "substitute_unit_price": "3.00"},
                "order_line_removed": {"line_no": 1, "reason": "unavailable"},
                "order_picked": {"picked_line_count": 2, "picked_amount": "10.00"},
                "order_delivered": {"delivered_within_slot": True},
                "order_returned": {"returned_line_count": 1, "returned_amount": "0.70",
                                   "returned_line_no": 1},
            }
            ops = plan(event(kind, seq=2, payload=payloads.get(kind, {})))
            self.assertTrue(ops, f"{kind} sem plano de escrita")

    def test_unknown_event_type_is_refused_not_ignored(self):
        with self.assertRaises(OrdersOltpError) as ctx:
            plan(event("order_teleported", seq=2))
        self.assertIn("vocabulario", str(ctx.exception))

    def test_the_advance_carries_both_guards_in_one_where(self):
        """Ordem E estado na MESMA clausula. Conferir antes num SELECT seria corrida."""
        ops = plan(event("order_payment_authorized", seq=2))
        sql = " ".join(ops[0].sql.split())
        self.assertIn("last_sequence_no = %s", sql)
        self.assertIn("status = any(%s)", sql)
        # seq - 1 e o estado anterior esperado; a lista vem do STATE_BEFORE declarado.
        self.assertEqual(ops[0].params[-2], 1)
        self.assertEqual(ops[0].params[-1], list(STATE_BEFORE["order_payment_authorized"]))

    def test_picked_checks_the_declared_amount_against_the_oltp_state(self):
        """Se o valor declarado no evento e o estado do OLTP divergirem, a transacao cai.

        Sem isto, o fold do OLTP e o fold do Silver poderiam partir de bases diferentes — e
        `orders-reconcile`, no Marco 6, compararia duas coisas erradas e PASSARIA."""
        ops = plan(event("order_picked", seq=2,
                         payload={"picked_line_count": 2, "picked_amount": "10.00"}))
        checks = [op for op in ops if isinstance(op, Check)]
        self.assertEqual([c.expected for c in checks], [0, Decimal("10.00"), 2])

    def test_picking_fulfils_the_lines_nobody_touched(self):
        """A linha intocada so vira `fulfilled` na separacao — nao na colocacao.

        `order_picked` e o unico evento que declara separacao. Rotular `fulfilled` antes dele
        afirma um fato que o log nao tem: era o defeito que o Silver carregava em 5.508 linhas
        de 298 pedidos cancelados ou com pagamento recusado."""
        ops = plan(event("order_picked", seq=2,
                         payload={"picked_line_count": 2, "picked_amount": "10.00"}))
        flip = " ".join(ops[1].sql.split())
        self.assertIn("set status = 'fulfilled'", flip)
        self.assertIn("status = 'placed'", flip)
        # E nenhuma linha pode continuar indefinida depois disso.
        self.assertEqual(ops[2].expected, 0)
        self.assertIn("sobrou linha indefinida", ops[2].detail)

    def test_dying_before_picking_closes_the_lines_as_not_picked(self):
        """Cancelamento e recusa de pagamento encerram as linhas em `not_picked`, com valor
        zero. Deixa-las `placed` guardaria um estado pendente num pedido terminal."""
        for kind in ("order_cancelled", "order_payment_failed"):
            ops = plan(event(kind, seq=2, payload={"cancelled_by": "customer",
                                                   "reason": "x", "decline_reason": "y"}))
            close = " ".join(ops[1].sql.split())
            self.assertIn("set status = 'not_picked'", close)
            self.assertIn("line_amount = 0", close)
            self.assertIn("fulfilled_source_product_id = null", close)

    def test_the_line_vocabulary_is_the_same_on_both_sides(self):
        """O MESMO vocabulario de silver_order_line.line_status, e nao um parecido: uma
        tabela de traducao entre dois folds e onde `orders-reconcile` compararia duas coisas
        erradas e passaria."""
        from retail_platform.orders_oltp import DDL
        for value in ("placed", "fulfilled", "substituted", "removed", "not_picked"):
            self.assertIn(f"'{value}'", DDL)

    def test_removed_line_keeps_no_amount_and_no_product(self):
        ops = plan(event("order_line_removed", seq=2,
                         payload={"line_no": 1, "reason": "unavailable"}))
        sql = " ".join(ops[1].sql.split())
        self.assertIn("line_amount = 0", sql)
        self.assertIn("fulfilled_source_product_id = null", sql)
        # E so mexe numa linha que ainda esta 'placed': reaplicar nao reescreve historia.
        self.assertIn("status = 'placed'", sql)

    def test_line_events_recompute_net_amount_from_the_lines(self):
        """`net_amount` e derivado das linhas do proprio OLTP, nao acumulado a mao.

        Acumular (`net = net - x`) faz o erro de UMA aplicacao virar erro permanente;
        recomputar da soma faz o estado se autocorrigir e torna a reconciliacao com o
        Silver uma comparacao entre dois folds independentes, que e o ponto dela."""
        for kind, payload in (
            ("order_line_substituted", {"line_no": 1, "quantity": 2, "source_product_id": "1",
                                        "substitute_source_product_id": "9",
                                        "substitute_unit_price": "3.00"}),
            ("order_line_removed", {"line_no": 1, "reason": "unavailable"}),
        ):
            ops = plan(event(kind, seq=2, payload=payload))
            self.assertIn("sum(line_amount)", " ".join(ops[-1].sql.split()))

    def test_timestamps_are_parsed_in_python_not_left_to_the_server(self):
        ops = plan(event("order_payment_authorized", seq=2, at="2026-08-27T07:30:00Z"))
        self.assertEqual(ops[0].params[2],
                         datetime(2026, 8, 27, 7, 30, tzinfo=timezone.utc))


class TestOutboxStatement(unittest.TestCase):
    def test_the_outbox_carries_the_log_line_verbatim(self):
        """O que vai para o broker e A LINHA DO LOG, nao uma reserializacao dela.

        Reserializar passaria por jsonb, que reordena chaves pelo proprio criterio — e o
        `event_id` e o sha256 do manifesto deixariam de ser verificaveis ponta a ponta."""
        payload = event("order_payment_authorized", seq=2,
                        payload={"payment_method": "card", "authorized_amount": "10.00"})
        line = canonical_line(payload)
        statement = outbox_statement(payload, line)
        self.assertEqual(statement.params[-1], line)
        self.assertIn("on conflict (event_id) do nothing", " ".join(statement.sql.split()))

    def test_zero_rows_in_the_outbox_is_not_an_error(self):
        """`expect_rows=None` de proposito: 0 linhas aqui e o sinal de 'ja aplicado'."""
        statement = outbox_statement(event("order_placed", payload=PLACED_PAYLOAD), "{}")
        self.assertIsNone(statement.expect_rows)


class TestTransactionBoundary(unittest.TestCase):
    """A propriedade que justifica o Kafka: estado e evento, ou os dois, ou nenhum."""

    def _apply(self, connection, evt):
        return apply_event(connection, evt, canonical_line(evt))

    def test_outbox_and_state_share_one_transaction_with_a_single_commit_at_the_end(self):
        connection = FakeConnection()
        self.assertTrue(self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD)))
        self.assertEqual(
            connection.tags,
            ["insert:outbox", "insert:orders", "insert:order_line", "insert:order_line",
             "commit"],
        )
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)

    def test_no_commit_happens_between_the_outbox_and_the_state(self):
        """ESTE e o teste que pega o defeito real. Um `commit()` a mais depois do insert no
        outbox transformaria o padrao em dual-write sem mudar nada de aparencia: o outbox
        continuaria com a linha certa, o estado continuaria certo, e uma queda entre os dois
        commits publicaria um evento que nunca aconteceu."""
        connection = FakeConnection()
        self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        commit_at = connection.tags.index("commit")
        self.assertEqual(commit_at, len(connection.tags) - 1)
        self.assertNotIn("commit", connection.tags[:commit_at])

    def test_a_failure_in_the_middle_ends_in_rollback_and_never_in_commit(self):
        connection = FakeConnection(fail_on=lambda label, _: label == "insert:order_line")
        with self.assertRaises(FakePgError):
            self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.journal[-1], ("rollback",))

    def test_a_failure_on_the_outbox_itself_rolls_back(self):
        """O caso que `make orders-prove-atomicity` reproduz contra o Postgres de verdade."""
        connection = FakeConnection(fail_on=lambda label, _: label == "insert:outbox")
        with self.assertRaises(FakePgError):
            self._apply(connection, event("order_placed", payload=PLACED_PAYLOAD))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        # E, decisivo: nenhuma escrita de estado chegou a ser emitida.
        self.assertNotIn("insert:orders", connection.tags)

    def test_an_out_of_order_event_is_refused_by_rowcount_not_ignored(self):
        """Um update que nao encontra o pedido no estado esperado afeta zero linhas e o
        Postgres NAO reclama. Sem conferir rowcount, aplicar fora de ordem seria silencioso —
        o pior modo de falha possivel numa cadeia de eventos."""
        connection = FakeConnection(rowcounts={"update:orders": 0})
        with self.assertRaises(OrdersOltpError) as ctx:
            self._apply(connection, event("order_payment_authorized", seq=2))
        self.assertIn("fora de ordem", str(ctx.exception))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_a_declared_amount_that_disagrees_with_the_oltp_kills_the_transaction(self):
        # Fila de resultados: nenhuma linha indefinida, depois uma soma que NAO fecha.
        connection = FakeConnection(
            results={"select": [[(0,)], [(Decimal("9.00"),)], [(2,)]]}
        )
        with self.assertRaises(OrdersOltpError) as ctx:
            self._apply(connection, event("order_picked", seq=2,
                                          payload={"picked_line_count": 2,
                                                   "picked_amount": "10.00"}))
        self.assertIn("nao fecha", str(ctx.exception))
        self.assertEqual(connection.commits, 0)

    def test_reapplying_an_event_writes_nothing_and_reports_it(self):
        """Idempotencia por `event_id unique`, nao por adivinhacao: o outbox e o registro de
        que o evento JA foI aplicado, e ele commita junto com o estado — entao os dois nao
        conseguem discordar sobre o que ja aconteceu."""
        evt = event("order_placed", payload=PLACED_PAYLOAD)
        connection = FakeConnection(applied_event_ids=[evt["event_id"]])
        self.assertFalse(self._apply(connection, evt))
        self.assertEqual(connection.tags, ["insert:outbox", "rollback"])
        self.assertEqual(connection.commits, 0)

    def test_the_two_guards_agree_the_state_guard_would_also_have_refused(self):
        """As duas guardas sao independentes e concordam por construcao: reaplicar um evento
        ja aplicado nao passa nem pelo outbox (conflito) nem pelo estado (a sequencia ja
        avancou). O outbox e que decide, porque `pular` e o comportamento certo num replay —
        mas se ele falhasse, a guarda de ordem ainda recusaria."""
        connection = FakeConnection(rowcounts={"update:orders": 0})
        with self.assertRaises(OrdersOltpError):
            self._apply(connection, event("order_payment_authorized", seq=2))


class TestApplyPartition(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def _partition(self, events, *, ingestion_date="2026-08-27", wh="mad1", tamper=False):
        path = os.path.join(self.root, f"ingestion_date={ingestion_date}", f"wh={wh}")
        os.makedirs(path, exist_ok=True)
        blob = "".join(canonical_line(e) + "\n" for e in events).encode("utf-8")
        with open(os.path.join(path, "order_events.jsonl"), "wb") as handle:
            handle.write(blob if not tamper else blob.replace(b"mad1", b"bcn1", 1))
        manifest = {
            "manifest_version": 1,
            "run_id": "20260828T000000Z_mad1_20260827",
            "complete": True,
            "source": {"name": "simulated_orders", "lang": "es", "wh": wh},
            "partition": {"ingestion_date": ingestion_date, "warehouse": wh},
            "totals": {"event_rows": len(events)},
            "schema_fingerprint": {},
            "files": [{
                "path": f"ingestion_date={ingestion_date}/wh={wh}/order_events.jsonl",
                "sha256": hashlib.sha256(blob).hexdigest(),
                "bytes": len(blob),
                "records": len(events),
                "stage": "order_events",
            }],
        }
        with open(os.path.join(path, "_manifest.json"), "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        open(os.path.join(path, "_SUCCESS"), "w").close()
        return path

    def test_a_tampered_log_is_refused_before_a_single_write(self):
        """Conferir depois de aplicar ja e tarde: o outbox e o que vai para o broker."""
        path = self._partition([event("order_placed", payload=PLACED_PAYLOAD)], tamper=True)
        connection = FakeConnection()
        with self.assertRaises(OrdersOltpError) as ctx:
            apply_partition(connection, path)
        self.assertIn("diverge do manifesto", str(ctx.exception))
        self.assertEqual(connection.journal, [])

    def test_a_partition_of_another_source_is_refused(self):
        path = self._partition([event("order_placed", payload=PLACED_PAYLOAD)])
        manifest_file = os.path.join(path, "_manifest.json")
        with open(manifest_file, encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["source"]["name"] = "simulated_oltp"
        with open(manifest_file, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        with self.assertRaises((OrdersOltpError, ManifestError)):
            apply_partition(FakeConnection(), path)

    def test_one_transaction_per_event_not_one_per_partition(self):
        """Uma transacao por particao provaria 'o dia inteiro e atomico', que nenhuma loja
        garante. O recorte tem de ser o mesmo de um OLTP de verdade: uma mudanca de estado."""
        events = [
            event("order_placed", payload=PLACED_PAYLOAD),
            event("order_payment_authorized", seq=2, at="2026-08-27T07:10:00Z",
                  payload={"payment_method": "card", "authorized_amount": "10.00"}),
            event("order_cancelled", seq=3, at="2026-08-27T07:20:00Z",
                  payload={"cancelled_by": "customer", "reason": "changed_mind"}),
        ]
        connection = FakeConnection()
        result = apply_partition(connection, self._partition(events))
        self.assertEqual(result.events, 3)
        self.assertEqual(result.applied, 3)
        self.assertEqual(result.skipped, 0)
        self.assertEqual(result.orders, 1)
        self.assertEqual(connection.commits, 3)

    def test_replaying_the_same_partition_applies_nothing(self):
        events = [event("order_placed", payload=PLACED_PAYLOAD)]
        path = self._partition(events)
        connection = FakeConnection(applied_event_ids=[e["event_id"] for e in events])
        result = apply_partition(connection, path)
        self.assertEqual(result.applied, 0)
        self.assertEqual(result.skipped, 1)
        self.assertEqual(connection.commits, 0)

    def test_read_log_preserves_the_line_byte_for_byte(self):
        events = [event("order_placed", payload=PLACED_PAYLOAD)]
        path = self._partition(events)
        rows = read_log(path)
        self.assertEqual(rows[0][1], canonical_line(events[0]))


class TestVerifyOutbox(unittest.TestCase):
    """A prova de fidelidade: o log reconstituido do outbox reproduz o sha256 do manifesto.

    Contar linhas nao provaria isso. Somar valores tambem nao. Reproduzir os bytes prova que
    a cadeia inteira — ler, aplicar em transacao, gravar, reler, reordenar — nao perdeu, nao
    duplicou e nao alterou nada.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.events = [
            event("order_placed", payload=PLACED_PAYLOAD),
            event("order_payment_authorized", seq=2, at="2026-08-27T07:10:00Z",
                  payload={"payment_method": "card", "authorized_amount": "10.00"}),
        ]
        self.lines = [canonical_line(e) for e in self.events]
        blob = "".join(line + "\n" for line in self.lines).encode("utf-8")
        self.path = os.path.join(self._tmp.name, "ingestion_date=2026-08-27", "wh=mad1")
        os.makedirs(self.path)
        with open(os.path.join(self.path, "order_events.jsonl"), "wb") as handle:
            handle.write(blob)
        manifest = {
            "manifest_version": 1, "run_id": "r", "complete": True,
            "source": {"name": "simulated_orders", "lang": "es", "wh": "mad1"},
            "partition": {"ingestion_date": "2026-08-27", "warehouse": "mad1"},
            "totals": {}, "schema_fingerprint": {},
            "files": [{"path": "ingestion_date=2026-08-27/wh=mad1/order_events.jsonl",
                       "sha256": hashlib.sha256(blob).hexdigest(), "bytes": len(blob),
                       "records": 2, "stage": "order_events"}],
        }
        with open(os.path.join(self.path, "_manifest.json"), "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        open(os.path.join(self.path, "_SUCCESS"), "w").close()

    def test_a_faithful_outbox_reproduces_the_manifest_digest(self):
        connection = FakeConnection(results={"select": [(line,) for line in self.lines]})
        proof = verify_outbox(connection, self.path)
        self.assertTrue(proof["matches"])
        self.assertEqual(proof["outbox_sha256"], proof["declared_sha256"])
        self.assertEqual(proof["outbox_rows"], 2)

    def test_a_missing_event_makes_the_digest_diverge(self):
        connection = FakeConnection(results={"select": [(self.lines[0],)]})
        proof = verify_outbox(connection, self.path)
        self.assertFalse(proof["matches"])

    def test_a_duplicated_event_makes_the_digest_diverge(self):
        rows = [(line,) for line in self.lines] + [(self.lines[0],)]
        proof = verify_outbox(FakeConnection(results={"select": rows}), self.path)
        self.assertFalse(proof["matches"])

    def test_an_altered_event_makes_the_digest_diverge(self):
        rows = [(self.lines[0].replace('"quantity": 2', '"quantity": 3'),),
                (self.lines[1],)]
        proof = verify_outbox(FakeConnection(results={"select": rows}), self.path)
        self.assertFalse(proof["matches"])

    def test_the_rebuild_orders_by_the_canonical_key_not_by_insertion(self):
        """`outbox_id` e ordem de INSERCAO. Hoje as duas coincidem; depender da coincidencia
        faria esta conferencia passar por sorte no dia em que um replay parcial reinserisse
        um evento antigo depois de um recente."""
        connection = FakeConnection(results={"select": [(line,) for line in self.lines]})
        verify_outbox(connection, self.path)
        sql = " ".join(connection.journal[0][1:2])  # etiqueta
        self.assertEqual(sql, "select")
        # A clausula real: conferida no proprio modulo, aqui garantimos que ha um order by
        # canonico declarado e nao por outbox_id.
        from retail_platform.orders_oltp import rebuild_log
        import inspect
        source = inspect.getsource(rebuild_log)
        self.assertIn("order by o.occurred_at, o.order_id, o.sequence_no", source)
        self.assertNotIn("order by o.outbox_id", source)


if __name__ == "__main__":
    unittest.main()
