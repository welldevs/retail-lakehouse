"""Impressao digital estavel e totais reconferiveis."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_orders_source import events as ev
from simulated_orders_source.orders_generator import generate
from simulated_orders_source.premises import Premises
from simulated_orders_source.reference_data import load
from simulated_orders_source.schema import (
    PAYLOAD_FIELDS,
    fingerprint,
    group_by_order,
    missing_fields,
    totals_of,
)

from .support import ORDER_DATE, PREMISES, WH, write_reference


class FingerprintTest(unittest.TestCase):
    def test_cobre_todo_o_vocabulario(self):
        # A diferenca desta Source: a impressao digital e do CONTRATO DECLARADO, nao da uniao
        # das chaves observadas. Um dia sem devolucao nao teria order_returned no observado, e
        # duas particoes corretas divergiriam por sorteio.
        digital = fingerprint()
        self.assertEqual(sorted(digital["payload_keys"]), sorted(ev.EVENT_TYPES))

    def test_e_estavel_entre_chamadas(self):
        self.assertEqual(fingerprint()["sha256"], fingerprint()["sha256"])

    def test_todo_tipo_tem_payload_declarado(self):
        self.assertEqual(set(PAYLOAD_FIELDS), set(ev.EVENT_TYPES))

    def test_dois_tipos_tem_payload_vazio_de_proposito(self):
        self.assertEqual(PAYLOAD_FIELDS[ev.ORDER_PICKING_STARTED], ())
        self.assertEqual(PAYLOAD_FIELDS[ev.ORDER_DISPATCHED], ())


class MissingFieldsTest(unittest.TestCase):
    def test_denuncia_campo_de_envelope_ausente(self):
        self.assertIn("envelope.wh", missing_fields([{"event_type": ev.ORDER_DISPATCHED}]))

    def test_denuncia_campo_de_payload_ausente(self):
        evento = {f: "x" for f in ev.ENVELOPE_FIELDS}
        evento["event_type"] = ev.ORDER_PICKED
        evento["payload"] = {"picked_line_count": 1}
        self.assertIn("order_picked.picked_amount", missing_fields([evento]))

    def test_denuncia_campo_de_linha_ausente(self):
        evento = {f: "x" for f in ev.ENVELOPE_FIELDS}
        evento["event_type"] = ev.ORDER_PLACED
        evento["payload"] = {f: "x" for f in PAYLOAD_FIELDS[ev.ORDER_PLACED]}
        evento["payload"]["lines"] = [{"line_no": 1}]
        self.assertIn("line.unit_price", missing_fields([evento]))


class TotaisTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = load(write_reference(os.path.join(self.tmp.name, "ref")))
        self.events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.totals = totals_of(self.events)

    def test_contagens_fecham_com_o_log(self):
        self.assertEqual(self.totals["event_rows"], len(self.events))
        self.assertEqual(self.totals["order_rows"], len(group_by_order(self.events)))
        self.assertEqual(sum(self.totals["event_rows_by_type"].values()), len(self.events))
        self.assertEqual(sum(self.totals["orders_by_state"].values()), self.totals["order_rows"])

    def test_todo_tipo_do_vocabulario_aparece_na_contagem_mesmo_com_zero(self):
        # Se um tipo sumisse da contagem por nao ter ocorrido, comparar dois dias exigiria
        # saber quais chaves faltam em qual — e o manifesto passaria a depender do sorteio.
        self.assertEqual(sorted(self.totals["event_rows_by_type"]), sorted(ev.EVENT_TYPES))

    def test_log_quebrado_conta_em_vez_de_explodir(self):
        quebrado = [e for e in self.events if e["event_type"] != ev.ORDER_PAYMENT_AUTHORIZED]
        totals = totals_of(quebrado)  # nao levanta
        self.assertIn(ev.INVALID, totals["orders_by_state"])

    def test_log_vazio(self):
        totals = totals_of([])
        self.assertEqual(totals["event_rows"], 0)
        self.assertEqual(totals["order_rows"], 0)


if __name__ == "__main__":
    unittest.main()
