"""A maquina de estados e o envelope: o contrato em codigo."""

from __future__ import annotations

import unittest

from simulated_orders_source import events as ev


class VocabularioTest(unittest.TestCase):
    def test_todo_tipo_tem_estado_antes_e_depois(self):
        self.assertEqual(set(ev.EVENT_TYPES), set(ev.STATE_AFTER))
        self.assertEqual(set(ev.EVENT_TYPES), set(ev.STATE_BEFORE))

    def test_todo_estado_resultante_esta_declarado(self):
        for tipo, estado in ev.STATE_AFTER.items():
            self.assertIn(estado, ev.STATES, f"{tipo} resulta em estado nao declarado")

    def test_so_order_placed_abre_o_agregado(self):
        abrem = [t for t, antes in ev.STATE_BEFORE.items() if None in antes]
        self.assertEqual(abrem, [ev.ORDER_PLACED])

    def test_delivered_nao_e_terminal_porque_devolucao_ainda_pode_vir(self):
        self.assertNotIn(ev.DELIVERED, ev.TERMINAL_STATES)
        self.assertIn(ev.DELIVERED, ev.STATE_BEFORE[ev.ORDER_RETURNED])

    def test_invalid_nao_e_estado_do_dominio(self):
        # O sentinela nao pode ser alcancavel por nenhuma transicao: se fosse, um pedido
        # corrompido viraria um estado analisavel em vez de um erro.
        self.assertNotIn(ev.INVALID, ev.STATES)
        self.assertNotIn(ev.INVALID, ev.STATE_AFTER.values())


class EventIdTest(unittest.TestCase):
    def test_e_deterministico(self):
        self.assertEqual(ev.event_id("ord_x_1_000001", 3), ev.event_id("ord_x_1_000001", 3))

    def test_muda_com_o_pedido_e_com_a_posicao(self):
        base = ev.event_id("ord_x_1_000001", 3)
        self.assertNotEqual(base, ev.event_id("ord_x_1_000002", 3))
        self.assertNotEqual(base, ev.event_id("ord_x_1_000001", 4))

    def test_nao_usa_relogio_nem_entropia(self):
        # Dois processos diferentes tem de concordar, senao o consumidor a jusante nao
        # consegue deduplicar entre replays.
        import subprocess
        import sys

        code = (
            "import sys; sys.path.insert(0, 'src');"
            "from simulated_orders_source.events import event_id;"
            "print(event_id('ord_mad1_20260824_000007', 5))"
        )
        saidas = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=__file__.rsplit("/tests/", 1)[0],
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 7, 4242)
        }
        self.assertEqual(len(saidas), 1, f"event_id variou entre processos: {saidas}")
        self.assertEqual(saidas.pop(), ev.event_id("ord_mad1_20260824_000007", 5))


class TransicaoTest(unittest.TestCase):
    def test_caminho_feliz(self):
        estado = None
        for tipo in (
            ev.ORDER_PLACED,
            ev.ORDER_PAYMENT_AUTHORIZED,
            ev.ORDER_PICKING_STARTED,
            ev.ORDER_PICKED,
            ev.ORDER_DISPATCHED,
            ev.ORDER_DELIVERED,
        ):
            estado = ev.apply_transition(estado, tipo)
        self.assertEqual(estado, ev.DELIVERED)

    def test_evento_depois_de_terminal_e_recusado(self):
        estado = ev.apply_transition(None, ev.ORDER_PLACED)
        estado = ev.apply_transition(estado, ev.ORDER_PAYMENT_FAILED)
        with self.assertRaises(ev.EventError) as caught:
            ev.apply_transition(estado, ev.ORDER_PICKING_STARTED)
        self.assertIn("terminal", str(caught.exception))

    def test_transicao_fora_de_ordem_e_recusada(self):
        estado = ev.apply_transition(None, ev.ORDER_PLACED)
        with self.assertRaises(ev.EventError):
            ev.apply_transition(estado, ev.ORDER_DISPATCHED)

    def test_tipo_desconhecido_e_recusado(self):
        with self.assertRaises(ev.EventError):
            ev.apply_transition(None, "order_teleported")

    def test_eventos_de_linha_nao_mexem_no_estado_do_pedido(self):
        estado = ev.PICKING
        for tipo in (ev.ORDER_LINE_SUBSTITUTED, ev.ORDER_LINE_REMOVED):
            self.assertEqual(ev.apply_transition(estado, tipo), ev.PICKING)


class FoldTest(unittest.TestCase):
    def _log(self, *tipos):
        return [{"event_type": tipo} for tipo in tipos]

    def test_strict_levanta_no_log_quebrado(self):
        with self.assertRaises(ev.EventError):
            ev.fold(self._log(ev.ORDER_PLACED, ev.ORDER_DISPATCHED))

    def test_nao_strict_devolve_sentinela_em_vez_de_explodir(self):
        # Contar um log adulterado tem de produzir numero divergente, nunca excecao: senao
        # "particao invalida" (codigo 1) viraria "excecao nao tratada" (codigo 3).
        self.assertEqual(
            ev.fold(self._log(ev.ORDER_PLACED, ev.ORDER_DISPATCHED), strict=False), ev.INVALID
        )

    def test_log_vazio_nao_tem_estado(self):
        self.assertIsNone(ev.fold([]))


class EnvelopeTest(unittest.TestCase):
    def test_traz_exatamente_os_campos_declarados(self):
        envelope = ev.envelope(
            order_id="ord_mad1_20260824_000001",
            wh="mad1",
            sequence_no=1,
            event_type=ev.ORDER_PLACED,
            occurred_at="2026-08-24T09:00:00Z",
            payload={},
            producer="simulated_orders",
        )
        self.assertEqual(sorted(envelope), sorted(ev.ENVELOPE_FIELDS))

    def test_recusa_tipo_fora_do_vocabulario(self):
        with self.assertRaises(ev.EventError):
            ev.envelope("o", "mad1", 1, "order_teleported", "2026-08-24T09:00:00Z", {}, "p")


if __name__ == "__main__":
    unittest.main()
