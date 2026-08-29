"""Caminho, imutabilidade e historico da particao."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_orders_source import partition as part


class CaminhoTest(unittest.TestCase):
    def test_monta_o_layout_hive(self):
        self.assertEqual(
            part.partition_path("data/orders", "2026-08-24", "mad1"),
            os.path.join("data/orders", "ingestion_date=2026-08-24", "wh=mad1"),
        )

    def test_recusa_travessia_de_caminho(self):
        for ruim in ("../etc", "mad1/../..", "wh=mad1/x"):
            with self.assertRaises(part.PartitionError):
                part.partition_path("data/orders", "2026-08-24", ruim)

    def test_recusa_data_malformada(self):
        for ruim in ("24-08-2026", "2026-8-4", "hoje", ""):
            with self.assertRaises(part.PartitionError):
                part.partition_path("data/orders", ruim, "mad1")

    def test_arquivo_de_dados_e_o_log(self):
        self.assertEqual(part.EVENTS_FILE, "order_events.jsonl")
        self.assertTrue(part.events_path("p").endswith("order_events.jsonl"))


class ImutabilidadeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.partition = os.path.join(self.tmp.name, "ingestion_date=2026-08-24", "wh=mad1")
        os.makedirs(self.partition)

    def _completa(self):
        part.write_manifest(self.partition, {"complete": True, "files": [], "config": {}})
        part.mark_success(self.partition, True, "run-1")

    def test_particao_nova_e_gravavel(self):
        self.assertIsNone(part.assert_writable(self.partition, overwrite=False))

    def test_particao_completa_recusa_reescrita(self):
        self._completa()
        with self.assertRaises(part.PartitionError) as caught:
            part.assert_writable(self.partition, overwrite=False)
        self.assertIn("imutavel", str(caught.exception))

    def test_overwrite_explicito_libera(self):
        self._completa()
        self.assertIsNotNone(part.assert_writable(self.partition, overwrite=True))

    def test_apagar_o_manifesto_nao_contorna_a_imutabilidade(self):
        # _SUCCESS sozinho ainda testemunha uma particao completa.
        self._completa()
        os.unlink(part.manifest_path(self.partition))
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, overwrite=False)

    def test_success_so_existe_quando_completa(self):
        part.mark_success(self.partition, True, "run-1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "run-2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


class HistoricoTest(unittest.TestCase):
    def test_sem_execucao_anterior_o_historico_e_vazio(self):
        self.assertEqual(part.build_history(None), [])

    def test_guarda_seed_e_digest_das_premissas(self):
        # As duas entradas que trocam os pedidos por tras dos mesmos order_id. Sem as duas
        # registradas, um --overwrite nao deixaria rastro.
        anterior = {
            "run_id": "run-1",
            "complete": True,
            "config": {"seed": 7, "orders": 3, "premises_sha256": "abc"},
            "totals": {"event_rows": 20, "order_rows": 3},
        }
        historico = part.build_history(anterior)
        self.assertEqual(len(historico), 1)
        self.assertEqual(historico[0]["seed"], 7)
        self.assertEqual(historico[0]["premises_sha256"], "abc")
        self.assertEqual(historico[0]["event_rows"], 20)

    def test_acumula_execucoes(self):
        anterior = {"config": {}, "totals": {}, "history": [{"run_id": "run-0"}]}
        self.assertEqual(len(part.build_history(anterior)), 2)


if __name__ == "__main__":
    unittest.main()
