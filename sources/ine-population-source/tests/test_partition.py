"""Identidade e regras da particao: tokens e imutabilidade e _SUCCESS."""

from __future__ import annotations

import os
import tempfile
import unittest

from ine_population_source import partition as part


class TokenValidationTest(unittest.TestCase):
    def test_rejeita_travessia_de_caminho(self):
        for value in ("..", "../evil", "a/b", "a\\b", "/abs", "."):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_token("table_id", value)

    def test_rejeita_vazio_e_tipos_errados(self):
        for value in ("", None, 12, "-comeca-com-hifen"):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_token("table_id", value)

    def test_aceita_table_ids_reais(self):
        for value in ("31304", "9688", "9689", "9690", "9691"):
            self.assertEqual(part.validate_token("table_id", value), value)

    def test_data_precisa_ser_iso(self):
        self.assertEqual(part.validate_date("2026-08-15"), "2026-08-15")
        for value in ("2026-8-15", "15/08/2026", "hoje", "", None):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_date(value)

    def test_table_id_malicioso_nao_escapa_da_particao(self):
        with self.assertRaises(part.PartitionError):
            part.table_path("/tmp/p", "../../etc/passwd")


class PartitionPathTest(unittest.TestCase):
    def test_particao_tem_um_nivel_so_sem_segundo_eixo(self):
        path = part.partition_path("/root", "2026-08-15")
        self.assertEqual(path, os.path.join("/root", "ingestion_date=2026-08-15"))


class PartitionRulesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = part.partition_path(self.root, "2026-01-01")
        os.makedirs(self.partition, exist_ok=True)

    def _write(self, complete: bool):
        part.write_manifest(
            self.partition,
            {
                "run_id": "r1",
                "complete": complete,
                "source": {"name": "ine_population_api"},
                "totals": {"http_requests": 1, "series_count": 5},
                "files": [],
            },
        )

    def test_particao_inexistente_e_gravavel(self):
        self.assertIsNone(part.assert_writable(self.partition, False))

    def test_particao_incompleta_pode_ser_retomada(self):
        self._write(complete=False)
        self.assertIsNotNone(part.assert_writable(self.partition, False))

    def test_particao_completa_e_imutavel_sem_overwrite(self):
        self._write(complete=True)
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, False)

    def test_particao_completa_pode_ser_reescrita_com_overwrite(self):
        self._write(complete=True)
        self.assertIsNotNone(part.assert_writable(self.partition, True))

    def test_historico_acumula_execucoes(self):
        self._write(complete=False)
        previous = part.read_manifest(self.partition)
        history = part.build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["run_id"], "r1")
        self.assertEqual(history[0]["http_requests"], 1)

        previous["history"] = history
        previous["run_id"] = "r2"
        part.write_manifest(self.partition, previous)
        self.assertEqual(len(part.build_history(part.read_manifest(self.partition))), 2)

    def test_marcador_success_segue_o_estado_da_particao(self):
        part.mark_success(self.partition, True, "r1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


if __name__ == "__main__":
    unittest.main()
