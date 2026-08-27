"""Caminho, tokens e imutabilidade da particao."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_oltp_source import MANIFEST_VERSION
from simulated_oltp_source.partition import (
    PartitionError,
    assert_writable,
    build_history,
    mark_success,
    manifest_path,
    partition_path,
    read_manifest,
    success_path,
    validate_date,
    validate_token,
    write_manifest,
)


class CaminhoTest(unittest.TestCase):
    def test_particao_usa_o_eixo_wh(self):
        """O eixo tem de ser `wh=`: e o unico que manifest.py da plataforma entende."""
        self.assertEqual(
            partition_path("raiz", "2026-08-27", "mad1"),
            os.path.join("raiz", "ingestion_date=2026-08-27", "wh=mad1"),
        )

    def test_token_com_separador_de_caminho_e_recusado(self):
        for maligno in ("../etc", "a/b", "..", "", "-comeca-com-hifen"):
            with self.subTest(token=maligno):
                with self.assertRaises(PartitionError):
                    validate_token("wh", maligno)

    def test_data_fora_do_formato_iso_e_recusada(self):
        for maligno in ("2026-8-27", "27-08-2026", "hoje", ""):
            with self.subTest(data=maligno):
                with self.assertRaises(PartitionError):
                    validate_date(maligno)

    def test_data_iso_valida_passa(self):
        self.assertEqual(validate_date("2026-08-27"), "2026-08-27")


class ImutabilidadeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.partition = partition_path(self.tmp.name, "2026-08-27", "mad1")
        os.makedirs(self.partition, exist_ok=True)

    def completar(self, seed=7):
        write_manifest(
            self.partition,
            {"run_id": "r1", "complete": True, "files": [], "config": {"seed": seed}},
        )
        mark_success(self.partition, True, "r1")

    def test_particao_nova_e_gravavel(self):
        self.assertIsNone(assert_writable(self.partition, overwrite=False))

    def test_particao_completa_e_imutavel(self):
        self.completar()
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_overwrite_libera_a_reescrita(self):
        self.completar()
        previous = assert_writable(self.partition, overwrite=True)
        self.assertEqual(previous["run_id"], "r1")

    def test_apagar_o_manifesto_nao_contorna_a_imutabilidade(self):
        """_SUCCESS sozinho ainda testemunha uma particao completa."""
        self.completar()
        os.unlink(manifest_path(self.partition))
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_manifesto_ilegivel_reprova_sem_overwrite(self):
        with open(manifest_path(self.partition), "w") as handle:
            handle.write("{ nao e json")
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_success_e_removido_quando_a_particao_nao_esta_completa(self):
        self.completar()
        mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(success_path(self.partition)))

    def test_write_manifest_carimba_a_versao(self):
        write_manifest(self.partition, {"files": []})
        self.assertEqual(read_manifest(self.partition)["manifest_version"], MANIFEST_VERSION)


class HistoricoTest(unittest.TestCase):
    def test_historico_vazio_para_particao_nova(self):
        self.assertEqual(build_history(None), [])

    def test_historico_guarda_a_seed_da_execucao_anterior(self):
        """Sem a seed no historico, um --overwrite trocaria as pessoas por tras dos
        mesmos customer_id sem deixar rastro."""
        anterior = {
            "run_id": "r1",
            "started_at_utc": "2026-08-27T00:00:00Z",
            "complete": True,
            "config": {"seed": 42, "count": 200},
            "totals": {"customer_rows": 200},
        }
        historico = build_history(anterior)
        self.assertEqual(len(historico), 1)
        self.assertEqual(historico[0]["seed"], 42)
        self.assertEqual(historico[0]["customer_rows"], 200)

    def test_historico_acumula(self):
        anterior = {"run_id": "r2", "history": [{"run_id": "r1"}], "config": {}, "totals": {}}
        self.assertEqual(len(build_history(anterior)), 2)


if __name__ == "__main__":
    unittest.main()
