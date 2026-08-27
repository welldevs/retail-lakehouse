"""Extracao ponta a ponta: particao, manifesto e reprodutibilidade byte a byte."""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

import simulated_oltp_source
from simulated_oltp_source import MANIFEST_VERSION, SOURCE_NAME
from simulated_oltp_source.canonical import digest, read_bytes
from simulated_oltp_source.extract import EXIT_FATAL, EXIT_OK, run
from simulated_oltp_source.partition import (
    CUSTOMERS_FILE,
    customers_path,
    manifest_path,
    partition_path,
    success_path,
)

from tests import support

SRC_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(simulated_oltp_source.__file__)))


def silent(handler, args) -> int:
    with contextlib.redirect_stdout(io.StringIO()):
        return handler(args)


class ExtractTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = support.write_reference(os.path.join(self.tmp.name, "ref"))
        self.out = os.path.join(self.tmp.name, "data")

    def extract(self, **overrides) -> int:
        return silent(run, support.extract_args(self.reference, self.out, **overrides))

    def partition(self, wh=support.WH_A) -> str:
        return partition_path(self.out, "2026-08-27", wh)

    def manifest(self, wh=support.WH_A) -> dict:
        with open(manifest_path(self.partition(wh)), encoding="utf-8") as handle:
            return json.load(handle)

    def customers(self, wh=support.WH_A) -> list:
        with open(customers_path(self.partition(wh)), encoding="utf-8") as handle:
            return json.load(handle)


class ParticaoTest(ExtractTestCase):
    def test_grava_dados_manifesto_e_success(self):
        self.assertEqual(self.extract(), EXIT_OK)
        for path in (
            customers_path(self.partition()),
            manifest_path(self.partition()),
            success_path(self.partition()),
        ):
            self.assertTrue(os.path.exists(path), path)

    def test_success_guarda_o_run_id(self):
        self.extract()
        with open(success_path(self.partition()), encoding="utf-8") as handle:
            self.assertEqual(handle.read().strip(), self.manifest()["run_id"])

    def test_o_caminho_do_arquivo_no_manifesto_e_relativo_a_raiz_do_snapshot(self):
        """A plataforma resolve files[].path subindo dois niveis quando ha eixo wh=."""
        self.extract()
        entry = self.manifest()["files"][0]
        self.assertEqual(
            entry["path"],
            os.path.join("ingestion_date=2026-08-27", f"wh={support.WH_A}", CUSTOMERS_FILE),
        )
        self.assertTrue(os.path.exists(os.path.join(self.out, entry["path"])))

    def test_particao_completa_e_imutavel_sem_overwrite(self):
        self.extract()
        self.assertEqual(self.extract(), EXIT_FATAL)

    def test_overwrite_reescreve_e_registra_a_execucao_anterior(self):
        self.extract(seed=1)
        self.assertEqual(self.extract(seed=2, overwrite=True), EXIT_OK)
        historico = self.manifest()["history"]
        self.assertEqual(len(historico), 1)
        self.assertEqual(historico[0]["seed"], 1)


class ManifestoTest(ExtractTestCase):
    def setUp(self):
        super().setUp()
        self.extract()

    def test_identidade_e_versao(self):
        manifest = self.manifest()
        self.assertEqual(manifest["manifest_version"], MANIFEST_VERSION)
        self.assertEqual(manifest["source"]["name"], SOURCE_NAME)
        self.assertEqual(manifest["source"]["wh"], support.WH_A)
        self.assertEqual(manifest["partition"]["warehouse"], support.WH_A)
        self.assertEqual(manifest["partition"]["ingestion_date"], "2026-08-27")
        self.assertTrue(manifest["complete"])

    def test_a_seed_vive_no_manifesto_e_nao_em_cada_registro(self):
        """Constante da particao inteira: repetir em 200 registros seria ruido."""
        manifest = self.manifest()
        self.assertEqual(manifest["config"]["seed"], 7)
        self.assertEqual(manifest["config"]["count"], 50)
        self.assertNotIn("seed", self.customers()[0])

    def test_registra_a_proveniencia_de_cada_insumo_do_silver(self):
        reference = self.manifest()["reference"]
        self.assertEqual(reference["callejero_ingestion_date"], "2026-08-25")
        self.assertEqual(reference["population_ingestion_date"], "2026-08-26")
        self.assertEqual(reference["population_year"], 2025)
        self.assertEqual(reference["age_year"], 2022)
        self.assertEqual(reference["age_fk_periodo"], 27)
        self.assertEqual(reference["orphan_tramos_excluded"], 3)

    def test_totais_batem_com_o_arquivo_produzido(self):
        customers = self.customers()
        totals = self.manifest()["totals"]
        self.assertEqual(totals["customer_rows"], len(customers))
        self.assertEqual(
            totals["municipalities_used"],
            len({c["municipality_code"] for c in customers}),
        )
        self.assertEqual(
            totals["house_number_null"],
            sum(1 for c in customers if c["house_number"] is None),
        )

    def test_checksum_e_tamanho_declarados_batem_com_o_disco(self):
        entry = self.manifest()["files"][0]
        blob = read_bytes(customers_path(self.partition()))
        self.assertEqual(entry["sha256"], digest(blob))
        self.assertEqual(entry["bytes"], len(blob))
        self.assertEqual(entry["records"], len(self.customers()))


class ReprodutibilidadeTest(ExtractTestCase):
    def _bytes(self) -> bytes:
        return read_bytes(customers_path(self.partition()))

    def test_mesma_seed_produz_arquivo_byte_identico(self):
        self.extract(seed=13)
        primeiro = self._bytes()
        self.extract(seed=13, overwrite=True)
        self.assertEqual(primeiro, self._bytes())

    def test_seeds_diferentes_produzem_arquivos_diferentes(self):
        self.extract(seed=13)
        primeiro = self._bytes()
        self.extract(seed=14, overwrite=True)
        self.assertNotEqual(primeiro, self._bytes())

    def test_saida_nao_depende_de_pythonhashseed(self):
        """Hash de str e aleatorizado por processo. Se a geracao iterasse um set ou um
        dict reconstruido fora de ordem, a mesma seed daria saidas diferentes entre
        execucoes — e nenhum teste no mesmo processo pegaria isso."""
        programa = (
            "import json, sys;"
            "from simulated_oltp_source import reference_data;"
            "from simulated_oltp_source.customers_generator import generate;"
            "ref = reference_data.load(sys.argv[1]);"
            f"print(json.dumps(generate(ref, {support.WH_A!r}, 120, 5, '2026-08-27')))"
        )
        saidas = []
        for hashseed in ("0", "1", "12345"):
            env = dict(os.environ, PYTHONPATH=SRC_ROOT, PYTHONHASHSEED=hashseed)
            resultado = subprocess.run(
                [sys.executable, "-c", programa, self.reference],
                capture_output=True, text=True, env=env, check=True,
            )
            saidas.append(resultado.stdout)
        self.assertEqual(len(set(saidas)), 1, "a saida mudou com PYTHONHASHSEED")


class FalhaTest(ExtractTestCase):
    def test_referencia_ausente_reprova_sem_criar_particao(self):
        codigo = silent(
            run,
            support.extract_args(os.path.join(self.tmp.name, "nao-existe"), self.out),
        )
        self.assertEqual(codigo, EXIT_FATAL)
        self.assertFalse(os.path.exists(customers_path(self.partition())))

    def test_warehouse_fora_da_referencia_reprova(self):
        self.assertEqual(self.extract(wh="inexistente"), EXIT_FATAL)

    def test_count_invalido_reprova(self):
        self.assertEqual(self.extract(count=0), EXIT_FATAL)

    def test_data_malformada_reprova(self):
        self.assertEqual(self.extract(date="27-08-2026"), EXIT_FATAL)


if __name__ == "__main__":
    unittest.main()
