"""Extracao ponta a ponta, sem rede: cada gap fechado tem um teste que falha sem a correcao."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from ine_population_source import extract, partition as part
from ine_population_source.canonical import digest, read_json, write_json
from tests.support import extract_args, make_fetcher_factory, series, table_payload

TABLE_31304 = table_payload(series("A", "Total. Madrid. Ambos sexos.", (2025, 6779888)))
TABLE_9689 = table_payload(series("B", "Total. Barcelona. Ambos sexos.", (2025, 5716544)))
RESPONSES = {"/ES/DATOS_TABLA/31304": TABLE_31304, "/ES/DATOS_TABLA/9689": TABLE_9689}


def run_extract(out, responses=None, errors=None, **overrides):
    factory, created = make_fetcher_factory(
        dict(RESPONSES if responses is None else responses), errors
    )
    args = extract_args(out, **overrides)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = extract.run(args, fetcher_factory=factory)
    return code, created[0] if created else None, buffer.getvalue()


class ExtractionHappyPathTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_extracao_completa(self):
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 2)

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(manifest["source"]["name"], "ine_population_api")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01"})
        self.assertEqual(manifest["totals"]["series_count"], 2)
        self.assertEqual(manifest["totals"]["table_files"], 2)
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])
        self.assertIn("sha256", manifest["schema_fingerprint"])

    def test_marcador_success_criado_quando_completa(self):
        run_extract(self.root)
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_totais_batem_com_a_soma_dos_arquivos(self):
        run_extract(self.root, tables=["31304", "9689"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(
            manifest["totals"]["bytes"], sum(f["bytes"] for f in manifest["files"])
        )
        self.assertEqual(
            manifest["totals"]["table_files"],
            sum(1 for f in manifest["files"] if f["stage"] == "tables"),
        )


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def test_lista_de_tables_vazia_e_recusada(self):
        code, fetcher, _ = run_extract(self.root, tables=[])
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)

    def test_table_id_malicioso_e_recusado(self):
        self.assertEqual(run_extract(self.root, tables=["../../etc"])[0], 2)

    def test_data_invalida_e_recusada(self):
        self.assertEqual(run_extract(self.root, date="15-08-2026")[0], 2)


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def _partial(self):
        return run_extract(
            self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"}
        )

    def test_falha_parcial_devolve_1_e_nao_marca_success(self):
        code, _, _ = self._partial()
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_reexecucao_retoma_reaproveitando_o_que_ja_existe(self):
        self._partial()
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        # apenas a tabela que faltava; a ja gravada foi reaproveitada
        self.assertEqual(fetcher.requests, 1)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["series_count"], 2)

    def test_historico_preserva_a_execucao_anterior(self):
        self._partial()
        run_extract(self.root, tables=["31304", "9689"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["history"]), 1)
        self.assertFalse(manifest["history"][0]["complete"])

    def test_particao_completa_e_imutavel(self):
        run_extract(self.root)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)
        self.assertIn("imutavel", output)

    def test_overwrite_reescreve_particao_completa(self):
        run_extract(self.root)
        code, fetcher, _ = run_extract(self.root, overwrite=True)
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 1)


class CorruptAndTamperedFilesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        run_extract(self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})
        self.target = part.table_path(self.partition, 31304)

    def test_arquivo_truncado_e_rebaixado_em_vez_de_derrubar_a_execucao(self):
        with open(self.target, "wb") as handle:
            handle.write(b'[{"COD": "A", "Nomb')
        code, fetcher, output = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("ilegivel", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["series_count"], 2)

    def test_arquivo_vazio_e_rebaixado(self):
        open(self.target, "wb").close()
        self.assertEqual(run_extract(self.root, tables=["31304", "9689"])[0], 0)

    def test_arquivo_valido_mas_com_outra_forma_e_rebaixado(self):
        write_json(self.target, {"nao": "e uma lista"})
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)

    def test_arquivo_adulterado_e_rebaixado_da_fonte_e_registrado_como_anomalia(self):
        payload, _ = read_json(self.target)
        payload.append({"COD": "injetado", "Nombre": "x", "Data": [{"Anyo": 2020, "Valor": 1}]})
        write_json(self.target, payload)
        code, fetcher, output = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("divergente do manifesto anterior", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        restaurado, _ = read_json(self.target)
        codigos = [s["COD"] for s in restaurado]
        self.assertNotIn("injetado", codigos)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["anomalies"]), 1)
        self.assertEqual(
            manifest["anomalies"][0]["kind"], "conteudo_divergente_do_manifesto_anterior"
        )

    def test_adulteracao_nao_e_lavada_por_reexecucao(self):
        payload, _ = read_json(self.target)
        payload.append({"COD": "injetado", "Nombre": "x", "Data": [{"Anyo": 2020, "Valor": 1}]})
        write_json(self.target, payload)
        run_extract(self.root, tables=["31304", "9689"])
        run_extract(self.root, tables=["31304", "9689"], overwrite=True)
        restaurado, _ = read_json(self.target)
        codigos = [s["COD"] for s in restaurado]
        self.assertNotIn("injetado", codigos)


class PartitionWithoutManifestTest(unittest.TestCase):
    """Sem manifesto anterior, as travas do contrato nao podem ficar desligadas."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_manifesto_preliminar_existe_antes_de_qualquer_tabela(self):
        responses = dict(RESPONSES)
        responses["/ES/DATOS_TABLA/31304"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        manifest = part.read_manifest(self.partition)
        self.assertIsNotNone(manifest, "interrupcao deixou a particao sem manifesto")
        self.assertFalse(manifest["complete"])
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_arquivo_fora_da_forma_canonica_e_rebaixado(self):
        os.makedirs(os.path.join(self.partition, "tables"), exist_ok=True)
        alvo = part.table_path(self.partition, 31304)
        with open(alvo, "w", encoding="utf-8") as handle:
            json.dump([{"COD": "falsificado", "Nombre": "x", "Data": []}], handle, indent=4)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("forma canonica", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(
            any(a["kind"] == "arquivo_nao_canonico" for a in manifest["anomalies"])
        )
        entrada = next(f for f in manifest["files"] if f["path"].endswith("table_id=31304.json"))
        with open(alvo, "rb") as handle:
            self.assertEqual(entrada["sha256"], digest(handle.read()))

    def test_success_sem_manifesto_ainda_bloqueia_reescrita(self):
        run_extract(self.root)
        os.unlink(part.manifest_path(self.partition))
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("_SUCCESS", output)

    def test_success_sem_manifesto_cede_a_overwrite(self):
        run_extract(self.root)
        os.unlink(part.manifest_path(self.partition))
        self.assertEqual(run_extract(self.root, overwrite=True)[0], 0)


class CorruptPreviousManifestTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        run_extract(self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})

    def _corromper(self, conteudo=b"{nao e json"):
        with open(part.manifest_path(self.partition), "wb") as handle:
            handle.write(conteudo)

    def test_manifesto_ilegivel_falha_com_codigo_2_e_nao_3(self):
        self._corromper()
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("ilegivel", output)

    def test_manifesto_ilegivel_e_resgatado_por_overwrite(self):
        self._corromper()
        self.assertEqual(run_extract(self.root, overwrite=True)[0], 0)

    def test_manifesto_sem_lista_files_falha_com_codigo_2(self):
        write_json(part.manifest_path(self.partition), {"files": "nao e lista"})
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("files", output)


class InventorySweepTest(unittest.TestCase):
    """O manifesto precisa descrever a particao inteira, nao so o que a execucao tocou."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_arquivo_de_table_id_removido_da_config_continua_declarado(self):
        run_extract(self.root, tables=["31304", "9689"])
        # a proxima execucao so pede 31304; o arquivo de 9689 continua em disco
        code, _, output = run_extract(self.root, tables=["31304"], overwrite=True)
        self.assertEqual(code, 0)
        self.assertIn("[inventario]", output)
        manifest = part.read_manifest(self.partition)
        declarados = {f["table_id"] for f in manifest["files"] if f["stage"] == "tables"}
        self.assertIn("9689", declarados)
        self.assertEqual(manifest["totals"]["series_count"], 2)


class SourceShapeChangeTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_payload_que_nao_e_lista_vira_falha_registrada_nao_fatal(self):
        """Ao contrario da arvore da Mercadona, uma tabela com forma inesperada nao e
        fatal para a particao inteira: e so mais uma falha por table_id, reexecutavel."""
        code, _, _ = run_extract(self.root, responses={"/ES/DATOS_TABLA/31304": {"nao": "lista"}})
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertIn("lista JSON", manifest["failures"][0]["error"])

    def test_tabela_indisponivel_e_falha_registrada(self):
        code, _, _ = run_extract(self.root, errors={"/ES/DATOS_TABLA/31304": "HTTP 503"})
        self.assertEqual(code, 1)

    def test_serie_sem_pontos_registra_zero_pontos_mas_conta_como_serie(self):
        responses = {"/ES/DATOS_TABLA/31304": [{"COD": "A", "Nombre": "x", "Data": []}]}
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(manifest["totals"]["series_count"], 1)
        self.assertEqual(manifest["totals"]["data_point_count"], 0)


if __name__ == "__main__":
    unittest.main()
