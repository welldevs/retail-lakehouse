"""Extracao ponta a ponta, sem rede: cada gap fechado tem um teste que falha sem a correcao."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from mercadona_catalog_source import extract, partition as part
from mercadona_catalog_source.canonical import digest, read_json, write_json
from tests.support import catalog, extract_args, make_fetcher_factory, tree

TREE = tree((1, "Pai", [(10, "Alfa"), (11, "Beta")]))
CATALOGS = {
    "/categories/10/": catalog(10, "Alfa", ("g1", [(1, "p1", "1.00"), (2, "p2", "2.00")])),
    "/categories/11/": catalog(11, "Beta", ("g1", [(3, "p3", "3.00")])),
}
RESPONSES = {"/categories/": TREE, **CATALOGS}


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
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_extracao_completa(self):
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 3)  # arvore + 2 categorias

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 2)
        self.assertEqual(manifest["source"]["name"], "mercadona_catalog_api")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01", "warehouse": "mad1"})
        self.assertEqual(manifest["totals"]["product_rows"], 3)
        self.assertEqual(manifest["totals"]["unique_product_ids"], 3)
        self.assertEqual(manifest["totals"]["catalog_files"], 2)
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])
        self.assertIn("sha256", manifest["schema_fingerprint"])

    def test_marcador_success_criado_quando_completa(self):
        run_extract(self.root)
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_totais_batem_com_a_soma_dos_arquivos(self):
        run_extract(self.root)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(
            manifest["totals"]["bytes"], sum(f["bytes"] for f in manifest["files"])
        )
        self.assertEqual(
            manifest["totals"]["product_rows"],
            sum(f["records"] for f in manifest["files"] if f["stage"] == "catalog"),
        )


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def test_limit_zero_e_recusado_em_vez_de_processar_tudo(self):
        code, fetcher, _ = run_extract(self.root, limit=0)
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)

    def test_limit_negativo_e_recusado(self):
        self.assertEqual(run_extract(self.root, limit=-3)[0], 2)

    def test_limit_valido_produz_particao_parcial(self):
        code, _, _ = run_extract(self.root, limit=1)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(
            os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        )
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["totals"]["catalog_files"], 1)

    def test_warehouse_malicioso_e_recusado(self):
        self.assertEqual(run_extract(self.root, wh="../../etc")[0], 2)

    def test_data_invalida_e_recusada(self):
        self.assertEqual(run_extract(self.root, date="15-08-2026")[0], 2)


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def _partial(self):
        return run_extract(self.root, errors={"/categories/11/": "HTTP 500"})

    def test_falha_parcial_devolve_1_e_nao_marca_success(self):
        code, _, _ = self._partial()
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_reexecucao_retoma_reaproveitando_o_que_ja_existe(self):
        self._partial()
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        # arvore + apenas a categoria que faltava; a ja gravada foi reaproveitada
        self.assertEqual(fetcher.requests, 2)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["product_rows"], 3)

    def test_historico_preserva_a_execucao_anterior(self):
        self._partial()
        run_extract(self.root)
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
        self.assertEqual(fetcher.requests, 3)

    def test_idioma_divergente_na_mesma_particao_e_recusado(self):
        self._partial()
        code, _, output = run_extract(self.root, lang="en")
        self.assertEqual(code, 2)
        self.assertIn("lang", output)


class CorruptAndTamperedFilesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})
        self.target = part.catalog_path(self.partition, 10)

    def test_arquivo_truncado_e_rebaixado_em_vez_de_derrubar_a_execucao(self):
        with open(self.target, "wb") as handle:
            handle.write(b'{"id": 10, "categ')
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("ilegivel", output)
        self.assertIn("/categories/10/", fetcher.paths)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["product_rows"], 3)

    def test_arquivo_vazio_e_rebaixado(self):
        open(self.target, "wb").close()
        self.assertEqual(run_extract(self.root)[0], 0)

    def test_arquivo_valido_mas_com_outra_forma_e_rebaixado(self):
        write_json(self.target, ["nao", "e", "objeto"])
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("/categories/10/", fetcher.paths)

    def test_arquivo_adulterado_e_rebaixado_da_fonte_e_registrado_como_anomalia(self):
        payload, _ = read_json(self.target)
        payload["categories"][0]["products"].append(
            {"id": "999", "display_name": "injetado", "price_instructions": {"unit_price": "0.01"}}
        )
        write_json(self.target, payload)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("divergente do manifesto anterior", output)
        # rebaixado da fonte: o conteudo injetado nao sobrevive
        self.assertIn("/categories/10/", fetcher.paths)
        restaurado, _ = read_json(self.target)
        nomes = [p["display_name"] for g in restaurado["categories"] for p in g["products"]]
        self.assertNotIn("injetado", nomes)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["anomalies"]), 1)
        self.assertEqual(
            manifest["anomalies"][0]["kind"], "conteudo_divergente_do_manifesto_anterior"
        )
        self.assertEqual(
            manifest["totals"]["product_rows"],
            sum(f["records"] for f in manifest["files"] if f["stage"] == "catalog"),
        )

    def test_adulteracao_nao_e_lavada_por_reexecucao(self):
        payload, _ = read_json(self.target)
        payload["categories"][0]["products"].append(
            {"id": "999", "display_name": "injetado", "price_instructions": {"unit_price": "0.01"}}
        )
        write_json(self.target, payload)
        run_extract(self.root)
        # segunda reexecucao: o conteudo ja foi restaurado, nada de injetado sobrevive
        run_extract(self.root, overwrite=True)
        restaurado, _ = read_json(self.target)
        nomes = [p["display_name"] for g in restaurado["categories"] for p in g["products"]]
        self.assertNotIn("injetado", nomes)


class PartitionWithoutManifestTest(unittest.TestCase):
    """Sem manifesto anterior, as travas do contrato nao podem ficar desligadas.

    Regressao do achado mais grave da auditoria: uma execucao interrompida deixava a
    particao sem manifesto, e a retomada aceitava qualquer conteudo em disco, com
    qualquer idioma, por cima de qualquer particao.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_manifesto_preliminar_existe_logo_apos_a_arvore(self):
        responses = dict(RESPONSES)
        responses["/categories/11/"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        manifest = part.read_manifest(self.partition)
        self.assertIsNotNone(manifest, "interrupcao deixou a particao sem manifesto")
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["source"]["lang"], "es")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_retomada_apos_interrupcao_nao_aceita_outro_idioma(self):
        responses = dict(RESPONSES)
        responses["/categories/11/"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        code, _, output = run_extract(self.root, lang="en")
        self.assertEqual(code, 2)
        self.assertIn("lang", output)

    def test_arquivo_fora_da_forma_canonica_e_rebaixado(self):
        os.makedirs(os.path.join(self.partition, "catalog"), exist_ok=True)
        alvo = part.catalog_path(self.partition, 10)
        with open(alvo, "w", encoding="utf-8") as handle:
            json.dump({"id": 10, "name": "falsificado", "categories": []}, handle, indent=4)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("forma canonica", output)
        self.assertIn("/categories/10/", fetcher.paths)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(
            any(a["kind"] == "arquivo_nao_canonico" for a in manifest["anomalies"])
        )
        # o digest gravado e o canonico, nao o do arquivo forjado
        entrada = next(f for f in manifest["files"] if f["path"].endswith("category_id=10.json"))
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
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})

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
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_arquivo_nao_visitado_por_limit_entra_no_manifesto(self):
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})  # grava a 10
        code, _, output = run_extract(self.root, limit=1)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        declarados = {
            f["category_id"] for f in manifest["files"] if f["stage"] == "catalog"
        }
        self.assertEqual(declarados, {10})
        self.assertEqual(manifest["totals"]["catalog_files"], 1)

    def test_arquivo_de_categoria_que_saiu_da_arvore_continua_declarado(self):
        run_extract(self.root)
        # a fonte remove a categoria 11 da arvore; o arquivo continua em disco
        responses = dict(RESPONSES)
        responses["/categories/"] = tree((1, "Pai", [(10, "Alfa")]))
        code, _, output = run_extract(self.root, responses=responses, overwrite=True)
        self.assertEqual(code, 0)
        self.assertIn("[inventario]", output)
        manifest = part.read_manifest(self.partition)
        declarados = {
            f["category_id"] for f in manifest["files"] if f["stage"] == "catalog"
        }
        self.assertIn(11, declarados)
        self.assertEqual(manifest["totals"]["product_rows"], 3)


class SourceShapeChangeTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_arvore_com_formato_inesperado_e_fatal_mas_preserva_o_download(self):
        code, _, output = run_extract(self.root, responses={"/categories/": {"itens": []}})
        self.assertEqual(code, 2)
        self.assertIn("formato da arvore", output)
        # o download bem-sucedido nao e descartado por causa do formato
        payload, _ = read_json(part.categories_path(self.partition))
        self.assertEqual(payload, {"itens": []})

    def test_arvore_sem_categorias_de_nivel_2_e_fatal(self):
        code, _, _ = run_extract(self.root, responses={"/categories/": tree((1, "Pai", []))})
        self.assertEqual(code, 2)

    def test_arvore_indisponivel_e_fatal(self):
        code, _, _ = run_extract(self.root, errors={"/categories/": "HTTP 503"})
        self.assertEqual(code, 2)

    def test_catalogo_que_nao_e_objeto_vira_falha_registrada(self):
        responses = dict(RESPONSES)
        responses["/categories/10/"] = ["lista", "inesperada"]
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertIn("objeto JSON", manifest["failures"][0]["error"])

    def test_catalogo_sem_produtos_registra_zero_linhas(self):
        responses = dict(RESPONSES)
        responses["/categories/10/"] = {"id": 10, "name": "Alfa", "secoes": []}
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 0)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["product_rows"], 1)


if __name__ == "__main__":
    unittest.main()
