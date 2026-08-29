"""extract e validate ponta a ponta, sobre uma referencia de fixture."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from simulated_orders_source import extract as extract_module
from simulated_orders_source import validate as validate_module
from simulated_orders_source.partition import events_path, manifest_path, partition_path

from .support import ORDER_DATE, WH, Args, catalog_rows, customer_rows, write_reference


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = write_reference(os.path.join(self.tmp.name, "ref"))
        self.root = os.path.join(self.tmp.name, "orders")
        self.partition = partition_path(self.root, ORDER_DATE, WH)

    def extract(self, **kwargs):
        args = Args(
            reference=self.reference,
            out=self.root,
            wh=WH,
            date=ORDER_DATE,
            seed=42,
            overwrite=False,
        )
        args.__dict__.update(kwargs)
        with redirect_stdout(io.StringIO()) as saida:
            code = extract_module.run(args)
        return code, saida.getvalue()

    def validate(self, strict=True):
        args = Args(partition=self.partition, reference=self.reference, strict=strict)
        with redirect_stdout(io.StringIO()) as saida:
            code = validate_module.run(args)
        return code, saida.getvalue()

    def manifest(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            return json.load(handle)

    def rewrite_log(self, events):
        with open(events_path(self.partition), "w", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    def log(self):
        with open(events_path(self.partition), encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]


class ExtractTest(Base):
    def test_grava_log_manifesto_e_success(self):
        code, _ = self.extract()
        self.assertEqual(code, 0)
        for name in ("order_events.jsonl", "_manifest.json", "_SUCCESS"):
            self.assertTrue(os.path.exists(os.path.join(self.partition, name)), name)

    def test_a_particao_nao_tem_estado_dobrado_ao_lado(self):
        # Nao existe orders.json: o estado e o fold, e o fold mora no Silver. Duas
        # representacoes da mesma verdade divergem.
        self.extract()
        arquivos = sorted(os.listdir(self.partition))
        self.assertEqual(arquivos, ["_SUCCESS", "_manifest.json", "order_events.jsonl"])

    def test_manifesto_declara_as_tres_entradas_reprodutiveis(self):
        self.extract()
        config = self.manifest()["config"]
        self.assertEqual(config["seed"], 42)
        self.assertIn("premises_sha256", config)
        self.assertIn("reference", config)

    def test_manifesto_declara_a_linhagem_do_insumo(self):
        self.extract()
        reference = self.manifest()["reference"]
        self.assertEqual(reference["price_as_of"], ORDER_DATE)
        self.assertEqual(reference["price_source"], "observed")
        self.assertIn("customer_ingestion_dates", reference)

    def test_reextrair_particao_completa_e_recusado(self):
        self.extract()
        code, saida = self.extract()
        self.assertEqual(code, 2)
        self.assertIn("imutavel", saida)

    def test_overwrite_regrava_e_acumula_historico(self):
        self.extract()
        code, _ = self.extract(overwrite=True)
        self.assertEqual(code, 0)
        self.assertEqual(len(self.manifest()["history"]), 1)

    def test_referencia_ausente_e_fatal_sem_gravar_nada(self):
        code, saida = self.extract(reference=os.path.join(self.tmp.name, "vazio"))
        self.assertEqual(code, 2)
        self.assertIn("ERRO", saida)
        self.assertFalse(os.path.exists(os.path.join(self.partition, "_SUCCESS")))

    def test_dia_fora_da_janela_e_fatal(self):
        code, _ = self.extract(date="2026-01-01")
        self.assertEqual(code, 2)


class ValidateTest(Base):
    def setUp(self):
        super().setUp()
        self.extract()

    def test_particao_intacta_passa_em_strict(self):
        code, saida = self.validate()
        self.assertEqual(code, 0, saida)

    def test_checksum_adulterado_reprova(self):
        events = self.log()
        events[0]["wh"] = "bcn1"
        self.rewrite_log(events)
        self.assertEqual(self.validate()[0], 1)

    def test_arquivo_orfao_reprova(self):
        with open(os.path.join(self.partition, "sobra.json"), "w", encoding="utf-8") as handle:
            handle.write("{}")
        code, saida = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("fora do manifesto", saida)

    def test_success_removido_reprova(self):
        os.unlink(os.path.join(self.partition, "_SUCCESS"))
        code, saida = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("_SUCCESS", saida)

    def test_referencia_de_outras_premissas_reprova(self):
        # Trocar a tabela de premissas e regerar mudaria as taxas por tras dos mesmos
        # order_id: o digest no manifesto e o que torna isso visivel.
        outra = write_reference(os.path.join(self.tmp.name, "ref2"))
        with open(os.path.join(outra, "premises.json"), encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["seed_sha256"] = "f" * 64
        with open(os.path.join(outra, "premises.json"), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        args = Args(partition=self.partition, reference=outra, strict=False)
        with redirect_stdout(io.StringIO()) as saida:
            code = validate_module.run(args)
        self.assertEqual(code, 1)
        self.assertIn("premissas", saida.getvalue())

    def test_manifesto_ausente_reprova_sem_traceback(self):
        os.unlink(manifest_path(self.partition))
        code, saida = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("manifesto nao encontrado", saida)

    def test_log_fora_de_ordem_reprova(self):
        events = self.log()
        events.reverse()
        self.rewrite_log(events)
        code, saida = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("fora de ordem", saida)

    def test_log_adulterado_reprova_com_codigo_1_e_nunca_com_excecao(self):
        # Contar um log quebrado tem de produzir divergencia, nunca traceback: senao
        # "particao invalida" viraria "excecao nao tratada" e o orquestrador nao saberia
        # distinguir dado ruim de bug do validador.
        events = [e for e in self.log() if e["event_type"] != "order_payment_authorized"]
        self.rewrite_log(events)
        code, _ = self.validate()
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
