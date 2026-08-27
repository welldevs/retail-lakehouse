"""A validacao fecha o loop: prova o resultado pousado, nao so a regra em fixture.

Cada teste aqui corrompe a particao de um jeito diferente e exige que a validacao
reprove. Um validador que so passa em particao boa nao prova nada.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from simulated_oltp_source.canonical import write_json
from simulated_oltp_source.extract import run as extract_run
from simulated_oltp_source.partition import (
    customers_path,
    manifest_path,
    partition_path,
    success_path,
)
from simulated_oltp_source.validate import EXIT_FAILED, EXIT_OK, run as validate_run

from tests import support


class ValidateTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = support.write_reference(os.path.join(self.tmp.name, "ref"))
        self.out = os.path.join(self.tmp.name, "data")
        with contextlib.redirect_stdout(io.StringIO()):
            extract_run(support.extract_args(self.reference, self.out))
        self.partition = partition_path(self.out, "2026-08-27", support.WH_A)

    def validate(self, strict=False) -> tuple[int, str]:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = validate_run(support.validate_args(self.partition, self.reference, strict))
        return code, buffer.getvalue()

    def rewrite_customers(self, mutate) -> None:
        """Reescreve customers.json e reconcilia o manifesto, para que a falha testada
        seja a que o teste quer — e nao um checksum divergente escondendo as outras."""
        with open(customers_path(self.partition), encoding="utf-8") as handle:
            customers = json.load(handle)
        mutate(customers)
        sha, size = write_json(customers_path(self.partition), customers)
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["files"][0].update(sha256=sha, bytes=size, records=len(customers))
        manifest["totals"].update(
            customer_rows=len(customers),
            municipalities_used=len({c["municipality_code"] for c in customers}),
            house_number_null=sum(1 for c in customers if c["house_number"] is None),
            bytes=size,
        )
        manifest["config"]["count"] = len(customers)
        write_json(manifest_path(self.partition), manifest)


class ParticaoBoaTest(ValidateTestCase):
    def test_particao_recem_gerada_passa(self):
        codigo, saida = self.validate(strict=True)
        self.assertEqual(codigo, EXIT_OK, saida)
        self.assertIn("coerencia geoespacial validados", saida)

    def test_relata_a_coerencia_de_todos_os_clientes(self):
        _, saida = self.validate()
        self.assertIn("50/50 clientes", saida)


class CoerenciaGeograficaTest(ValidateTestCase):
    def test_cep_de_fora_da_auf_reprova(self):
        """O teste negativo que prova que a garantia central e mesmo verificada."""
        self.rewrite_customers(
            lambda customers: customers[0].update(postal_code="99999")
        )
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("postal_code", saida)

    def test_municipio_fora_da_service_area_reprova(self):
        """Endereco coerente com a linha de origem, mas num municipio que a AUF do
        warehouse nao cobre. So a checagem de service area pega este caso: todos os
        campos batem com o candidato, entao a comparacao campo a campo passa."""
        intruso = dict(support.CANDIDATES[0], municipality_code="003")
        reference = support.write_reference(
            os.path.join(self.tmp.name, "com-intruso"),
            candidates=support.CANDIDATES + [intruso],
        )
        self.reference = reference
        self.rewrite_customers(
            lambda c: c[0].update(
                candidate_index=len(support.CANDIDATES), municipality_code="003"
            )
        )
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("fora da Area Urbana Funcional", saida)

    def test_candidate_index_fora_da_referencia_reprova(self):
        self.rewrite_customers(lambda c: c[0].update(candidate_index=999999))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("candidate_index", saida)

    def test_rua_que_nao_e_a_do_tramo_reprova(self):
        self.rewrite_customers(lambda c: c[0].update(street_name="RUA INVENTADA"))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("street_name", saida)

    def test_wh_divergente_da_particao_reprova(self):
        self.rewrite_customers(lambda c: c[0].update(wh=support.WH_B))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("nao e o da particao", saida)

    def test_customer_id_duplicado_reprova(self):
        self.rewrite_customers(lambda c: c[1].update(customer_id=c[0]["customer_id"]))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("duplicado", saida)


class NumeracaoTest(ValidateTestCase):
    def test_numero_em_via_sem_numeracao_reprova(self):
        def fabricar(customers):
            for customer in customers:
                if customer["numbering_type"] == "0":
                    customer["house_number"] = 42
                    return
            raise AssertionError("a fixture nao gerou nenhum tramo sem numeracao")

        self.rewrite_customers(fabricar)
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("endereco fabricado", saida)

    def test_paridade_errada_reprova(self):
        def quebrar(customers):
            for customer in customers:
                if customer["numbering_type"] == "1" and customer["house_number"]:
                    customer["house_number"] += 1
                    return
            raise AssertionError("a fixture nao gerou numero impar")

        self.rewrite_customers(quebrar)
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("contraria numbering_type", saida)

    def test_numero_fora_da_faixa_real_reprova(self):
        def estourar(customers):
            for customer in customers:
                if customer["numbering_type"] == "1" and customer["house_number"]:
                    customer["house_number"] = 10001
                    return
            raise AssertionError("a fixture nao gerou numero impar")

        self.rewrite_customers(estourar)
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("fora da faixa real", saida)


class IntegridadeTest(ValidateTestCase):
    def test_checksum_divergente_reprova(self):
        with open(customers_path(self.partition), "a", encoding="utf-8") as handle:
            handle.write(" ")
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("checksum divergente", saida)

    def test_arquivo_nao_declarado_no_manifesto_reprova(self):
        with open(os.path.join(self.partition, "sobra.json"), "w") as handle:
            handle.write("{}")
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("fora do manifesto", saida)

    def test_success_ausente_contradiz_complete(self):
        os.unlink(success_path(self.partition))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("_SUCCESS", saida)

    def test_manifesto_ausente_reprova(self):
        os.unlink(manifest_path(self.partition))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("manifesto nao encontrado", saida)

    def test_totais_adulterados_no_manifesto_reprovam(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["totals"]["customer_rows"] = 999
        write_json(manifest_path(self.partition), manifest)
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("totals.customer_rows", saida)

    def test_versao_de_manifesto_desconhecida_reprova(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["manifest_version"] = 99
        write_json(manifest_path(self.partition), manifest)
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("manifest_version", saida)

    def test_schema_fingerprint_divergente_reprova(self):
        self.rewrite_customers(lambda c: c[0].update(campo_novo=1))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("schema_fingerprint", saida)

    def test_campo_obrigatorio_removido_reprova(self):
        self.rewrite_customers(lambda c: c[0].pop("birth_year"))
        codigo, saida = self.validate()
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("birth_year", saida)


class ReferenciaTest(ValidateTestCase):
    def test_referencia_ausente_reprova(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            codigo = validate_run(
                support.validate_args(self.partition, os.path.join(self.tmp.name, "nada"))
            )
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("referencia inutilizavel", buffer.getvalue())

    def test_referencia_de_outro_snapshot_do_callejero_reprova(self):
        """Validar contra uma referencia diferente da que gerou nao prova nada."""
        outra = support.write_reference(os.path.join(self.tmp.name, "outra"))
        path = os.path.join(outra, "address_candidates.json")
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["callejero_ingestion_date"] = "2026-01-01"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            codigo = validate_run(support.validate_args(self.partition, outra))
        self.assertEqual(codigo, EXIT_FAILED)
        self.assertIn("outro snapshot do Callejero", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
