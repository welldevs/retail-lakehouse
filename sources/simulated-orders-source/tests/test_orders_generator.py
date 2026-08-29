"""O gerador: a regra de ouro, o determinismo e o fold nao-trivial."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal

from simulated_orders_source import events as ev
from simulated_orders_source.orders_generator import (
    GenerationError,
    _quantity_weights,
    _sample_indices,
    day_seed,
    generate,
)
from simulated_orders_source.premises import Premises
from simulated_orders_source.reference_data import load
from simulated_orders_source.schema import group_by_order, totals_of

from .support import ORDER_DATE, PREMISES, WH, catalog_rows, customer_rows, write_reference

PACKAGE_DIR = __file__.rsplit("/tests/", 1)[0]


def _reference(directory, **kwargs):
    return load(write_reference(directory, **kwargs))


class SubSeedTest(unittest.TestCase):
    def test_muda_com_armazem_e_com_dia(self):
        base = day_seed(1, "mad1", "2026-08-24")
        self.assertNotEqual(base, day_seed(1, "bcn1", "2026-08-24"))
        self.assertNotEqual(base, day_seed(1, "mad1", "2026-08-25"))
        self.assertNotEqual(base, day_seed(2, "mad1", "2026-08-24"))

    def test_nao_depende_de_pythonhashseed(self):
        # sha256, e nao hash(): o hash de str em CPython e aleatorizado por processo, e
        # derivar a sub-seed dele faria a particao inteira depender de PYTHONHASHSEED.
        code = (
            "import sys; sys.path.insert(0, 'src');"
            "from simulated_orders_source.orders_generator import day_seed;"
            "print(day_seed(20260828, 'mad1', '2026-08-24'))"
        )
        saidas = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=PACKAGE_DIR,
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 13, 5150)
        }
        self.assertEqual(len(saidas), 1, f"sub-seed variou entre processos: {saidas}")


class AmostragemTest(unittest.TestCase):
    def test_indices_sao_distintos_e_na_ordem_do_sorteio(self):
        import random

        tirados = _sample_indices(random.Random(7), 50, 10)
        self.assertEqual(len(tirados), 10)
        self.assertEqual(len(set(tirados)), 10)

    def test_pedir_mais_do_que_existe_reprova(self):
        import random

        with self.assertRaises(GenerationError):
            _sample_indices(random.Random(7), 3, 4)

    def test_pesos_de_quantidade_sao_decrescentes_e_somam_um(self):
        cumulative = _quantity_weights(4)
        self.assertAlmostEqual(cumulative[-1], 1.0)
        passos = [cumulative[0]] + [
            cumulative[i] - cumulative[i - 1] for i in range(1, len(cumulative))
        ]
        self.assertEqual(passos, sorted(passos, reverse=True))


class RegraDeOuroTest(unittest.TestCase):
    """O pedido e inventado; cliente, produto e preco nao."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))
        self.premises = Premises(PREMISES)
        self.events = generate(self.reference, self.premises, WH, ORDER_DATE, 42)
        self.grouped = group_by_order(self.events)

    def test_todo_produto_pedido_existe_no_catalogo_daquele_armazem_e_data(self):
        catalogo = {p["source_product_id"] for p in self.reference.catalog_of(WH, ORDER_DATE)}
        for eventos in self.grouped.values():
            for linha in eventos[0]["payload"]["lines"]:
                self.assertIn(linha["source_product_id"], catalogo)

    def test_todo_preco_pago_e_o_preco_observado(self):
        precos = {
            p["source_product_id"]: p["unit_price"]
            for p in self.reference.catalog_of(WH, ORDER_DATE)
        }
        for eventos in self.grouped.values():
            for linha in eventos[0]["payload"]["lines"]:
                self.assertEqual(
                    Decimal(linha["unit_price"]), precos[linha["source_product_id"]]
                )

    def test_todo_cliente_pertence_ao_armazem_do_pedido(self):
        do_armazem = {c["customer_id"] for c in self.reference.customers_of(WH)}
        for eventos in self.grouped.values():
            self.assertIn(eventos[0]["payload"]["customer_id"], do_armazem)

    def test_substituto_vem_do_mesmo_catalogo(self):
        catalogo = {p["source_product_id"] for p in self.reference.catalog_of(WH, ORDER_DATE)}
        vistos = 0
        for eventos in self.grouped.values():
            for event in eventos:
                if event["event_type"] == ev.ORDER_LINE_SUBSTITUTED:
                    vistos += 1
                    self.assertIn(event["payload"]["substitute_source_product_id"], catalogo)
        self.assertGreater(vistos, 0, "nenhuma substituicao gerada: o teste nao olhou nada")

    def test_um_cliente_pede_no_maximo_uma_vez_por_dia(self):
        clientes = [e[0]["payload"]["customer_id"] for e in self.grouped.values()]
        self.assertEqual(len(clientes), len(set(clientes)))


class FoldNaoTrivialTest(unittest.TestCase):
    """Se o valor final fosse derivavel do primeiro evento, o log seria um carimbo de data."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))

    def test_valor_separado_difere_do_colocado(self):
        events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        totals = totals_of(events)
        self.assertGreater(totals["substituted_lines"] + totals["removed_lines"], 0)
        self.assertNotEqual(totals["gross_amount_placed"], totals["net_amount_picked"])

    def test_sem_substituicao_nem_remocao_o_fold_fica_trivial(self):
        # O contraponto explicito: com as duas taxas em zero, o valor separado passa a ser
        # derivavel do primeiro evento. E este o cenario que o teste dbt invertido vigia.
        plano = dict(PREMISES, substitution_rate="0", removal_rate="0")
        events = generate(self.reference, Premises(plano), WH, ORDER_DATE, 42)
        totals = totals_of(events)
        self.assertEqual(totals["substituted_lines"], 0)
        self.assertEqual(totals["removed_lines"], 0)
        for eventos in group_by_order(events).values():
            picked = [e for e in eventos if e["event_type"] == ev.ORDER_PICKED]
            if picked:
                self.assertEqual(
                    picked[0]["payload"]["picked_amount"], eventos[0]["payload"]["gross_amount"]
                )


class EstruturaDoLogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))
        self.events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)

    def test_log_esta_totalmente_ordenado(self):
        chaves = [(e["occurred_at"], e["order_id"], e["sequence_no"]) for e in self.events]
        self.assertEqual(chaves, sorted(chaves))

    def test_sequence_no_e_contiguo_por_pedido(self):
        for order_id, eventos in group_by_order(self.events).items():
            self.assertEqual(
                [e["sequence_no"] for e in eventos],
                list(range(1, len(eventos) + 1)),
                f"{order_id} com buraco no sequence_no",
            )

    def test_todo_pedido_comeca_em_order_placed(self):
        for eventos in group_by_order(self.events).values():
            self.assertEqual(eventos[0]["event_type"], ev.ORDER_PLACED)

    def test_toda_sequencia_e_aceita_pela_maquina_de_estados(self):
        for eventos in group_by_order(self.events).values():
            ev.fold(eventos)  # strict: levanta se o gerador emitiu transicao impossivel

    def test_o_tempo_nao_regride_dentro_de_um_pedido(self):
        for eventos in group_by_order(self.events).values():
            instantes = [e["occurred_at"] for e in eventos]
            self.assertEqual(instantes, sorted(instantes))

    def test_nenhum_evento_carimba_relogio(self):
        # Todo occurred_at deriva do dia da particao. Um pedido colocado hoje seria a prova
        # de que datetime.now() vazou para dentro do gerador.
        for event in self.events:
            if event["event_type"] == ev.ORDER_PLACED:
                self.assertTrue(event["occurred_at"].startswith(ORDER_DATE))


class ReprodutibilidadeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))

    def test_mesma_entrada_produz_a_mesma_saida(self):
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.assertEqual(a, b)

    def test_outra_seed_produz_outra_saida(self):
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 43)
        self.assertNotEqual(a, b)

    def test_outra_tabela_de_premissas_produz_outra_saida(self):
        # E por isso que o sha256 das premissas vai para o manifesto: sem ele, trocar uma
        # taxa e regerar mudaria os pedidos sob os mesmos order_id sem deixar rastro.
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(
            self.reference, Premises(dict(PREMISES, basket_lines_mode="5")), WH, ORDER_DATE, 42
        )
        self.assertNotEqual(a, b)

    def test_dia_diferente_nao_reembaralha_o_dia_anterior(self):
        # A propriedade aditiva desta Source: acrescentar um dia a janela deixa as particoes
        # ja geradas byte a byte identicas, porque cada dia deriva a propria sub-seed.
        calendario = [
            {"wh": WH, "order_date": d, "price_as_of": ORDER_DATE, "price_source": "observed"}
            for d in (ORDER_DATE, "2026-08-25")
        ]
        reference = _reference(os.path.join(self.tmp.name, "ref2"), calendar=calendario)
        primeiro = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        generate(reference, Premises(PREMISES), WH, "2026-08-25", 42)
        self.assertEqual(primeiro, generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42))


class RecusaTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_pedido_antes_de_o_cliente_existir_e_recusado(self):
        # Restricao 6: nenhum pedido nasce antes do cliente.
        reference = _reference(
            os.path.join(self.tmp.name, "ref"),
            customers=customer_rows(first="2026-09-01"),
            customer_ingestion_dates=["2026-09-01"],
        )
        with self.assertRaises(GenerationError) as caught:
            generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.assertIn("existia", str(caught.exception))

    def test_taxa_que_zera_os_pedidos_e_recusada(self):
        reference = _reference(os.path.join(self.tmp.name, "ref2"))
        plano = dict(PREMISES, daily_order_rate="0")
        with self.assertRaises(GenerationError):
            generate(reference, Premises(plano), WH, ORDER_DATE, 42)

    def test_cesta_maior_que_o_catalogo_nao_estoura(self):
        # Degrada para o catalogo inteiro em vez de levantar IndexError.
        reference = _reference(
            os.path.join(self.tmp.name, "ref3"), catalog=catalog_rows(total=6)
        )
        events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        for eventos in group_by_order(events).values():
            self.assertLessEqual(len(eventos[0]["payload"]["lines"]), 6)


if __name__ == "__main__":
    unittest.main()
