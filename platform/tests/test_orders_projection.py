"""A projeção viva: tipos, fusão monotônica, retry e a costura entre os dois sinks.

TRÊS CAMADAS, o mesmo padrão de test_orders_oltp.py e test_orders_stream.py:

  1. `_normalize` e `_as_key` são PURAS — tipos e comparação entre motores, sem nada de pé.
  2. A fusão monotônica e o laço de retry, contra `fake_iceberg.FakeIcebergTable`. É lógica
     sobre `last_sequence_no`, não propriedade do formato, e é a parte que decide se dois
     escritores se atropelam.
  3. A PARIDADE DA COSTURA: os dois sinks expõem a mesma interface pequena. Era a promessa
     do Marco 5, e uma promessa de interface só vale se estiver asserida.

O que NÃO está aqui: que o Iceberg recusa um commit vindo de snapshot velho. Um duplo que
levanta a exceção só demonstra que o duplo levanta. Isso é `make orders-prove-projection`.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from retail_platform.orders_projection import (
    COLUMNS,
    WRITER_REBUILD,
    WRITER_STREAM,
    IcebergProjection,
    OrdersProjectionError,
    _as_key,
    _normalize,
    arrow_schema,
    partitions_of,
)

from .fake_iceberg import FakeCatalog, FakeIcebergTable


def estado(order_id="ord_a", seq=1, status="PLACED", **extra):
    base = {
        "order_id": order_id, "wh": "mad1", "order_date": date(2026, 8, 27),
        "customer_id": "cust_mad1_000001", "status": status, "last_sequence_no": seq,
        "events_applied": seq,
        "placed_at": datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc),
        "confirmed_at": None, "picking_started_at": None, "picked_at": None,
        "dispatched_at": None, "terminal_at": None, "line_count": 3,
        "picked_line_count": None, "substituted_lines": 0, "removed_lines": 0,
        "gross_amount": "10.00", "net_amount": None, "returned_amount": None,
        "delivered_within_slot": None, "picking_minutes": None, "sla_breached": None,
    }
    base.update(extra)
    return base


class TestNormalize(unittest.TestCase):
    def test_money_becomes_decimal_never_float(self):
        """Precedente de `silver_product_price` e obrigação 4.4 do contrato da Mercadona.
        Um float aqui reintroduziria erro de arredondamento no valor do pedido."""
        linha = _normalize(estado(gross_amount="10.00", net_amount="9.35"), WRITER_STREAM)
        self.assertEqual(linha["gross_amount"], Decimal("10.00"))
        self.assertIsInstance(linha["gross_amount"], Decimal)
        self.assertIsInstance(linha["net_amount"], Decimal)

    def test_null_money_stays_null(self):
        """`net_amount` nulo significa "não houve separação", não zero — a mesma disciplina
        do OLTP e do Silver, e o defeito que o Marco 4 achou."""
        linha = _normalize(estado(net_amount=None), WRITER_STREAM)
        self.assertIsNone(linha["net_amount"])

    def test_the_writer_is_stamped_by_the_projection_not_by_the_state(self):
        """Proveniência é de quem escreve, não do fold. Se viesse do estado, os dois
        escritores poderiam carimbar a mesma coisa e a coluna não diria nada."""
        linha = _normalize(estado(), WRITER_REBUILD)
        self.assertEqual(linha["written_by"], WRITER_REBUILD)

    def test_every_declared_column_is_produced(self):
        linha = _normalize(estado(), WRITER_STREAM)
        self.assertEqual(sorted(linha), sorted(COLUMNS))

    def test_the_key_column_is_the_only_one_that_cannot_be_null(self):
        """A nulidade das outras tem SIGNIFICADO — ver net_amount. Declarar tudo não-nulo
        obrigaria a inventar um valor onde a resposta correta é "não existe"."""
        schema = arrow_schema()
        nao_nulas = [c.name for c in schema if not c.nullable]
        self.assertEqual(nao_nulas, ["order_id"])


class TestComparisonKey(unittest.TestCase):
    """A normalização que decide se três motores concordam. Errar aqui é pior que não ter."""

    def test_the_same_number_in_three_types_compares_equal(self):
        self.assertEqual(_as_key(Decimal("10.00")), _as_key(10))
        self.assertEqual(_as_key(Decimal("10.00")), _as_key(10.0))

    def test_it_normalizes_scale_but_never_rounds(self):
        """A primeira versão formatava com `:.2f`, e isso fazia 10.001 e 10.00 compararem
        IGUAIS — uma reconciliação que arredonda justamente a diferença que veio verificar.
        Hoje as três fontes são decimal(12,2) e o defeito não apareceria; apareceria no dia
        em que uma mudasse de escala, que é o pior dia para descobri-lo."""
        self.assertNotEqual(_as_key(Decimal("10.001")), _as_key(Decimal("10.00")))
        self.assertNotEqual(_as_key(Decimal("10.01")), _as_key(Decimal("10.00")))

    def test_null_is_not_zero_and_true_is_not_one(self):
        self.assertIsNone(_as_key(None))
        self.assertNotEqual(_as_key(None), _as_key(0))
        self.assertNotEqual(_as_key(True), _as_key(1))


class TestMonotonicMerge(unittest.TestCase):
    """A propriedade que separa "dois escritores" de "dois escritores que funcionam"."""

    def _projection(self, table, writer=WRITER_STREAM):
        return IcebergProjection(table, writer=writer, cat=FakeCatalog(table))

    def test_a_state_that_advances_is_written(self):
        table = FakeIcebergTable([_normalize(estado(seq=3), WRITER_STREAM)])
        projection = self._projection(table)
        projection.save([estado(seq=4, status="PICKED")])
        stats = projection.commit()
        self.assertEqual(stats.rows_written, 1)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 4)

    def test_a_state_that_does_not_advance_is_dropped_as_stale(self):
        """RETRY NÃO BASTA. Se o outro escritor já gravou o pedido no sequence_no 7 e a nossa
        tentativa carrega o 5, o retry cego escreve o 5 por cima: o commit passa, a tabela
        REGRIDE, e nada reprova. A guarda é a mesma de `last_sequence_no` que protege o OLTP
        e o consumidor, agora protegendo a escrita concorrente."""
        table = FakeIcebergTable([_normalize(estado(seq=7, status="DELIVERED"), WRITER_STREAM)])
        projection = self._projection(table, WRITER_REBUILD)
        projection.save([estado(seq=5, status="PICKING")])
        stats = projection.commit()
        self.assertEqual(stats.rows_dropped_as_stale, 1)
        self.assertEqual(stats.rows_written, 0)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 7)
        self.assertEqual(table.rows["ord_a"]["status"], "DELIVERED")
        # E nenhum upsert chegou a ser emitido: não escrever é o resultado CORRETO aqui.
        self.assertEqual(table.upserts, 0)

    def test_an_equal_sequence_is_also_dropped(self):
        """`>` e não `>=`: reescrever o mesmo estado gastaria um snapshot inteiro para nada,
        e num formato copy-on-write um snapshot custa reescrever a tabela."""
        table = FakeIcebergTable([_normalize(estado(seq=5), WRITER_STREAM)])
        projection = self._projection(table)
        projection.save([estado(seq=5)])
        self.assertEqual(projection.commit().rows_dropped_as_stale, 1)

    def test_an_unknown_order_is_always_written(self):
        projection = self._projection(FakeIcebergTable([]))
        projection.save([estado(seq=1)])
        self.assertEqual(projection.commit().rows_written, 1)

    def test_a_mixed_batch_writes_only_what_advances(self):
        table = FakeIcebergTable([
            _normalize(estado("ord_a", seq=9), WRITER_STREAM),
            _normalize(estado("ord_b", seq=2), WRITER_STREAM),
        ])
        projection = self._projection(table, WRITER_REBUILD)
        projection.save([estado("ord_a", seq=4), estado("ord_b", seq=6),
                         estado("ord_c", seq=1)])
        stats = projection.commit()
        self.assertEqual((stats.rows_written, stats.rows_dropped_as_stale), (2, 1))
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 9)
        self.assertEqual(table.rows["ord_b"]["last_sequence_no"], 6)
        self.assertIn("ord_c", table.rows)


class TestRetryLoop(unittest.TestCase):
    def test_a_conflict_is_retried_after_reloading(self):
        table = FakeIcebergTable([], conflicts_before_success=2)
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([estado(seq=1)])
        stats = projection.commit()
        self.assertEqual(stats.conflicts, 2)
        self.assertEqual(stats.attempts, 3)
        self.assertEqual(stats.rows_written, 1)

    def test_the_retry_re_reads_before_writing_again(self):
        """ESTE é o teste que separa retry cego de fusão monotônica. Entre a recusa e o
        retry, outro escritor commitou um estado MAIS NOVO. Um retry que reusasse a leitura
        anterior escreveria o antigo por cima e o commit passaria."""
        table = FakeIcebergTable(
            [_normalize(estado(seq=1), WRITER_STREAM)],
            conflicts_before_success=1,
            on_conflict_state=[_normalize(estado(seq=9, status="DELIVERED"), WRITER_STREAM)],
        )
        projection = IcebergProjection(table, writer=WRITER_REBUILD, cat=FakeCatalog(table))
        projection.save([estado(seq=3, status="PICKING")])
        stats = projection.commit()
        self.assertEqual(stats.rows_written, 0)
        self.assertEqual(stats.rows_dropped_as_stale, 1)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 9)
        self.assertEqual(table.rows["ord_a"]["status"], "DELIVERED")

    def test_the_refresh_reloads_the_table_it_was_given(self):
        """Um identificador cravado numa funcao de refresh funciona ate existir um segundo
        objeto, e ai escreve no lugar errado sem erro nenhum.

        A primeira versao recarregava `TABLE_NAME`. No primeiro teste que usou uma tabela de
        sonda, um conflito fez o escritor da sonda recarregar a tabela de PRODUCAO e gravar
        nela — quatro linhas sinteticas entraram em `live_order_state`, e so a reconciliacao,
        tres passos adiante, apontou."""
        class SondaCatalog(FakeCatalog):
            def __init__(self, table):
                super().__init__(table)
                self.pedidos = []

            def load_table(self, name):
                self.pedidos.append(name)
                return super().load_table(name)

        table = FakeIcebergTable([], conflicts_before_success=1)
        cat = SondaCatalog(table)
        projection = IcebergProjection(table, cat=cat)
        projection.save([estado(seq=1)])
        projection.commit()
        self.assertTrue(cat.pedidos, "o refresh nao recarregou nada")
        for pedido in cat.pedidos:
            self.assertEqual(pedido, table.name(),
                             "o refresh recarregou um nome que nao e o da propria tabela")

    def test_it_gives_up_loudly_instead_of_looping_forever(self):
        table = FakeIcebergTable([], conflicts_before_success=999)
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([estado(seq=1)])
        with self.assertRaises(OrdersProjectionError) as ctx:
            projection.commit()
        self.assertIn("contencao", str(ctx.exception))

    def test_rollback_discards_the_buffer(self):
        table = FakeIcebergTable([])
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([estado(seq=1)])
        projection.rollback()
        self.assertEqual(projection.commit().rows_written, 0)
        self.assertEqual(table.upserts, 0)


class TestSinkParity(unittest.TestCase):
    """A costura declarada no Marco 5, asserida em vez de prometida."""

    def test_both_sinks_expose_the_same_small_interface(self):
        """Se `orders-project` precisasse de um método a mais para escrever no Iceberg, a
        costura teria vazado — e trocar o armazenamento deixaria de ser trocar o sink."""
        from retail_platform.orders_stream import PostgresProjection

        exigidos = ("load", "save", "commit", "rollback", "count", "digest")
        for classe in (PostgresProjection, IcebergProjection):
            for metodo in exigidos:
                self.assertTrue(callable(getattr(classe, metodo, None)),
                                f"{classe.__name__} nao expoe {metodo}")

    def test_the_digest_ignores_provenance(self):
        """`written_by` é proveniência, não estado. Incluí-lo faria o digest mudar quando o
        MESMO conteúdo fosse escrito pelo outro escritor — e a propriedade que se quer medir
        é justamente que os dois produzem o mesmo estado."""
        um = FakeIcebergTable([_normalize(estado(seq=4), WRITER_STREAM)])
        outro = FakeIcebergTable([_normalize(estado(seq=4), WRITER_REBUILD)])
        self.assertEqual(
            IcebergProjection(um, cat=FakeCatalog(um)).digest(),
            IcebergProjection(outro, cat=FakeCatalog(outro)).digest(),
        )

    def test_the_digest_changes_when_state_changes(self):
        um = FakeIcebergTable([_normalize(estado(seq=4), WRITER_STREAM)])
        outro = FakeIcebergTable([_normalize(estado(seq=5), WRITER_STREAM)])
        self.assertNotEqual(
            IcebergProjection(um, cat=FakeCatalog(um)).digest(),
            IcebergProjection(outro, cat=FakeCatalog(outro)).digest(),
        )


class TestPartitionSelection(unittest.TestCase):
    def test_through_limits_the_batch_layer_to_settled_history(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as raiz:
            for dia in ("2026-08-24", "2026-08-25", "2026-08-26"):
                caminho = os.path.join(raiz, f"ingestion_date={dia}", "wh=mad1")
                os.makedirs(caminho)
                open(os.path.join(caminho, "_SUCCESS"), "w").close()
            self.assertEqual(len(partitions_of(raiz)), 3)
            self.assertEqual(len(partitions_of(raiz, through="2026-08-25")), 2)

    def test_a_partition_without_success_is_not_read(self):
        """`_SUCCESS` é o último objeto que a Source grava. Ler uma partição sem ele seria
        ler uma extração interrompida como se estivesse completa."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as raiz:
            os.makedirs(os.path.join(raiz, "ingestion_date=2026-08-24", "wh=mad1"))
            self.assertEqual(partitions_of(raiz), [])


if __name__ == "__main__":
    unittest.main()
