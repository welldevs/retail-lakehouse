"""Duplo de tabela Iceberg: estado em memória, conflito sob demanda.

O QUE ELE PROVA. A fusão monotônica e o laço de retry são a parte de `IcebergProjection` que
decide se dois escritores se atropelam — e ela é lógica pura sobre `last_sequence_no`, não
propriedade do formato. Exercê-la offline é rápido e determinístico, e permite forçar o
conflito exatamente onde se quer.

O QUE ELE NÃO PROVA, e por isso `make orders-prove-projection` existe: que o Iceberg de
verdade RECUSA um commit vindo de snapshot velho. Um duplo que levanta
`CommitFailedException` só demonstra que o duplo levanta. Mesmo par de fake_pg.py com
`make orders-prove-atomicity` e de fake_kafka.py com `make orders-prove-stream`.
"""

from __future__ import annotations


class FakeSnapshot:
    def __init__(self, snapshot_id):
        self.snapshot_id = snapshot_id


class FakeMetadata:
    def __init__(self, snapshots):
        self.snapshots = snapshots


class _FakeArrow:
    def __init__(self, linhas):
        self._linhas = linhas

    def to_pylist(self):
        return [dict(linha) for linha in self._linhas]

    @property
    def num_rows(self):
        return len(self._linhas)


class _FakeScan:
    def __init__(self, linhas, campos):
        self._linhas = linhas
        self._campos = campos

    def to_arrow(self):
        if self._campos is None:
            return _FakeArrow(self._linhas)
        return _FakeArrow([{c: linha.get(c) for c in self._campos}
                           for linha in self._linhas])


class FakeIcebergTable:
    """Tabela com `scan`/`upsert`/`refresh` e um roteiro de conflitos.

    `conflicts_before_success` faz os N primeiros `upsert` levantarem
    `CommitFailedException`, que é como o pyiceberg sinaliza "o branch mudou desde que você
    carregou esta referência".

    `on_conflict_state` permite que outro escritor "apareça" entre a recusa e o retry — é o
    cenário que separa retry cego de fusão monotônica.
    """

    def __init__(self, linhas=(), *, conflicts_before_success=0, on_conflict_state=None):
        self.rows = {linha["order_id"]: dict(linha) for linha in linhas}
        self.conflicts_before_success = conflicts_before_success
        self.on_conflict_state = on_conflict_state
        self.upserts = 0
        self.refreshes = 0
        self.journal: list[tuple] = []
        self._snapshots = [FakeSnapshot(1)]

    # ---- interface consumida por IcebergProjection -------------------------
    def scan(self, selected_fields=None, snapshot_id=None):
        self.journal.append(("scan", tuple(selected_fields or ())))
        return _FakeScan(list(self.rows.values()), selected_fields)

    def upsert(self, tabela_arrow, join_cols=("order_id",)):
        from pyiceberg.exceptions import CommitFailedException

        self.upserts += 1
        if self.conflicts_before_success > 0:
            self.conflicts_before_success -= 1
            self.journal.append(("conflito",))
            if self.on_conflict_state:
                # Outro escritor commitou primeiro. É exatamente aqui que um retry cego
                # escreveria um estado antigo por cima.
                for linha in self.on_conflict_state:
                    self.rows[linha["order_id"]] = dict(linha)
                self.on_conflict_state = None
            raise CommitFailedException("branch main has changed")
        linhas = tabela_arrow.to_pylist()
        self.journal.append(("upsert", tuple(sorted(l["order_id"] for l in linhas))))
        for linha in linhas:
            self.rows[linha["order_id"]] = dict(linha)
        self._snapshots.append(FakeSnapshot(len(self._snapshots) + 1))
        return None

    def refresh(self):
        self.refreshes += 1
        self.journal.append(("refresh",))

    @property
    def metadata(self):
        return FakeMetadata(list(self._snapshots))

    def name(self):
        return ("projection", "live_order_state")


class FakeCatalog:
    """Catálogo que devolve sempre a MESMA tabela — o refresh do duplo é o `refresh()`."""

    def __init__(self, table):
        self._table = table
        self.loads = 0

    def load_table(self, _name):
        self.loads += 1
        self._table.journal.append(("load_table",))
        return self._table
