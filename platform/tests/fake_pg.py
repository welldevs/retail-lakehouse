"""Duplo de conexao Postgres que GRAVA A FRONTEIRA DA TRANSACAO.

O QUE ELE PROVA, E O QUE ELE NAO PROVA — a distincao importa mais que o codigo.

  PROVA: que `apply_event` emite a linha do outbox e as mudancas de estado entre o mesmo
  inicio e o mesmo `commit`, sem commit intermediario, e que qualquer falha no meio termina
  em `rollback` e nunca em `commit`. Esse e o defeito que se comete de verdade ao escrever
  um outbox — um `commit()` a mais no meio, e o padrao vira dual-write sem que nada mude de
  aparencia. Um duplo que grava a ordem das chamadas pega isso, e pega offline.

  NAO PROVA: que `rollback` desfaz alguma coisa. Isso e propriedade do MOTOR, e nenhum
  duplo pode demonstra-la — um duplo que "desfaz" so demonstra que o duplo desfaz. A prova
  correspondente e `make orders-prove-atomicity`, contra um Postgres de verdade, com um
  trigger que faz o insert no outbox explodir.

As duas provas sao necessarias e nenhuma substitui a outra. E o mesmo par de
`fake_s3.py` (valida o checksum como o servidor validaria) e `verify-landing` (le o objeto
de volta do MinIO de verdade).
"""

from __future__ import annotations

import re


class FakePgError(Exception):
    """O que o duplo levanta no lugar de psycopg.Error."""


# ANCORADOS NO INICIO, e nao em qualquer posicao. Um `select ... for update skip locked` —
# que e exatamente como o publisher drena o outbox — contem a palavra "update" no meio, e um
# regex sem ancora o etiquetaria como escrita. O teste entao asseriria sobre a etiqueta
# errada e passaria pelo motivo errado; foi o que aconteceu na primeira versao deste duplo.
_INSERT = re.compile(r"^insert\s+into\s+(\w+)", re.I)
_UPDATE = re.compile(r"^update\s+(\w+)", re.I)
_SELECT = re.compile(r"^(select|with)\b", re.I)
_DELETE = re.compile(r"^delete\s+from\s+(\w+)", re.I)


def tag(sql: str) -> str:
    """Etiqueta legivel do comando, para que a asercao do teste fale a lingua do dominio."""
    text = " ".join(sql.split())
    for pattern, prefix in ((_INSERT, "insert"), (_UPDATE, "update"), (_DELETE, "delete")):
        found = pattern.match(text)
        if found:
            return f"{prefix}:{found.group(1).lower()}"
    if _SELECT.match(text):
        return "select"
    return text.split(" ")[0].lower()


class FakeCursor:
    def __init__(self, connection):
        self._connection = connection
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        label = tag(sql)
        self._connection.journal.append(("execute", label, params))
        if self._connection.fail_on is not None and self._connection.fail_on(label, params):
            raise FakePgError(f"falha injetada em {label}")
        self.rowcount = self._connection.rowcount_for(label, params)
        self._connection._pending = self._connection.result_for(label, params)
        return self

    def fetchone(self):
        rows = self._connection._pending
        return rows[0] if rows else None

    def fetchall(self):
        return list(self._connection._pending)


class FakeConnection:
    """Conexao de mentira com diario. `journal` e o artefato que os testes leem.

    `rowcounts` e `results` sao mapas de etiqueta -> valor (ou callable(params)), para que
    um teste escreva o cenario em vez de descrever SQL.
    """

    def __init__(self, *, rowcounts=None, results=None, fail_on=None, applied_event_ids=()):
        self.journal: list[tuple] = []
        self.rowcounts = rowcounts or {}
        self.results = results or {}
        self.fail_on = fail_on
        # Eventos que o outbox ja tem. E o que faz `on conflict do nothing` devolver 0
        # linhas, que e como o replay descobre que nao ha nada a fazer.
        self.applied_event_ids = set(applied_event_ids)
        self._pending: list = []
        self._queues: dict = {}
        self.closed = False

    # ---- interface consumida por orders_oltp.py ----------------------------
    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.journal.append(("commit",))

    def rollback(self):
        self.journal.append(("rollback",))

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---- roteiro ------------------------------------------------------------
    def rowcount_for(self, label, params):
        if label == "insert:outbox":
            # params[0] e event_id. Ja aplicado -> 0 linhas, exatamente como o Postgres
            # responde a um `on conflict (event_id) do nothing`.
            return 0 if params and params[0] in self.applied_event_ids else 1
        value = self.rowcounts.get(label, 1)
        return value(params) if callable(value) else value

    def result_for(self, label, params):
        value = self.results.get(label, [])
        if callable(value):
            return value(params)
        # Roteiro em fila: uma lista DE listas e consumida em ordem, uma por chamada. E o que
        # permite escrever um cenario em que tres SELECTs seguidos devolvem coisas
        # diferentes — o caso do `order_picked`, que confere tres invariantes distintos.
        if value and isinstance(value[0], list):
            queue = self._queues.setdefault(label, list(value))
            return queue.pop(0) if queue else []
        return value

    # ---- leitura do diario --------------------------------------------------
    @property
    def tags(self) -> list[str]:
        return [row[1] if row[0] == "execute" else row[0] for row in self.journal]

    @property
    def commits(self) -> int:
        return sum(1 for row in self.journal if row[0] == "commit")

    @property
    def rollbacks(self) -> int:
        return sum(1 for row in self.journal if row[0] == "rollback")

    def params_of(self, label) -> list:
        return [row[2] for row in self.journal if row[0] == "execute" and row[1] == label]
