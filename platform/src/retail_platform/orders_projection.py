"""A projeção viva em Iceberg: dois escritores, um leitor, e a fusão monotônica.

POR QUE ICEBERG, E POR QUE SÓ AGORA.

O gatilho escrito era *"um segundo engine precisar **escrever** a mesma tabela"*. Ele não
disparou até aqui e está registrado que não disparou. Dispara neste marco porque
`live_order_state` passa a ter dois escritores por desenho:

    orders-project              consumidor em streaming, evento a evento
    orders-rebuild-projection   reconstrução em lote, a partir do RAW

e um leitor concorrente (o DuckDB, via `silver_live_order_state`). **O gatilho disparou por
CONCORRÊNCIA, não por volume** — no volume desta fase um parquet reescrito com `os.replace`
atômico funcionaria. O que o Iceberg compra aqui é isolamento de snapshot entre dois
escritores e um leitor, mais time travel na projeção. Escrever isso é a diferença entre uma
decisão e uma moda.

O QUE O EXPERIMENTO FECHADO (`make spike-iceberg`) MEDIU ANTES DESTE MÓDULO EXISTIR:

  - o catálogo SQL sobre o Postgres funciona, e `upsert(join_cols=["order_id"])` também;
  - dois escritores concorrentes NÃO se sobrescrevem em silêncio: o que parte de um snapshot
    velho leva `CommitFailedException`. Isso é o controle otimista funcionando — se passasse
    calado, seria lost update;
  - o DuckDB lê pelo `metadata_location`, e SÓ por ele: ele se recusa a adivinhar qual é o
    metadado corrente varrendo o storage, porque isso pode ler metadado não commitado.

A TERCEIRA CONCLUSÃO MUDOU O DESENHO. O modelo dbt não pode apontar para um caminho fixo:
quem sabe qual metadado é o corrente é o CATÁLOGO, e é a ele que se pergunta. `make silver`
resolve o `metadata_location` e o passa como var. O atalho existe (`SET
unsafe_enable_version_guessing = true`, medido e funcionando) e foi recusado: ler metadado
não commitado é exatamente o que uma leitura concorrente não pode fazer.

RETRY NÃO BASTA — A FUSÃO PRECISA SER MONOTÔNICA. Reconciliar um conflito recarregando e
tentando de novo resolve o conflito de COMMIT, e ainda assim perde dado: se o outro escritor
gravou o pedido X no `sequence_no` 7 e a nossa tentativa carrega o 5, o retry cego escreve o
5 por cima. O commit passa, a tabela regride. Por isso cada tentativa relê o estado das
chaves afetadas e descarta as próprias linhas que não avançam — a mesma guarda de
`last_sequence_no` que protege o OLTP e o consumidor, agora protegendo a escrita concorrente.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from .orders_stream import DEFAULT_SEEDS_DIR, fold_event, read_sla_minutes

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "retail")
CATALOG_DB = os.environ.get("ICEBERG_CATALOG_DB", "iceberg_catalog")
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "projection")
TABLE_NAME = f"{NAMESPACE}.live_order_state"
WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE", "s3://retail-lakehouse/iceberg"
)
MAX_COMMIT_ATTEMPTS = 20

WRITER_STREAM = "stream"
WRITER_REBUILD = "rebuild"


class OrdersProjectionError(Exception):
    """O catálogo, a tabela ou a reconciliação recusaram."""


# --------------------------------------------------------------------------------------
# Esquema
# --------------------------------------------------------------------------------------
#
# `written_by` é PROVENIÊNCIA, não uma segunda cópia do estado: registra qual dos dois
# escritores tocou a linha por último. Sem ela, "os dois escritores escreveram de verdade"
# seria uma afirmação sobre logs, e não um fato consultável na própria tabela.

COLUMNS = (
    "order_id", "wh", "order_date", "customer_id", "status", "last_sequence_no",
    "events_applied", "placed_at", "updated_at", "confirmed_at", "picking_started_at",
    "picked_at", "dispatched_at", "terminal_at", "line_count", "picked_line_count",
    "substituted_lines", "removed_lines", "gross_amount", "net_amount", "returned_amount",
    "delivered_within_slot", "picking_minutes", "sla_breached", "written_by",
)

_TIMESTAMPS = ("placed_at", "updated_at", "confirmed_at", "picking_started_at",
               "picked_at", "dispatched_at", "terminal_at")
_MONEY = ("gross_amount", "net_amount", "returned_amount")
_INTS = ("last_sequence_no", "events_applied", "line_count", "picked_line_count",
         "substituted_lines", "removed_lines")


def arrow_schema():
    import pyarrow as pa

    campos = {
        "order_id": pa.string(), "wh": pa.string(), "order_date": pa.date32(),
        "customer_id": pa.string(), "status": pa.string(), "written_by": pa.string(),
        "delivered_within_slot": pa.bool_(), "sla_breached": pa.bool_(),
        "picking_minutes": pa.decimal128(10, 2),
    }
    for nome in _TIMESTAMPS:
        campos[nome] = pa.timestamp("us", tz="UTC")
    for nome in _MONEY:
        campos[nome] = pa.decimal128(12, 2)
    for nome in _INTS:
        campos[nome] = pa.int32()
    # `order_id` é a chave do upsert e não pode ser nula; o resto pode, e a nulidade tem
    # significado (ver `net_amount`, que é indeterminado até a separação).
    return pa.schema([
        pa.field(nome, campos[nome], nullable=(nome != "order_id")) for nome in COLUMNS
    ])


def _normalize(state: dict, writer: str) -> dict:
    """Estado do fold -> tipos do Iceberg. Decimal para dinheiro, nunca float."""
    linha = {}
    for nome in COLUMNS:
        valor = state.get(nome)
        if nome == "written_by":
            valor = writer
        elif nome in _MONEY and valor is not None:
            valor = Decimal(str(valor))
        elif nome == "picking_minutes" and valor is not None:
            valor = Decimal(str(valor))
        elif nome in _TIMESTAMPS and isinstance(valor, str):
            valor = datetime.fromisoformat(valor.replace("Z", "+00:00"))
        elif nome == "order_date" and isinstance(valor, str):
            valor = date.fromisoformat(valor)
        linha[nome] = valor
    return linha


# --------------------------------------------------------------------------------------
# Catálogo
# --------------------------------------------------------------------------------------

def catalog(config=None, *, dsn: str = None, warehouse: str = None):
    """Catálogo SQL sobre o Postgres do OLTP. Importa pyiceberg sob demanda.

    `py-io-impl=PyArrowFileIO` e NÃO s3fs: o extra `s3fs` do pyiceberg arrasta um
    `aiobotocore` que fixa um botocore antigo e quebra o `boto3` que a plataforma usa para o
    RAW. O pip aceita instalar e o estrago aparece depois, noutro módulo. Medido.
    """
    from .config import from_env

    config = config or from_env()
    try:
        from pyiceberg.catalog.sql import SqlCatalog
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise OrdersProjectionError(
            "pyiceberg nao esta instalado neste venv. Rode `make venv`."
        ) from exc

    porta = os.environ.get("OLTP_PORT", "5433")
    uri = dsn or os.environ.get(
        "ICEBERG_CATALOG_URI",
        f"postgresql+psycopg://oltp:oltp@localhost:{porta}/{CATALOG_DB}",
    )
    try:
        return SqlCatalog(CATALOG_NAME, **{
            "uri": uri,
            "warehouse": warehouse or WAREHOUSE,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": config.endpoint,
            "s3.access-key-id": config.access_key,
            "s3.secret-access-key": config.secret_key,
            "s3.region": config.region,
        })
    except Exception as exc:
        raise OrdersProjectionError(
            f"nao foi possivel abrir o catalogo Iceberg em {uri}: {exc}\n"
            "O plano de stream nao sobe com `make up`. Rode `make stream-up`."
        ) from exc


def ensure_catalog_database(port: str = None) -> bool:
    """Cria o banco do catálogo se faltar. Devolve True se criou."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise OrdersProjectionError("psycopg nao esta instalado neste venv.") from exc
    porta = port or os.environ.get("OLTP_PORT", "5433")
    admin = f"postgresql://oltp:oltp@localhost:{porta}/postgres"
    with psycopg.connect(admin, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (CATALOG_DB,))
            if cur.fetchone():
                return False
            cur.execute(f'create database "{CATALOG_DB}"')
    return True


def ensure_table(cat=None):
    cat = cat or catalog()
    cat.create_namespace_if_not_exists(NAMESPACE)
    if not cat.table_exists(TABLE_NAME):
        cat.create_table(TABLE_NAME, schema=arrow_schema())
    return cat.load_table(TABLE_NAME)


def metadata_location(cat=None) -> str:
    """Onde está o metadado CORRENTE, segundo o catálogo.

    É esta função que o `make silver` chama. O DuckDB recusa descobrir isso sozinho — e a
    recusa dele está certa: varrer o storage atrás do metadado mais novo pode encontrar um
    que ainda não foi commitado. Quem sabe é o catálogo.
    """
    cat = cat or catalog()
    if not cat.table_exists(TABLE_NAME):
        raise OrdersProjectionError(
            f"{TABLE_NAME} nao existe no catalogo. Rode `make orders-projection-iceberg-init`."
        )
    return cat.load_table(TABLE_NAME).metadata_location


# --------------------------------------------------------------------------------------
# O sink: a MESMA interface pequena de PostgresProjection
# --------------------------------------------------------------------------------------

@dataclass
class CommitStats:
    attempts: int = 0
    conflicts: int = 0
    rows_written: int = 0
    rows_dropped_as_stale: int = 0


class IcebergProjection:
    """Sink em Iceberg. `load` / `save` / `commit` / `rollback` / `count` / `digest`.

    A interface é idêntica à de `PostgresProjection` de propósito: era a costura declarada no
    Marco 5, e trocar o armazenamento não muda uma linha do consumidor. Se esta classe
    precisasse de um método a mais para `orders-project` funcionar, a costura teria vazado.

    A diferença de semântica está escondida onde deve estar: no Postgres, `save` escreve e
    `commit` fecha a transação; aqui, `save` acumula e `commit` faz o upsert — porque o
    Iceberg não tem transação aberta, cada commit é um snapshot novo.
    """

    def __init__(self, table, *, writer: str = WRITER_STREAM, cat=None):
        self._table = table
        self._catalog = cat
        self._writer = writer
        self._buffer: dict = {}
        self.stats = CommitStats()

    # ---- leitura -----------------------------------------------------------
    def _current(self, order_ids=None) -> dict:
        """Estado corrente, relido do snapshot mais novo.

        Varre a tabela inteira e filtra em memória em vez de montar um `In(...)` com
        milhares de valores: neste volume (~6.400 linhas) a varredura de duas colunas é
        trivial, e um filtro gigante empurrado para o scan não é. Se a projeção crescer
        ordens de grandeza, este é o ponto a trocar — e está dito aqui para não virar
        descoberta.
        """
        linhas = self._table.scan(selected_fields=COLUMNS).to_arrow().to_pylist()
        if order_ids is None:
            return {linha["order_id"]: linha for linha in linhas}
        alvo = set(order_ids)
        return {l["order_id"]: l for l in linhas if l["order_id"] in alvo}

    def load(self, order_ids) -> dict:
        if not order_ids:
            return {}
        return self._current(order_ids)

    def save(self, states) -> None:
        for state in states:
            self._buffer[state["order_id"]] = state

    def rollback(self) -> None:
        self._buffer.clear()

    def commit(self) -> CommitStats:
        """Upsert com retry E FUSÃO MONOTÔNICA. Ver o cabeçalho do módulo.

        Retry sozinho resolveria o conflito de commit e ainda assim perderia dado: se o outro
        escritor já gravou um `sequence_no` maior para o mesmo pedido, reescrever por cima faz
        a tabela REGREDIR — e o commit passaria, porque do ponto de vista do Iceberg nada
        está errado. Por isso cada tentativa relê e descarta as próprias linhas que não
        avançam.
        """
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        if not self._buffer:
            return self.stats

        stats = CommitStats()
        for tentativa in range(1, MAX_COMMIT_ATTEMPTS + 1):
            stats.attempts = tentativa
            correntes = self._current(self._buffer.keys())
            candidatas = [
                estado for order_id, estado in sorted(self._buffer.items())
                if order_id not in correntes
                or estado["last_sequence_no"] > correntes[order_id]["last_sequence_no"]
            ]
            stats.rows_dropped_as_stale = len(self._buffer) - len(candidatas)
            if not candidatas:
                # Outro escritor já gravou um estado igual ou mais novo para todas as
                # chaves. Não escrever é o resultado CORRETO, não uma falha.
                self._buffer.clear()
                self.stats = stats
                return stats
            lote = pa.Table.from_pylist(
                [_normalize(estado, self._writer) for estado in candidatas],
                schema=arrow_schema(),
            )
            try:
                self._table.upsert(lote, join_cols=["order_id"])
                stats.rows_written = len(candidatas)
                self._buffer.clear()
                self.stats = stats
                return stats
            except CommitFailedException:
                stats.conflicts += 1
                self._refresh()
        raise OrdersProjectionError(
            f"{MAX_COMMIT_ATTEMPTS} tentativas de commit sem sucesso: a tabela esta sob "
            f"contencao acima do que este escritor sabe tratar"
        )

    def _refresh(self) -> None:
        """Recarrega A PRÓPRIA tabela, e não um nome cravado.

        A primeira versão recarregava `TABLE_NAME`. Enquanto só existiu uma tabela, isso
        funcionou — e no primeiro teste que usou uma tabela de sonda, um conflito fez o
        escritor da sonda recarregar a tabela de PRODUÇÃO e gravar nela. Quatro linhas
        sintéticas entraram em `live_order_state`, e a reconciliação foi quem apontou.

        Um identificador cravado numa função de refresh é sempre isto: funciona até existir
        um segundo objeto, e aí escreve no lugar errado sem erro nenhum.
        """
        if self._catalog is not None:
            self._table = self._catalog.load_table(self._table.name())
        else:
            self._table.refresh()

    # ---- observação ---------------------------------------------------------
    def count(self) -> int:
        return self._table.scan(selected_fields=("order_id",)).to_arrow().num_rows

    def digest(self) -> str:
        """Impressão digital, comparável com a de `PostgresProjection`.

        `written_by` FICA DE FORA: ele é proveniência, não estado. Incluí-lo faria o digest
        mudar quando o mesmo conteúdo fosse escrito pelo outro escritor — e a propriedade que
        se quer medir é justamente que os dois produzem o mesmo estado.
        """
        colunas = [c for c in COLUMNS if c != "written_by"]
        linhas = self._table.scan(selected_fields=tuple(colunas)).to_arrow().to_pylist()
        sha = hashlib.sha256()
        for linha in sorted(linhas, key=lambda l: l["order_id"]):
            sha.update(json.dumps([str(linha.get(c)) for c in colunas],
                                  ensure_ascii=False).encode("utf-8"))
            sha.update(b"\n")
        return sha.hexdigest()

    def writers_seen(self) -> dict:
        linhas = self._table.scan(selected_fields=("written_by",)).to_arrow().to_pylist()
        saida: dict = {}
        for linha in linhas:
            saida[linha["written_by"]] = saida.get(linha["written_by"], 0) + 1
        return saida

    def snapshots(self) -> list:
        return [s.snapshot_id for s in self._table.metadata.snapshots]


# --------------------------------------------------------------------------------------
# O segundo escritor: reconstrução em lote a partir do RAW
# --------------------------------------------------------------------------------------

@dataclass
class RebuildResult:
    partitions: int = 0
    events: int = 0
    orders: int = 0
    commits: int = 0
    conflicts: int = 0
    dropped_as_stale: int = 0
    partitions_read: list = field(default_factory=list)


def reset_table(cat=None):
    """Apaga e recria `live_order_state`. Existe por causa de UMA condicao especifica.

    A fusao da projecao e MONOTONICA: uma linha com `last_sequence_no` menor que a gravada e
    descartada como velha, e e assim que os dois escritores convivem sem se atropelar. A
    fusao assume, sem dizer, que um `order_id` sempre se refere ao MESMO pedido.

    Essa premissa cai quando a Source e regerada com outra `seed`, outra referencia, outras
    premissas ou outro `demand_model_version` — as quatro condicoes nao-aditivas do
    CONTRACT. Ai o pedido `ord_mad1_20260824_000001` e um pedido DIFERENTE com o mesmo id, e
    o merge monotonico mistura os dois: MEDIDO em 2026-08-31, um rebuild depois da
    recalibracao descartou 4.028 linhas por "estado igual ou mais novo" e deixou a projecao
    com uma mistura de dois universos. `orders-reconcile` pegou.

    Reset e destrutivo de proposito e nunca acontece sozinho — so com --reset explicito.
    """
    cat = cat or catalog()
    if cat.table_exists(TABLE_NAME):
        cat.drop_table(TABLE_NAME)
    return ensure_table(cat)


def rebuild(cat, partitions, *, sla_minutes: int = None,
            seeds_dir: str = DEFAULT_SEEDS_DIR, batch_size: int = 500,
            reset: bool = False) -> RebuildResult:
    """Reconstrói a projeção a partir do log em disco. É o SEGUNDO ESCRITOR.

    ELE COMPARTILHA `fold_event` COM O CONSUMIDOR, E ISSO É DELIBERADO — mas exige uma
    ressalva, senão a leitura fica errada: o acordo entre este caminho e o streaming NÃO é
    evidência de correção, é evidência de que os dois escritores não se atropelam. A
    evidência de correção vem de `orders-reconcile`, que compara com `silver_order` — uma
    implementação genuinamente independente, em window function sobre o log inteiro.

    Confundir as duas coisas seria o mesmo defeito que este projeto já registrou duas vezes:
    uma verificação que compara algo consigo mesmo e passa.
    """
    from .orders_oltp import read_log, verify_log

    sla = sla_minutes if sla_minutes is not None else read_sla_minutes(seeds_dir)
    tabela = reset_table(cat) if reset else ensure_table(cat)
    projection = IcebergProjection(tabela, writer=WRITER_REBUILD, cat=cat)
    resultado = RebuildResult()
    estados: dict = {}

    for caminho in partitions:
        verify_log(caminho)
        resultado.partitions += 1
        resultado.partitions_read.append(caminho)
        for evento, _linha in read_log(caminho):
            resultado.events += 1
            atual = estados.get(evento["order_id"])
            estados[evento["order_id"]] = fold_event(atual, evento, sla_minutes=sla)

    resultado.orders = len(estados)
    ordenados = [estados[k] for k in sorted(estados)]
    for inicio in range(0, len(ordenados), batch_size):
        projection.save(ordenados[inicio:inicio + batch_size])
        stats = projection.commit()
        resultado.commits += 1
        resultado.conflicts += stats.conflicts
        resultado.dropped_as_stale += stats.rows_dropped_as_stale
    return resultado


# --------------------------------------------------------------------------------------
# Reconciliação: três folds independentes
# --------------------------------------------------------------------------------------

_COMPARE = ("status", "last_sequence_no", "line_count", "picked_line_count",
            "gross_amount", "net_amount", "substituted_lines", "removed_lines")


def _as_key(valor):
    """Normaliza para comparar entre três motores sem confundir TIPO com DIVERGÊNCIA.

    NORMALIZA A ESCALA, NUNCA ARREDONDA. A primeira versão formatava com `f"{v:.2f}"`, e isso
    fazia `10.001` e `10.00` compararem IGUAIS — uma reconciliação que arredonda a diferença
    que veio verificar. Hoje as três fontes usam `decimal(12,2)` e o defeito não teria
    aparecido; apareceria no dia em que uma delas mudasse de escala, que é o pior dia
    possível para descobri-lo.

    `Decimal.normalize()` remove zeros à direita sem tocar no valor: `10.00` e `10` viram a
    mesma chave, `10.001` não.
    """
    if valor is None:
        return None
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, Decimal):
        return f"num:{valor.normalize()}"
    if isinstance(valor, float):
        return f"num:{Decimal(str(valor)).normalize()}"
    if isinstance(valor, int):
        return f"num:{Decimal(valor).normalize()}"
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    return str(valor)


def _rows_from_iceberg(cat) -> dict:
    tabela = ensure_table(cat)
    linhas = tabela.scan(selected_fields=("order_id",) + _COMPARE).to_arrow().to_pylist()
    return {l["order_id"]: {c: _as_key(l.get(c)) for c in _COMPARE} for l in linhas}


def _rows_from_silver(config) -> dict:
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    cursor = connection.execute(
        """
        select order_id, order_status, last_sequence_no, line_count_placed,
               line_count_picked, gross_amount_placed, net_amount,
               substituted_lines, removed_lines
          from silver_order
        """
    )
    saida = {}
    for linha in cursor.fetchall():
        saida[linha[0]] = dict(zip(_COMPARE, [_as_key(v) for v in linha[1:]]))
    return saida


def _rows_from_oltp(dsn=None) -> dict:
    from .orders_oltp import connect

    with connect(dsn) as connection:
        with connection.cursor() as cur:
            cur.execute(
                """
                select o.order_id, o.status, o.last_sequence_no, o.line_count,
                       o.picked_line_count, o.gross_amount, o.net_amount,
                       count(*) filter (where l.status = 'substituted'),
                       count(*) filter (where l.status = 'removed')
                  from orders o left join order_line l on l.order_id = o.order_id
                 group by o.order_id
                """
            )
            return {
                linha[0]: dict(zip(_COMPARE, [_as_key(v) for v in linha[1:]]))
                for linha in cur.fetchall()
            }


def reconcile(config=None, *, cat=None, dsn=None, limit: int = 10) -> dict:
    """Compara os TRÊS folds. Divergiu, reprova — não arredonda, não ignora.

    `silver_order` é a única das três implementações que não compartilha código com nenhuma
    outra: window function em SQL sobre o log inteiro. É contra ela que a comparação vale
    como verificação; o acordo entre projeção e OLTP vale como evidência de transporte.
    """
    from .config import from_env

    config = config or from_env()
    cat = cat or catalog(config)
    fontes = {
        "iceberg": _rows_from_iceberg(cat),
        "silver": _rows_from_silver(config),
        "oltp": _rows_from_oltp(dsn),
    }
    chaves = {nome: set(linhas) for nome, linhas in fontes.items()}
    todas = set().union(*chaves.values())

    faltando = {
        nome: sorted(todas - presentes)[:limit] for nome, presentes in chaves.items()
        if todas - presentes
    }
    divergencias = []
    comuns = set.intersection(*chaves.values()) if chaves else set()
    for order_id in sorted(comuns):
        for coluna in _COMPARE:
            valores = {nome: fontes[nome][order_id][coluna] for nome in fontes}
            if len(set(map(repr, valores.values()))) > 1:
                divergencias.append({"order_id": order_id, "coluna": coluna, **valores})
    return {
        "orders": {nome: len(linhas) for nome, linhas in fontes.items()},
        "compared": len(comuns),
        "missing": faltando,
        "divergences": divergencias[:limit],
        "divergence_count": len(divergencias),
        "ok": not faltando and not divergencias,
    }


def partitions_of(root: str = "data/orders", *, through: str = None) -> list:
    """Partições de pedidos em disco, em ordem determinística.

    `through` limita a `ingestion_date <= through`, que é o recorte natural do par lambda: a
    camada em lote cobre a história já assentada, e o streaming cobre a cauda viva. Sem ele,
    as duas camadas recalculam exatamente a mesma coisa — o que funciona, e desperdiça.
    """
    saida = []
    if not os.path.isdir(root):
        return saida
    for dia in sorted(os.listdir(root)):
        if not dia.startswith("ingestion_date="):
            continue
        if through is not None and dia.split("=", 1)[1] > through:
            continue
        caminho_dia = os.path.join(root, dia)
        for eixo in sorted(os.listdir(caminho_dia)):
            caminho = os.path.join(caminho_dia, eixo)
            if os.path.exists(os.path.join(caminho, "_SUCCESS")):
                saida.append(caminho)
    return saida
