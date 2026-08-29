"""O OLTP de pedidos e seu outbox transacional.

POR QUE ISTO EXISTE. O gatilho escrito para o Kafka em ARCHITECTURE.md e "uma source
genuinamente event-driven: POS, webhook, CDC DE UM OLTP". Republicar o log em lote num
topico nao satisfaz esse gatilho — adicionaria um broker e zero informacao. O que satisfaz
e o evento NASCER dentro da transacao que muda o estado do pedido. E isso que este modulo
faz, e a propriedade que ele prova — atomicidade — e a unica razao pela qual o proximo
marco tem direito de existir.

A PROPRIEDADE, DITA COM PRECISAO: para todo evento, ou (a mudanca de estado E a linha do
outbox) sao visiveis, ou nenhuma das duas e. Nunca uma sem a outra. Sem isso, o estado do
OLTP e o topico divergem sem que nada reprove, e a divergencia so aparece semanas depois
num numero plausivel.

O QUE O DUPLO PROVA E O QUE SO O POSTGRES PROVA. Os testes offline (fake_pg.py) provam a
FRONTEIRA: que as duas escritas saem entre o mesmo BEGIN e o mesmo COMMIT, sem commit
intermediario — que e o defeito real que se comete escrevendo isto (um `commit()` a mais no
meio transforma outbox em dual-write). Que a fronteira SIGNIFICA alguma coisa e propriedade
do motor, e so se prova contra um Postgres de verdade: `make orders-prove-atomicity`
injeta um trigger que faz o insert no outbox explodir e confere que a mudanca de estado
tambem nao sobreviveu. As duas provas sao necessarias e nenhuma substitui a outra.

TRES GUARDAS INDEPENDENTES, QUE PRECISAM CONCORDAR:

  1. `outbox.event_id` UNIQUE          — reaplicar o mesmo evento nao duplica nada
  2. `orders.last_sequence_no`         — o OLTP RECUSA evento fora de ordem
  3. `orders.status` em `from_states`  — o OLTP RECUSA transicao invalida

A guarda 2 e o que da sentido a chave de particao do Kafka no Marco 5: se o OLTP aceitasse
evento fora de ordem, preservar ordem por `order_id` no broker seria enfeite. E porque ele
recusa que `key = order_id` passa a ser uma exigencia, e nao uma preferencia.

A MAQUINA DE ESTADOS E REDECLARADA AQUI, NAO IMPORTADA. A Source e FROZEN e esta plataforma
nunca importa o codigo dela — o contrato e fisico. Declarar as transicoes de novo, a partir
do CONTRACT.md secao 5, e o mesmo principio de `verify.py` reler o objeto em vez de confiar
no que acabou de escrever: verificacao em vez de confianca. Se as duas declaracoes
divergirem, `orders-apply` reprova em voz alta na primeira transicao afetada.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from . import manifest as manifest_reader

SOURCE_NAME = "simulated_orders"
EVENTS_FILE = "order_events.jsonl"

DEFAULT_DSN = os.environ.get(
    "OLTP_DSN", "postgresql://oltp:oltp@localhost:5433/oltp"
)


class OrdersOltpError(Exception):
    """O OLTP recusou o log: fora de ordem, transicao invalida ou valor divergente."""


# --------------------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------------------
#
# `outbox.event_json` guarda A LINHA CANONICA DO LOG, verbatim — nao uma reserializacao.
# E ela que vai para o broker no Marco 5, e e por isso que o log reconstruido a partir do
# outbox tem de bater sha256 com o manifesto da particao (`orders-outbox --verify`).
#
# As colunas do envelope existem para ROTEAR (chave, ordem, filtro), nao para guardar a
# verdade de novo. Para que as duas nao possam divergir, quatro CHECKs amarram cada coluna
# ao proprio JSON: uma linha nao consegue ser roteada sob uma chave que discorda do payload
# que ela carrega. E o motor que garante, nao a convencao.
#
# `payload` NAO e coluna: `event_json::jsonb -> 'payload'` responde qualquer consulta sem
# guardar uma segunda copia. Duas representacoes da mesma verdade divergem.
#
# `created_at default now()` e o UNICO relogio desta cadeia, e e legitimo: o log da Source
# nao tem `recorded_at` de proposito ("sem relogio"), porque atraso e reordenacao sao
# carimbados por quem RECEBE. Este e o momento de receber.

DDL = """
create table if not exists orders (
    order_id             text        primary key,
    wh                   text        not null,
    order_date           date        not null,
    customer_id          text        not null,
    status               text        not null,
    last_sequence_no     integer     not null check (last_sequence_no > 0),
    placed_at            timestamptz not null,
    updated_at           timestamptz not null,
    delivery_slot_start  timestamptz not null,
    delivery_slot_end    timestamptz not null,
    line_count           integer     not null check (line_count > 0),
    picked_line_count    integer,
    gross_amount         numeric(12,2) not null,
    -- NULL ATE SER DETERMINADO, e nao `gross` nem zero. `net_amount` responde "quanto foi
    -- efetivamente separado", e um pedido cancelado antes da separacao nunca respondeu isso.
    -- Inicializar com o valor colocado faria a coluna responder OUTRA pergunta ("quanto
    -- ainda vale o pedido") com o MESMO nome que `silver_order.net_amount` usa para a
    -- primeira — e `orders-reconcile`, no Marco 6, compararia duas perguntas diferentes.
    -- Medido: 298 dos 6.400 pedidos (196 cancelados, 102 com pagamento recusado) morrem
    -- antes da separacao. Eram exatamente as 298 divergencias entre os dois folds.
    net_amount           numeric(12,2),
    returned_amount      numeric(12,2) not null default 0,
    delivered_within_slot boolean,
    constraint orders_status_vocabulary check (status in (
        'PLACED', 'CONFIRMED', 'PICKING', 'PICKED', 'IN_TRANSIT', 'DELIVERED',
        'PAYMENT_FAILED', 'CANCELLED', 'DELIVERY_FAILED', 'RETURNED'))
);

create table if not exists order_line (
    order_id                    text    not null references orders(order_id),
    line_no                     integer not null check (line_no > 0),
    source_product_id           text    not null,
    quantity                    integer not null check (quantity > 0),
    unit_price                  numeric(12,2) not null,
    status                      text    not null,
    fulfilled_source_product_id text,
    fulfilled_unit_price        numeric(12,2),
    line_amount                 numeric(12,2) not null,
    primary key (order_id, line_no),
    -- O MESMO vocabulario de `silver_order_line.line_status`, e nao um parecido. Se os dois
    -- lados nomeassem o mesmo fato de formas diferentes, `orders-reconcile` (Marco 6)
    -- precisaria de uma tabela de traducao — e tabela de traducao entre dois folds e
    -- exatamente onde "compara duas coisas erradas e passa" mora.
    constraint order_line_status_vocabulary check (status in (
        'placed', 'fulfilled', 'substituted', 'removed', 'not_picked')),
    -- Linha removida nao tem produto cumprido nem valor. Linha substituida tem os dois.
    -- Sem isto, um applier com defeito deixaria `line_amount` antigo numa linha removida e
    -- o `net_amount` do pedido fecharia contra uma soma errada sem nada reprovar.
    constraint order_line_removed_has_no_amount check (
        status <> 'removed' or (fulfilled_source_product_id is null and line_amount = 0)),
    constraint order_line_substituted_has_a_substitute check (
        status <> 'substituted' or fulfilled_source_product_id is not null),
    constraint order_line_fulfilled_has_a_product check (
        status <> 'fulfilled' or fulfilled_source_product_id is not null),
    -- Linha de pedido que morreu antes da separacao nao entregou nada: sem produto cumprido
    -- e sem valor. O que o cliente PEDIU continua em source_product_id e unit_price.
    constraint order_line_not_picked_has_no_amount check (
        status <> 'not_picked'
        or (fulfilled_source_product_id is null and line_amount = 0))
);

create table if not exists outbox (
    outbox_id     bigserial   primary key,
    event_id      text        not null unique,
    order_id      text        not null,
    event_type    text        not null,
    event_version integer     not null,
    sequence_no   integer     not null,
    occurred_at   timestamptz not null,
    wh            text        not null,
    producer      text        not null,
    event_json    text        not null,
    created_at    timestamptz not null default now(),
    published_at  timestamptz,
    constraint outbox_routing_matches_event check (
        event_id    = (event_json::jsonb ->> 'event_id')
        and order_id    = (event_json::jsonb ->> 'order_id')
        and event_type  = (event_json::jsonb ->> 'event_type')
        and wh          = (event_json::jsonb ->> 'wh')
        and sequence_no = (event_json::jsonb ->> 'sequence_no')::integer)
);

-- Indice PARCIAL: a fila do publisher e so o que ainda nao foi publicado. Depois que o
-- outbox acumular milhoes de linhas publicadas, um indice cheio faria o publisher varrer
-- historico a cada rodada. Este indice encolhe conforme a fila drena, que e o oposto.
create index if not exists outbox_unpublished
    on outbox (outbox_id) where published_at is null;

create index if not exists outbox_order_sequence on outbox (order_id, sequence_no);
"""

DROP = """
drop table if exists outbox;
drop table if exists order_line;
drop table if exists orders;
"""


# --------------------------------------------------------------------------------------
# O plano de escrita: puro, testavel sem banco nenhum
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Statement:
    """Uma escrita, com o numero de linhas que ela OBRIGATORIAMENTE tem de afetar.

    `expect_rows` e a guarda: um update que nao encontra o pedido no estado esperado afeta
    zero linhas e o Postgres nao reclama. Sem conferir o rowcount, aplicar um evento fora
    de ordem seria silencioso — o pior modo de falha possivel numa cadeia de eventos.
    """

    sql: str
    params: tuple
    expect_rows: int | None
    detail: str


@dataclass(frozen=True)
class Check:
    """Uma conferencia escalar dentro da mesma transacao. Divergiu, a transacao inteira cai."""

    sql: str
    params: tuple
    expected: object
    detail: str


# Transicoes validas, redeclaradas do CONTRACT.md secao 5 da Source. `from_states` e o
# conjunto de estados a partir dos quais o evento pode ser aplicado.
STATE_BEFORE = {
    "order_payment_authorized": ("PLACED",),
    "order_payment_failed": ("PLACED",),
    "order_cancelled": ("PLACED", "CONFIRMED"),
    "order_picking_started": ("CONFIRMED",),
    "order_line_substituted": ("PICKING",),
    "order_line_removed": ("PICKING",),
    "order_picked": ("PICKING",),
    "order_dispatched": ("PICKED",),
    "order_delivered": ("IN_TRANSIT",),
    "order_delivery_failed": ("IN_TRANSIT",),
    "order_returned": ("DELIVERED",),
}

STATE_AFTER = {
    "order_payment_authorized": "CONFIRMED",
    "order_payment_failed": "PAYMENT_FAILED",
    "order_cancelled": "CANCELLED",
    "order_picking_started": "PICKING",
    "order_line_substituted": "PICKING",
    "order_line_removed": "PICKING",
    "order_picked": "PICKED",
    "order_dispatched": "IN_TRANSIT",
    "order_delivered": "DELIVERED",
    "order_delivery_failed": "DELIVERY_FAILED",
    "order_returned": "RETURNED",
}

_INSERT_ORDER = """
insert into orders (
    order_id, wh, order_date, customer_id, status, last_sequence_no,
    placed_at, updated_at, delivery_slot_start, delivery_slot_end,
    line_count, gross_amount)
values (%s, %s, %s, %s, 'PLACED', %s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_LINE = """
insert into order_line (
    order_id, line_no, source_product_id, quantity, unit_price, status, line_amount)
values (%s, %s, %s, %s, %s, 'placed', %s)
"""

# A guarda de ordem E a de estado na MESMA clausula where. Se qualquer uma nao valer, o
# update afeta zero linhas e `expect_rows=1` derruba a transacao. Fazer as duas conferencias
# num SELECT antes seria uma condicao de corrida: entre ler e escrever, outra sessao pode
# ter avancado o pedido.
_ADVANCE = """
update orders
   set status = %s,
       last_sequence_no = %s,
       updated_at = %s{extra}
 where order_id = %s
   and last_sequence_no = %s
   and status = any(%s)
"""

_RECOMPUTE_NET = """
update orders
   set net_amount = (select coalesce(sum(line_amount), 0)
                       from order_line where order_id = %s)
 where order_id = %s
"""


def _parse_ts(value: str) -> datetime:
    """ISO-8601 com sufixo Z. Convertido em Python para nao depender do parser do servidor."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _advance(event, status, *, extra="", extra_params=()):
    order_id = event["order_id"]
    seq = event["sequence_no"]
    allowed = list(STATE_BEFORE[event["event_type"]])
    params = (
        (status, seq, _parse_ts(event["occurred_at"]))
        + tuple(extra_params)
        + (order_id, seq - 1, allowed)
    )
    return Statement(
        _ADVANCE.format(extra=extra),
        params,
        expect_rows=1,
        detail=(
            f"{order_id} seq={seq} {event['event_type']}: pedido inexistente, evento fora "
            f"de ordem (esperado last_sequence_no={seq - 1}) ou estado fora de {allowed}"
        ),
    )


def plan(event) -> list:
    """As escritas de estado de UM evento. Funcao pura: nao le banco, nao tem relogio.

    Nao inclui a linha do outbox — quem monta a transacao e `_apply_event`, e a ordem la
    e deliberada (outbox primeiro, ver o comentario naquele ponto).
    """
    kind = event["event_type"]
    order_id = event["order_id"]
    payload = event["payload"]
    occurred_at = _parse_ts(event["occurred_at"])

    if kind == "order_placed":
        gross = Decimal(payload["gross_amount"])
        ops = [
            Statement(
                _INSERT_ORDER,
                (
                    order_id,
                    event["wh"],
                    occurred_at.date(),
                    payload["customer_id"],
                    event["sequence_no"],
                    occurred_at,
                    occurred_at,
                    _parse_ts(payload["delivery_slot_start"]),
                    _parse_ts(payload["delivery_slot_end"]),
                    payload["line_count"],
                    gross,
                ),
                expect_rows=1,
                detail=f"{order_id}: pedido ja existe no OLTP",
            )
        ]
        for line in payload["lines"]:
            unit = Decimal(line["unit_price"])
            ops.append(
                Statement(
                    _INSERT_LINE,
                    (
                        order_id,
                        line["line_no"],
                        line["source_product_id"],
                        line["quantity"],
                        unit,
                        unit * line["quantity"],
                    ),
                    expect_rows=1,
                    detail=f"{order_id} linha {line['line_no']}: ja existe no OLTP",
                )
            )
        return ops

    if kind in ("order_line_substituted", "order_line_removed"):
        line_no = payload["line_no"]
        # A guarda de ordem VEM PRIMEIRO. Mexer na linha antes e depois descobrir que o
        # evento estava fora de ordem funcionaria (a transacao cai inteira), mas deixaria a
        # mensagem de erro apontando para o sintoma em vez da causa.
        ops = [_advance(event, STATE_AFTER[kind])]
        if kind == "order_line_substituted":
            amount = Decimal(payload["substitute_unit_price"]) * payload["quantity"]
            ops.append(
                Statement(
                    """
                    update order_line
                       set status = 'substituted',
                           fulfilled_source_product_id = %s,
                           fulfilled_unit_price = %s,
                           line_amount = %s
                     where order_id = %s and line_no = %s and status = 'placed'
                    """,
                    (
                        payload["substitute_source_product_id"],
                        Decimal(payload["substitute_unit_price"]),
                        amount,
                        order_id,
                        line_no,
                    ),
                    expect_rows=1,
                    detail=f"{order_id} linha {line_no}: inexistente ou ja alterada",
                )
            )
        else:
            ops.append(
                Statement(
                    """
                    update order_line
                       set status = 'removed',
                           fulfilled_source_product_id = null,
                           fulfilled_unit_price = null,
                           line_amount = 0
                     where order_id = %s and line_no = %s and status = 'placed'
                    """,
                    (order_id, line_no),
                    expect_rows=1,
                    detail=f"{order_id} linha {line_no}: inexistente ou ja alterada",
                )
            )
        ops.append(
            Statement(
                _RECOMPUTE_NET,
                (order_id, order_id),
                expect_rows=1,
                detail=f"{order_id}: nao foi possivel recomputar net_amount",
            )
        )
        return ops

    if kind == "order_picked":
        # O `picked_amount` declarado no evento e conferido CONTRA O ESTADO DO PROPRIO OLTP,
        # dentro da transacao. Nao e desconfianca da Source: e a unica forma de garantir que
        # o fold do OLTP e o fold do Silver partem do mesmo lugar. Se um dia divergirem, o
        # marco 6 (`orders-reconcile`) compararia duas coisas erradas e PASSARIA.
        return [
            _advance(
                event,
                "PICKED",
                extra=", picked_line_count = %s",
                extra_params=(payload["picked_line_count"],),
            ),
            # A LINHA INTOCADA SO VIRA `fulfilled` AQUI. Nao existe evento por linha dizendo
            # "esta foi separada como pedida" — `order_picked` declara isso no grao do
            # pedido, e o OLTP ja confere `picked_line_count` contra o proprio estado logo
            # abaixo. Distribuir um fato declarado nao e inventar; rotular `fulfilled` na
            # colocacao, como este modelo fazia antes, era.
            Statement(
                """
                update order_line
                   set status = 'fulfilled',
                       fulfilled_source_product_id = source_product_id,
                       fulfilled_unit_price = unit_price
                 where order_id = %s and status = 'placed'
                """,
                (order_id,),
                # Quantas linhas continuam intocadas varia por pedido; a guarda e o Check
                # logo abaixo, que exige que NENHUMA linha continue indefinida.
                expect_rows=None,
                detail=f"{order_id}: nao foi possivel cumprir as linhas intocadas",
            ),
            Check(
                "select count(*) from order_line where order_id = %s and status = 'placed'",
                (order_id,),
                0,
                detail=f"{order_id}: sobrou linha indefinida depois da separacao",
            ),
            Check(
                "select coalesce(sum(line_amount), 0) from order_line where order_id = %s",
                (order_id,),
                Decimal(payload["picked_amount"]),
                detail=f"{order_id}: picked_amount declarado nao fecha com as linhas do OLTP",
            ),
            Check(
                "select count(*) from order_line where order_id = %s and status <> 'removed'",
                (order_id,),
                payload["picked_line_count"],
                detail=f"{order_id}: picked_line_count declarado nao fecha com as linhas",
            ),
            Statement(
                _RECOMPUTE_NET,
                (order_id, order_id),
                expect_rows=1,
                detail=f"{order_id}: nao foi possivel recomputar net_amount",
            ),
        ]

    if kind == "order_delivered":
        return [
            _advance(
                event,
                "DELIVERED",
                extra=", delivered_within_slot = %s",
                extra_params=(payload["delivered_within_slot"],),
            )
        ]

    if kind == "order_returned":
        return [
            _advance(
                event,
                "RETURNED",
                extra=", returned_amount = %s",
                extra_params=(Decimal(payload["returned_amount"]),),
            )
        ]

    if kind in ("order_cancelled", "order_payment_failed"):
        # O pedido morreu antes da separacao. As linhas nao foram cumpridas nem removidas:
        # ninguem chegou a toca-las. Deixa-las `placed` faria o OLTP guardar para sempre um
        # estado "pendente" de um pedido terminal.
        return [
            _advance(event, STATE_AFTER[kind]),
            Statement(
                """
                update order_line
                   set status = 'not_picked',
                       fulfilled_source_product_id = null,
                       fulfilled_unit_price = null,
                       line_amount = 0
                 where order_id = %s and status = 'placed'
                """,
                (order_id,),
                expect_rows=None,
                detail=f"{order_id}: nao foi possivel encerrar as linhas do pedido",
            ),
        ]

    if kind in STATE_AFTER:
        return [_advance(event, STATE_AFTER[kind])]

    raise OrdersOltpError(
        f"{event['order_id']} seq={event['sequence_no']}: event_type {kind!r} fora do "
        f"vocabulario que este OLTP conhece"
    )


_INSERT_OUTBOX = """
insert into outbox (
    event_id, order_id, event_type, event_version, sequence_no,
    occurred_at, wh, producer, event_json)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict (event_id) do nothing
"""


def outbox_statement(event, event_json: str) -> Statement:
    """A linha do outbox. `event_json` e A LINHA DO LOG, verbatim — nao reserializada."""
    return Statement(
        _INSERT_OUTBOX,
        (
            event["event_id"],
            event["order_id"],
            event["event_type"],
            event["event_version"],
            event["sequence_no"],
            _parse_ts(event["occurred_at"]),
            event["wh"],
            event["producer"],
            event_json,
        ),
        # None de proposito: 0 linhas aqui NAO e erro, e o sinal de "ja aplicado".
        expect_rows=None,
        detail=f"{event['event_id']}: insert no outbox",
    )


# --------------------------------------------------------------------------------------
# Execucao
# --------------------------------------------------------------------------------------

@dataclass
class ApplyResult:
    partition: str
    events: int = 0
    applied: int = 0
    skipped: int = 0
    orders: int = 0
    by_type: dict = field(default_factory=dict)


def _execute(cur, ops) -> None:
    for op in ops:
        cur.execute(op.sql, op.params)
        if isinstance(op, Check):
            row = cur.fetchone()
            got = row[0] if row else None
            if got != op.expected:
                raise OrdersOltpError(
                    f"{op.detail}: OLTP={got!r} evento={op.expected!r}"
                )
        elif op.expect_rows is not None and cur.rowcount != op.expect_rows:
            raise OrdersOltpError(
                f"{op.detail} (afetou {cur.rowcount} linhas, esperado {op.expect_rows})"
            )


def apply_event(connection, event, event_json: str) -> bool:
    """Aplica UM evento. Devolve True se aplicou, False se ja estava aplicado.

    A TRANSACAO E ESTE CORPO INTEIRO, e o `commit` no fim e o unico que existe. Um
    `commit()` a mais entre o outbox e a mudanca de estado transformaria esta funcao em
    dual-write — e e exatamente esse defeito que `test_orders_oltp` grava e reprova.

    OUTBOX PRIMEIRO, e nao por gosto: o `on conflict do nothing` no `event_id` unico e o
    que decide se o evento ja foi aplicado. Perguntar isso ao estado (`last_sequence_no`)
    daria a MESMA resposta — as duas guardas concordam por construcao — mas em forma de
    RECUSA, nao de "pular". Num replay do mesmo dia, pular e o comportamento correto.
    """
    try:
        with connection.cursor() as cur:
            outbox = outbox_statement(event, event_json)
            cur.execute(outbox.sql, outbox.params)
            if cur.rowcount == 0:
                # Ja aplicado. Nada a desfazer, mas o rollback fecha a transacao de forma
                # explicita em vez de deixar uma transacao aberta viajando para o proximo
                # evento — que e como um `idle in transaction` nasce.
                connection.rollback()
                return False
            _execute(cur, plan(event))
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise


def read_log(partition_path: str) -> list[tuple[dict, str]]:
    """(evento, linha canonica) na ordem do arquivo, que ja e a ordem de aplicacao."""
    path = os.path.join(partition_path, EVENTS_FILE)
    if not os.path.exists(path):
        raise OrdersOltpError(f"log de eventos nao encontrado: {path}")
    out = []
    with open(path, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")
            if not line:
                continue
            try:
                out.append((json.loads(line), line))
            except ValueError as exc:
                raise OrdersOltpError(f"{path}:{number}: linha ilegivel: {exc}") from exc
    return out


def verify_log(partition_path: str):
    """Confere o sha256 do log contra o manifesto ANTES de replicar qualquer coisa.

    Replicar um log adulterado no OLTP contaminaria o outbox, e o outbox e o que vai para o
    broker. O ponto de conferir aqui, e nao depois, e que depois ja e tarde.
    """
    partition = manifest_reader.read(partition_path)
    if partition.source_name != SOURCE_NAME:
        raise OrdersOltpError(
            f"particao de {partition.source_name!r}: este OLTP so aplica {SOURCE_NAME!r}"
        )
    entries = [entry for entry in partition.files if entry.path.endswith(EVENTS_FILE)]
    if len(entries) != 1:
        raise OrdersOltpError(
            f"manifesto declara {len(entries)} arquivos de log; esperado exatamente 1"
        )
    entry = entries[0]
    digest, size = entry.digest_on_disk()
    if digest != entry.sha256 or size != entry.bytes:
        raise OrdersOltpError(
            f"log diverge do manifesto: sha256 em disco {digest} != {entry.sha256}"
        )
    return partition


def apply_partition(connection, partition_path: str, *, verify: bool = True) -> ApplyResult:
    """Replica o log de uma particao no OLTP, um evento por transacao.

    UMA TRANSACAO POR EVENTO, e nao por pedido ou por particao. E o unico recorte que
    corresponde ao que um OLTP de verdade faz — cada mudanca de estado e um negocio
    fechado — e e o unico em que a propriedade provada aqui significa o que promete. Uma
    transacao por particao provaria "o dia inteiro e atomico", que nenhuma loja garante.
    """
    if verify:
        verify_log(partition_path)
    result = ApplyResult(partition=partition_path)
    orders = set()
    for event, line in read_log(partition_path):
        result.events += 1
        if apply_event(connection, event, line):
            result.applied += 1
            result.by_type[event["event_type"]] = result.by_type.get(event["event_type"], 0) + 1
        else:
            result.skipped += 1
        orders.add(event["order_id"])
    result.orders = len(orders)
    return result


# --------------------------------------------------------------------------------------
# Conferencia do outbox
# --------------------------------------------------------------------------------------

def rebuild_log(connection, ingestion_date: str, wh: str) -> bytes:
    """Reconstitui o log a partir do outbox, na ordem canonica da Source.

    A ordem e `(occurred_at, order_id, sequence_no)` porque e a que a Source declara no
    CONTRACT — nao `outbox_id`, que e ordem de INSERCAO. As duas coincidem hoje; depender
    da coincidencia faria esta conferencia passar por sorte.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            select o.event_json
              from outbox o
              join orders r on r.order_id = o.order_id
             where r.order_date = %s and o.wh = %s
             order by o.occurred_at, o.order_id, o.sequence_no
            """,
            (ingestion_date, wh),
        )
        rows = cur.fetchall()
    return "".join(f"{row[0]}\n" for row in rows).encode("utf-8")


def verify_outbox(connection, partition_path: str) -> dict:
    """Prova que o outbox nao perdeu, nao duplicou e nao alterou nada.

    E a prova mais forte que este marco tem para dar, e nao custa nada: se o log
    reconstituido a partir das linhas do outbox reproduz o sha256 do manifesto, entao toda
    a cadeia — ler, aplicar em transacao, gravar o outbox, reler, reordenar — e fiel byte a
    byte. Contar linhas nao provaria isso; comparar somas tambem nao.
    """
    partition = verify_log(partition_path)
    entry = [e for e in partition.files if e.path.endswith(EVENTS_FILE)][0]
    blob = rebuild_log(connection, partition.ingestion_date, partition.axis_value)
    digest = hashlib.sha256(blob).hexdigest()
    return {
        "partition": partition_path,
        "ingestion_date": partition.ingestion_date,
        "wh": partition.axis_value,
        "declared_sha256": entry.sha256,
        "outbox_sha256": digest,
        "declared_bytes": entry.bytes,
        "outbox_bytes": len(blob),
        "declared_rows": entry.records,
        "outbox_rows": blob.count(b"\n"),
        "matches": digest == entry.sha256 and len(blob) == entry.bytes,
    }


def outbox_summary(connection) -> dict:
    with connection.cursor() as cur:
        cur.execute(
            """
            select count(*), count(*) filter (where published_at is null),
                   count(distinct order_id)
              from outbox
            """
        )
        total, unpublished, orders = cur.fetchone()
        cur.execute(
            "select event_type, count(*) from outbox group by 1 order by 1"
        )
        by_type = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("select status, count(*) from orders group by 1 order by 1")
        by_status = {row[0]: row[1] for row in cur.fetchall()}
    return {
        "outbox_rows": total,
        "unpublished": unpublished,
        "orders_in_outbox": orders,
        "by_event_type": by_type,
        "orders_by_status": by_status,
    }


# --------------------------------------------------------------------------------------
# Conexao
# --------------------------------------------------------------------------------------

def connect(dsn: str = None):
    """Conexao com o OLTP. Importa psycopg sob demanda, como boto3 e o conector Snowflake.

    O import tardio e o que mantem `plan()` e `outbox_statement()` — as funcoes que carregam
    toda a regra — testaveis num ambiente sem psycopg e sem banco de pe.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise OrdersOltpError(
            "psycopg nao esta instalado neste venv. Rode `make venv`."
        ) from exc
    try:
        return psycopg.connect(dsn or DEFAULT_DSN)
    except psycopg.Error as exc:
        raise OrdersOltpError(
            f"nao foi possivel conectar no OLTP ({dsn or DEFAULT_DSN}): {exc}\n"
            "O plano de stream nao sobe com `make up`. Rode `make stream-up`."
        ) from exc


def create_schema(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(DDL)
    connection.commit()


def drop_schema(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(DROP)
    connection.commit()
