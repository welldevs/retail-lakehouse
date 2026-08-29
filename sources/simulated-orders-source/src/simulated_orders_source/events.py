"""Vocabulario de eventos, maquina de estados e envelope.

E o CONTRATO em codigo. O gerador emite a partir daqui e o validate reconfere a partir daqui
— um so lugar define o que e um evento valido, entao os dois nao podem divergir.

O ENVELOPE E DETERMINISTICO E SEM RELOGIO
-----------------------------------------
`event_id` e `sha256(order_id|sequence_no)`, nunca `uuid4()`: nao ha entropia nem relogio
nesta Source. `occurred_at` deriva da data da particao e dos offsets declarados nas
premissas, nunca de `datetime.now()`. E a mesma garantia 5 do contrato da Fase 1
("birth_year deriva de ingestion_date, nunca do relogio"), aplicada ao tempo do evento.

`recorded_at` NAO EXISTE aqui, de proposito. Atraso e reordenacao sao propriedades do
TRANSPORTE, e quem os mede e quem consome — do outro lado do broker, com o proprio relogio.
Carimba-los aqui seria inventar um atraso que ninguem observou, e ainda destruiria a
reprodutibilidade byte a byte do log.

VOCABULARIO: PRESERVAR, NAO PROMETER
-------------------------------------
`order_line_removed` carrega `reason = "unavailable"`, e nao "out_of_stock". Nao existe fato
de estoque em nenhuma fonte desta plataforma; nomear como se existisse prometeria um dado que
ninguem mediu. Mesma disciplina que fez a Fase 1 preservar `numbering_type` do INE em vez de
inventar um `address_precision`.
"""

from __future__ import annotations

import hashlib

# ---------------------------------------------------------------------------------
# Estados
# ---------------------------------------------------------------------------------

PLACED = "PLACED"
CONFIRMED = "CONFIRMED"
PICKING = "PICKING"
PICKED = "PICKED"
IN_TRANSIT = "IN_TRANSIT"
DELIVERED = "DELIVERED"
PAYMENT_FAILED = "PAYMENT_FAILED"
CANCELLED = "CANCELLED"
DELIVERY_FAILED = "DELIVERY_FAILED"
RETURNED = "RETURNED"

STATES = (
    PLACED,
    CONFIRMED,
    PICKING,
    PICKED,
    IN_TRANSIT,
    DELIVERED,
    PAYMENT_FAILED,
    CANCELLED,
    DELIVERY_FAILED,
    RETURNED,
)

# Sentinela para um log ADULTERADO ou incompleto. Nao e um estado do dominio: nenhum pedido
# nasce ou termina aqui. Existe para que CONTAR eventos de um log quebrado seja possivel sem
# levantar excecao — quem reporta a transicao invalida e a checagem da maquina de estados, e
# um contador que explode transformaria "particao invalida" (codigo 1) em "excecao nao
# tratada" (codigo 3), apagando a diferenca entre dado ruim e bug do validador.
INVALID = "INVALID"

# Estados dos quais nenhum evento pode sair. `DELIVERED` NAO esta aqui: uma devolucao ainda
# pode acontecer depois dele, e e justamente por isso que ele nao encerra o pedido.
TERMINAL_STATES = (PAYMENT_FAILED, CANCELLED, DELIVERY_FAILED, RETURNED)

# ---------------------------------------------------------------------------------
# Tipos de evento
# ---------------------------------------------------------------------------------

ORDER_PLACED = "order_placed"
ORDER_PAYMENT_AUTHORIZED = "order_payment_authorized"
ORDER_PAYMENT_FAILED = "order_payment_failed"
ORDER_CANCELLED = "order_cancelled"
ORDER_PICKING_STARTED = "order_picking_started"
ORDER_LINE_SUBSTITUTED = "order_line_substituted"
ORDER_LINE_REMOVED = "order_line_removed"
ORDER_PICKED = "order_picked"
ORDER_DISPATCHED = "order_dispatched"
ORDER_DELIVERED = "order_delivered"
ORDER_DELIVERY_FAILED = "order_delivery_failed"
ORDER_RETURNED = "order_returned"

EVENT_TYPES = (
    ORDER_PLACED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_CANCELLED,
    ORDER_PICKING_STARTED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_LINE_REMOVED,
    ORDER_PICKED,
    ORDER_DISPATCHED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_RETURNED,
)

# Estado do pedido DEPOIS de cada evento. Os dois eventos de linha nao mudam o estado do
# pedido — eles mudam a CESTA, que e o que torna o fold nao-trivial.
STATE_AFTER = {
    ORDER_PLACED: PLACED,
    ORDER_PAYMENT_AUTHORIZED: CONFIRMED,
    ORDER_PAYMENT_FAILED: PAYMENT_FAILED,
    ORDER_CANCELLED: CANCELLED,
    ORDER_PICKING_STARTED: PICKING,
    ORDER_LINE_SUBSTITUTED: PICKING,
    ORDER_LINE_REMOVED: PICKING,
    ORDER_PICKED: PICKED,
    ORDER_DISPATCHED: IN_TRANSIT,
    ORDER_DELIVERED: DELIVERED,
    ORDER_DELIVERY_FAILED: DELIVERY_FAILED,
    ORDER_RETURNED: RETURNED,
}

# Estado exigido ANTES de cada evento. `None` para o evento que abre o agregado.
STATE_BEFORE = {
    ORDER_PLACED: (None,),
    ORDER_PAYMENT_AUTHORIZED: (PLACED,),
    ORDER_PAYMENT_FAILED: (PLACED,),
    ORDER_CANCELLED: (PLACED, CONFIRMED),
    ORDER_PICKING_STARTED: (CONFIRMED,),
    ORDER_LINE_SUBSTITUTED: (PICKING,),
    ORDER_LINE_REMOVED: (PICKING,),
    ORDER_PICKED: (PICKING,),
    ORDER_DISPATCHED: (PICKED,),
    ORDER_DELIVERED: (IN_TRANSIT,),
    ORDER_DELIVERY_FAILED: (IN_TRANSIT,),
    ORDER_RETURNED: (DELIVERED,),
}

# Campos do envelope, presentes em TODO evento e sempre com estes nomes.
ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "order_id",
    "sequence_no",
    "wh",
    "producer",
    "payload",
)

EVENT_VERSION = 1

# Vocabularios fechados dos payloads.
CANCELLED_BY = ("customer", "system")
CANCEL_REASONS = ("changed_mind", "slot_no_longer_suitable", "duplicate_order")
DECLINE_REASONS = ("insufficient_funds", "card_expired", "issuer_declined")
REMOVAL_REASON = "unavailable"
DELIVERY_FAILURE_REASONS = ("customer_absent", "address_unreachable")
PAYMENT_METHODS = ("card", "wallet")

LINE_FULFILLED = "fulfilled"
LINE_SUBSTITUTED = "substituted"
LINE_REMOVED = "removed"
LINE_STATUSES = (LINE_FULFILLED, LINE_SUBSTITUTED, LINE_REMOVED)


class EventError(Exception):
    """Evento fora do vocabulario, ou transicao que a maquina de estados nao permite."""


def event_id(order_id: str, sequence_no: int) -> str:
    """Identidade deterministica do evento.

    Derivada do agregado e da posicao, e nao de relogio ou entropia: reexecutar a mesma
    (referencia, premissas, seed, dia, armazem) tem de produzir os MESMOS event_id, senao o
    consumidor a jusante nao consegue deduplicar entre replays.
    """
    blob = f"{order_id}|{sequence_no}".encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:32]


def envelope(
    order_id: str,
    wh: str,
    sequence_no: int,
    event_type: str,
    occurred_at: str,
    payload: dict,
    producer: str,
) -> dict:
    if event_type not in STATE_AFTER:
        raise EventError(f"tipo de evento fora do vocabulario: {event_type!r}")
    return {
        "event_id": event_id(order_id, sequence_no),
        "event_type": event_type,
        "event_version": EVENT_VERSION,
        "occurred_at": occurred_at,
        "order_id": order_id,
        "sequence_no": sequence_no,
        "wh": wh,
        "producer": producer,
        "payload": payload,
    }


def apply_transition(state, event_type: str) -> str:
    """Aplica um evento a um estado. Levanta EventError se a transicao nao existir.

    E a mesma funcao que o gerador usa para construir e que o validate usa para reconferir,
    de proposito: um gerador que emitisse uma transicao invalida seria pego pelo seu proprio
    vocabulario, e nao apenas por um teste dbt tres camadas adiante.
    """
    allowed = STATE_BEFORE.get(event_type)
    if allowed is None:
        raise EventError(f"tipo de evento fora do vocabulario: {event_type!r}")
    if state in TERMINAL_STATES:
        raise EventError(
            f"evento {event_type!r} depois do estado terminal {state!r}: um agregado "
            f"encerrado nao recebe mais evento"
        )
    if state not in allowed:
        raise EventError(
            f"transicao invalida: {event_type!r} exige estado em {allowed}, mas o pedido "
            f"esta em {state!r}"
        )
    return STATE_AFTER[event_type]


def fold(events, strict: bool = True) -> str:
    """Estado resultante de uma sequencia de eventos de UM pedido, em ordem.

    `strict=True` (padrao) levanta EventError na primeira transicao invalida: e o que o
    gerador usa, porque emitir uma transicao impossivel e bug do gerador.

    `strict=False` devolve o sentinela INVALID e para: e o que um CONTADOR usa, porque contar
    um log adulterado tem de produzir um numero divergente, e nao uma excecao.
    """
    state = None
    for event in events:
        try:
            state = apply_transition(state, event["event_type"])
        except EventError:
            if strict:
                raise
            return INVALID
    return state
