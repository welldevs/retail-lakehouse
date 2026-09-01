"""Forma dos eventos, impressao digital de schema e totais reconferiveis.

A IMPRESSAO DIGITAL E DO CONTRATO DECLARADO, NAO DO QUE POR ACASO SAIU
-----------------------------------------------------------------------
Nas outras quatro sources a impressao digital e a UNIAO das chaves observadas. Aqui isso
seria um defeito, e a Fase 1 ja tinha registrado o motivo em outra forma ("uma chave opcional
faria a impressao digital da particao depender da seed"): nem todo tipo de evento aparece em
todo dia — um dia sem nenhuma devolucao nao teria `order_returned` no conjunto observado, e a
impressao digital de duas particoes corretas divergiria por sorteio.

Entao a impressao digital cobre o VOCABULARIO INTEIRO, estatico: os campos do envelope mais
os campos declarados do payload de cada um dos 12 tipos. Ela muda quando o CODIGO muda, que e
exatamente o que ela existe para detectar. O que de fato saiu naquele dia fica em
`totals.event_rows_by_type`, onde variar e legitimo.

`missing_fields` faz o caminho inverso: confere o observado contra o declarado. Os dois juntos
pegam as duas direcoes do desvio.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from .events import (
    ENVELOPE_FIELDS,
    EVENT_TYPES,
    ORDER_CANCELLED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_DISPATCHED,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_PICKED,
    ORDER_PICKING_STARTED,
    ORDER_PLACED,
    ORDER_RETURNED,
    TERMINAL_STATES,
    fold,
)

# Campos declarados do payload de cada tipo. Dois tipos tem payload vazio de proposito: o
# instante e a transicao SAO a informacao, e um campo inventado ali seria ruido.
PAYLOAD_FIELDS = {
    ORDER_PLACED: (
        "customer_id",
        "customer_ingestion_date",
        "province_code",
        "municipality_code",
        "postal_code",
        "price_as_of",
        "price_source",
        "delivery_slot_start",
        "delivery_slot_end",
        "line_count",
        "gross_amount",
        "lines",
    ),
    ORDER_PAYMENT_AUTHORIZED: ("payment_method", "authorized_amount"),
    ORDER_PAYMENT_FAILED: ("decline_reason",),
    ORDER_CANCELLED: ("cancelled_by", "reason"),
    ORDER_PICKING_STARTED: (),
    ORDER_LINE_SUBSTITUTED: (
        "line_no",
        "source_product_id",
        "substitute_source_product_id",
        "substitute_unit_price",
        "quantity",
    ),
    ORDER_LINE_REMOVED: ("line_no", "source_product_id", "quantity", "reason"),
    ORDER_PICKED: ("picked_line_count", "picked_amount"),
    ORDER_DISPATCHED: (),
    ORDER_DELIVERED: ("delivered_within_slot", "delivered_line_count", "delivered_amount"),
    ORDER_DELIVERY_FAILED: ("reason",),
    ORDER_RETURNED: ("returned_line_count", "returned_line_no", "returned_amount"),
}

LINE_FIELDS = (
    "line_no",
    "source_product_id",
    "category_id",
    "subgroup_id",
    "quantity",
    "unit_price",
)


def events_of(payload: object) -> list:
    """Eventos de um log. Lista vazia para qualquer forma inesperada."""
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def fingerprint() -> dict:
    """Impressao digital do vocabulario declarado. Nao depende da seed nem do dia."""
    declared = {
        "envelope_keys": sorted(ENVELOPE_FIELDS),
        "line_keys": sorted(LINE_FIELDS),
        "payload_keys": {tipo: sorted(PAYLOAD_FIELDS[tipo]) for tipo in sorted(EVENT_TYPES)},
    }
    blob = json.dumps(declared, ensure_ascii=False, sort_keys=True).encode("utf-8")
    declared["sha256"] = hashlib.sha256(blob).hexdigest()
    return declared


def missing_fields(events) -> list[str]:
    """Campos declarados e ausentes em algum evento, agregados (nao um erro por linha)."""
    absent: set = set()
    for event in events:
        absent.update(f"envelope.{f}" for f in ENVELOPE_FIELDS if f not in event)
        tipo = event.get("event_type")
        payload = event.get("payload")
        if tipo not in PAYLOAD_FIELDS or not isinstance(payload, dict):
            continue
        absent.update(f"{tipo}.{f}" for f in PAYLOAD_FIELDS[tipo] if f not in payload)
        if tipo == ORDER_PLACED:
            for line in payload.get("lines") or []:
                if isinstance(line, dict):
                    absent.update(f"line.{f}" for f in LINE_FIELDS if f not in line)
    return sorted(absent)


def group_by_order(events) -> dict:
    """Eventos por pedido, cada lista em ordem de sequence_no.

    `dict` de saida e consultado e ordenado pelo chamador, nunca iterado para produzir
    conteudo — a ordem de insercao aqui vem do log, que ja e deterministica.
    """
    grouped: dict[str, list] = {}
    for event in events:
        grouped.setdefault(event["order_id"], []).append(event)
    for order_id in grouped:
        grouped[order_id].sort(key=lambda e: e["sequence_no"])
    return grouped


def totals_of(events) -> dict:
    """Totais do log, derivados do FOLD e nao de um estado gravado ao lado.

    E isto que o manifesto declara e que o teste dbt reconfere contra o Silver. Os valores
    monetarios existem em duas versoes de proposito: `gross_amount` e o que foi COLOCADO e
    `net_amount` e o que foi SEPARADO. Que os dois difiram e a prova de que o fold e
    nao-trivial — se fossem sempre iguais, o log seria um carimbo de data.
    """
    grouped = group_by_order(events)
    by_type: dict[str, int] = {tipo: 0 for tipo in EVENT_TYPES}
    for event in events:
        tipo = event.get("event_type")
        if tipo in by_type:
            by_type[tipo] += 1

    by_state: dict[str, int] = {}
    gross = Decimal("0")
    net = Decimal("0")
    line_rows = 0
    customers: set = set()
    carried_forward_orders = 0

    for order_id in sorted(grouped):
        do_pedido = grouped[order_id]
        # strict=False: contar nao pode explodir num log adulterado. Ver events.fold.
        estado = fold(do_pedido, strict=False)
        by_state[estado] = by_state.get(estado, 0) + 1

        placed = do_pedido[0].get("payload") or {}
        gross += Decimal(str(placed.get("gross_amount", "0")))
        line_rows += len(placed.get("lines") or [])
        if placed.get("customer_id") is not None:
            customers.add(placed["customer_id"])
        if placed.get("price_source") not in (None, "observed"):
            carried_forward_orders += 1

        for event in do_pedido:
            if event.get("event_type") == ORDER_PICKED:
                net += Decimal(str((event.get("payload") or {}).get("picked_amount", "0")))

    return {
        "event_rows": len(events),
        "order_rows": len(grouped),
        "line_rows": line_rows,
        "customers_used": len(customers),
        "substituted_lines": by_type[ORDER_LINE_SUBSTITUTED],
        "removed_lines": by_type[ORDER_LINE_REMOVED],
        "carried_forward_orders": carried_forward_orders,
        "gross_amount_placed": str(gross),
        "net_amount_picked": str(net),
        "event_rows_by_type": by_type,
        "orders_by_state": dict(sorted(by_state.items())),
        "orders_terminal": sum(by_state.get(s, 0) for s in TERMINAL_STATES),
    }
