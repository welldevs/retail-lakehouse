"""Validacao independente de uma particao do log de eventos.

Rele o arquivo declarado no manifesto, recalculando checksum, contagem, impressao digital e
TODOS os totais em vez de aceitar os valores registrados, e varre o diretorio para detectar
arquivos que o manifesto nao declara.

ALEM DISSO, e e aqui que esta o valor: rele a mesma referencia que o `extract` usou e
reconfere, evento a evento, as quatro garantias que nenhum checksum alcanca —

  1. o cliente do pedido pertence ao armazem da particao (regra medida: 0 CEPs cruzam armazem);
  2. todo produto pedido existe no catalogo DAQUELE armazem NAQUELA data;
  3. o preco pago e o preco observado daquele (armazem, data, produto), casa decimal a casa
     decimal — comparado como Decimal, nunca como float;
  4. a sequencia de eventos de cada pedido e contigua e a maquina de estados a aceita.

Um validador que so confere checksum provaria integridade, nao coerencia. Dai --reference ser
obrigatorio, como na Source de clientes.

Dois tipos de problema:
  - INTEGRIDADE e COERENCIA -> sempre fatal (codigo 1).
  - QUALIDADE/DISTRIBUICAO  -> medida e reportada; fatal apenas com --strict.
"""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json, read_jsonl
from .events import (
    ENVELOPE_FIELDS,
    EVENT_VERSION,
    EventError,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PICKED,
    ORDER_PLACED,
    TERMINAL_STATES,
    apply_transition,
    event_id,
)
from .partition import MANIFEST_NAME, SUCCESS_NAME, manifest_path, success_path
from .reference_data import ReferenceError, load
from .schema import events_of, fingerprint, group_by_order, missing_fields, totals_of

EXIT_OK = 0
EXIT_FAILED = 1

MAX_REPORTED = 20


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def _decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _check_envelope(events, warehouse, order_date) -> list[str]:
    """Envelope: identidade deterministica, armazem, dia e vocabulario."""
    problemas: list[str] = []
    vistos: set = set()
    for event in events:
        faltando = [f for f in ENVELOPE_FIELDS if f not in event]
        if faltando:
            problemas.append(f"evento sem campo do envelope {faltando}: {event.get('event_id')}")
            continue
        if event["wh"] != warehouse:
            problemas.append(
                f"evento {event['event_id']} com wh={event['wh']!r}, mas a particao e de "
                f"{warehouse!r}"
            )
        if event["event_version"] != EVENT_VERSION:
            problemas.append(
                f"evento {event['event_id']} com event_version={event['event_version']!r}, "
                f"suportado={EVENT_VERSION}"
            )
        # event_id e derivavel: recalcular e a unica forma de provar que ele nao foi
        # inventado nem embaralhado entre pedidos.
        esperado = event_id(event["order_id"], event["sequence_no"])
        if event["event_id"] != esperado:
            problemas.append(
                f"event_id {event['event_id']} nao deriva de "
                f"({event['order_id']}, {event['sequence_no']}); esperado {esperado}"
            )
        if event["event_id"] in vistos:
            problemas.append(f"event_id duplicado: {event['event_id']}")
        vistos.add(event["event_id"])
        if not event["occurred_at"].startswith(order_date) and event["event_type"] == ORDER_PLACED:
            problemas.append(
                f"pedido {event['order_id']} colocado em {event['occurred_at']}, fora do dia "
                f"da particao ({order_date})"
            )
    return problemas


def _check_orders(grouped, reference, warehouse, order_date) -> tuple[list[str], list[str], int]:
    """Coerencia pedido a pedido contra a referencia. Retorna (erros, avisos, aprovados)."""
    problemas: list[str] = []
    avisos: list[str] = []
    aprovados = 0

    try:
        calendar = reference.calendar_of(warehouse, order_date)
    except ReferenceError as exc:
        return [str(exc)], [], 0

    price_as_of = calendar["price_as_of"]
    catalogo = {row["source_product_id"]: row for row in reference.catalog_of(warehouse, price_as_of)}
    clientes = {row["customer_id"]: row for row in reference.customers_of(warehouse)}

    for order_id in sorted(grouped):
        eventos = grouped[order_id]
        falhou = False

        # ---- sequencia contigua ------------------------------------------------
        esperado = list(range(1, len(eventos) + 1))
        observado = [e["sequence_no"] for e in eventos]
        if observado != esperado:
            problemas.append(
                f"{order_id}: sequence_no nao contiguo — observado {observado[:8]}..., "
                f"esperado 1..{len(eventos)}"
            )
            falhou = True

        # ---- maquina de estados -------------------------------------------------
        estado = None
        try:
            for event in eventos:
                estado = apply_transition(estado, event["event_type"])
        except EventError as exc:
            problemas.append(f"{order_id}: {exc}")
            falhou = True

        primeiro = eventos[0]
        if primeiro["event_type"] != ORDER_PLACED:
            problemas.append(
                f"{order_id}: primeiro evento e {primeiro['event_type']!r}, e nao "
                f"{ORDER_PLACED!r}"
            )
            falhou = True
            continue

        placed = primeiro["payload"]

        # ---- o cliente e daquele armazem, e ja existia --------------------------
        cliente = clientes.get(placed.get("customer_id"))
        if cliente is None:
            problemas.append(
                f"{order_id}: cliente {placed.get('customer_id')!r} nao pertence a "
                f"{warehouse!r} na referencia"
            )
            falhou = True
        else:
            versao = reference.customer_version_at(cliente["first_ingestion_date"], order_date)
            if versao is None:
                problemas.append(
                    f"{order_id}: cliente {cliente['customer_id']} so existe a partir de "
                    f"{cliente['first_ingestion_date']}, depois de {order_date}"
                )
                falhou = True
            elif placed.get("customer_ingestion_date") != versao:
                problemas.append(
                    f"{order_id}: customer_ingestion_date={placed.get('customer_ingestion_date')!r} "
                    f"nao e a versao vigente em {order_date} ({versao})"
                )
                falhou = True
            for campo in ("province_code", "municipality_code", "postal_code"):
                if placed.get(campo) != cliente[campo]:
                    problemas.append(
                        f"{order_id}: {campo}={placed.get(campo)!r} diverge do cadastro do "
                        f"cliente ({cliente[campo]!r})"
                    )
                    falhou = True

        # ---- a base de preco declarada e a do calendario ------------------------
        if placed.get("price_as_of") != price_as_of:
            problemas.append(
                f"{order_id}: price_as_of={placed.get('price_as_of')!r}, mas o calendario diz "
                f"{price_as_of!r} para {warehouse}/{order_date}"
            )
            falhou = True
        if placed.get("price_source") != calendar["price_source"]:
            problemas.append(
                f"{order_id}: price_source={placed.get('price_source')!r}, mas o calendario "
                f"diz {calendar['price_source']!r}"
            )
            falhou = True

        # ---- produto e preco de cada linha --------------------------------------
        linhas = {}
        bruto = Decimal("0")
        for linha in placed.get("lines") or []:
            linhas[linha["line_no"]] = dict(linha)
            produto = catalogo.get(linha["source_product_id"])
            if produto is None:
                problemas.append(
                    f"{order_id} linha {linha['line_no']}: produto "
                    f"{linha['source_product_id']!r} nao existe no catalogo de {warehouse} em "
                    f"{price_as_of}. Pedir o que o armazem nao vende seria inventar sortimento."
                )
                falhou = True
                continue
            preco = _decimal(linha.get("unit_price"))
            if preco is None or preco != produto["unit_price"]:
                problemas.append(
                    f"{order_id} linha {linha['line_no']}: unit_price="
                    f"{linha.get('unit_price')!r}, mas o observado em {warehouse}/{price_as_of} "
                    f"para {linha['source_product_id']} e {produto['unit_price']}"
                )
                falhou = True
                continue
            if not isinstance(linha.get("quantity"), int) or linha["quantity"] < 1:
                problemas.append(
                    f"{order_id} linha {linha['line_no']}: quantity invalida "
                    f"({linha.get('quantity')!r})"
                )
                falhou = True
                continue
            bruto += preco * linha["quantity"]

        declarado = _decimal(placed.get("gross_amount"))
        if declarado is None or declarado != bruto:
            problemas.append(
                f"{order_id}: gross_amount={placed.get('gross_amount')!r} nao e a soma das "
                f"linhas colocadas ({bruto})"
            )
            falhou = True
        if placed.get("line_count") != len(linhas):
            problemas.append(
                f"{order_id}: line_count={placed.get('line_count')!r} diverge das "
                f"{len(linhas)} linha(s) presentes"
            )
            falhou = True

        # ---- o fold: substituicao e remocao mudam a cesta ------------------------
        efetivo = {no: dict(linha) for no, linha in linhas.items()}
        for event in eventos[1:]:
            payload = event["payload"]
            if event["event_type"] == ORDER_LINE_SUBSTITUTED:
                no = payload.get("line_no")
                if no not in efetivo:
                    problemas.append(f"{order_id}: substituicao de linha inexistente {no!r}")
                    falhou = True
                    continue
                substituto = catalogo.get(payload.get("substitute_source_product_id"))
                if substituto is None:
                    problemas.append(
                        f"{order_id} linha {no}: substituto "
                        f"{payload.get('substitute_source_product_id')!r} nao existe no "
                        f"catalogo de {warehouse} em {price_as_of}"
                    )
                    falhou = True
                    continue
                preco = _decimal(payload.get("substitute_unit_price"))
                if preco is None or preco != substituto["unit_price"]:
                    problemas.append(
                        f"{order_id} linha {no}: substitute_unit_price="
                        f"{payload.get('substitute_unit_price')!r}, observado "
                        f"{substituto['unit_price']}"
                    )
                    falhou = True
                    continue
                efetivo[no]["unit_price"] = str(preco)
            elif event["event_type"] == ORDER_LINE_REMOVED:
                no = payload.get("line_no")
                if no not in efetivo:
                    problemas.append(f"{order_id}: remocao de linha inexistente {no!r}")
                    falhou = True
                    continue
                if payload.get("reason") != "unavailable":
                    problemas.append(
                        f"{order_id} linha {no}: reason={payload.get('reason')!r} fora do "
                        f"vocabulario"
                    )
                    falhou = True
                efetivo.pop(no)
            elif event["event_type"] == ORDER_PICKED:
                liquido = sum(
                    (_decimal(l["unit_price"]) * l["quantity"] for l in efetivo.values()),
                    Decimal("0"),
                )
                declarado_picked = _decimal(payload.get("picked_amount"))
                if declarado_picked is None or declarado_picked != liquido:
                    problemas.append(
                        f"{order_id}: picked_amount={payload.get('picked_amount')!r} nao fecha "
                        f"com as linhas cumpridas ({liquido})"
                    )
                    falhou = True
                if payload.get("picked_line_count") != len(efetivo):
                    problemas.append(
                        f"{order_id}: picked_line_count={payload.get('picked_line_count')!r} "
                        f"diverge das {len(efetivo)} linha(s) cumpridas"
                    )
                    falhou = True

        # ---- o tempo nao anda para tras dentro de um pedido ----------------------
        instantes = [e["occurred_at"] for e in eventos]
        if instantes != sorted(instantes):
            problemas.append(f"{order_id}: occurred_at regride dentro do pedido")
            falhou = True

        if not falhou:
            aprovados += 1

    # ---- avisos de distribuicao ----------------------------------------------
    total = len(grouped)
    if total:
        mudaram = sum(
            1
            for eventos in grouped.values()
            if any(
                e["event_type"] in (ORDER_LINE_SUBSTITUTED, ORDER_LINE_REMOVED) for e in eventos
            )
        )
        if mudaram == 0:
            # O aviso mais importante desta Source. Se nenhuma cesta mudou, o valor final e
            # derivavel do primeiro evento, o fold e trivial e o modelo de eventos vira
            # decoracao — que e exatamente o que o ARCHITECTURE.md recusa.
            avisos.append(
                "nenhum pedido teve linha substituida ou removida: o fold ficou trivial e o "
                "log deixa de justificar o modelo de eventos"
            )
        terminais = sum(
            1 for eventos in grouped.values()
            if any(e["event_type"] in TERMINAL_STATES for e in eventos)
        )
        if terminais == total and total > 1:
            avisos.append("todos os pedidos terminaram no mesmo evento")

    return problemas, avisos, aprovados


def run(args) -> int:
    partition = args.partition
    errors: list[str] = []
    warnings: list[str] = []

    path = manifest_path(partition)
    if not os.path.exists(path):
        print(f"ERRO: manifesto nao encontrado em {path}")
        return EXIT_FAILED
    try:
        manifest, _ = read_json(path)
    except CorruptFileError as exc:
        print(f"ERRO: manifesto ilegivel: {exc}")
        return EXIT_FAILED
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        print(f"ERRO: manifesto sem a lista 'files': {path}")
        return EXIT_FAILED

    # A raiz do snapshot fica dois niveis acima: ingestion_date= e wh=.
    root = os.path.abspath(os.path.join(partition, "..", ".."))
    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        errors.append(
            f"manifest_version {version!r} diferente do suportado ({MANIFEST_VERSION})"
        )

    partition_block = manifest.get("partition") or {}
    warehouse = partition_block.get("warehouse")
    order_date = partition_block.get("ingestion_date")
    config = manifest.get("config") or {}

    print(f"particao ......... {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"warehouse ........ {warehouse}")
    print(f"dia do pedido .... {order_date}")
    print(f"seed ............. {config.get('seed')}")
    print(f"premissas ........ {str(config.get('premises_sha256'))[:16]}...")
    print(f"demanda .......... {config.get('demand_model_version')}")
    print(f"completa ......... {manifest.get('complete')}")

    # ---- Integridade dos arquivos declarados -------------------------------
    checked = 0
    declared: set = set()
    events: list = []
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or "path" not in entry:
            errors.append(f"entrada de manifesto malformada: {entry!r}")
            continue
        target = os.path.join(root, entry["path"])
        if not _inside(root, target):
            errors.append(f"caminho fora da raiz do snapshot: {entry['path']}")
            continue
        declared.add(os.path.abspath(target))
        if not os.path.exists(target):
            errors.append(f"arquivo ausente: {entry['path']}")
            continue
        blob = read_bytes(target)
        checked += 1
        if digest(blob) != entry.get("sha256"):
            errors.append(f"checksum divergente: {entry['path']}")
        if len(blob) != entry.get("bytes"):
            errors.append(f"tamanho divergente: {entry['path']}")
        try:
            rows, _ = read_jsonl(target)
        except CorruptFileError as exc:
            errors.append(f"arquivo ilegivel: {exc}")
            continue
        events.extend(events_of(rows))
        if len(rows) != entry.get("records"):
            errors.append(
                f"contagem divergente em {entry['path']}: "
                f"manifesto={entry.get('records')} arquivo={len(rows)}"
            )
    print(f"arquivos ......... {checked}/{len(manifest['files'])} lidos e conferidos")

    if manifest.get("failures"):
        errors.append(f"{len(manifest['failures'])} falha(s) registrada(s) no manifesto")

    # ---- Arquivos em disco nao declarados no manifesto ---------------------
    orphans: list[str] = []
    known_root_files = {MANIFEST_NAME, SUCCESS_NAME}
    partition_abs = os.path.abspath(partition)
    for base, _, names in os.walk(partition_abs):
        for name in sorted(names):
            candidate_path = os.path.abspath(os.path.join(base, name))
            if base == partition_abs and name in known_root_files:
                continue
            if candidate_path not in declared:
                orphans.append(os.path.relpath(candidate_path, partition_abs))
    print(f"orfaos ........... {len(orphans)}")
    if orphans:
        errors.append(f"arquivo(s) em disco fora do manifesto: {orphans}")

    # ---- Marcador _SUCCESS coerente com 'complete' -------------------------
    has_success = os.path.exists(success_path(partition))
    if bool(manifest.get("complete")) != has_success:
        errors.append(
            f"_SUCCESS {'presente' if has_success else 'ausente'} contradiz "
            f"complete={manifest.get('complete')}"
        )

    # ---- Schema e campos obrigatorios --------------------------------------
    absent = missing_fields(events)
    if absent:
        errors.append(f"campo(s) declarado(s) ausente(s) em algum evento: {absent[:MAX_REPORTED]}")
    declared_fingerprint = manifest.get("schema_fingerprint") or {}
    if fingerprint().get("sha256") != declared_fingerprint.get("sha256"):
        errors.append(
            f"schema_fingerprint divergente: manifesto={declared_fingerprint.get('sha256')} "
            f"vocabulario deste codigo={fingerprint().get('sha256')}"
        )

    # ---- Ordenacao total do log ---------------------------------------------
    chaves = [(e.get("occurred_at"), e.get("order_id"), e.get("sequence_no")) for e in events]
    if chaves != sorted(chaves):
        errors.append(
            "log fora de ordem: as linhas tem de estar ordenadas por "
            "(occurred_at, order_id, sequence_no)"
        )

    errors.extend(_check_envelope(events, warehouse, order_date)[:MAX_REPORTED])

    # ---- Totais reconferidos contra a releitura ----------------------------
    observed_totals = totals_of(events)
    observed_totals["bytes"] = sum(
        e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)
    )
    totals = manifest.get("totals") or {}
    for key, observed in sorted(observed_totals.items()):
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} divergente: manifesto={totals.get(key)} observado={observed}"
            )
    if totals.get("order_rows") != config.get("orders"):
        errors.append(
            f"config.orders={config.get('orders')} nao bate com "
            f"totals.order_rows={totals.get('order_rows')}"
        )

    # ---- Coerencia contra a referencia --------------------------------------
    try:
        reference = load(args.reference)
    except ReferenceError as exc:
        print()
        print(f"FALHOU: referencia inutilizavel: {exc}")
        return EXIT_FAILED

    declared_demand = (manifest.get("config") or {}).get("demand_model_version")
    if declared_demand != reference.demand.version:
        errors.append(
            f"referencia com outro modelo de demanda: manifesto={declared_demand} "
            f"arquivo={reference.demand.version}. O mix por tras destes pedidos nao e o que "
            f"o perfil deste diretorio descreve."
        )

    declared_premises = (manifest.get("reference") or {}).get("premises_sha256")
    if declared_premises != reference.premises_sha256:
        errors.append(
            f"referencia com outra tabela de premissas: manifesto={declared_premises} "
            f"arquivo={reference.premises_sha256}. As taxas por tras destes pedidos nao sao "
            f"as que estao neste diretorio."
        )

    grouped = group_by_order(events)
    order_errors, order_warnings, aprovados = _check_orders(
        grouped, reference, warehouse, order_date
    )
    errors.extend(order_errors[:MAX_REPORTED])
    if len(order_errors) > MAX_REPORTED:
        errors.append(f"... e mais {len(order_errors) - MAX_REPORTED} problema(s) de coerencia")
    warnings.extend(order_warnings)
    print(f"coerencia ........ {aprovados}/{len(grouped)} pedidos com cliente, produto e "
          f"preco reais de {warehouse}")
    print(f"fold ............. colocado {observed_totals['gross_amount_placed']} / "
          f"separado {observed_totals['net_amount_picked']} "
          f"({observed_totals['substituted_lines']} substituicao(oes), "
          f"{observed_totals['removed_lines']} remocao(oes))")

    if args.strict and warnings:
        errors.append(f"{len(warnings)} aviso(s) de distribuicao (--strict)")

    print()
    for item in warnings:
        print(f"AVISO: {item}")
    if errors:
        print(f"FALHOU ({len(errors)} problema(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: integridade, schema, totais, maquina de estados e coerencia validados.")
    return EXIT_OK
