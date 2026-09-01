"""Validacao independente de uma particao do snapshot.

Rele todos os arquivos declarados no manifesto, recalculando checksum, contagens e
impressao digital em vez de aceitar os valores registrados, e varre o diretorio para
detectar arquivos que o manifesto nao declara.

ALEM DISSO, E ESTA E A DIFERENCA DESTA SOURCE: rele o mesmo arquivo de referencia que o
`extract` usou e reconfere, cliente a cliente, que o endereco atribuido existe de verdade
e pertence a Area Urbana Funcional do warehouse certo. Nenhuma das outras tres sources
revalida contra um insumo externo, mas aqui a garantia central — "100% dos CEPs pertencem
ao warehouse certo" — so pode ser reconferida assim. Um teste unitario prova a regra sobre
fixtures; isto prova o resultado sobre o dado que foi realmente pousado. Dai `--reference`
ser obrigatorio.

Dois tipos de problema:
  - INTEGRIDADE e COERENCIA GEOGRAFICA -> sempre fatal (codigo 1).
  - QUALIDADE/DISTRIBUICAO -> medida e reportada; fatal apenas com --strict.
"""

from __future__ import annotations

import os

from . import MANIFEST_VERSION
from .canonical import CorruptFileError, digest, read_bytes, read_json
from .customers_generator import NUMBERING_EVEN, NUMBERING_NONE, NUMBERING_ODD
from .partition import MANIFEST_NAME, SUCCESS_NAME, manifest_path, success_path
from .reference_data import ReferenceError, load
from .schema import count_customers, customers_of, fingerprint, missing_fields

EXIT_OK = 0
EXIT_FAILED = 1

# Campos do cliente que TEM de ser identicos a linha de origem em address_candidates.json.
# Sao o endereco atribuido: se algum divergir, o cliente nao mora onde diz morar.
#
# `municipality_name` NAO entra: os dois produtos do INE renderizam o mesmo municipio de
# forma diferente em 370 de 370 casos no escopo ("BRUC (EL)" no Callejero contra
# "Bruc, El" na tabela 29005). O cliente carrega o nome da 29005, que e o legivel e o mesmo
# de warehouse_service_area_seed; o Callejero guarda o dele em municipality_name_callejero.
# Comparar nomes de vocabularios diferentes reprovaria uma particao correta — o que se
# compara e o CODIGO do municipio, logo abaixo, que e a chave de verdade.
GEO_FIELDS = (
    "wh",
    "province_code",
    "municipality_code",
    "street_name",
    "postal_code",
    "numbering_type",
)


def _inside(root: str, path: str) -> bool:
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    return path_abs == root_abs or path_abs.startswith(root_abs + os.sep)


def _check_house_number(customer: dict, candidate: dict) -> str | None:
    """A regra de numeracao, reconferida contra a faixa real do tramo."""
    number = customer.get("house_number")
    numbering = candidate["numbering_type"]

    if numbering == NUMBERING_NONE:
        if number is not None:
            return (
                f"{customer['customer_id']}: house_number={number} num tramo sem "
                f"numeracao (numbering_type='0') — endereco fabricado"
            )
        return None
    if number is None:
        return None  # faixa degenerada ou sem numero da paridade certa: legitimo
    if not isinstance(number, int) or number < 1:
        return f"{customer['customer_id']}: house_number invalido ({number!r})"

    parity = 1 if numbering == NUMBERING_ODD else 0
    if numbering in (NUMBERING_ODD, NUMBERING_EVEN) and number % 2 != parity:
        return (
            f"{customer['customer_id']}: house_number={number} contraria "
            f"numbering_type={numbering!r}"
        )
    low = candidate.get("number_from")
    high = candidate.get("number_to")
    if isinstance(low, int) and isinstance(high, int):
        if not (min(low, high) <= number <= max(low, high)):
            return (
                f"{customer['customer_id']}: house_number={number} fora da faixa real "
                f"[{low}, {high}] do tramo"
            )
    return None


def _check_minimum_age(customers: list, reference, ingestion_date: str) -> list:
    """Nenhum titular de conta abaixo da idade minima que a referencia declara.

    FATAL, e nao aviso de distribuicao. O que isto pega ja aconteceu: em 2026-08-31, 18,01%
    da base tinha menos de 18 anos — 3.602 de 20.000, com idade a partir de zero, incluindo
    titular de conta recem-nascido. Enquanto a idade nao fazia nada aquilo era inofensivo; a
    partir do momento em que ela governa a demanda, 18% da base entra na faixa mais jovem do
    benchmark sendo crianca e nada quebra.

    A idade e reconstruida do jeito que o gerador a construiu — `ano(ingestion_date) -
    birth_year` — e nao do relogio. Comparar com a data de hoje faria esta validacao mudar de
    resposta sozinha com o passar do tempo, e uma particao imutavel comecaria a reprovar sem
    que nada nela tivesse mudado.

    A idade minima vem da REFERENCIA, nunca de uma constante deste arquivo: quem decide quem
    pode ter cadastro e a plataforma, em customer_premises_seed.
    """
    minima = reference.min_customer_age
    if not isinstance(minima, int):
        return [
            "referencia sem min_customer_age: nao ha contra o que conferir a idade dos "
            "clientes desta particao"
        ]
    ano = int(str(ingestion_date)[:4])
    menores: list[str] = []
    sem_idade: list[str] = []
    for customer in customers:
        nascimento = customer.get("birth_year")
        if not isinstance(nascimento, int):
            sem_idade.append(f"{customer.get('customer_id')}: birth_year={nascimento!r}")
            continue
        idade = ano - nascimento
        if idade < minima:
            menores.append(f"{customer.get('customer_id')}: {idade} anos")

    problemas = []
    if menores:
        problemas.append(
            f"{len(menores)} cliente(s) abaixo de min_customer_age={minima} em "
            f"{ingestion_date}: {menores[:10]}"
        )
    # SEPARADO dos menores, e nao somado a eles: um birth_year nulo nao e uma idade baixa,
    # e uma idade que nao existe. Juntar os dois numa contagem so faria o relatorio afirmar
    # que ha N criancas quando talvez nao haja nenhuma, e o operador iria procurar a coisa
    # errada. Fatal do mesmo jeito — idade inverificavel nao pode passar.
    if sem_idade:
        problemas.append(
            f"{len(sem_idade)} cliente(s) sem birth_year inteiro, entao a idade nao e "
            f"verificavel: {sem_idade[:10]}"
        )
    return problemas


def _check_reference(customers: list, reference, warehouse: str) -> tuple[list, list, int]:
    """Coerencia geoespacial de cada cliente contra a referencia. O coracao da validacao.

    Devolve (erros, avisos, clientes_aprovados). O terceiro valor e contado, e nao
    derivado de `len(customers) - len(errors)`: erros que nao sao por cliente (como
    customer_id duplicado) desalinhariam a conta.
    """
    errors: list[str] = []
    warnings: list[str] = []
    aprovados = 0

    # Municipios que a Area Urbana Funcional deste warehouse realmente cobre. Conjunto so
    # para teste de pertinencia — nunca iterado, entao nao influencia nenhuma amostragem.
    service_area = {
        (m["province_code"], m["municipality_code"]) for m in reference.municipalities(warehouse)
    }
    total_candidates = reference.candidate_count()

    seen_ids: set = set()
    duplicated: set = set()
    municipalities_used: set = set()
    with_number = 0

    for customer in customers:
        customer_id = customer.get("customer_id")
        if customer_id in seen_ids:
            duplicated.add(customer_id)
        seen_ids.add(customer_id)

        if customer.get("wh") != warehouse:
            errors.append(
                f"{customer_id}: wh={customer.get('wh')!r} nao e o da particao ({warehouse!r})"
            )
            continue

        index = customer.get("candidate_index")
        if not isinstance(index, int) or not (0 <= index < total_candidates):
            errors.append(
                f"{customer_id}: candidate_index={index!r} fora da referencia "
                f"(0..{total_candidates - 1})"
            )
            continue
        candidate = reference.candidate(index)

        divergent = [
            field
            for field in GEO_FIELDS
            if customer.get(field) != candidate.get(field)
        ]
        if divergent:
            errors.append(
                f"{customer_id}: campo(s) {divergent} divergem da linha "
                f"{index} de address_candidates.json"
            )
            continue

        key = (customer["province_code"], customer["municipality_code"])
        if key not in service_area:
            errors.append(
                f"{customer_id}: municipio {key[0]}/{key[1]} fora da Area Urbana "
                f"Funcional de {warehouse}"
            )
            continue
        municipalities_used.add(key)

        problem = _check_house_number(customer, candidate)
        if problem:
            errors.append(problem)
            continue
        if customer.get("house_number") is not None:
            with_number += 1
        aprovados += 1

    if duplicated:
        errors.append(f"customer_id duplicado na particao: {sorted(duplicated)[:10]}")

    available = len(service_area)
    if len(municipalities_used) == 1 and available > 1 and len(customers) > 10:
        warnings.append(
            f"todos os {len(customers)} clientes cairam num unico municipio, com "
            f"{available} disponiveis na AUF — o peso populacional pode estar degenerado"
        )
    if customers and with_number == 0:
        warnings.append(
            "nenhum cliente recebeu numero de casa: legitimo se todos os tramos "
            "sorteados forem numbering_type='0', suspeito caso contrario"
        )
    return errors, warnings, aprovados


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

    warehouse = (manifest.get("partition") or {}).get("warehouse")
    config = manifest.get("config") or {}

    print(f"particao ......... {partition}")
    print(f"run_id ........... {manifest.get('run_id')}")
    print(f"source ........... {(manifest.get('source') or {}).get('name')}")
    print(f"warehouse ........ {warehouse}")
    print(f"seed ............. {config.get('seed')}")
    print(f"completa ......... {manifest.get('complete')}")

    # ---- Integridade dos arquivos declarados -------------------------------
    checked = 0
    declared: set = set()
    payloads: list = []
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
            payload, _ = read_json(target)
        except CorruptFileError as exc:
            errors.append(f"arquivo ilegivel: {exc}")
            continue
        payloads.append(payload)
        records = count_customers(payload)
        if records != entry.get("records"):
            errors.append(
                f"contagem divergente em {entry['path']}: "
                f"manifesto={entry.get('records')} arquivo={records}"
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

    customers = [row for payload in payloads for row in customers_of(payload)]

    # ---- Schema e campos obrigatorios --------------------------------------
    absent = missing_fields(customers)
    if absent:
        errors.append(f"campo(s) obrigatorio(s) ausente(s) em algum registro: {absent}")
    observed_fingerprint = fingerprint(payloads)
    declared_fingerprint = manifest.get("schema_fingerprint") or {}
    if observed_fingerprint.get("sha256") != declared_fingerprint.get("sha256"):
        errors.append(
            f"schema_fingerprint divergente: manifesto="
            f"{declared_fingerprint.get('sha256')} observado={observed_fingerprint.get('sha256')}"
        )
    # A chave house_number tem de existir em todo registro, mesmo nula: e o que impede a
    # impressao digital da particao de depender da seed.
    if customers and "house_number" not in set(observed_fingerprint.get("customer_keys") or []):
        errors.append("nenhum registro declara a chave house_number")

    # ---- Totais reconferidos contra a releitura ----------------------------
    totals = manifest.get("totals") or {}
    observed_totals = {
        "customer_rows": len(customers),
        "municipalities_used": len(
            {(c.get("province_code"), c.get("municipality_code")) for c in customers}
        ),
        "house_number_null": sum(1 for c in customers if c.get("house_number") is None),
        "bytes": sum(e.get("bytes", 0) for e in manifest["files"] if isinstance(e, dict)),
    }
    for key, observed in observed_totals.items():
        if totals.get(key) != observed:
            errors.append(
                f"totals.{key} divergente: manifesto={totals.get(key)} observado={observed}"
            )
    if totals.get("customer_rows") != config.get("count"):
        errors.append(
            f"config.count={config.get('count')} nao bate com "
            f"totals.customer_rows={totals.get('customer_rows')}"
        )

    # ---- Coerencia geoespacial contra a referencia --------------------------
    try:
        reference = load(args.reference)
    except ReferenceError as exc:
        print()
        print(f"FALHOU: referencia inutilizavel: {exc}")
        return EXIT_FAILED

    declared_reference = (manifest.get("reference") or {}).get("callejero_ingestion_date")
    if declared_reference != reference.callejero_ingestion_date:
        errors.append(
            f"referencia de outro snapshot do Callejero: manifesto={declared_reference} "
            f"arquivo={reference.callejero_ingestion_date}"
        )

    ingestion_date = (manifest.get("partition") or {}).get("ingestion_date")
    errors.extend(_check_minimum_age(customers, reference, ingestion_date))
    print(f"cadastro ......... idade minima {reference.min_customer_age} "
          f"(alvo da referencia: {reference.customer_allocation.get('rule')})")

    geo_errors, geo_warnings, aprovados = _check_reference(customers, reference, warehouse)
    errors.extend(geo_errors[:20])
    if len(geo_errors) > 20:
        errors.append(f"... e mais {len(geo_errors) - 20} problema(s) de coerencia geografica")
    warnings.extend(geo_warnings)
    print(f"coerencia ........ {aprovados}/{len(customers)} clientes com "
          f"endereco real na AUF de {warehouse}")

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
    print("OK: integridade, schema, totais e coerencia geoespacial validados.")
    return EXIT_OK
