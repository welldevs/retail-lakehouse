"""O portao do `dbt build` do Silver: o que EXCLUIR e que VAR passar, decidido num lugar so.

POR QUE ESTE MODULO EXISTE, e o defeito concreto que o criou.

O projeto dbt do Silver tem 21 modelos, e nem todos podem ser construidos sempre. Dois
motivos, e os dois sao legitimos:

  1. UMA SOURCE QUE AINDA NAO ATERRISSOU NADA. `read_json` sobre um prefixo vazio nao
     devolve zero linhas — ele FALHA. Um repositorio recem-clonado, ou uma source nova,
     derrubaria o build inteiro por causa de uma arvore que nem deveria existir ainda.
  2. A PROJECAO ICEBERG. `silver_live_order_state` le por `metadata_location`, e esse
     caminho vem do CATALOGO, nunca de uma varredura do storage — o DuckDB recusa adivinhar,
     e a recusa esta certa. Sem o plano de stream de pe, nao ha catalogo a quem perguntar.

Ate 2026-08-31 essa decisao existia em SEIS lugares: o alvo `silver` do Makefile, com os
cinco portoes; e cada uma das cinco DAGs, com um portao so — o da propria source. Nenhuma
tinha o do Iceberg.

O RESULTADO FOI EXATAMENTE O QUE SE ESPERA DE LOGICA DUPLICADA: a DAG
`mercadona_catalog_daily` nao tinha portao NENHUM (a Mercadona sempre tem dado, entao
ninguem sentiu falta) e passou a reprovar todo dia assim que `silver_live_order_state`
nasceu, no Marco 6 — 4 armazens extraidos, validados e aterrissados com sucesso, e o
`silver` caindo no fim com "no version-hint could be found". O dado estava certo; o portao
e que morava no arquivo errado.

Agora ha um lugar. `plan()` e PURA — recebe o que foi observado e devolve os argumentos —
para que a decisao inteira seja testavel sem rede, sem MinIO e sem catalogo. `resolve()` faz
a observacao. `build()` roda o dbt. Makefile e DAGs chamam o mesmo verbo.

O QUE ESTE MODULO NAO FAZ: nao decide o que e um erro. Se uma source TEM dado e o modelo
dela falha, o build reprova — como deve. Excluir e para o que ainda nao existe, nunca para o
que esta quebrado.
"""

from __future__ import annotations

import os
import subprocess

# Cada source com o prefixo que a identifica no RAW e os modelos que morrem sem ele.
# A ordem e a de chegada das sources, e os nomes sao os que o dbt conhece.
SOURCE_MODELS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("mercadona_catalog_api", (
        "silver_product_price", "silver_category", "silver_price_change", "raw_manifest",
    )),
    ("ine_population_api", (
        "silver_ine_population_series", "silver_ine_population_by_municipality",
    )),
    ("ine_callejero", (
        "silver_callejero_sections", "silver_callejero_population_units",
        "silver_callejero_streets", "silver_callejero_pseudo_addresses",
        "silver_callejero_tramos",
    )),
    ("simulated_oltp", (
        "silver_customer", "silver_oltp_manifest",
    )),
    ("simulated_orders", (
        "silver_order_event", "silver_order", "silver_order_line", "silver_orders_manifest",
        # O teste do selo le `silver_orders_manifest`; sem particao de pedido aterrissada ele
        # tentaria `ref()` de algo que nao foi construido — mesmo motivo dos nos do Iceberg.
        "assert_raw_matches_the_frozen_capture",
    )),
)

# AS TABELAS ICEBERG, e os nos que morrem sem cada uma.
#
# Modelo e teste entram e saem JUNTOS: excluir so o modelo deixaria o teste tentando
# `ref()` de algo que nao foi construido.
#
# POR QUE ISTO VIROU UMA LISTA NA FASE 7. Era um par de constantes, porque so havia a
# projecao viva. Quando o ledger de estoque chegou, as opcoes eram um segundo par de
# constantes com a mesma logica escrita de novo — que e como o portao tinha nascido
# espalhado por seis arquivos — ou uma lista. O ganho pratico e que a terceira tabela
# Iceberg, se existir, e uma linha aqui e nada mais.
#
# Cada entrada e (chave, var do dbt, nos). A CHAVE e o que o `resolve()` usa para pedir o
# metadado; a VAR e como o dbt recebe o caminho.
ICEBERG_TABLES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("live_order_state", "live_order_state_metadata", (
        "silver_live_order_state", "assert_live_projection_matches_batch_fold",
    )),
    # O LEDGER DE ESTOQUE E ESCRITO PELO SPARK, que e opcional por decisao. Um repositorio
    # onde ninguem rodou `make stock-ledger` constroi o Silver inteiro sem ele — e essa e
    # a propriedade que faz o Spark nao entrar no caminho padrao do projeto.
    ("stock_ledger", "stock_ledger_metadata", (
        "silver_stock_ledger",
        "assert_stock_ledger_conserves_the_balance",
        "assert_stock_replenishment_respects_the_lead_time",
        "assert_stock_window_can_exercise_replenishment",
    )),
)

# Compatibilidade com quem so precisa da projecao viva.
ICEBERG_NODES = ICEBERG_TABLES[0][2]
ICEBERG_VAR = ICEBERG_TABLES[0][1]


def plan(landed: dict, iceberg_metadata) -> list[str]:
    """Argumentos do `dbt build`, dado o que foi observado. PURA: nao olha nada.

    `landed` mapeia prefixo da source -> se ha objeto aterrissado. Prefixo ausente do dicio-
    nario conta como NAO aterrissado, que e o lado seguro: excluir um modelo que poderia ter
    sido construido custa uma execucao; tentar construir um que nao pode custa o build.

    `iceberg_metadata` mapeia chave de ICEBERG_TABLES -> caminho do metadado, ou None. Uma
    string simples continua aceita e vale para a projecao viva, que era o unico caso ate a
    Fase 7 — as DAGs e o Makefile chamam `build()`, nunca `plan()`, entao a forma antiga so
    sobrevive por causa dos testes que a exercitam.
    """
    if iceberg_metadata is None or isinstance(iceberg_metadata, str):
        iceberg_metadata = {ICEBERG_TABLES[0][0]: iceberg_metadata}

    excluir: list[str] = []
    for prefixo, modelos in SOURCE_MODELS:
        if not landed.get(prefixo):
            excluir.extend(modelos)

    variaveis: dict[str, str] = {}
    for chave, var, nos in ICEBERG_TABLES:
        caminho = iceberg_metadata.get(chave)
        if caminho:
            variaveis[var] = caminho
        else:
            excluir.extend(nos)

    argumentos: list[str] = []
    if excluir:
        argumentos += ["--exclude", *excluir]
    if variaveis:
        # `--vars` como YAML inline. O caminho e uma URI s3:// com barras e dois-pontos,
        # entao vai entre aspas: sem elas o YAML le `s3:` como chave e o var chega vazio.
        corpo = ", ".join(f'{var}: "{caminho}"' for var, caminho in sorted(variaveis.items()))
        argumentos += ["--vars", f"{{{corpo}}}"]
    return argumentos


def has_landed(prefix: str, config=None) -> bool:
    """Existe ao menos um objeto sob <raw_bucket>/<prefixo>/?"""
    from .config import from_env

    config = config or from_env()
    resposta = config.client().list_objects_v2(
        Bucket=config.raw_bucket, Prefix=prefix.rstrip("/") + "/", MaxKeys=1
    )
    return resposta.get("KeyCount", 0) > 0


def iceberg_metadata() -> dict:
    """Caminho do metadado corrente de cada tabela, ou None onde o catalogo nao responder.

    Ausencia NAO e erro aqui, e por dois motivos diferentes: o plano de stream sobe sob
    demanda (`make stream-up`), e o Spark e opcional por decisao. Um `make silver` sem
    nenhum dos dois deve construir todos os outros modelos em paz.
    """
    saida: dict[str, str | None] = {}
    try:
        from .orders_projection import metadata_location

        saida["live_order_state"] = metadata_location() or None
    except Exception:
        saida["live_order_state"] = None
    try:
        from .stock_ledger import metadata_location as estoque

        saida["stock_ledger"] = estoque() or None
    except Exception:
        saida["stock_ledger"] = None
    return saida


def resolve(config=None) -> tuple[list[str], dict]:
    """Observa o mundo e devolve (argumentos do dbt, o que foi observado)."""
    from .config import from_env

    config = config or from_env()
    landed = {}
    for prefixo, _ in SOURCE_MODELS:
        try:
            landed[prefixo] = has_landed(prefixo, config)
        except Exception:
            # Object storage fora do ar e problema de verdade — mas quem reporta e o dbt,
            # com a mensagem do motor, e nao um portao que engoliu a excecao.
            landed[prefixo] = False
    metadado = iceberg_metadata()
    return plan(landed, metadado), {"landed": landed, "iceberg_metadata": metadado}


def build(project_dir: str, profiles_dir: str, *, dbt: str = None,
          extra: list[str] = None, config=None) -> tuple[int, list[str], dict]:
    """Roda `dbt build` com o portao aplicado. Devolve (codigo, argumentos, observado)."""
    argumentos, observado = resolve(config)
    executavel = dbt or os.environ.get("RETAIL_DBT", "dbt")
    comando = [
        executavel, "build",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        *argumentos, *(extra or []),
    ]
    return subprocess.call(comando), argumentos, observado
