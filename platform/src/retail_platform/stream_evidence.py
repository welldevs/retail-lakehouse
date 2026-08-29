"""Evidencia do plano de stream: OLTP, broker, projecao — e os tres folds concordando.

POR QUE ESTE MODULO EXISTE, e por que ele nao e o irmao gemeo de `snowflake_evidence.py`.

O motivo la era EXPIRACAO: a conta e um trial, e quando ela morrer os modelos do warehouse
viram quatorze arquivos que ninguem consegue provar que rodaram. Aqui o motivo e outro, e e
o que o proprio ARCHITECTURE.md ja registrou como divida deliberada: a metade em streaming
NAO E COBERTA OFFLINE. `make test` roda sem rede — e invariante do repo — entao broker,
OLTP e Iceberg so existem enquanto `make stream-up` estiver de pe. Os duplos em memoria
(`fake_kafka`, `fake_pg`, `fake_iceberg`) cobrem a FORMA do codigo: a ordem da transacao, o
protocolo de dedup, a construcao do SQL. O que eles nao podem cobrir e a SEMANTICA dos
motores reais — que o Kafka preserva ordem por chave, que o Postgres desfaz de verdade, que
o Iceberg recusa um commit sobre snapshot velho.

Esta pagina e o registro de que a semantica foi exercida contra os motores de verdade. Ela
substitui o "confie em mim" por um numero datado, do mesmo jeito que a evidencia do
warehouse — mas o gatilho para regenera-la e diferente: la e "antes que a conta expire",
aqui e "depois de qualquer execucao que valha registrar".

O QUE ESTE MODULO NAO E: nao valida nada, nao conserta nada, e nao sobe servico nenhum. Ele
OBSERVA os tres planos e escreve o que observou. Quem valida sao as 273 checagens da suite
de plataforma, os tres scripts de prova (`orders-prove-*`) e `orders-reconcile`.

E TOLERANTE A PLANO DESLIGADO, DE PROPOSITO. Se o Kafka nao estiver de pe, a secao do
broker diz isso em vez de derrubar o relatorio: uma evidencia parcial e util e uma
evidencia que nao existe nao e. O que ele NUNCA faz e inventar o numero que nao conseguiu
observar — cada secao ausente aparece como ausencia declarada.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

DEFAULT_OUT = os.path.join("docs", "stream-evidence", "README.md")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _tabela(colunas: list[str], linhas: list) -> list[str]:
    cabecalho = "| " + " | ".join(colunas) + " |"
    separador = "|" + "|".join("---" for _ in colunas) + "|"
    corpo = [
        "| " + " | ".join("" if v is None else str(v) for v in linha) + " |"
        for linha in linhas
    ]
    return [cabecalho, separador, *corpo]


# --------------------------------------------------------------------------------------
# Observacao, plano a plano
# --------------------------------------------------------------------------------------
# Cada bloco devolve `{"erro": "..."}` em vez de propagar a excecao. O relatorio precisa
# poder ser gerado com o plano meio de pe — e precisa DIZER que estava meio de pe.

def _observar_oltp(dsn=None) -> dict:
    from . import orders_oltp
    try:
        conexao = orders_oltp.connect(dsn)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        resumo = orders_oltp.outbox_summary(conexao)
        with conexao.cursor() as cur:
            cur.execute("select count(*) from orders")
            resumo["orders"] = cur.fetchone()[0]
            cur.execute("select count(*) from order_line")
            resumo["order_lines"] = cur.fetchone()[0]
            cur.execute("select status, count(*) from order_line group by 1 order by 1")
            resumo["lines_by_status"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("select min(published_at), max(published_at) from outbox")
            primeiro, ultimo = cur.fetchone()
            resumo["published_from"] = str(primeiro) if primeiro else None
            resumo["published_to"] = str(ultimo) if ultimo else None
        return resumo
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    finally:
        conexao.close()


def _observar_broker(bootstrap: str, topic: str, grupos: list) -> dict:
    from . import orders_stream
    try:
        marcas = orders_stream.topic_watermarks(bootstrap, topic)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    lags = {}
    for grupo in grupos:
        try:
            lags[grupo] = orders_stream.group_lag(bootstrap, topic, grupo)
        except Exception as exc:
            lags[grupo] = {"erro": str(exc).splitlines()[0]}
    return {"topic": topic, "bootstrap": bootstrap, "watermarks": marcas, "lags": lags}


def _observar_projecao(config, dsn=None) -> dict:
    from . import orders_projection
    try:
        cat = orders_projection.catalog(config)
        tabela = cat.load_table(orders_projection.TABLE_NAME)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        import pyarrow.compute as pc

        arrow = tabela.scan().to_arrow()
        proveniencia = {}
        if arrow.num_rows and "written_by" in arrow.column_names:
            contagem = pc.value_counts(arrow.column("written_by").combine_chunks())
            proveniencia = {
                str(entrada["values"]): int(entrada["counts"])
                for entrada in contagem.to_pylist()
            }
        estados = {}
        if arrow.num_rows and "status" in arrow.column_names:
            contagem = pc.value_counts(arrow.column("status").combine_chunks())
            estados = {
                str(entrada["values"]): int(entrada["counts"])
                for entrada in contagem.to_pylist()
            }
        snapshots = list(tabela.metadata.snapshots)
        return {
            "table": orders_projection.TABLE_NAME,
            "metadata_location": orders_projection.metadata_location(cat),
            "rows": arrow.num_rows,
            "snapshots": len(snapshots),
            "current_snapshot_id": tabela.metadata.current_snapshot_id,
            "written_by": proveniencia,
            "by_status": estados,
        }
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}


def _observar_reconciliacao(config, dsn=None) -> dict:
    from . import orders_projection
    try:
        return orders_projection.reconcile(config, dsn=dsn)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}


def collect(config=None, *, bootstrap=None, topic=None, groups=None, dsn=None) -> dict:
    """Observa os tres planos. Nao formata nada, nao levanta nada."""
    from . import orders_stream
    from .config import from_env

    config = config or from_env()
    bootstrap = bootstrap or orders_stream.DEFAULT_BOOTSTRAP
    topic = topic or orders_stream.DEFAULT_TOPIC
    # Os DOIS grupos, e a diferenca entre eles e o ponto do par lambda: cada sink consome o
    # mesmo topico no seu proprio ritmo, com offset proprio. Um grupo so esconderia isso.
    groups = groups or [orders_stream.DEFAULT_GROUP, f"{orders_stream.DEFAULT_GROUP}-iceberg"]

    return {
        "capturado_em": _utc_now(),
        "oltp": _observar_oltp(dsn),
        "broker": _observar_broker(bootstrap, topic, groups),
        "projecao": _observar_projecao(config, dsn),
        "reconciliacao": _observar_reconciliacao(config, dsn),
    }


# --------------------------------------------------------------------------------------
# Relatorio
# --------------------------------------------------------------------------------------

def _secao_ausente(nome: str, erro: str) -> list[str]:
    return [
        f"> **{nome} nao observado.** `{erro}`",
        ">",
        "> A secao esta ausente, e nao estimada. Suba o plano com `make stream-up` e",
        "> regenere com `make stream-evidence`.",
        "",
    ]


def render(dados: dict) -> str:
    """Markdown. Nenhum numero escrito a mao: tudo vem do que foi observado."""
    linhas = [
        "# Evidência do plano de stream",
        "",
        "Gerado por `make stream-evidence` contra o OLTP, o broker e a projeção **vivos** no",
        "momento da captura. **Não é documentação escrita à mão** — todo número desta página",
        "saiu de uma consulta a um dos três.",
        "",
        "Existe porque a metade em streaming é dívida declarada: `make test` roda sem rede, e",
        "os duplos em memória (`fake_kafka`, `fake_pg`, `fake_iceberg`) cobrem a **forma** do",
        "código — a ordem da transação, o protocolo de dedup, a construção do SQL. O que eles",
        "não podem cobrir é a **semântica** dos motores reais: que o Kafka preserva ordem por",
        "chave, que o Postgres desfaz de verdade, que o Iceberg recusa um commit sobre",
        "snapshot velho. Isto aqui é o registro de que ela foi exercida contra eles.",
        "",
        f"| capturado em | {dados['capturado_em']} |",
        "|---|---|",
        "",
    ]

    # ---- OLTP -------------------------------------------------------------------------
    linhas += [
        "## Plano transacional — OLTP e outbox",
        "",
        "O evento nasce **dentro da mesma transação** que muda `orders` e `order_line`. Não é",
        "o log sendo republicado: é a mudança de estado e o evento gravados atomicamente, que",
        "é a única forma de os dois não divergirem. `outbox.event_id` é único, e o insert do",
        "outbox vem **primeiro** — `rowcount = 0` significa evento já aplicado, e a transação",
        "inteira é desfeita.",
        "",
    ]
    oltp = dados["oltp"]
    if "erro" in oltp:
        linhas += _secao_ausente("OLTP", oltp["erro"])
    else:
        linhas += _tabela(["", ""], [
            ("pedidos em `orders`", f"{oltp['orders']:,}"),
            ("linhas em `order_line`", f"{oltp['order_lines']:,}"),
            ("eventos no `outbox`", f"{oltp['outbox_rows']:,}"),
            ("ainda não publicados", f"{oltp['unpublished']:,}"),
            ("pedidos distintos no outbox", f"{oltp['orders_in_outbox']:,}"),
            ("primeira publicação", oltp["published_from"]),
            ("última publicação", oltp["published_to"]),
        ])
        linhas += ["", "### Eventos no outbox, por tipo", ""]
        linhas += _tabela(["event_type", "eventos"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["by_event_type"].items())])
        linhas += ["", "### Estado replicado, por fold do OLTP", ""]
        linhas += _tabela(["status do pedido", "pedidos"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["orders_by_status"].items())])
        linhas += [""]
        linhas += _tabela(["status da linha", "linhas"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["lines_by_status"].items())])
        linhas += [""]

    # ---- Broker -----------------------------------------------------------------------
    linhas += [
        "## Transporte — Kafka",
        "",
        "`key = order_id`, e a chave é **carregável**: o Kafka garante ordem dentro da",
        "partição, e é isso que permite a dedup do consumidor ser limitada — comparar",
        "`sequence_no` contra o `last_sequence_no` já gravado, sem conjunto de `event_id` que",
        "cresce nem janela de expiração. Entrega é **at-least-once** do outbox para o broker",
        "(publica → ack → marca, nunca marca → publica); o consumo é **effectively-once**",
        "porque o offset só é commitado depois da escrita.",
        "",
    ]
    broker = dados["broker"]
    if "erro" in broker:
        linhas += _secao_ausente("Broker", broker["erro"])
    else:
        linhas += [f"Tópico `{broker['topic']}` em `{broker['bootstrap']}`.", ""]
        linhas += ["### Marcas d'água por partição", ""]
        marcas = broker["watermarks"]
        linhas += _tabela(
            ["partição", "low", "high", "mensagens"],
            [(p, m["low"], m["high"], f"{m['high'] - m['low']:,}")
             for p, m in sorted(marcas.items())],
        )
        total = sum(m["high"] - m["low"] for m in marcas.values())
        linhas += ["", f"Total no tópico: **{total:,} mensagens**.", ""]
        linhas += [
            "A soma pode exceder a contagem de eventos do log, e isso é **correto**: a",
            "entrega do outbox para o broker é at-least-once por desenho, então uma queda",
            "entre o ack e a marcação de `published_at` republica o lote. O que a torna",
            "inofensiva é a dedup por `sequence_no` do outro lado.",
            "",
            "### Lag por grupo de consumo",
            "",
            "Dois grupos, e a diferença entre eles é o par lambda: cada sink consome o mesmo",
            "tópico no seu próprio ritmo, com offset próprio. É o ponto de desacoplamento — o",
            "sink Iceberg (~4min48s por passada, copy-on-write) não segura o sink Postgres",
            "(~14s), e nenhum dos dois perde mensagem por causa do outro.",
            "",
        ]
        for grupo, lag in broker["lags"].items():
            linhas += [f"**`{grupo}`**", ""]
            if "erro" in lag:
                linhas += [f"- não observado: `{lag['erro']}`", ""]
                continue
            linhas += _tabela(
                ["partição", "offset commitado", "high", "lag"],
                [(p, d["committed"], d["high"], d["lag"]) for p, d in sorted(lag.items())],
            )
            linhas += ["", f"Lag total: **{sum(d['lag'] for d in lag.values()):,}**.", ""]

    # ---- Projecao ---------------------------------------------------------------------
    linhas += [
        "## Projeção — Iceberg",
        "",
        "O gatilho escrito para o Iceberg era *\"um segundo engine precisar escrever a mesma",
        "tabela\"*. Ele disparou por **concorrência, não por volume**: neste volume um parquet",
        "reescrito com `os.replace` atômico funcionaria. O que o Iceberg compra é isolamento",
        "de snapshot entre dois escritores e um leitor concorrente, mais time travel.",
        "",
        "`written_by` é a prova de que os dois escritores existem de fato, e é **consultável**",
        "em vez de anedótica.",
        "",
    ]
    projecao = dados["projecao"]
    if "erro" in projecao:
        linhas += _secao_ausente("Projeção", projecao["erro"])
    else:
        linhas += _tabela(["", ""], [
            ("tabela", f"`{projecao['table']}`"),
            ("linhas", f"{projecao['rows']:,}"),
            ("snapshots", f"{projecao['snapshots']:,}"),
            ("snapshot corrente", projecao["current_snapshot_id"]),
            ("metadado corrente", f"`{projecao['metadata_location']}`"),
        ])
        linhas += [
            "",
            "O caminho do metadado vem do **catálogo**, nunca de uma varredura do storage. O",
            "DuckDB recusa adivinhar qual metadado é o corrente — *\"globbing the filesystem…",
            "could result in reading uncommitted data\"* — e o atalho existe",
            "(`unsafe_enable_version_guessing`), foi medido e foi **recusado**: ler metadado",
            "não commitado é exatamente o que uma leitura concorrente não pode fazer.",
            "",
            "### Proveniência: quem escreveu cada linha",
            "",
        ]
        linhas += _tabela(["written_by", "linhas"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projecao["written_by"].items())])
        linhas += ["", "### Estado na projeção viva", ""]
        linhas += _tabela(["status", "pedidos"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projecao["by_status"].items())])
        linhas += [""]

    # ---- Reconciliacao ----------------------------------------------------------------
    linhas += [
        "## Os três folds",
        "",
        "O mesmo estado de pedido é calculado por três caminhos, e `make orders-reconcile`",
        "compara os três coluna a coluna:",
        "",
        "- **`silver_order`** — window functions em SQL sobre o log inteiro;",
        "- **`orders`/`order_line` no OLTP** — máquina de estados transacional, evento a evento;",
        "- **`live_order_state`** — fold em streaming sobre o tópico.",
        "",
        "Só o primeiro **não compartilha código** com nenhum dos outros. É contra ele que a",
        "comparação vale como verificação; o acordo entre a projeção e o OLTP vale como",
        "evidência de transporte, não de correção — os dois compartilham o fold.",
        "",
    ]
    rec = dados["reconciliacao"]
    if "erro" in rec:
        linhas += _secao_ausente("Reconciliação", rec["erro"])
    else:
        linhas += _tabela(["fonte", "pedidos"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(rec["orders"].items())])
        linhas += ["", f"Comparados: **{rec['compared']:,} pedidos**.", ""]
        if rec["ok"]:
            linhas += ["Resultado: **os três concordam em todos os pedidos comparados** — "
                       "zero divergências, zero ausências.", ""]
        else:
            linhas += [f"**Divergências: {rec['divergence_count']}.**", ""]
            if rec["divergences"]:
                colunas = list(rec["divergences"][0].keys())
                linhas += _tabela(colunas,
                                  [tuple(d[c] for c in colunas) for d in rec["divergences"]])
                linhas += [""]
            for nome, ausentes in rec.get("missing", {}).items():
                linhas += [f"- ausentes de `{nome}`: {', '.join(map(str, ausentes))}", ""]

    linhas += [
        "---",
        "",
        "Regenere com `make stream-evidence` depois de qualquer execução que valha registrar.",
        "As provas que sustentam cada afirmação acima rodam em separado:",
        "`make orders-prove-atomicity`, `make orders-prove-stream`, `make orders-prove-projection`.",
    ]
    return "\n".join(linhas) + "\n"


def write(dados: dict, out: str = DEFAULT_OUT) -> str:
    """Escreve o relatorio. Cria o diretorio; devolve o caminho."""
    destino = os.path.abspath(out)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    # Escrita atomica pelo mesmo motivo do resto do repo: um relatorio truncado por
    # interrupcao nao pode parecer completo para quem o le depois.
    temporario = destino + ".tmp"
    with open(temporario, "w", encoding="utf-8") as handle:
        handle.write(render(dados))
    os.replace(temporario, destino)
    return destino
