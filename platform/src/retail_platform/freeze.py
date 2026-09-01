"""O SELO DA CAPTURA: `make freeze` e `make freeze-check`.

O PROBLEMA QUE ISTO RESOLVE, e ele nao e teorico.

As tres ultimas revisoes de documentacao deste projeto existiram pelo mesmo motivo: uma
regeracao mudou numeros que ja estavam escritos, e nada avisou. A disciplina "nao mexa no
RAW depois de fechar" foi respeitada por atencao, nunca por verificacao — e atencao nao
reprova build nenhum.

Isto transforma a disciplina em teste.

O QUE E SELADO, E O QUE E DELIBERADAMENTE IGNORADO.

Cada `_manifest.json` do RAW declara os arquivos da particao com `path`, `sha256`, `bytes` e
`records`. O selo cobre EXATAMENTE isso — os dados. Ficam de fora, de proposito:

    run_id, started_at_utc, finished_at_utc, duration_seconds, history

Sao metadados de EXECUCAO. Inclui-los faria um re-land de dado byte-identico quebrar o selo,
e um selo que quebra num no-op treina quem revisa a ignora-lo — o mesmo defeito do carimbo de
data que o CONTRACT do painel tinha, e pela mesma razao: um alarme que dispara sem causa e
pior que nenhum alarme.

POR QUE O RAW E SELADO E NAO REPRODUZIDO. Ele NAO e reproduzivel, e prometer que seja seria
falso: a API da Mercadona e viva, o Callejero e download manual semestral, e a URL do MAPA
aponta para "ultimos datos". Tudo a JUSANTE e deterministico dada a mesma RAW. O `capture_id`
e o que da nome a essa condicao: numa maquina nova o operador roda `make freeze` e sela a
CAPTURA DELE, e o teste passa a guardar a dele.

DOIS ARTEFATOS, e a duplicidade tem motivo:

    docs/FREEZE.md                          para quem le
    platform/dbt/seeds/frozen_capture_seed.csv   para quem verifica

O markdown nao e legivel por SQL, e o CSV nao e legivel por gente. Os dois saem da MESMA
coleta, no mesmo comando — nao ha um segundo lugar onde a verdade mora.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
DEFAULT_PAGE = os.path.join("docs", "FREEZE.md")
DEFAULT_SEED = os.path.join("platform", "dbt", "seeds", "frozen_capture_seed.csv")

MANIFEST_NAME = "_manifest.json"
CAMPOS = ("source", "partition_key", "files", "records", "bytes", "content_sha256")


class FreezeError(RuntimeError):
    """Falha ao selar ou conferir a captura."""


def _sha256_do_conteudo(manifesto: dict) -> str:
    """sha256 da lista ORDENADA de (path, sha256, bytes, records) declarada no manifesto.

    Ordenada porque a ordem em que a Source lista os arquivos e detalhe de implementacao —
    duas execucoes com o mesmo dado podem enumerar diferente, e o selo nao pode depender
    disso. Canonico via JSON com separadores fixos: `json.dumps` com espaco variavel mudaria
    o hash sem nenhum byte de dado ter mudado.
    """
    entradas = sorted(
        (str(f.get("path")), str(f.get("sha256")), int(f.get("bytes") or 0),
         int(f.get("records") or 0))
        for f in manifesto.get("files") or []
    )
    corpo = json.dumps(entradas, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(corpo.encode("utf-8")).hexdigest()


def collect(config=None) -> list[dict]:
    """Percorre o RAW e devolve uma linha por particao selada, em ordem estavel."""
    from .config import from_env

    config = config or from_env()
    cliente = config.client()
    paginador = cliente.get_paginator("list_objects_v2")

    registros: list[dict] = []
    for pagina in paginador.paginate(Bucket=config.raw_bucket):
        for item in pagina.get("Contents") or []:
            chave = item["Key"]
            if not chave.endswith("/" + MANIFEST_NAME):
                continue
            corpo = cliente.get_object(Bucket=config.raw_bucket, Key=chave)["Body"].read()
            manifesto = json.loads(corpo)
            prefixo = chave[: -len("/" + MANIFEST_NAME)]
            fonte, _, particao = prefixo.partition("/")
            arquivos = manifesto.get("files") or []
            registros.append({
                "source": fonte,
                "partition_key": particao,
                "files": len(arquivos),
                "records": sum(int(f.get("records") or 0) for f in arquivos),
                "bytes": sum(int(f.get("bytes") or 0) for f in arquivos),
                "content_sha256": _sha256_do_conteudo(manifesto),
            })
    registros.sort(key=lambda r: (r["source"], r["partition_key"]))
    if not registros:
        raise FreezeError(
            f"nenhum _manifest.json em s3://{config.raw_bucket}/. Nada foi aterrissado — "
            "rode `make daily` (ou os alvos de land) antes de selar."
        )
    return registros


def capture_id(registros: list[dict]) -> str:
    """sha256 agregado sobre os selos de particao. UM numero nomeia a captura inteira.

    Ele existe para que uma conversa sobre "os 4.939 produtos" possa dizer DE QUAL captura
    esses 4.939 sao, sem repetir uma tabela de 81 linhas.
    """
    corpo = json.dumps(
        [[r["source"], r["partition_key"], r["content_sha256"]] for r in registros],
        separators=(",", ":"), ensure_ascii=True,
    )
    return hashlib.sha256(corpo.encode("utf-8")).hexdigest()


def escrever_seed(registros: list[dict], destino: str = DEFAULT_SEED) -> str:
    caminho = destino if os.path.isabs(destino) else os.path.join(REPO, destino)
    buffer = io.StringIO()
    escritor = csv.DictWriter(buffer, fieldnames=CAMPOS, lineterminator="\n")
    escritor.writeheader()
    escritor.writerows({c: r[c] for c in CAMPOS} for r in registros)
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(buffer.getvalue())
    os.replace(temporario, caminho)
    return caminho


def ler_seed(origem: str = DEFAULT_SEED) -> list[dict]:
    caminho = origem if os.path.isabs(origem) else os.path.join(REPO, origem)
    if not os.path.exists(caminho):
        raise FreezeError(
            f"{origem} nao existe: a captura nunca foi selada. Rode `make freeze`."
        )
    with open(caminho, encoding="utf-8") as arquivo:
        return [
            {"source": l["source"], "partition_key": l["partition_key"],
             "files": int(l["files"]), "records": int(l["records"]),
             "bytes": int(l["bytes"]), "content_sha256": l["content_sha256"]}
            for l in csv.DictReader(arquivo)
        ]


def comparar(selado: list[dict], atual: list[dict]) -> list[str]:
    """Diferencas entre o selo e o RAW de agora. Lista vazia = a captura esta intacta."""
    por_chave = lambda lista: {(r["source"], r["partition_key"]): r for r in lista}  # noqa: E731
    a, b = por_chave(selado), por_chave(atual)
    problemas: list[str] = []
    for chave in sorted(set(a) - set(b)):
        problemas.append(f"particao SELADA e AUSENTE do RAW: {chave[0]}/{chave[1]}")
    for chave in sorted(set(b) - set(a)):
        problemas.append(f"particao NOVA, fora do selo: {chave[0]}/{chave[1]}")
    for chave in sorted(set(a) & set(b)):
        if a[chave]["content_sha256"] != b[chave]["content_sha256"]:
            problemas.append(
                f"conteudo MUDOU em {chave[0]}/{chave[1]}: "
                f"selado={a[chave]['content_sha256'][:16]}... "
                f"atual={b[chave]['content_sha256'][:16]}..."
            )
    return problemas


def render(registros: list[dict]) -> str:
    identidade = capture_id(registros)
    por_fonte: dict[str, list[dict]] = {}
    for r in registros:
        por_fonte.setdefault(r["source"], []).append(r)

    linhas = [
        "# A captura selada",
        "",
        "**Gerado por `make freeze`.** Não editar à mão.",
        "",
        f"**`capture_id` = `{identidade}`**",
        "",
        f"Selado em {datetime.now(timezone.utc).strftime('%Y-%m-%d')} · "
        f"{len(registros)} partições · "
        f"{sum(r['records'] for r in registros):,} registros · "
        f"{sum(r['bytes'] for r in registros) / (1024**3):.2f} GB".replace(",", "."),
        "",
        "## O que este selo é, e o que ele não é",
        "",
        "**O RAW deste projeto não é reproduzível, e prometer que fosse seria falso.** A API",
        "da Mercadona é viva, o Callejero é um download manual semestral, e a URL do MAPA",
        "aponta para \"últimos datos\". Rodar a extração amanhã produz outra captura — e isso",
        "não é defeito, é a natureza de fontes públicas.",
        "",
        "O que **é** garantido: tudo a jusante é determinístico **dada a mesma RAW**. O",
        "`capture_id` é o nome dessa condição. Numa máquina nova, o operador roda `make",
        "freeze` e sela a captura **dele**; `make freeze-check` passa a guardar a dele.",
        "",
        "**O selo cobre os dados, não a execução.** Cada `content_sha256` é o hash da lista",
        "ordenada de `(path, sha256, bytes, records)` declarada no manifesto da partição.",
        "Ficam deliberadamente de fora `run_id`, `started_at_utc`, `finished_at_utc`,",
        "`duration_seconds` e `history`: incluí-los faria um re-land de dado byte-idêntico",
        "quebrar o selo, e um alarme que dispara sem causa é pior que nenhum alarme — a mesma",
        "razão pela qual o `CONTRACT.md` do painel carrega o sha256 da origem e não a data da",
        "geração.",
        "",
        "## Como conferir",
        "",
        "```bash",
        "make freeze-check   # relê o RAW e compara com este selo; sai 1 em qualquer diferença",
        "```",
        "",
        "Alterar um único byte de uma partição selada reprova. Acrescentar uma partição nova",
        "também — porque uma janela que cresce depois do fechamento invalida todo número já",
        "publicado sobre ela.",
        "",
        "## Resumo por source",
        "",
        "| Source | Partições | Registros | GB |",
        "|---|---|---|---|",
    ]
    for fonte in sorted(por_fonte):
        grupo = por_fonte[fonte]
        linhas.append(
            f"| `{fonte}` | {len(grupo)} | "
            + f"{sum(r['records'] for r in grupo):,}".replace(",", ".")
            + f" | {sum(r['bytes'] for r in grupo) / (1024**3):.3f} |"
        )
    linhas += ["", "## Partições", ""]
    for fonte in sorted(por_fonte):
        linhas += [f"### `{fonte}`", "",
                   "| Partição | Arquivos | Registros | Bytes | `content_sha256` |",
                   "|---|---|---|---|---|"]
        for r in por_fonte[fonte]:
            linhas.append(
                f"| `{r['partition_key']}` | {r['files']} | "
                + f"{r['records']:,}".replace(",", ".")
                + f" | {r['bytes']:,}".replace(",", ".")
                + f" | `{r['content_sha256'][:24]}…` |"
            )
        linhas.append("")
    return "\n".join(linhas) + "\n"


def escrever_pagina(registros: list[dict], destino: str = DEFAULT_PAGE) -> str:
    caminho = destino if os.path.isabs(destino) else os.path.join(REPO, destino)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(render(registros))
    os.replace(temporario, caminho)
    return caminho
