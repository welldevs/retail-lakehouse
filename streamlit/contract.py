#!/usr/bin/env python3
"""Gera streamlit/CONTRACT.md a partir de indicators.py.

    make dashboard-contract

POR QUE GERADO E NAO ESCRITO A MAO. O CONTRACT existe para CONFERENCIA: alguem vai ler a
consulta ao lado da explicacao e decidir se o indicador esta certo antes de reconstrui-lo no
Power BI. Se os dois morassem em arquivos diferentes, divergiriam no primeiro ajuste de SQL —
e a conferencia continuaria "passando", porque ninguem le um SQL e um texto lado a lado
procurando desacordo.

E o mesmo mecanismo de `docs/warehouse-evidence/` e do DDL do STAGE: o documento e derivado
da coisa, entao nao ha um segundo lugar onde a verdade vive.

O que este script NAO faz: nao conecta em nada. O CONTRACT e revisavel sem credencial, sem
Snowflake e sem rede — como o `make warehouse-ddl`.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import indicators as I  # noqa: E402

DESTINO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CONTRACT.md")


def render() -> str:
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    grupos: dict[str, list] = {}
    for indicador in I.INDICADORES:
        grupos.setdefault(indicador.grupo, []).append(indicador)

    linhas = [
        "# Contrato dos indicadores do painel",
        "",
        f"**Gerado por `make dashboard-contract` em {agora}.** Não editar à mão: este arquivo",
        "é derivado de [`indicators.py`](indicators.py), que é onde a consulta e a explicação",
        "moram juntas. Editar aqui cria o segundo lugar onde o indicador vive, e os dois",
        "divergem no primeiro ajuste de SQL — com o detalhe cruel de que a conferência",
        "continuaria passando, porque ninguém lê um SQL e um texto lado a lado procurando",
        "desacordo.",
        "",
        "## Para que serve",
        "",
        "Conferir cada indicador **antes** de reconstruí-lo no Power BI. Para cada um:",
        "a pergunta que responde, o grão da fonte, se o dado é observado ou sintético, o SQL",
        "exato que o painel executa, e as **armadilhas** — os casos em que a medida óbvia",
        "produz um número plausível e errado, que é a única classe de erro que nenhum teste",
        "pega.",
        "",
        "## Fronteira e credencial",
        "",
        "| | |",
        "|---|---|",
        f"| Destino | `{I.DB}.MART` — e **somente** MART |",
        "| Papel | `RETAIL_READER`, o papel de BI. Lê MART; recusado em GOLD e STAGE |",
        "| Sessão | abre com `use secondary roles none` — sem isso a restrição passaria por engano |",
        "| Autenticação | par de chaves RSA, de `~/.snowflake/config.toml`. Nenhum segredo no repo |",
        "| Escrita | nenhuma. O painel não cria, não altera e não apaga nada |",
        "",
        "O painel roda uma sonda ao vivo que confirma a recusa em GOLD e STAGE, porque um",
        "painel que afirma respeitar um limite sem demonstrar está pedindo confiança.",
        "",
        "## Parâmetros",
        "",
        "Toda consulta com eixo de data aceita três parâmetros ligados (*bound*), nunca",
        "concatenados — então não há como injetar nada pelo seletor da interface:",
        "",
        "| Parâmetro | Tipo | Uso |",
        "|---|---|---|",
        "| `%(inicio)s` | data ISO | limite inferior, inclusivo |",
        "| `%(fim)s` | data ISO | limite superior, inclusivo |",
        "| `%(armazens)s` | lista por vírgula | `array_contains(wh::variant, split(%(armazens)s, ','))` |",
        "",
        f"Os indicadores marcados **sem eixo de data** ignoram `inicio`/`fim`: trazem a versão",
        "vigente.",
        "",
        "## Índice",
        "",
    ]

    for grupo in sorted(grupos):
        linhas.append(f"- **{grupo}**")
        for indicador in grupos[grupo]:
            linhas.append(f"  - [{indicador.titulo}](#{indicador.chave})")
    linhas += ["- [O que o painel NÃO exibe](#o-que-o-painel-não-exibe)", ""]

    for grupo in sorted(grupos):
        linhas += [f"## {grupo}", ""]
        for indicador in grupos[grupo]:
            linhas += [
                f'<a id="{indicador.chave}"></a>',
                f"### {indicador.titulo}",
                "",
                "| | |",
                "|---|---|",
                f"| Chave | `{indicador.chave}` |",
                f"| Pergunta | {indicador.pergunta} |",
                f"| Grão da fonte | `{indicador.grao}` |",
                f"| Tipo do dado | {indicador.tipo} |",
                f"| Marts | {', '.join(f'`{m}`' for m in indicador.marts)} |",
                f"| Eixo de data | {'sim' if indicador.datado else '**não** — versão vigente'} |",
                "",
            ]
            if indicador.armadilhas:
                linhas += ["**Armadilhas ao reconstruir no Power BI**", ""]
                linhas += [f"{n}. {texto}" for n, texto in enumerate(indicador.armadilhas, 1)]
                linhas += [""]
            linhas += ["```sql", indicador.sql.strip(), "```", ""]

    linhas += [
        "## O que o painel NÃO exibe",
        "",
        "Uma lista de ausências declaradas vale mais que um indicador inventado. Cada item",
        "traz o **gatilho** que o destravaria, para que a conversa seja sobre o que falta e",
        "não sobre o que poderia ser aproximado.",
        "",
    ]
    for titulo, motivo, gatilho in I.FORA_DE_ALCANCE:
        linhas += [f"### {titulo}", "", motivo, "", f"**Gatilho:** {gatilho}", ""]

    linhas += [
        "---",
        "",
        "## Consultas auxiliares",
        "",
        "Não são indicadores de negócio. `FRESCOR` é o que torna uma carga nova **visível**:",
        "sem ele, os números mudam e ninguém sabe que a base mudou.",
        "",
        "```sql",
        I.FRESCOR.strip(),
        "```",
        "",
        "```sql",
        I.JANELA.strip(),
        "```",
        "",
        "```sql",
        I.ARMAZENS.strip(),
        "```",
        "",
    ]
    return "\n".join(linhas) + "\n"


def main() -> int:
    texto = render()
    temporario = DESTINO + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(texto)
    os.replace(temporario, DESTINO)
    print(f"escrito em ....... {os.path.relpath(DESTINO)}")
    print(f"indicadores ...... {len(I.INDICADORES)}")
    print(f"fora de alcance .. {len(I.FORA_DE_ALCANCE)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
