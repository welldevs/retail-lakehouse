"""Fatos estruturais observados num arquivo do Callejero, sem interpretar conteudo.

Os arquivos sao texto de largura fixa em ISO-8859-1 com fim de linha CRLF (medido
contra os arquivos reais — ver CONTRACT.md secao 2). Esta Source nao parseia campos:
so observa fatos estruturais (quantas linhas, quais larguras aparecem) para o manifesto
e para a revalidacao — o mesmo papel que `schema_fingerprint` cumpre na source de
populacao, adaptado a um formato sem chaves nomeadas.

Uma divergencia de largura entre duas ingestion_date sucessivas e sinal de que o INE
mudou o layout do arquivo — informacao para o consumidor (Silver), nao algo que esta
Source tenta corrigir ou validar contra um valor esperado fixo: o layout observado hoje
nao e assumido estavel para sempre.
"""

from __future__ import annotations

ENCODING = "latin-1"  # ISO-8859-1, medido. Nunca convertido ao gravar (CONTRACT.md).


def line_stats(path: str) -> dict:
    """Numero de linhas e distribuicao de larguras (sem o terminador) do arquivo.

    Defensivo: um arquivo vazio ou ilegivel no encoding esperado nao levanta excecao
    aqui — devolve estatisticas vazias. Quem decide se isso e falha e o chamador
    (intake/validate), nao esta funcao.
    """
    widths: dict[int, int] = {}
    count = 0
    try:
        with open(path, encoding=ENCODING, newline="") as handle:
            for raw_line in handle:
                count += 1
                width = len(raw_line.rstrip("\r\n"))
                widths[width] = widths.get(width, 0) + 1
    except OSError:
        return {"line_count": 0, "widths": {}}
    return {"line_count": count, "widths": widths}


def dominant_width(stats: dict) -> int | None:
    """A largura mais frequente, ou None se o arquivo estiver vazio.

    Nao afirma que TODA linha tem essa largura — so relata a mais comum. Um arquivo com
    larguras heterogeneas ainda produz um resultado (nao e erro aqui); e o chamador que
    decide o que fazer com isso.
    """
    widths = stats.get("widths") or {}
    if not widths:
        return None
    return max(widths.items(), key=lambda item: item[1])[0]
