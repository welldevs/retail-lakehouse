"""A documentacao conferida como codigo. Sem rede.

POR QUE ESTA SUITE EXISTE. A revisao de 2026-09-01 encontrou, so na documentacao: uma
arvore de diretorios que anunciava 7 marts onde havia 8 e omitia 8 dos 14 seeds, uma
tabela de cobertura que dizia 131 testes onde havia 376, e a MESMA secao afirmando 29 e 16
para o mesmo modulo em dois paragrafos. Nada disso quebra nada — e exatamente por isso
sobreviveu a seis fases.

O que da para verificar por maquina, aqui esta. O que nao da (se o texto esta CERTO)
continua sendo leitura humana; estes testes so garantem que ele nao esta apontando para
coisa que nao existe.

Deliberadamente NAO confere contagem de teste escrita em prosa: o remedio ali foi parar de
escrever o numero, nao automatizar a copia dele. Ver ARCHITECTURE.md, "Cobertura de teste".
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import unittest

RAIZ = pathlib.Path(__file__).resolve().parents[2]


def _versionados(padrao: str) -> list[pathlib.Path]:
    saida = subprocess.run(
        ["git", "ls-files", padrao], cwd=RAIZ, capture_output=True, text=True, check=True
    ).stdout
    return [RAIZ / linha for linha in saida.splitlines() if linha]


class ArvoreDeEstruturaTest(unittest.TestCase):
    """A arvore do README nomeia caminho por caminho o que existe no repositorio.

    E o mapa que alguem le antes de abrir qualquer arquivo, entao um caminho errado nele
    custa mais que um comentario errado: manda a pessoa procurar o que nao ha.
    """

    def _caminhos_da_arvore(self) -> list[str]:
        texto = (RAIZ / "README.md").read_text(encoding="utf-8")
        bloco = re.search(r"## Estrutura\n\n```\n(.*?)\n```", texto, re.S)
        self.assertIsNotNone(bloco, "o bloco '## Estrutura' sumiu do README")
        pilha: list[str] = []
        caminhos: list[str] = []
        for linha in bloco.group(1).splitlines():
            achado = re.match(r"^([│ ]*)(?:├── |└── )(\S+)", linha)
            if not achado:
                continue
            profundidade = len(achado.group(1)) // 4
            pilha = pilha[:profundidade] + [achado.group(2).rstrip("/")]
            caminhos.append("/".join(pilha))
        return caminhos

    def test_todo_caminho_da_arvore_existe(self):
        caminhos = self._caminhos_da_arvore()
        self.assertGreater(len(caminhos), 80, "a arvore encolheu — o parser provavelmente quebrou")
        ausentes = sorted(c for c in caminhos if not (RAIZ / c).exists())
        self.assertEqual(
            ausentes, [],
            "a arvore do README nomeia caminhos que nao existem:\n  " + "\n  ".join(ausentes),
        )


class LinksRelativosTest(unittest.TestCase):
    """Um link quebrado em Markdown nao falha em lugar nenhum: ele so nao abre.

    Confere so os RELATIVOS. URL externa nao entra: conferi-la exigiria rede, e a suite e
    offline por invariante do repositorio.
    """

    def test_todo_link_relativo_resolve(self):
        quebrados = []
        for arquivo in _versionados("*.md"):
            base = arquivo.parent
            texto = arquivo.read_text(encoding="utf-8")
            for alvo in re.findall(r"\]\(([^)\s]+)\)", texto):
                alvo = alvo.split("#")[0]
                if not alvo or alvo.startswith(("http://", "https://", "mailto:", "s3://")):
                    continue
                if not (base / alvo).exists():
                    quebrados.append(f"{arquivo.relative_to(RAIZ)} -> {alvo}")
        self.assertEqual(
            sorted(quebrados), [],
            "links relativos que nao resolvem:\n  " + "\n  ".join(sorted(quebrados)),
        )


class SemLixoVersionadoTest(unittest.TestCase):
    """Um log de execucao commitado por engano nao atrapalha nada, e por isso fica anos.

    `platform/logs/dbt.log` ficou desde o primeiro commit: 16 linhas de uma invocacao
    abortada do dbt, referenciada em lugar nenhum. O dbt escreve `logs/` RELATIVO ao cwd,
    nao ao `--project-dir`, entao rodar dbt de dentro de `platform/` recria o diretorio.
    """

    def test_nenhum_log_nem_artefato_de_execucao_esta_versionado(self):
        proibidos = []
        for arquivo in _versionados("*"):
            relativo = arquivo.relative_to(RAIZ).as_posix()
            if re.search(r"(^|/)(logs?/|.*\.log$|\.duckdb$|__pycache__/)", relativo):
                proibidos.append(relativo)
        self.assertEqual(
            sorted(proibidos), [],
            "artefatos de execucao versionados:\n  " + "\n  ".join(sorted(proibidos)),
        )


class MakefileSeExplicaTest(unittest.TestCase):
    """`make help` e a unica interface do repositorio. Alvo fora dele nao existe na
    pratica, e script sem alvo e um arquivo que ninguem sabe como rodar.

    Achado na revisao de 2026-09-01: cinco alvos reais fora do help (`logs`, `stream-logs`,
    `orders-oltp-init`, `orders-projection-init`, `iceberg-metadata`) e quatro scripts
    `derive_*` sem alvo nenhum — justamente os que derivam os seeds VERSIONADOS, ou seja,
    a procedencia de quatro CSVs so estava no docstring de um arquivo que o README nao
    dizia como executar.
    """

    def _makefile(self) -> str:
        return (RAIZ / "Makefile").read_text(encoding="utf-8")

    def test_todo_alvo_aparece_no_make_help(self):
        texto = self._makefile()
        alvos = set(re.findall(r"^([a-z][a-z0-9_-]*):", texto, re.M)) - {"help"}
        bloco = re.search(r"^help:\n(.*?)(?=\n[^\t\n])", texto, re.S | re.M)
        self.assertIsNotNone(bloco, "o alvo `help` sumiu do Makefile")
        citados = set(re.findall(r'@echo "  ([a-z][a-z0-9_-]*)', bloco.group(1)))
        ausentes = sorted(alvos - citados)
        self.assertEqual(
            ausentes, [],
            "alvos que existem e o `make help` nao cita:\n  " + "\n  ".join(ausentes),
        )

    def test_todo_script_tem_alvo_no_makefile(self):
        texto = self._makefile()
        orfaos = sorted(
            arquivo.name for arquivo in _versionados("scripts/*")
            if arquivo.name not in texto
        )
        self.assertEqual(
            orfaos, [],
            "scripts sem alvo no Makefile — sem forma documentada de rodar:\n  "
            + "\n  ".join(orfaos),
        )

    def test_toda_variavel_do_makefile_e_usada(self):
        """Variavel definida e nunca referenciada e configuracao que promete um efeito e
        nao tem nenhum — pior que ausente, porque parece existir."""
        texto = self._makefile()
        mortas = sorted(
            nome for nome in set(re.findall(r"^([A-Z][A-Z0-9_]*)\s*[?:]?=", texto, re.M))
            if f"$({nome})" not in texto
        )
        self.assertEqual(mortas, [], "variaveis do Makefile nunca referenciadas: " + ", ".join(mortas))


if __name__ == "__main__":
    unittest.main()
