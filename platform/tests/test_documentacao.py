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
        bloco = re.search(r"## Structure\n\n```\n(.*?)\n```", texto, re.S)
        self.assertIsNotNone(bloco, "o bloco '## Structure' sumiu do README")
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

        # data/ e seus filhos sao a unica excecao: a propria arvore os marca "outside
        # version control" (.gitignore linha 36) porque sao scratch de extracao, nunca
        # codigo-fonte. Um checkout limpo nunca os tem antes de rodar o pipeline — a CI
        # e quem expos isso, passando so localmente onde o pipeline ja rodou alguma vez.
        fora_de_versionamento = {c for c in caminhos if c == "data" or c.startswith("data/")}
        normais = [c for c in caminhos if c not in fora_de_versionamento]

        ausentes = sorted(c for c in normais if not (RAIZ / c).exists())
        self.assertEqual(
            ausentes, [],
            "a arvore do README nomeia caminhos que nao existem:\n  " + "\n  ".join(ausentes),
        )

        nao_ignorados = sorted(
            c for c in fora_de_versionamento
            if subprocess.run(["git", "check-ignore", "-q", c + "/"], cwd=RAIZ).returncode != 0
        )
        self.assertEqual(
            nao_ignorados, [],
            "a arvore marca estes caminhos como fora de versionamento, "
            "mas o .gitignore nao os cobre:\n  " + "\n  ".join(nao_ignorados),
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


class ReferenciaPorNomeDeSecaoTest(unittest.TestCase):
    """`[DOC.md § "Titulo"](DOC.md)` tem de apontar para um titulo que existe la.

    POR QUE ISTO NAO ERA COBERTO. `LinksRelativosTest` confere que o ARQUIVO existe, e todas
    estas referencias apontam para arquivos que existem — entao ela passava. O que ninguem
    conferia era o RÓTULO: a secao nomeada podia ter sido renomeada, ou ter mudado de
    arquivo, e o link continuaria "valido" levando o leitor ao documento errado.

    Isso deixou de ser hipotetico na Fase 7, quando 17 secoes de narrativa mudaram de
    ARCHITECTURE.md para DECISIONS.md. Cinco referencias precisaram ser redirecionadas — e
    duas outras pareceram quebradas ate eu descobrir que meu proprio verificador so olhava
    titulos de nivel 2. O teste olha TODOS os niveis.

    A COMPARACAO E POR PREFIXO de proposito: as referencias abreviam ("Fase 2" aponta para
    "Fase 2: camada analitica no Snowflake"). Exigir o titulo inteiro obrigaria a repetir
    frases longas dentro do texto corrido, e a abreviacao nao introduz ambiguidade — nao ha
    duas secoes cujo titulo comece igual.
    """

    PADRAO = re.compile(r'\[([A-Za-z_.\/-]*\.md) § "([^"]+)"\]')

    def _titulos(self, caminho: pathlib.Path) -> set[str]:
        try:
            texto = caminho.read_text(encoding="utf-8")
        except OSError:
            return set()
        return {
            linha.lstrip("#").strip()
            for linha in texto.split("\n")
            if re.match(r"^#{1,6}\s", linha)
        }

    def test_toda_secao_citada_por_nome_existe_no_documento_citado(self):
        problemas = []
        for caminho in _versionados("*.md"):
            try:
                texto = caminho.read_text(encoding="utf-8")
            except OSError:
                continue
            for alvo, titulo in self.PADRAO.findall(texto):
                destino = (caminho.parent / alvo).resolve()
                if not destino.exists():
                    # LinksRelativosTest ja cobre arquivo ausente; aqui so o rotulo.
                    continue
                titulos = self._titulos(destino)
                if not any(t == titulo or t.startswith(titulo) or titulo in t
                           for t in titulos):
                    problemas.append(
                        f"{caminho.relative_to(RAIZ)} cita "
                        f'{alvo} § "{titulo}", que nao existe la'
                    )
        self.assertEqual(problemas, [], "\n" + "\n".join(problemas))


class TetoDeLinhasTest(unittest.TestCase):
    """README e ARCHITECTURE nao podem voltar a crescer sem limite.

    O TAMANHO E, POR SI, UM QUESTIONAMENTO DE COMPLEXIDADE. Antes da Fase 7 o README tinha
    1.435 linhas e o ARCHITECTURE 2.475, e boa parte era a MESMA explicacao em dois lugares,
    envelhecendo em ritmos diferentes. A narrativa foi para DECISIONS.md e os dois cairam
    para menos da metade.

    O TETO NAO E UMA META ESTETICA, e o numero nao foi escolhido por gosto: e o tamanho de
    hoje com uma folga estreita. O efeito pretendido nao e forcar cortes — e fazer com que
    CRESCER seja uma decisao, e nao um acumulo. Quem precisar de mais espaco muda o numero
    aqui, e essa mudanca aparece no diff.

    ONDE O TEXTO DEVE IR quando o teto apertar: historia para DECISIONS.md, escopo futuro
    para BACKLOG.md, e detalhe de uma Source para o README dela.

    O TETO DO README SUBIU DE 700 PARA 760 em 2026-09-02, e a razao vai escrita porque o
    proprio teste diz que crescer tem de ser decisao e nao acumulo: entrou a secao "As quatorze
    perguntas do fechamento", um INDICE de uma linha por pergunta apontando para onde a
    evidencia mora. Ela nao duplica explicacao — e o oposto disso, e existe para que um leitor
    possa conferir se a documentacao sustenta o que afirma sem ler 3.000 linhas.
    """

    TETOS = {
        "README.md": 760,
        "ARCHITECTURE.md": 700,
    }

    def test_nenhum_documento_de_estado_passa_do_teto(self):
        for nome, teto in self.TETOS.items():
            with self.subTest(nome):
                linhas = len((RAIZ / nome).read_text(encoding="utf-8").splitlines())
                self.assertLessEqual(
                    linhas, teto,
                    f"{nome} tem {linhas} linhas (teto {teto}). Historia vai para "
                    f"DECISIONS.md, escopo futuro para BACKLOG.md.",
                )

    def test_os_quatro_documentos_de_raiz_existem_e_tem_papel_distinto(self):
        """A estrutura que o projeto declara: estado, historia, escopo, execucao."""
        for nome in ("README.md", "ARCHITECTURE.md", "DECISIONS.md", "BACKLOG.md"):
            with self.subTest(nome):
                self.assertTrue((RAIZ / nome).exists(), f"{nome} nao existe")


class AncorasResolvemTest(unittest.TestCase):
    """`](#secao)` e `](DOC.md#secao)` tem de apontar para um titulo que existe.

    POR QUE ISTO E SEPARADO DE `LinksRelativosTest`. Aquela confere que o ARQUIVO existe, e
    um link com ancora quebrada aponta para um arquivo que existe — entao ela passa, e o
    leitor cai no topo do documento sem saber que errou de lugar. Um indice inteiro pode
    apodrecer assim sem nenhum teste reclamar, e foi o risco concreto da Fase 7, quando 17
    secoes mudaram de arquivo e um indice novo nasceu com 11 ancoras.

    A REGRA DE SLUG E A DO GITHUB, e uma versao anterior deste verificador estava ERRADA:
    ela colapsava espacos repetidos, e o GitHub nao colapsa. Isso produziu cinco falsos
    positivos num documento correto — um verificador errado custa mais caro que verificador
    nenhum, porque ensina a ignorar o resultado.
    """

    @staticmethod
    def _slug(titulo: str) -> str:
        texto = titulo.lower()
        texto = re.sub(r"[^\w\s-]", "", texto, flags=re.UNICODE)
        return texto.strip().replace(" ", "-")

    def _titulos(self, caminho: pathlib.Path) -> set[str]:
        """Titulos MAIS ancoras HTML explicitas.

        `streamlit/CONTRACT.md` e gerado e usa `<a id="chave"></a>` antes de cada indicador,
        porque a chave do indicador e mais estavel que o titulo dele. Um verificador que so
        olhasse cabecalho reprovaria 22 ancoras corretas — e verificador que reprova o certo
        e pior que verificador nenhum, porque ensina a ignorar o resultado. Ja aconteceu
        neste projeto, com um slug que colapsava espacos repetidos e o GitHub nao colapsa.
        """
        try:
            texto = caminho.read_text(encoding="utf-8")
        except OSError:
            return set()
        cabecalhos = {self._slug(linha.lstrip("#").strip())
                      for linha in texto.split("\n") if re.match(r"^#{1,6}\s", linha)}
        explicitas = set(re.findall(r'<a\s+id="([^"]+)"', texto))
        return cabecalhos | explicitas

    def test_toda_ancora_aponta_para_um_titulo_existente(self):
        padrao = re.compile(r"\]\(([^)\s]*)#([^)\s]+)\)")
        problemas = []
        for caminho in _versionados("*.md"):
            try:
                texto = caminho.read_text(encoding="utf-8")
            except OSError:
                continue
            for alvo, ancora in padrao.findall(texto):
                if alvo.startswith(("http://", "https://")):
                    continue
                destino = caminho if not alvo else (caminho.parent / alvo).resolve()
                if not destino.exists():
                    continue  # arquivo ausente e assunto de LinksRelativosTest
                if ancora not in self._titulos(destino):
                    problemas.append(
                        f"{caminho.relative_to(RAIZ)} -> {alvo or caminho.name}#{ancora}")
        self.assertEqual(problemas, [], "\n" + "\n".join(problemas))


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
