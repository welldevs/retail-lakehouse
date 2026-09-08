"""Os indicadores do painel, conferidos sem Snowflake e sem rede.

MORA EM platform/tests/ DE PROPOSITO, e nao em streamlit/. O painel compartilha o venv da
plataforma (as dependencias de UI sao um `optional-dependencies` do mesmo pyproject), entao
`make test` ja o alcanca — e o que importa e que estas checagens rodem SEMPRE, junto do resto,
e nao num alvo separado que alguem lembra de chamar.

O QUE ESTA SUITE PROTEGE, e o que ela deliberadamente nao tenta.

Nao tenta conferir NUMERO: para isso e preciso o Snowflake, e a suite e offline por
invariante do repositorio. O smoke test que executa o app de verdade e
`make dashboard-check`, que exige conta viva.

O que ela protege e a propriedade que o painel inteiro apoia: **o CONTRACT.md e derivado de
indicators.py, entao nao pode divergir dele**. Se alguem ajustar um SQL e nao regenerar o
documento, a conferencia continua "passando" — porque ninguem le um SQL e um texto lado a lado
procurando desacordo. Este teste transforma a regeneracao em obrigacao verificada, e e o
analogo direto do DDL do STAGE ser derivado do recorte em vez de escrito a mao.

E protege a fronteira do parametro: toda consulta com eixo de data tem de LIGAR o filtro
(`%(inicio)s`), nunca monta-lo por concatenacao. O seletor da interface e entrada de usuario.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DASHBOARD = os.path.join(RAIZ, "streamlit")
sys.path.insert(0, DASHBOARD)

import contract as C  # noqa: E402
import indicators as I  # noqa: E402

MART_DIR = os.path.join(RAIZ, "platform", "dbt", "models", "warehouse", "mart")


class EstruturaTest(unittest.TestCase):
    def test_todo_indicador_declara_os_campos_de_conferencia(self):
        """Um indicador sem grao declarado nao e conferivel: e o grao que diz se uma soma e
        legitima. Sem tipo, ninguem sabe se pode ler o numero como medida do mundo."""
        for indicador in I.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertTrue(indicador.titulo.strip())
                self.assertTrue(indicador.pergunta.strip().endswith("?"),
                                "a pergunta tem de ser uma pergunta")
                self.assertTrue(indicador.grao.strip())
                self.assertTrue(indicador.tipo.strip())
                self.assertTrue(indicador.marts)
                self.assertTrue(indicador.sql.strip())

    def test_as_chaves_sao_unicas(self):
        chaves = [i.chave for i in I.INDICADORES]
        self.assertEqual(len(chaves), len(set(chaves)))

    def test_todo_indicador_tem_ao_menos_uma_armadilha_declarada(self):
        """Nao e burocracia. Cada um destes indicadores agrega, e agregacao e onde a medida
        obvia produz numero plausivel e errado — percentil que nao soma, contagem que nao e
        aditiva, denominador trocado. Um indicador sem nenhuma ressalva provavelmente nao foi
        pensado como quem vai reconstrui-lo em outra ferramenta."""
        for indicador in I.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertTrue(indicador.armadilhas,
                                f"{indicador.chave} nao declara armadilha nenhuma")
                # Nao basta a tupla existir. Uma entrada vazia — ou um placeholder de tres
                # palavras — passava por `assertTrue` e nao avisa ninguem de nada. Descoberto
                # ao provar que este proprio teste sabe reprovar: a injecao trocou a
                # armadilha por string vazia e SO o teste de sincronia do CONTRACT pegou.
                for armadilha in indicador.armadilhas:
                    self.assertGreater(
                        len(armadilha.strip()), 60,
                        f"{indicador.chave}: armadilha curta demais para explicar um modo "
                        f"de falha — {armadilha!r}",
                    )

    def test_os_marts_citados_existem_como_modelo_no_disco(self):
        """Um nome errado aqui so falha em execucao, contra a conta viva — e o CONTRACT ja
        teria sido publicado com a fonte errada."""
        no_disco = {
            os.path.splitext(arquivo)[0].upper()
            for arquivo in os.listdir(MART_DIR) if arquivo.endswith(".sql")
        }
        self.assertTrue(no_disco)
        for indicador in I.INDICADORES:
            for mart in indicador.marts:
                self.assertIn(mart, no_disco, f"{indicador.chave} cita {mart}, que nao existe")

    def test_todo_mart_do_disco_e_usado_por_algum_indicador(self):
        """O inverso, e e o que pega o mart NOVO: quem cria um mart e nao o expoe deixa uma
        tabela sem nenhum consumidor, que e como um mart morre em silencio."""
        usados = {m for i in I.INDICADORES for m in i.marts}
        for arquivo in os.listdir(MART_DIR):
            if not arquivo.endswith(".sql"):
                continue
            nome = os.path.splitext(arquivo)[0].upper()
            self.assertIn(nome, usados, f"{nome} nao aparece em nenhum indicador")


class ParametroLigadoTest(unittest.TestCase):
    def test_o_filtro_de_data_e_ligado_e_nunca_concatenado(self):
        for indicador in I.INDICADORES:
            if not indicador.datado:
                continue
            with self.subTest(indicador.chave):
                self.assertIn("%(inicio)s", indicador.sql)
                self.assertIn("%(fim)s", indicador.sql)

    def test_o_filtro_de_armazem_e_ligado_e_nunca_concatenado(self):
        """O multiselect da interface e entrada de usuario. `split(%(armazens)s, ',')` liga o
        valor; montar um `in (...)` por concatenacao nao."""
        for indicador in I.INDICADORES:
            if not indicador.datado:
                continue
            with self.subTest(indicador.chave):
                self.assertIn("%(armazens)s", indicador.sql)

    def test_nenhuma_consulta_usa_format_ou_f_string_de_valor(self):
        """As unicas interpolacoes permitidas sao as constantes do modulo (DB, filtros). Um
        `{`/`}` sobrando numa consulta significa que alguem tentou montar valor na string."""
        for indicador in I.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertNotIn("{", indicador.sql)
                self.assertNotIn("}", indicador.sql)

    def test_o_painel_le_somente_o_schema_mart(self):
        """A fronteira do papel `RETAIL_READER`, afirmada tambem no codigo: uma consulta que
        cite GOLD ou STAGE seria recusada pelo motor, mas falharia em execucao e contra a
        conta viva. Aqui falha antes."""
        for indicador in I.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertNotIn(".GOLD.", indicador.sql.upper())
                self.assertNotIn(".STAGE.", indicador.sql.upper())
                self.assertIn(f"{I.DB}.MART.", indicador.sql)

    def test_nenhuma_consulta_escreve(self):
        proibido = ("insert ", "update ", "delete ", "merge ", "create ", "drop ",
                    "alter ", "truncate ", "grant ", "copy into")
        for indicador in I.INDICADORES:
            baixo = indicador.sql.lower()
            for verbo in proibido:
                with self.subTest(f"{indicador.chave}:{verbo.strip()}"):
                    self.assertNotIn(verbo, baixo)


class ContractEmSincroniaTest(unittest.TestCase):
    """O CONTRACT.md e derivado de indicators.py. Este teste e o que impede a divergencia.

    Sem ele, ajustar um SQL e esquecer de rodar `make dashboard-contract` deixa o documento de
    conferencia descrevendo uma consulta que nao existe mais — e a conferencia continua
    "passando", porque ninguem compara um SQL com um texto procurando desacordo. E o mesmo
    motivo pelo qual o DDL do STAGE e derivado do recorte.
    """

    def test_o_contract_no_disco_e_BYTE_A_BYTE_o_que_o_gerador_produz(self):
        """Comparacao EXATA, sem nenhuma linha isenta.

        Ate a revisao de 2026-09-01 o cabecalho trazia a data da geracao, e este teste
        precisava descartar essa linha antes de comparar. Uma isencao dentro do proprio
        teste de sincronia e uma faixa cega: qualquer coisa que caisse naquela linha
        deixava de ser conferida. O cabecalho passou a trazer o sha256 de indicators.py —
        funcao da origem, e nao do relogio — entao a comparacao pode ser total, e
        `make dashboard-contract` sobre um contrato em dia nao muda um byte.
        """
        caminho = os.path.join(DASHBOARD, "CONTRACT.md")
        self.assertTrue(os.path.exists(caminho), "CONTRACT.md nao existe — rode `make dashboard-contract`")
        with open(caminho, encoding="utf-8") as arquivo:
            no_disco = arquivo.read()

        self.assertEqual(
            no_disco, C.render(),
            "CONTRACT.md esta fora de sincronia com indicators.py. "
            "Rode `make dashboard-contract`.",
        )

    def test_o_cabecalho_carrega_o_sha256_da_origem_e_nenhuma_data(self):
        """Se o carimbo voltar a ser um relogio, o arquivo gerado fica eternamente sujo no
        git e o teste acima tem de voltar a isentar uma linha. Guardado aqui para que a
        regressao apareca como falha, e nao como ruido no diff."""
        cabecalho = C.render().splitlines()[8]
        self.assertIn(C.origem_sha256(), cabecalho)
        self.assertNotRegex(C.render(), r"Gerado por `make dashboard-contract` em \d{4}-")

    def test_o_contract_traz_o_sql_de_todo_indicador(self):
        gerado = C.render()
        for indicador in I.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertIn(indicador.sql.strip(), gerado)
                self.assertIn(indicador.titulo, gerado)

    def test_o_contract_traz_toda_armadilha(self):
        """A armadilha e a razao do documento existir. Uma que ficasse fora do CONTRACT
        estaria escrita para ninguem."""
        gerado = C.render()
        for indicador in I.INDICADORES:
            for armadilha in indicador.armadilhas:
                with self.subTest(indicador.chave):
                    self.assertIn(armadilha, gerado)

    def test_o_contract_declara_o_que_o_painel_nao_exibe(self):
        gerado = C.render()
        self.assertIn("What the panel does NOT show", gerado)
        for titulo, motivo, gatilho in I.FORA_DE_ALCANCE:
            with self.subTest(titulo):
                self.assertIn(titulo, gerado)
                self.assertIn(gatilho, gerado)

    def test_o_gerador_nao_importa_nada_que_conecte(self):
        """O CONTRACT tem de ser revisavel sem credencial e sem rede — mesma propriedade de
        `make warehouse-ddl`.

        A checagem e por AST e nao por busca de texto: a palavra "snowflake" aparece
        legitimamente no CORPO do documento gerado, que descreve a autenticacao. O que nao
        pode existir e o IMPORT — e a diferenca entre falar sobre a conexao e abrir uma.
        """
        with open(os.path.join(DASHBOARD, "contract.py"), encoding="utf-8") as arquivo:
            arvore = ast.parse(arquivo.read())
        importados = set()
        for no in ast.walk(arvore):
            if isinstance(no, ast.Import):
                importados.update(alias.name.split(".")[0] for alias in no.names)
            elif isinstance(no, ast.ImportFrom) and no.module:
                importados.add(no.module.split(".")[0])
        permitidos = {"os", "sys", "hashlib", "indicators", "__future__"}
        self.assertEqual(
            importados - permitidos, set(),
            "o gerador do CONTRACT so pode importar stdlib e indicators",
        )


class ContagemNaProsaTest(unittest.TestCase):
    """A documentacao diz quantos indicadores o painel tem. Esse numero e copiado a mao, e
    ja errou tres vezes: dizia 16 quando eram 18, 19 consultas quando eram 21 — e, na Fase 7,
    o README dizia 16 quando eram 22, porque este teste so vigiava o ARCHITECTURE.

    A TERCEIRA VEZ E A LICAO: guardar UM arquivo nao guarda a frase, guarda o arquivo. A
    mesma contagem morava em dois documentos e so um tinha teste — entao o outro apodreceu
    exatamente como se teste nenhum existisse. Agora os dois entram pela mesma lista, e
    acrescentar um terceiro documento com a frase e uma linha aqui.

    Nao vale automatizar toda contagem escrita em prosa — na maioria delas o remedio foi
    parar de escrever o numero. Esta fica porque a frase perde o sentido sem ele, e porque
    acrescentar um indicador e justamente o momento em que ninguem lembra da documentacao.
    """

    DOCUMENTOS = ("ARCHITECTURE.md", "README.md")

    def _texto(self, nome: str) -> str:
        raiz = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(raiz, nome), encoding="utf-8") as arquivo:
            return arquivo.read()

    def test_o_numero_de_indicadores_confere_em_todo_documento_que_o_cita(self):
        grupos = len({indicador.grupo for indicador in I.INDICADORES})
        frase = f"{len(I.INDICADORES)}\nindicators in {grupos} groups"
        frase_linha = f"{len(I.INDICADORES)} indicators in {grupos} groups"
        for nome in self.DOCUMENTOS:
            with self.subTest(nome):
                texto = self._texto(nome)
                self.assertTrue(
                    frase in texto or frase_linha in texto,
                    f"{nome} descreve outro numero de indicadores/grupos",
                )

    def test_o_numero_de_consultas_no_architecture_confere(self):
        total = len(I.INDICADORES) + 3  # FRESCOR, JANELA, ARMAZENS
        self.assertIn(f"the {total} queries", self._texto("ARCHITECTURE.md"))


class ForaDeAlcanceTest(unittest.TestCase):
    def test_todo_item_declara_motivo_E_gatilho(self):
        """Ausencia sem gatilho e desculpa; com gatilho e decisao. E a mesma disciplina da
        tabela de tecnologias nao adotadas do ARCHITECTURE.md."""
        self.assertTrue(I.FORA_DE_ALCANCE)
        for titulo, motivo, gatilho in I.FORA_DE_ALCANCE:
            with self.subTest(titulo):
                self.assertTrue(titulo.strip())
                self.assertGreater(len(motivo.strip()), 40)
                self.assertGreater(len(gatilho.strip()), 20)

    def test_a_lacuna_de_cliente_x_pedido_esta_declarada(self):
        """E a mais acionavel da lista e a que mais gente vai pedir: recompra, LTV, coorte.
        Nenhum mart junta cliente com pedido, e isso precisa estar escrito onde quem for
        montar o Power BI leia ANTES de prometer o indicador."""
        texto = " ".join(m for _, m, _ in I.FORA_DE_ALCANCE)
        self.assertIn("NO MART JOINS CUSTOMER WITH ORDER", texto)


if __name__ == "__main__":
    unittest.main()
