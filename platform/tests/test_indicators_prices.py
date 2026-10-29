"""Os indicadores do painel de precos, conferidos sem Postgres e sem rede.

IRMAO DE `test_dashboard_indicators.py`, NAO UMA COPIA REDUZIDA. As mesmas propriedades
importam aqui — grao/tipo/armadilha declarados, parametro sempre ligado, o painel so le
MART — mas a checagem BIDIRECIONAL de "todo mart do disco e usado" nao se aplica: este
painel e DELIBERADAMENTE exclusivo (2 dos 9 marts do warehouse), e o teste dessa
propriedade seria provar o oposto do que este arquivo existe para provar. Em vez disso,
`test_o_painel_usa_exatamente_os_dois_marts_declarados` fixa o escopo pela positiva.

Roda dentro de `make test`/`platform-test` (offline, por invariante do repositorio) porque
mora em `platform/tests/`, no mesmo venv que ja serve o painel de operacoes.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DASHBOARD = os.path.join(RAIZ, "streamlit")
sys.path.insert(0, DASHBOARD)

import price_contract as C  # noqa: E402
import price_indicators as PI  # noqa: E402

MART_DIR = os.path.join(RAIZ, "platform", "dbt", "models", "warehouse", "mart")
MARTS_ESPERADOS = {"MART_PRICE_EVOLUTION", "MART_ASSORTMENT_DAILY"}


class EstruturaTest(unittest.TestCase):
    def test_todo_indicador_declara_os_campos_de_conferencia(self):
        for indicador in PI.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertTrue(indicador.titulo.strip())
                self.assertTrue(indicador.pergunta.strip().endswith("?"),
                                "a pergunta tem de ser uma pergunta")
                self.assertTrue(indicador.grao.strip())
                self.assertTrue(indicador.tipo.strip())
                self.assertTrue(indicador.marts)
                self.assertTrue(indicador.sql.strip())

    def test_as_chaves_sao_unicas(self):
        chaves = [i.chave for i in PI.INDICADORES]
        self.assertEqual(len(chaves), len(set(chaves)))

    def test_todo_indicador_tem_ao_menos_uma_armadilha_declarada(self):
        for indicador in PI.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertTrue(indicador.armadilhas,
                                f"{indicador.chave} nao declara armadilha nenhuma")
                for armadilha in indicador.armadilhas:
                    self.assertGreater(
                        len(armadilha.strip()), 60,
                        f"{indicador.chave}: armadilha curta demais para explicar um modo "
                        f"de falha — {armadilha!r}",
                    )

    def test_os_marts_citados_existem_como_modelo_no_disco(self):
        no_disco = {
            os.path.splitext(arquivo)[0].upper()
            for arquivo in os.listdir(MART_DIR) if arquivo.endswith(".sql")
        }
        self.assertTrue(no_disco)
        for indicador in PI.INDICADORES:
            for mart in indicador.marts:
                self.assertIn(mart, no_disco, f"{indicador.chave} cita {mart}, que nao existe")

    def test_o_painel_usa_exatamente_os_dois_marts_declarados(self):
        """A propriedade positiva no lugar da bidirecional de `test_dashboard_indicators.py`:
        este painel e exclusivo por desenho, entao o escopo certo NAO e "todo mart usado" —
        e "so estes dois, e nenhum outro entrou por acidente"."""
        usados = {m for i in PI.INDICADORES for m in i.marts}
        self.assertEqual(usados, MARTS_ESPERADOS)


class ParametroLigadoTest(unittest.TestCase):
    def test_o_filtro_de_data_e_ligado_e_nunca_concatenado(self):
        for indicador in PI.INDICADORES:
            if not indicador.datado:
                continue
            with self.subTest(indicador.chave):
                self.assertIn("%(inicio)s", indicador.sql)
                self.assertIn("%(fim)s", indicador.sql)

    def test_o_filtro_de_armazem_e_ligado_e_nunca_concatenado(self):
        for indicador in PI.INDICADORES:
            if not indicador.datado:
                continue
            with self.subTest(indicador.chave):
                self.assertIn("%(armazens)s", indicador.sql)

    def test_o_filtro_de_produto_e_ligado_quando_usado(self):
        """`ilike %(termo)s`: o coringa `%` mora no VALOR ligado (montado em price_app.py),
        nunca dentro do texto SQL — sem isso, um nome de produto digitado pelo usuario
        entraria direto na consulta."""
        usam_termo = [i for i in PI.INDICADORES if "%(termo)s" in i.sql]
        self.assertGreaterEqual(len(usam_termo), 2, "esperava produtos_buscaveis + historico")
        for indicador in usam_termo:
            with self.subTest(indicador.chave):
                self.assertIn(PI.FILTRO_PRODUTO, indicador.sql)

    def test_nenhuma_consulta_usa_format_ou_f_string_de_valor(self):
        for indicador in PI.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertNotIn("{", indicador.sql)
                self.assertNotIn("}", indicador.sql)

    def test_o_painel_le_somente_o_schema_mart(self):
        for indicador in PI.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertNotIn('"GOLD"', indicador.sql)
                self.assertNotIn('"STAGE"', indicador.sql)
                self.assertIn(f"{PI.MART}.", indicador.sql)

    def test_nenhuma_consulta_escreve(self):
        proibido = ("insert ", "update ", "delete ", "merge ", "create ", "drop ",
                    "alter ", "truncate ", "grant ", "copy into")
        for indicador in PI.INDICADORES:
            baixo = indicador.sql.lower()
            for verbo in proibido:
                with self.subTest(f"{indicador.chave}:{verbo.strip()}"):
                    self.assertNotIn(verbo, baixo)


class ContractEmSincroniaTest(unittest.TestCase):
    def test_o_contract_no_disco_e_BYTE_A_BYTE_o_que_o_gerador_produz(self):
        caminho = os.path.join(DASHBOARD, "PRICE_CONTRACT.md")
        self.assertTrue(os.path.exists(caminho),
                        "PRICE_CONTRACT.md nao existe — rode `make price-dashboard-contract`")
        with open(caminho, encoding="utf-8") as arquivo:
            no_disco = arquivo.read()
        self.assertEqual(
            no_disco, C.render(),
            "PRICE_CONTRACT.md esta fora de sincronia com price_indicators.py. "
            "Rode `make price-dashboard-contract`.",
        )

    def test_o_cabecalho_carrega_o_sha256_da_origem_e_nenhuma_data(self):
        cabecalho = C.render().splitlines()[6]
        self.assertIn(C.origem_sha256(), cabecalho)
        self.assertNotRegex(C.render(), r"Gerado por `make price-dashboard-contract` em \d{4}-")

    def test_o_contract_traz_o_sql_de_todo_indicador(self):
        gerado = C.render()
        for indicador in PI.INDICADORES:
            with self.subTest(indicador.chave):
                self.assertIn(indicador.sql.strip(), gerado)
                self.assertIn(indicador.titulo, gerado)

    def test_o_contract_traz_toda_armadilha(self):
        gerado = C.render()
        for indicador in PI.INDICADORES:
            for armadilha in indicador.armadilhas:
                with self.subTest(indicador.chave):
                    self.assertIn(armadilha, gerado)

    def test_o_gerador_nao_importa_nada_que_conecte(self):
        with open(os.path.join(DASHBOARD, "price_contract.py"), encoding="utf-8") as arquivo:
            arvore = ast.parse(arquivo.read())
        importados = set()
        for no in ast.walk(arvore):
            if isinstance(no, ast.Import):
                importados.update(alias.name.split(".")[0] for alias in no.names)
            elif isinstance(no, ast.ImportFrom) and no.module:
                importados.add(no.module.split(".")[0])
        permitidos = {"os", "sys", "hashlib", "price_indicators", "__future__"}
        self.assertEqual(
            importados - permitidos, set(),
            "o gerador do PRICE_CONTRACT so pode importar stdlib e price_indicators",
        )


if __name__ == "__main__":
    unittest.main()
