"""A EVIDENCIA, conferida sem Snowflake e sem credencial.

O relatorio existe porque a conta e um trial e a metade Snowflake do projeto nao e
reproduzivel offline. Isso o torna o unico artefato do repositorio que nao pode ser
regenerado depois — e por isso o modo de falha que importa nao e "quebrou", e "escreveu
algo plausivel e errado". Os testes abaixo miram nisso: que numero nenhum seja escrito a
mao, que isolamento quebrado apareca em vez de sumir, e que uma amostra que falhou nao se
disfarce de amostra vazia.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.snowflake_evidence import AMOSTRAS, render  # noqa: E402
from retail_platform.snowflake_load import ROLES  # noqa: E402


def _dados(objetos=None, isolamento=None, amostras=None, papeis=None):
    return {
        "identidade": {
            "conta": "ACC123",
            "usuario": "ALGUEM",
            "papel": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH",
            "capturado_em": "2026-08-27 13:00:00 +00:00",
            "database": "RETAIL",
        },
        "objetos": objetos if objetos is not None else [
            ("STAGE", "STG_CUSTOMER", "RETAIL_LOADER", 20000),
            ("GOLD", "DIM_CUSTOMER", "RETAIL_TRANSFORMER", 20000),
            ("MART", "MART_CUSTOMER_BASE", "RETAIL_TRANSFORMER", 20000),
        ],
        "amostras": amostras if amostras is not None else {
            "MART_CUSTOMER_BASE": (["CUSTOMER_ID"], [("cust_bcn1_000000",)]),
        },
        "papeis_em_execucao": papeis if papeis is not None else (
            ["ROLE_NAME", "QUERY_TYPE", "QUERIES", "ULTIMA"],
            [
                ("RETAIL_LOADER", "COPY", 135, "2026-09-01 07:01"),
                ("RETAIL_TRANSFORMER", "CREATE_TABLE_AS_SELECT", 346, "2026-09-01 07:02"),
                ("RETAIL_READER", "SELECT", 732, "2026-09-01 06:43"),
            ],
        ),
        "isolamento": isolamento or [],
    }


class PapeisEmExecucaoTest(unittest.TestCase):
    """A matriz de isolamento prova o que cada papel PODE ler; esta secao prova o que cada
    um FEZ. Foi a distincao que custou quatro defeitos na Fase 2 — os tres papeis existiam,
    verificados, e nenhuma execucao passava por eles.

    Substitui a lista de prints que vivia em docs/warehouse-evidence/PRINTS.md: cinco dos
    seis itens daquela lista ja eram cobertos por esta pagina, e o sexto era este.
    """

    def test_o_verbo_de_cada_papel_e_publicado(self):
        texto = render(_dados())
        self.assertIn("CREATE_TABLE_AS_SELECT", texto)
        self.assertIn("`RETAIL_READER`", texto)
        self.assertIn("732", texto)

    def test_historico_vazio_declara_a_ausencia_em_vez_de_tabela_vazia(self):
        """Uma tabela sem linhas se le como "os papeis nao fizeram nada", que e
        indistinguivel de "nao ha separacao de papeis". Sao coisas diferentes: o
        query_history do Snowflake nao guarda mais de sete dias, e uma semana parada
        apaga a evidencia sem apagar a propriedade."""
        texto = render(_dados(papeis=(["ROLE_NAME"], [])))
        self.assertIn("No `RETAIL%` role executed", texto)
        self.assertIn("seven days", texto)

    def test_erro_ao_ler_o_historico_aparece_como_erro(self):
        """Mesma regra das amostras: falha que se disfarca de vazio e a unica coisa pior
        que falha."""
        texto = render(_dados(papeis=(["erro"], [("Insufficient privileges",)])))
        self.assertIn("Could not read the history", texto)
        self.assertIn("Insufficient privileges", texto)
        self.assertNotIn("No `RETAIL%` role executed", texto)


class RelatorioTest(unittest.TestCase):
    def test_a_identidade_da_conta_aparece(self):
        """Evidencia sem conta e sem data nao prova nada: ela existe justamente para
        sobreviver ao destino que descreve."""
        texto = render(_dados())
        for esperado in ("ACC123", "ALGUEM", "COMPUTE_WH", "2026-08-27"):
            self.assertIn(esperado, texto)

    def test_a_posse_de_cada_objeto_e_publicada(self):
        """A posse E a prova de que o papel foi vestido — sem ela o relatorio nao
        distingue este estado de tudo rodando como administrador."""
        texto = render(_dados())
        self.assertIn("RETAIL_LOADER", texto)
        self.assertIn("RETAIL_TRANSFORMER", texto)

    def test_o_total_e_somado_e_nao_escrito(self):
        texto = render(_dados(objetos=[
            ("STAGE", "A", "RETAIL_LOADER", 7),
            ("GOLD", "B", "RETAIL_TRANSFORMER", 35),
        ]))
        self.assertIn("42 rows", texto)

    def test_tabela_sem_contagem_nao_estoura_nem_vira_zero_silencioso(self):
        texto = render(_dados(objetos=[("GOLD", "V", "RETAIL_TRANSFORMER", None)]))
        self.assertIn("`V`", texto)

    def test_isolamento_quebrado_aparece_em_destaque(self):
        """O pior desfecho possivel seria um relatorio bonito sobre um controle furado."""
        texto = render(_dados(isolamento=["RETAIL_READER: NAO deveria ler GOLD e leu"]))
        self.assertIn("does NOT check out", texto)
        self.assertIn("RETAIL_READER", texto)

    def test_isolamento_intacto_e_declarado_como_tal(self):
        texto = render(_dados())
        self.assertIn("checks out completely", texto)
        self.assertNotIn("does NOT check out", texto)

    def test_a_matriz_publicada_cobre_os_tres_papeis(self):
        texto = render(_dados())
        for papel in ROLES:
            self.assertIn(papel, texto)

    def test_amostra_que_falhou_nao_se_disfarca_de_amostra_vazia(self):
        """Uma consulta de amostra que quebrou tem de aparecer como erro. Silenciada, o
        leitor concluiria que o mart esta vazio."""
        texto = render(_dados(amostras={
            "MART_X": (["erro"], [("SQL compilation error: invalid identifier",),]),
        }))
        self.assertIn("invalid identifier", texto)


# Os marts VAO CRESCER, e uma lista cravada aqui envelhece calada: acrescentar um mart sem
# acrescentar a amostra deixaria uma tabela sem nenhum registro depois que a conta expirar,
# e nada reprovaria. Derivar do disco faz o teste falhar no commit que criou o mart, que e
# quando o custo de lembrar e zero. Foi assim que os tres marts de Orders foram pegos.
MART_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dbt", "models", "warehouse", "mart",
)


class AmostrasTest(unittest.TestCase):
    def test_ha_amostra_para_cada_mart(self):
        no_disco = {
            os.path.splitext(nome)[0].upper()
            for nome in os.listdir(MART_DIR) if nome.endswith(".sql")
        }
        self.assertTrue(no_disco, f"nenhum mart encontrado em {MART_DIR}")
        # FACT_INGESTION_RUN e a UNICA excecao deliberada: e um fato de GOLD, nao um
        # MART, que entrou via CR-005 para tornar latencia/throughput consultaveis sem
        # coletor novo (AI_ENGINEERING_CONSTRAINTS.md secao 17). Nomeada aqui para que a
        # excecao seja assercao, nao lacuna silenciosa que um AMOSTRAS futuro esconderia.
        fora_do_mart = {"FACT_INGESTION_RUN"}
        self.assertTrue(fora_do_mart <= set(AMOSTRAS), "excecao esperada desapareceu")
        self.assertEqual(set(AMOSTRAS) - fora_do_mart, no_disco)

    def test_toda_amostra_e_limitada_e_parametrizada_pelo_database(self):
        """Sem limit, a 'amostra' viraria um extrato; sem {db}, o relatorio so serviria
        para o database chamado RETAIL — e o destino e trocavel de proposito."""
        for nome, sql in AMOSTRAS.items():
            self.assertIn("limit", sql, f"{nome} sem limit")
            self.assertIn("{db}", sql, f"{nome} com database cravado")


if __name__ == "__main__":
    unittest.main()
