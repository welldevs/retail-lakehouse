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


def _dados(objetos=None, isolamento=None, amostras=None):
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
        "isolamento": isolamento or [],
    }


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
        self.assertIn("42 linhas", texto)

    def test_tabela_sem_contagem_nao_estoura_nem_vira_zero_silencioso(self):
        texto = render(_dados(objetos=[("GOLD", "V", "RETAIL_TRANSFORMER", None)]))
        self.assertIn("`V`", texto)

    def test_isolamento_quebrado_aparece_em_destaque(self):
        """O pior desfecho possivel seria um relatorio bonito sobre um controle furado."""
        texto = render(_dados(isolamento=["RETAIL_READER: NAO deveria ler GOLD e leu"]))
        self.assertIn("NÃO confere", texto)
        self.assertIn("RETAIL_READER", texto)

    def test_isolamento_intacto_e_declarado_como_tal(self):
        texto = render(_dados())
        self.assertIn("confere inteira", texto)
        self.assertNotIn("NÃO confere", texto)

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
        self.assertEqual(set(AMOSTRAS), no_disco)

    def test_toda_amostra_e_limitada_e_parametrizada_pelo_database(self):
        """Sem limit, a 'amostra' viraria um extrato; sem {db}, o relatorio so serviria
        para o database chamado RETAIL — e o destino e trocavel de proposito."""
        for nome, sql in AMOSTRAS.items():
            self.assertIn("limit", sql, f"{nome} sem limit")
            self.assertIn("{db}", sql, f"{nome} com database cravado")


if __name__ == "__main__":
    unittest.main()
