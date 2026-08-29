"""O TRANSPORTE, conferido sem Snowflake e sem credencial.

Estes testes existem por causa de dois defeitos que so apareceram rodando contra a conta
de verdade, e que a suite do recorte nao tinha como pegar porque nao geram SQL invalido —
geram SQL VALIDO apontando para o lugar errado:

  1. `@%TABELA` resolve contra o schema CORRENTE da sessao, e `create schema` do Snowflake
     TROCA o schema corrente. Como ensure_schemas cria STAGE, GOLD e MART nessa ordem, a
     sessao terminava em MART e o PUT procurava RETAIL.MART.%STG_PRODUCT_PRICE.
  2. Sem OVERWRITE no PUT, o stage de tabela guarda o arquivo entre execucoes e o COPY
     seguinte leria o antigo junto do novo — as contagens dobrariam na segunda carga, nao
     na primeira, que e o pior momento possivel para descobrir.

Nenhum dos dois falha de forma obvia se voltar atras, e por isso viraram teste.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.snowflake_export import SPECS  # noqa: E402
from retail_platform.snowflake_load import (  # noqa: E402
    DEFAULT_LOAD_ROLE,
    ROLES,
    SCHEMAS,
    SnowflakeLoadError,
    bootstrap,
    qualified,
    require_schemas,
    require_warehouse,
    transfer_statements,
    verify,
)


class QualificacaoTest(unittest.TestCase):
    def setUp(self):
        self.put, self.copy = transfer_statements(
            "STG_PRODUCT_PRICE", "/tmp/STG_PRODUCT_PRICE.parquet", "RETAIL"
        )

    def test_o_stage_de_tabela_e_qualificado_com_database_e_schema(self):
        """Sem isto, o PUT segue o schema corrente da sessao — que `create schema` acabou
        de mudar para MART."""
        self.assertIn("@RETAIL.STAGE.%STG_PRODUCT_PRICE", self.put)
        self.assertIn("@RETAIL.STAGE.%STG_PRODUCT_PRICE", self.copy)

    def test_nunca_usa_a_forma_relativa(self):
        for comando in (self.put, self.copy):
            self.assertNotIn("@%STG", comando, "forma relativa ao schema corrente voltou")

    def test_a_tabela_alvo_tambem_e_qualificada(self):
        self.assertIn("copy into RETAIL.STAGE.STG_PRODUCT_PRICE", self.copy)

    def test_respeita_um_database_diferente(self):
        put, copy = transfer_statements("STG_CUSTOMER", "/tmp/x.parquet", "OUTRO_DB")
        self.assertIn("@OUTRO_DB.STAGE.%STG_CUSTOMER", put)
        self.assertIn("copy into OUTRO_DB.STAGE.STG_CUSTOMER", copy)

    def test_qualified_e_a_mesma_forma_usada_na_reconferencia(self):
        self.assertEqual(qualified("STG_CUSTOMER", "RETAIL"), "RETAIL.STAGE.STG_CUSTOMER")


class TransporteTest(unittest.TestCase):
    def test_o_put_sobrescreve_o_arquivo_anterior_no_stage(self):
        """Sem overwrite, a SEGUNDA carga dobraria as contagens — nao a primeira."""
        put, _ = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("overwrite = true", put)

    def test_o_put_nao_recomprime_o_parquet(self):
        put, _ = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("auto_compress = false", put)

    def test_o_copy_manda_o_snowflake_ler_o_logical_type_do_parquet(self):
        """Sem isto, TODO timestamp chega 56 milhoes de anos no futuro — e nada reprova.

        O DuckDB anota a unidade so no LogicalType moderno e deixa o ConvertedType legado
        em NONE; o leitor do Snowflake, por padrao, ignora o primeiro, cai no segundo e
        assume MILISSEGUNDOS. 1,787e15 microssegundos viram o ano 56.648.666.

        A carga nao reprova (a contagem de linhas esta certa), o dbt constroi tudo em verde
        (as duracoes viram numeros grandes, nao erros) e um funil de `count_if(x is not
        null)` continua exato. Este teste e barato justamente porque o defeito nao e.
        """
        _, copy = transfer_statements("STG_ORDER", "/tmp/a.parquet", "RETAIL")
        self.assertIn("use_logical_type = true", copy)

    def test_o_copy_casa_por_NOME_de_coluna_e_nao_por_posicao(self):
        """Acrescentar uma coluna no meio do recorte carregaria dado na coluna errada em
        silencio se o casamento fosse posicional — e o resultado continuaria plausivel."""
        _, copy = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("match_by_column_name = case_insensitive", copy)

    def test_o_copy_aborta_no_primeiro_erro(self):
        _, copy = transfer_statements("STG_CATEGORY", "/tmp/a.parquet", "RETAIL")
        self.assertIn("on_error = abort_statement", copy)

    def test_o_caminho_do_arquivo_e_absoluto(self):
        """PUT com caminho relativo depende do cwd do processo — numa DAG isso e outro
        diretorio."""
        put, _ = transfer_statements("STG_CATEGORY", "relativo/a.parquet", "RETAIL")
        self.assertIn(f"file://{os.path.abspath('relativo/a.parquet')}", put)

    def test_ha_comando_para_toda_spec_do_recorte(self):
        for spec in SPECS:
            put, copy = transfer_statements(spec["name"], f"/tmp/{spec['name']}.parquet", "RETAIL")
            self.assertIn(spec["name"], put)
            self.assertIn(spec["name"], copy)


class SchemasTest(unittest.TestCase):
    def test_as_tres_camadas_sao_criadas_com_comentario(self):
        self.assertEqual(set(SCHEMAS), {"STAGE", "GOLD", "MART"})
        for nome, comentario in SCHEMAS.items():
            self.assertTrue(comentario.strip(), f"{nome} sem comentario")
            self.assertNotIn("'", comentario, f"{nome}: aspa quebraria o DDL")


class _CursorFalso:
    """Devolve contagens combinadas, para exercitar `verify` sem rede."""

    def __init__(self, contagens):
        self.contagens = contagens
        self.ultimo = None

    def execute(self, sql):
        self.ultimo = sql
        for nome, valor in self.contagens.items():
            if nome in sql:
                if isinstance(valor, Exception):
                    raise valor
                self._valor = valor
                return self
        raise AssertionError(f"consulta inesperada: {sql}")

    def fetchone(self):
        return (self._valor,)

    def close(self):
        pass


class _ConexaoFalsa:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class ReconferenciaTest(unittest.TestCase):
    """Carregar sem reconferir e confiar no transporte — a mesma disciplina de
    verify-landing, um nivel acima."""

    def contagens(self, override=None):
        base = {spec["name"]: 10 for spec in SPECS}
        base.update(override or {})
        return base

    def test_silencio_quando_tudo_bate(self):
        con = _ConexaoFalsa(_CursorFalso(self.contagens()))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        self.assertEqual(verify(con, "/tmp", esperado, "RETAIL"), [])

    def test_acusa_divergencia_de_contagem(self):
        con = _ConexaoFalsa(_CursorFalso(self.contagens({"STG_CUSTOMER": 9})))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        problemas = verify(con, "/tmp", esperado, "RETAIL")
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_CUSTOMER", problemas[0])
        self.assertIn("10", problemas[0])
        self.assertIn("9", problemas[0])

    def test_acusa_tabela_que_o_export_nao_declarou(self):
        """Uma spec nova que ninguem exportou nao pode passar como se estivesse certa."""
        con = _ConexaoFalsa(_CursorFalso(self.contagens()))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS if spec["name"] != "STG_WAREHOUSE"}
        problemas = verify(con, "/tmp", esperado, "RETAIL")
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_WAREHOUSE", problemas[0])

    def test_acusa_tabela_ausente_no_destino_em_vez_de_estourar(self):
        con = _ConexaoFalsa(_CursorFalso(
            self.contagens({"STG_GEOGRAPHY": RuntimeError("does not exist")})
        ))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        problemas = verify(con, "/tmp", esperado, "RETAIL")
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_GEOGRAPHY", problemas[0])


class _CursorGravador:
    """Guarda todo SQL executado, para conferir o bootstrap sem tocar no Snowflake."""

    def __init__(self, respostas=None):
        self.sqls = []
        self.respostas = respostas or {}

    def execute(self, sql):
        self.sqls.append(sql)
        self._linhas = []
        for chave, valor in self.respostas.items():
            if chave in sql:
                self._linhas = valor
        return self

    def fetchall(self):
        return self._linhas

    def fetchone(self):
        return self._linhas[0] if self._linhas else (None,)

    def close(self):
        pass

    def emitiu(self, *fragmentos):
        return [s for s in self.sqls if all(f in s for f in fragmentos)]


class GovernancaTest(unittest.TestCase):
    """Os quatro defeitos que so apareceram ao VESTIR os papeis.

    Enquanto o pipeline rodava como ACCOUNTADMIN nenhum deles existia — administrador tem
    tudo. Nenhum falha de forma obvia se voltar atras: os tres primeiros dao erro de
    privilegio longe da causa, e o quarto nao da erro nenhum, so apaga os objetos da vista
    de quem administra a conta.
    """

    def setUp(self):
        self.cursor = _CursorGravador()
        bootstrap(self.cursor, "RETAIL", grant_to_user="ALGUEM", warehouse="COMPUTE_WH")

    def test_os_tres_papeis_recebem_usage_no_warehouse(self):
        """Dado sem compute nao se move. Sem este grant, a carga morre com 'No active
        warehouse selected' — erro que aponta para a sessao, nao para o grant que falta."""
        for papel in ROLES:
            self.assertTrue(
                self.cursor.emitiu("grant usage on warehouse COMPUTE_WH", papel),
                f"{papel} ficou sem usage no warehouse",
            )

    def test_a_posse_das_tabelas_vai_para_o_papel_que_as_reescreve(self):
        """`grant all` concede os privilegios APLICAVEIS, e posse nao e um deles. Sem
        ownership, `create or replace table` reprova — medido: 8 modelos de uma vez."""
        self.assertTrue(self.cursor.emitiu(
            "grant ownership on all tables in schema RETAIL.STAGE", "RETAIL_LOADER"))
        for camada in ("GOLD", "MART"):
            self.assertTrue(self.cursor.emitiu(
                f"grant ownership on all tables in schema RETAIL.{camada}",
                "RETAIL_TRANSFORMER"), f"{camada} sem transferencia de posse")

    def test_a_transferencia_de_posse_preserva_os_grants_existentes(self):
        """Sem `copy current grants`, mover a posse de MART revogaria o select do READER —
        vestir o papel silenciaria o consumidor, sem erro nenhum no caminho."""
        for comando in self.cursor.emitiu("grant ownership"):
            self.assertIn("copy current grants", comando)

    def test_os_papeis_ficam_pendurados_em_sysadmin(self):
        """Depois que a posse saiu de ACCOUNTADMIN e os papeis secundarios foram
        desligados, o administrador parou de enxergar os objetos: nao herdava esses papeis.
        Heranca sobe, nunca desce — o isolamento nao muda."""
        for papel in ROLES:
            self.assertTrue(
                self.cursor.emitiu(f"grant role {papel} to role SYSADMIN"),
                f"{papel} fora da hierarquia administrativa",
            )

    def test_os_papeis_secundarios_do_usuario_sao_desligados(self):
        """DEFAULT_SECONDARY_ROLES = ('ALL') e o default de contas modernas, e com ele a
        sessao ativa TODOS os papeis do usuario alem do primario. Sem desligar, vestir o
        papel e decorativo — e o dbt nao tem onde rodar `use secondary roles none`."""
        self.assertTrue(self.cursor.emitiu(
            "alter user ALGUEM set default_secondary_roles = ()"))

    def test_sem_usuario_nao_mexe_em_usuario_nenhum(self):
        cursor = _CursorGravador()
        bootstrap(cursor, "RETAIL")
        self.assertEqual(cursor.emitiu("alter user"), [])
        self.assertEqual(cursor.emitiu("grant role RETAIL_LOADER to user"), [])

    def test_o_bootstrap_e_o_unico_que_cria_infraestrutura(self):
        self.assertTrue(self.cursor.emitiu("create database if not exists RETAIL"))
        for nome in SCHEMAS:
            self.assertTrue(self.cursor.emitiu(f"create schema if not exists RETAIL.{nome}"))


class PapelDaCargaTest(unittest.TestCase):
    def test_a_carga_nao_roda_como_administrador(self):
        """O papel da carga sobrescreve o da conexao — a conexao aponta para o
        administrativo porque e ela que roda o bootstrap."""
        self.assertEqual(DEFAULT_LOAD_ROLE, "RETAIL_LOADER")

    def test_sessao_sem_compute_e_acusada_antes_de_tocar_em_dado(self):
        cursor = _CursorGravador({"current_warehouse": [(None,)]})
        with self.assertRaises(SnowflakeLoadError) as erro:
            require_warehouse(cursor)
        self.assertIn("warehouse", str(erro.exception))

    def test_sessao_com_compute_passa(self):
        cursor = _CursorGravador({"current_warehouse": [("COMPUTE_WH",)]})
        require_warehouse(cursor)

    def test_a_carga_confere_apenas_o_stage(self):
        """Conferir os tres schemas reprovaria a carga por um motivo CERTO: o
        information_schema devolve so o que o papel enxerga, e RETAIL_LOADER nao enxerga
        GOLD nem MART. Uma verificacao mais ampla que a necessidade transforma o controle
        funcionando em falha de execucao."""
        cursor = _CursorGravador({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, "RETAIL")

    def test_stage_ausente_manda_rodar_o_bootstrap(self):
        cursor = _CursorGravador({"information_schema.schemata": [("GOLD",), ("MART",)]})
        with self.assertRaises(SnowflakeLoadError) as erro:
            require_schemas(cursor, "RETAIL")
        self.assertIn("STAGE", str(erro.exception))
        self.assertIn("warehouse-bootstrap", str(erro.exception))

    def test_a_carga_nunca_cria_database_nem_schema(self):
        """Poder criar database era o sintoma de estar rodando como administrador."""
        cursor = _CursorGravador({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, "RETAIL")
        self.assertEqual(cursor.emitiu("create"), [])


if __name__ == "__main__":
    unittest.main()
