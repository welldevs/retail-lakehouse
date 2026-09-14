"""O carregador do segundo alvo de warehouse (Postgres local), conferido sem Postgres real.

Mesma disciplina de `test_snowflake_load.py`: tudo aqui roda contra cursor/conexao FALSOS,
sem rede e sem servidor de pe — o container `warehouse-postgres` e opt-in (profile proprio
do compose) e nao deveria ser pre-requisito de `make test`.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.snowflake_export import SPECS  # noqa: E402
from retail_platform.postgres_load import (  # noqa: E402
    DEFAULT_LOAD_ROLE,
    GOLD,
    MART,
    ROLES,
    SCHEMAS,
    STAGE,
    PostgresLoadError,
    bootstrap,
    connect,
    ddl_for,
    qualified,
    require_schemas,
    verify,
)


class DDLTest(unittest.TestCase):
    def test_ddl_cita_schema_e_tabela_em_maiusculo(self):
        """MAIUSCULO E CITADO, como no Snowflake: `sources.yml`/`dbt_project.yml` sao
        compartilhados pelos dois alvos e usam STAGE/GOLD/MART literais — sem aspas,
        Postgres dobraria para minusculo e dbt nunca encontraria a tabela."""
        ddl = ddl_for({"name": "STG_CATEGORY"}, [("category_id", "INTEGER")], schema=STAGE)
        self.assertIn('create table "STAGE"."STG_CATEGORY"', ddl)

    def test_decimal_vira_numeric_preservando_precisao(self):
        ddl = ddl_for({"name": "X"}, [("value", "DECIMAL(18,6)")], schema=STAGE)
        self.assertIn("numeric(18,6)", ddl)

    def test_tipo_desconhecido_falha_em_vez_de_virar_text(self):
        """Mesmo criterio do `_TYPE_MAP` do Snowflake: um tipo sem entrada deve FALHAR,
        nao perder precisao em silencio virando texto."""
        with self.assertRaises(PostgresLoadError):
            ddl_for({"name": "X"}, [("c", "BLOB")], schema=STAGE)

    def test_timestamp_com_fuso_vira_timestamptz(self):
        ddl = ddl_for({"name": "X"}, [("t", "TIMESTAMP WITH TIME ZONE")], schema=STAGE)
        self.assertIn("timestamptz", ddl)


class QualificacaoTest(unittest.TestCase):
    def test_qualified_cita_os_dois_identificadores(self):
        self.assertEqual(qualified("STG_CUSTOMER", "STAGE"), '"STAGE"."STG_CUSTOMER"')

    def test_qualified_e_a_mesma_forma_usada_no_ddl_e_na_reconferencia(self):
        ddl = ddl_for({"name": "STG_CUSTOMER"}, [("customer_id", "INTEGER")], schema=STAGE)
        self.assertIn(qualified("STG_CUSTOMER", STAGE), ddl)


class SchemasTest(unittest.TestCase):
    def test_as_tres_camadas_sao_os_mesmos_nomes_literais_do_dbt(self):
        self.assertEqual(set(SCHEMAS), {STAGE, GOLD, MART})
        self.assertEqual({STAGE, GOLD, MART}, {"STAGE", "GOLD", "MART"})
        for nome, comentario in SCHEMAS.items():
            self.assertTrue(comentario.strip(), f"{nome} sem comentario")
            # O comentario entra inline no DDL (ver bootstrap) porque `comment on schema`
            # nao aceita parametro ligado — uma aspa quebraria o SQL.
            self.assertNotIn("'", comentario, f"{nome}: aspa quebraria o DDL")


class _CursorFalso:
    """Devolve contagens combinadas, para exercitar `verify` sem rede."""

    def __init__(self, contagens):
        self.contagens = contagens

    def execute(self, sql, params=None):
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
    """Carregar sem reconferir e confiar no transporte — mesma disciplina do Snowflake."""

    def contagens(self, override=None):
        base = {spec["name"]: 10 for spec in SPECS}
        base.update(override or {})
        return base

    def test_silencio_quando_tudo_bate(self):
        con = _ConexaoFalsa(_CursorFalso(self.contagens()))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        self.assertEqual(verify(con, "/tmp", esperado), [])

    def test_acusa_divergencia_de_contagem(self):
        con = _ConexaoFalsa(_CursorFalso(self.contagens({"STG_CUSTOMER": 9})))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        problemas = verify(con, "/tmp", esperado)
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_CUSTOMER", problemas[0])
        self.assertIn("10", problemas[0])
        self.assertIn("9", problemas[0])

    def test_acusa_tabela_que_o_export_nao_declarou(self):
        con = _ConexaoFalsa(_CursorFalso(self.contagens()))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS if spec["name"] != "STG_WAREHOUSE"}
        problemas = verify(con, "/tmp", esperado)
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_WAREHOUSE", problemas[0])

    def test_acusa_tabela_ausente_no_destino_em_vez_de_estourar(self):
        con = _ConexaoFalsa(_CursorFalso(
            self.contagens({"STG_GEOGRAPHY": RuntimeError("relation does not exist")})
        ))
        esperado = {spec["name"]: {"rows": 10} for spec in SPECS}
        problemas = verify(con, "/tmp", esperado)
        self.assertEqual(len(problemas), 1)
        self.assertIn("STG_GEOGRAPHY", problemas[0])


class _CursorGravador:
    """Guarda todo SQL (e parametros) executado, para conferir o bootstrap sem Postgres."""

    def __init__(self, respostas=None):
        self.chamadas: list[tuple[str, tuple | None]] = []
        self.respostas = respostas or {}

    def execute(self, sql, params=None):
        self.chamadas.append((sql, params))
        self._linhas = []
        for chave, valor in self.respostas.items():
            if chave in sql:
                self._linhas = valor
        return self

    def fetchall(self):
        return self._linhas

    def fetchone(self):
        return self._linhas[0] if self._linhas else None

    def close(self):
        pass

    def emitiu(self, *fragmentos):
        return [sql for sql, _ in self.chamadas if all(f in sql for f in fragmentos)]


class GovernancaTest(unittest.TestCase):
    """Os tres papeis e os grants automaticos sobre tabela futura — o equivalente Postgres
    do 'grant on future tables' do Snowflake, sem o qual o full-refresh (drop+create)
    silenciaria o papel seguinte a cada carga."""

    def setUp(self):
        self.cursor = _CursorGravador()
        bootstrap(self.cursor, grant_to_user="alguem")

    def test_os_tres_schemas_sao_criados_citados_e_comentados(self):
        for nome in SCHEMAS:
            self.assertTrue(self.cursor.emitiu(f'create schema if not exists "{nome}"'))
            self.assertTrue(self.cursor.emitiu(f'comment on schema "{nome}"'))

    def test_os_tres_papeis_sao_criados(self):
        for nome in ROLES:
            self.assertTrue(self.cursor.emitiu(f"create role {nome}"))

    def test_loader_so_tem_usage_e_create_no_stage(self):
        self.assertTrue(self.cursor.emitiu(f'grant usage, create on schema "{STAGE}"', "retail_loader"))

    def test_transformer_recebe_select_automatico_em_tabela_futura_do_stage(self):
        """Sem `alter default privileges`, o TRANSFORMER ficaria sem enxergar a tabela que
        o LOADER recria a cada carga — `create table` novo nao herda grant nenhum."""
        self.assertTrue(self.cursor.emitiu(
            f'alter default privileges for role retail_loader in schema "{STAGE}"',
            "grant select on tables to retail_transformer",
        ))

    def test_reader_recebe_select_automatico_em_tabela_futura_do_mart(self):
        self.assertTrue(self.cursor.emitiu(
            f'alter default privileges for role retail_transformer in schema "{MART}"',
            "grant select on tables to retail_reader",
        ))

    def test_reader_nao_recebe_privilegio_futuro_em_gold(self):
        """So MART. O mesmo isolamento que o Snowflake prova com `check_isolation`."""
        self.assertFalse(self.cursor.emitiu(
            f'alter default privileges for role retail_transformer in schema "{GOLD}"',
            "retail_reader",
        ))

    def test_grant_to_user_concede_os_tres_papeis(self):
        for nome in ROLES:
            self.assertTrue(self.cursor.emitiu(f"grant {nome} to alguem"))

    def test_sem_usuario_nao_concede_papel_a_ninguem(self):
        cursor = _CursorGravador()
        bootstrap(cursor)
        self.assertEqual(cursor.emitiu("grant retail_loader to"), [])


class RequerSchemasTest(unittest.TestCase):
    def test_passa_quando_o_schema_existe(self):
        cursor = _CursorGravador({"information_schema.schemata": [("STAGE",)]})
        require_schemas(cursor, (STAGE,))

    def test_acusa_e_sugere_o_bootstrap_quando_falta(self):
        cursor = _CursorGravador({"information_schema.schemata": [("GOLD",), ("MART",)]})
        with self.assertRaises(PostgresLoadError) as erro:
            require_schemas(cursor, (STAGE,))
        self.assertIn(STAGE, str(erro.exception))
        self.assertIn("warehouse-postgres-bootstrap", str(erro.exception))


class PapelDaCargaTest(unittest.TestCase):
    def test_a_carga_nao_roda_como_administrador(self):
        """O papel da carga e explicito, nao o usuario administrativo do container."""
        self.assertEqual(DEFAULT_LOAD_ROLE, "retail_loader")

    def test_papel_desconhecido_e_recusado_antes_de_abrir_conexao(self):
        """`set role` nao aceita parametro ligado — o nome precisa ser validado ANTES de
        entrar no SQL, e a validacao roda antes de qualquer tentativa de rede."""
        with self.assertRaises(PostgresLoadError) as erro:
            connect(role="admin_do_mal")
        self.assertIn("papel desconhecido", str(erro.exception))


if __name__ == "__main__":
    unittest.main()
