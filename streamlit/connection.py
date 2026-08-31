"""Conexao do painel com o Snowflake — vestindo RETAIL_READER, e so ele.

POR QUE O PAPEL IMPORTA AQUI, e nao e detalhe de configuracao. O repositorio criou tres
papeis, um por VERBO do pipeline: `RETAIL_LOADER` escreve o STAGE, `RETAIL_TRANSFORMER` le o
STAGE e escreve GOLD/MART, `RETAIL_READER` le MART e mais nada. Enquanto o Snowflake era
consultado por `ACCOUNTADMIN`, esses papeis existiam, estavam verificados e nao eram usados —
divida que o ARCHITECTURE.md registrou e a Fase 2 fechou para a carga e para o dbt.

Este painel e o TERCEIRO verbo, e o primeiro consumidor a vestir `RETAIL_READER` de verdade.
Verificado, nao afirmado: com este papel, `select` em `RETAIL.GOLD.FACT_ORDER` e em
`RETAIL.STAGE.STG_ORDER` e RECUSADO pelo motor. E a mesma postura que o Power BI tera — e se
um indicador daqui precisar de algo que o papel nao alcanca, isso e informacao de projeto, nao
um obstaculo a contornar com um papel maior.

`use secondary roles none` na abertura da sessao. Sem isso a restricao passaria por engano:
contas Snowflake modernas nascem com `DEFAULT_SECONDARY_ROLES = ('ALL')` e ativam todos os
papeis do usuario alem do primario — foi exatamente assim que uma verificacao de RBAC passou
por engano neste projeto, e esta escrito no ARCHITECTURE.md.

CREDENCIAL: nenhuma linha deste arquivo le, guarda ou transporta segredo. Tudo vem de
`~/.snowflake/config.toml` pelo nome da conexao, que o proprio conector resolve — inclusive
`private_key_file`. Autenticacao por par de chaves RSA.
"""

from __future__ import annotations

import os
import sys

# O painel roda de dentro de streamlit/, e a plataforma vive em platform/src. Sem isto o
# `import retail_platform` falha com uma mensagem que nao diz por que.
_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PLATAFORMA = os.path.join(_RAIZ, "platform", "src")
if _PLATAFORMA not in sys.path:
    sys.path.insert(0, _PLATAFORMA)

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "RETAIL")
SCHEMA = "MART"
ROLE = os.environ.get("RETAIL_DASHBOARD_ROLE", "RETAIL_READER")
CONNECTION = os.environ.get("RETAIL_SNOWFLAKE_CONNECTION", "spark_retail")


class DashboardError(Exception):
    """O destino recusou, ou a conexao nao existe."""


def _dotenv() -> None:
    """Carrega .env.snowflake se ele existir. NAO sobrepoe o que ja esta no ambiente."""
    caminho = os.path.join(_RAIZ, ".env.snowflake")
    if not os.path.exists(caminho):
        return
    with open(caminho, encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            os.environ.setdefault(chave.strip(), valor.strip())


def open_session():
    """Sessao nova, com o papel de leitura e sem papeis secundarios."""
    _dotenv()
    from retail_platform.snowflake_load import SnowflakeLoadError, connect

    try:
        conexao = connect(CONNECTION, role=ROLE)
    except SnowflakeLoadError as exc:
        raise DashboardError(
            f"nao foi possivel conectar como {ROLE}: {exc}\n\n"
            f"Confira o bloco [connections.{CONNECTION}] em ~/.snowflake/config.toml e "
            f"se o papel {ROLE} foi criado por `make warehouse-bootstrap`."
        ) from exc
    cursor = conexao.cursor()
    try:
        # Ver o cabecalho: sem isto a restricao do papel passaria por engano.
        cursor.execute("use secondary roles none")
    finally:
        cursor.close()
    return conexao


def identity(conexao) -> dict:
    """Quem esta lendo, de onde. Vai no rodape do painel — evidencia, nao decoracao."""
    cursor = conexao.cursor()
    try:
        cursor.execute(
            "select current_account(), current_user(), current_role(), "
            "current_warehouse(), current_region(), "
            "to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')"
        )
        conta, usuario, papel, warehouse, regiao, agora = cursor.fetchone()
    finally:
        cursor.close()
    return {
        "conta": conta, "usuario": usuario, "papel": papel,
        "warehouse": warehouse, "regiao": regiao, "lido_em": agora,
        "database": DATABASE, "schema": SCHEMA,
    }


def run(conexao, sql: str, params: dict | None = None):
    """Executa e devolve um DataFrame do pandas.

    `fetch_pandas_all` e nao `fetchall`: `MART_PRICE_EVOLUTION` tem 112 mil linhas, e
    construir 112 mil tuplas Python para depois converter e o caminho longo.
    """
    cursor = conexao.cursor()
    try:
        cursor.execute(sql, params or {})
        try:
            return cursor.fetch_pandas_all()
        except Exception:
            # Alguns resultados (agregados de uma linha, tipos raros) nao tem caminho
            # Arrow. Cair para o caminho lento e correto e melhor que falhar.
            import pandas as pd

            colunas = [d[0] for d in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=colunas)
    finally:
        cursor.close()


def probe_isolation(conexao) -> list[tuple[str, bool, str]]:
    """Confere, ao vivo, que este papel NAO alcanca GOLD nem STAGE.

    Existe para que a afirmacao "o painel le so o MART" seja verificavel na propria tela, em
    vez de ser uma frase no README. Um painel que diz respeitar um limite e nao o demonstra
    esta pedindo confianca — que e exatamente o que este repositorio recusa.
    """
    alvos = [
        (f"{DATABASE}.{SCHEMA}.MART_ORDER_FUNNEL", True),
        (f"{DATABASE}.GOLD.FACT_ORDER", False),
        (f"{DATABASE}.STAGE.STG_ORDER", False),
    ]
    resultado = []
    for alvo, deveria_ler in alvos:
        cursor = conexao.cursor()
        try:
            cursor.execute(f"select count(*) from {alvo}")
            cursor.fetchone()
            resultado.append((alvo, deveria_ler is True, "leu"))
        except Exception as exc:
            primeira = str(exc).splitlines()[0]
            resultado.append((alvo, deveria_ler is False, f"recusado ({primeira[:40]})"))
        finally:
            cursor.close()
    return resultado
