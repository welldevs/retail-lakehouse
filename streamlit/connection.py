"""Conexao do painel com o Postgres local — vestindo retail_reader, e so ele.

POR QUE O PAPEL IMPORTA AQUI, e nao e detalhe de configuracao. O repositorio criou tres
papeis, um por VERBO do pipeline: `retail_loader` escreve o STAGE, `retail_transformer` le o
STAGE e escreve GOLD/MART, `retail_reader` le MART e mais nada. Este painel e o TERCEIRO
verbo, e continua sendo o unico consumidor a vestir o papel de leitura de verdade.

MOTOR: Postgres local, nao Snowflake. A conta trial usada por este projeto venceu (ver
DECISIONS.md, CR-007) — este arquivo troca so o TRANSPORTE. MART/GOLD/STAGE, os tres
papeis e a garantia de isolamento continuam os mesmos, porque a arvore models/warehouse/ e
neutra quanto ao motor (ver ARCHITECTURE.md).

SEM `use secondary roles none`. Aquela chamada existia para desarmar
`DEFAULT_SECONDARY_ROLES = ('ALL')` do Snowflake, que reativa todos os papeis do usuario por
baixo do primario. Postgres nao tem esse conceito: `set role` troca o papel ATIVO da sessao
sem nenhum papel "escondido" continuar valendo por tras — ver o cabecalho de
`retail_platform/postgres_load.py`.

CREDENCIAL: dev local, sem segredo real — mesmo criterio do MinIO e do OLTP simulado. Nao ha
arquivo de config fora do repo: os quatro `WAREHOUSE_PG_*` tem default embutido no proprio
`postgres_load.py`, e o `.env` da raiz so entra para quem sobrescreve o default.
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

HOST = os.environ.get("WAREHOUSE_PG_HOST", "localhost")
PORT = int(os.environ.get("WAREHOUSE_PG_PORT", "5434"))
DATABASE = os.environ.get("WAREHOUSE_PG_DB", "retail")
SCHEMA = '"MART"'
ROLE = os.environ.get("RETAIL_DASHBOARD_ROLE", "retail_reader")


class DashboardError(Exception):
    """O destino recusou, ou a conexao nao existe."""


def _dotenv() -> None:
    """Carrega o `.env` da raiz se ele existir. NAO sobrepoe o que ja esta no ambiente."""
    caminho = os.path.join(_RAIZ, ".env")
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
    """Sessao nova, com o papel de leitura.

    Duas linhas depois de `connect()`, e as duas existem pelo MESMO motivo: `set role`
    (dentro de `connect`) e transacional no Postgres — um `rollback()` posterior o desfaria
    silenciosamente, devolvendo a sessao ao usuario administrativo por baixo do papel de
    leitura. `commit()` sela a troca antes que qualquer coisa possa desfaze-la; `autocommit
    = True` garante que NENHUMA consulta seguinte (inclusive uma recusada por
    `probe_isolation`) abra uma transacao para desfazer. O painel so LE — nao ha atomicidade
    de escrita para proteger aqui.
    """
    _dotenv()
    from retail_platform.postgres_load import PostgresLoadError, connect

    try:
        conexao = connect(host=HOST, port=PORT, dbname=DATABASE, role=ROLE)
    except PostgresLoadError as exc:
        raise DashboardError(
            f"nao foi possivel conectar como {ROLE}: {exc}\n\n"
            f"Confira se o container esta de pe (`make warehouse-postgres-up`) e se "
            f"`make warehouse-bootstrap` ja rodou uma vez."
        ) from exc
    conexao.commit()
    conexao.autocommit = True
    return conexao


def identity(conexao) -> dict:
    """Quem esta lendo, de onde. Vai no rodape do painel — evidencia, nao decoracao."""
    cursor = conexao.cursor()
    try:
        cursor.execute(
            "select current_database(), session_user, current_user, "
            "inet_server_addr(), inet_server_port(), "
            "to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')"
        )
        banco, login, papel, host, porta, agora = cursor.fetchone()
    finally:
        cursor.close()
    return {
        # inet_server_addr() devolve NULL numa conexao por socket Unix; aqui e sempre
        # TCP (host/porta explicitos em connect()), mas o fallback evita um "None:None".
        "conta": f"{host or HOST}:{porta or PORT}", "usuario": login, "papel": papel,
        "warehouse": None, "regiao": None, "lido_em": agora,
        "database": banco, "schema": SCHEMA,
    }


def run(conexao, sql: str, params: dict | None = None):
    """Executa e devolve um DataFrame do pandas, com nome de coluna em MAIUSCULO.

    O UPPER() e o que faz `indicators.py`/`app.py` nao precisarem mudar uma linha sequer.
    Snowflake maiuscula todo identificador nao citado por padrao — e por isso app.py le
    `r["PEDIDOS_COLOCADOS"]` de um SQL que escreve `as pedidos_colocados`. Postgres faz o
    OPOSTO (dobra para minusculo), entao sem este upper() toda consulta devolveria as
    mesmas colunas com outro nome, e o painel inteiro quebraria por acesso de dicionario —
    nao por SQL errado. Um lugar so corrige os ~150 alias do painel de uma vez.
    """
    import pandas as pd

    cursor = conexao.cursor()
    try:
        cursor.execute(sql, params or {})
        colunas = [d[0].upper() for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=colunas)
    finally:
        cursor.close()


def probe_isolation(conexao) -> list[tuple[str, bool, str]]:
    """Confere, ao vivo, que este papel NAO alcanca GOLD nem STAGE.

    Existe para que a afirmacao "o painel le so o MART" seja verificavel na propria tela, em
    vez de ser uma frase no README. Um painel que diz respeitar um limite e nao o demonstra
    esta pedindo confianca — que e exatamente o que este repositorio recusa.

    STAGE e citado em maiusculo (`"STG_ORDER"`) porque quem cria essas tabelas e
    `postgres_load.py`, que preserva o nome do recorte tal como o Snowflake o conhece; GOLD e
    MART sao criados pelo dbt a partir do nome do ARQUIVO do modelo, que ja e minusculo.
    """
    alvos = [
        ('"MART".mart_order_funnel', True),
        ('"GOLD".fact_order', False),
        ('"STAGE"."STG_ORDER"', False),
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
