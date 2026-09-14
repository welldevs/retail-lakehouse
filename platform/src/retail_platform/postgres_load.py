"""Transporte do recorte para o Postgres local: DDL, COPY FROM STDIN, papeis nativos.

Segundo alvo do warehouse, ao lado de `snowflake_load.py` — mesmo criterio de separacao
produtor/transportador (testar o recorte nao deveria exigir credencial, e mover bytes nao
deveria exigir DuckDB). Existe porque a conta Snowflake usada neste projeto e um TRIAL, com
prazo: um Postgres local prova que a fronteira fisica em L2->L3 (COPY/`source()`, nunca
`ref()`) tambem troca de MOTOR, e nao so de conta. Ver DECISIONS.md.

SEM STAGE INTERNO. O stage de tabela do Snowflake resolve um problema que aqui nao existe —
um Postgres gerenciado nao enxergaria o MinIO local, mas ESTE Postgres roda no mesmo host
que o parquet ja foi escrito. O transporte e direto: ler o parquet com o DuckDB e mandar
`COPY ... FROM STDIN`, linha a linha, sem arquivo intermediario nem PUT.

FULL REFRESH POR DROP + CREATE, e nao `create or replace table`: Postgres nao tem essa
sintaxe para tabela. Como quem recria e sempre o mesmo papel (`retail_loader` no STAGE,
`retail_transformer` no GOLD/MART via dbt), a posse nunca precisa ser transferida — ao
contrario do Snowflake, onde `copy current grants` existe justamente para isso.

CREDENCIAL: dev local, sem segredo real — mesmo criterio do MinIO e do OLTP simulado
(usuario/senha fixos, documentados em .env.example). Nao ha par de chaves nem arquivo fora
do repo: e um Postgres que o proprio compose sobe, sob profile opt-in.

PAPEIS SEM A ARMADILHA DOS SECUNDARIOS. O Snowflake precisou de `use secondary roles none`
porque toda conta moderna nasce com `DEFAULT_SECONDARY_ROLES = ('ALL')` e ativa todos os
papeis do usuario por baixo do primario. Postgres nao tem esse conceito: um papel concedido
e herdado automaticamente (INHERIT e o default), e `set role` troca o papel ATIVO da sessao
sem qualquer papel "escondido" continuar valendo por tras. `connect(..., role=...)` aqui
conecta como o usuario administrativo do container e faz `set role` — o mesmo padrao do
`connect(connection_name, role=...)` do Snowflake, sem a ressalva.
"""

from __future__ import annotations

import os

from .snowflake_export import SPECS, STAGE_SCHEMA, _describe

DEFAULT_HOST = os.environ.get("WAREHOUSE_PG_HOST", "localhost")
DEFAULT_PORT = int(os.environ.get("WAREHOUSE_PG_PORT", "5434"))
DEFAULT_DBNAME = os.environ.get("WAREHOUSE_PG_DB", "retail")
# Usuario ADMINISTRATIVO do container (POSTGRES_USER) — quem roda bootstrap e quem o `set
# role` usa como base. Os tres papeis de negocio (retail_loader/transformer/reader) nao tem
# LOGIN proprio nesta rodada: sao papeis para GRANT e para `set role`, nao contas separadas.
# Diferente do Snowflake, que precisa de um usuario de servico por papel para isolamento de
# producao — aqui a fronteira que importa e a mesma provada pelo dbt (STAGE/GOLD/MART), e
# duplicar contas de login so para um Postgres de desenvolvimento nao paga o proprio custo.
DEFAULT_USER = os.environ.get("WAREHOUSE_PG_USER", "retail")
DEFAULT_PASSWORD = os.environ.get("WAREHOUSE_PG_PASSWORD", "retail")
DEFAULT_LOAD_ROLE = os.environ.get("RETAIL_WAREHOUSE_PG_LOAD_ROLE", "retail_loader")

STAGE = STAGE_SCHEMA
GOLD = "GOLD"
MART = "MART"

# MAIUSCULO E CITADO, como no Snowflake — nao minusculo. `sources.yml` (schema: STAGE) e
# `dbt_project.yml` (+schema: GOLD / MART) sao COMPARTILHADOS pelos dois alvos dbt e usam
# esses tres nomes literais; dbt sempre cita o identificador (`"GOLD"`), entao um schema
# criado aqui como `gold` minusculo nunca seria encontrado ("schema GOLD does not exist",
# medido). Citar em toda query e o preco de nao duplicar as duas arvores compartilhadas.
SCHEMAS = {
    STAGE: "Espelho 1:1 do recorte do Silver. Sem regra de negocio. Full refresh.",
    GOLD: "Modelo dimensional conformado: dim_* e fact_*. Escrito por dbt-postgres.",
    MART: "Tabelas por pergunta, com o grao declarado no cabecalho de cada modelo.",
}

# Mesmos tres verbos do Snowflake — carrega STAGE / le STAGE e escreve GOLD+MART / so le
# MART — modelados como papeis Postgres nativos.
ROLES = {
    "retail_loader": "Carrega o recorte do Silver em stage. Nao le gold nem mart.",
    "retail_transformer": "Le stage e escreve gold e mart. E o papel do dbt.",
    "retail_reader": "So leitura em mart. E o papel do painel e de terceiros.",
}


class PostgresLoadError(Exception):
    """O destino recusou o DDL, a copia, ou a conexao nao existe."""


def connect(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    dbname: str = DEFAULT_DBNAME,
    user: str = DEFAULT_USER,
    password: str = DEFAULT_PASSWORD,
    role: str | None = None,
):
    """Conexao ao Postgres local. Importa `psycopg` sob demanda, como `snowflake_load.connect`.

    `role` faz `set role` depois de conectar — troca o papel ATIVO da sessao sem exigir uma
    conta de login separada por papel. Validado contra ROLES antes de entrar no SQL: `set
    role` nao aceita parametro ligado, e o nome nunca deveria vir de entrada nao confiavel.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise PostgresLoadError(
            "psycopg nao esta instalado neste venv. Rode `make venv` para reinstalar as "
            "dependencias da plataforma."
        ) from exc

    if role is not None and role not in ROLES:
        raise PostgresLoadError(f"papel desconhecido: {role!r}. Esperado um de {sorted(ROLES)}.")

    try:
        conexao = psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except Exception as exc:
        raise PostgresLoadError(
            f"nao foi possivel conectar em {host}:{port}/{dbname} como {user!r}: {exc}"
        ) from exc

    if role is not None:
        with conexao.cursor() as cursor:
            cursor.execute(f"set role {role}")

    return conexao


def _qi(nome: str) -> str:
    """Identificador citado. Toda referencia a schema/tabela passa por aqui: sem aspas,
    Postgres dobraria STAGE/GOLD/MART para minusculo e nao acharia o que o bootstrap criou."""
    return f'"{nome}"'


def qualified(table: str, schema: str = STAGE) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def require_schemas(cursor, schemas: tuple[str, ...] = (STAGE,)) -> None:
    """Confere que o bootstrap ja rodou. NAO cria nada — mesma razao do Snowflake:
    reprovar aqui e mais legivel que um erro de privilegio vindo do meio do DDL."""
    cursor.execute(
        "select schema_name from information_schema.schemata where schema_name = any(%s)",
        (list(schemas),),
    )
    existentes = {linha[0] for linha in cursor.fetchall()}
    faltando = [nome for nome in schemas if nome not in existentes]
    if faltando:
        raise PostgresLoadError(
            f"schema ausente: {', '.join(faltando)}. Rode "
            f"`make warehouse-postgres-bootstrap` uma vez antes de carregar."
        )


# Mapeamento de tipo, deliberadamente curto pelo mesmo motivo do `_TYPE_MAP` do Snowflake:
# um tipo sem entrada aqui deve FALHAR, nao virar `text` por adivinhacao.
_TYPE_MAP = {
    "BOOLEAN": "boolean",
    "TINYINT": "bigint",
    "SMALLINT": "bigint",
    "INTEGER": "bigint",
    "BIGINT": "bigint",
    "HUGEINT": "numeric(38,0)",
    "UBIGINT": "numeric(20,0)",
    "UINTEGER": "bigint",
    "FLOAT": "double precision",
    "DOUBLE": "double precision",
    "VARCHAR": "text",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
}


def _postgres_type(duckdb_type: str) -> str:
    tipo = duckdb_type.upper().strip()
    if tipo.startswith("DECIMAL"):
        # DECIMAL(10,2) -> numeric(10,2). Mesma razao do Snowflake: precisao decimal nao
        # vira ponto flutuante.
        return "numeric" + tipo[len("DECIMAL"):]
    if tipo in _TYPE_MAP:
        return _TYPE_MAP[tipo]
    raise PostgresLoadError(
        f"tipo do DuckDB sem mapeamento para Postgres: {duckdb_type!r}. "
        f"Acrescente-o a _TYPE_MAP em vez de deixar o recorte cair num text."
    )


def ddl_for(spec: dict, columns: list[tuple[str, str]], schema: str = STAGE) -> str:
    """`create table` derivado do schema do proprio recorte — mesma disciplina do
    Snowflake: o DDL nunca e escrito a mao ao lado."""
    corpo = ",\n".join(f"    {nome} {_postgres_type(tipo)}" for nome, tipo in columns)
    return f"create table {qualified(spec['name'], schema)} (\n{corpo}\n)"


def bootstrap(cursor, grant_to_user: str | None = None) -> list[str]:
    """Schemas, papeis e grants (incl. `default privileges` para tabelas futuras).
    Idempotente, e separado de `load` de proposito — mesmo motivo do Snowflake: criar
    infraestrutura e trabalho de uma vez so, a carga diaria nao deveria repeti-lo.
    """
    feito = []

    def executa(sql, descricao):
        cursor.execute(sql)
        feito.append(descricao)

    for nome, comentario in SCHEMAS.items():
        executa(f"create schema if not exists {_qi(nome)}", f"schema {nome}")
        # `comment on schema` e DDL de utilitario: o protocolo estendido do Postgres nao
        # aceita `$1` nessa posicao ("syntax error at or near $1"), ao contrario de um
        # `select`/`insert` comum. Inline direto, como o Snowflake ja faz em `ddl_for` —
        # seguro porque SCHEMAS e constante do modulo, nunca entrada externa.
        executa(f"comment on schema {_qi(nome)} is '{comentario}'", f"comentario {nome}")

    for nome in ROLES:
        cursor.execute("select 1 from pg_roles where rolname = %s", (nome,))
        if cursor.fetchone() is None:
            executa(f"create role {nome}", f"role {nome}")
        else:
            feito.append(f"role {nome} ja existia")

    # LOADER: cria e le STAGE, e so.
    executa(f"grant usage, create on schema {_qi(STAGE)} to retail_loader", "loader: usage+create stage")

    # TRANSFORMER: LE o stage (nunca escreve nele) e escreve GOLD e MART.
    executa(f"grant usage on schema {_qi(STAGE)} to retail_transformer", "transformer: usage stage")
    # ALTER DEFAULT PRIVILEGES e o equivalente Postgres do "grant on future tables" do
    # Snowflake: toda tabela que retail_loader criar em STAGE dali em diante ja nasce
    # visivel ao TRANSFORMER, mesmo depois do full-refresh (drop + create) do proximo dia.
    executa(
        f"alter default privileges for role retail_loader in schema {_qi(STAGE)} "
        f"grant select on tables to retail_transformer",
        "transformer: select automatico em tabela futura do stage",
    )
    for camada in (GOLD, MART):
        executa(f"grant usage, create on schema {_qi(camada)} to retail_transformer",
                f"transformer: usage+create {camada}")

    # READER: so MART, so leitura — inclusive o que ainda nao existe, pelo mesmo mecanismo.
    executa(f"grant usage on schema {_qi(MART)} to retail_reader", "reader: usage mart")
    executa(
        f"alter default privileges for role retail_transformer in schema {_qi(MART)} "
        f"grant select on tables to retail_reader",
        "reader: select automatico em tabela futura do mart",
    )

    if grant_to_user:
        for nome in ROLES:
            executa(f"grant {nome} to {grant_to_user}", f"{nome} -> {grant_to_user}")

    return feito


def load(connection, duck_connection, stage_dir: str, schema: str = STAGE) -> dict:
    """Recria as tabelas STAGE (drop + create) e carrega via COPY FROM STDIN.

    `duck_connection` serve para DESCREVER o recorte (schema da tabela) e para LER o
    parquet linha a linha — nenhuma credencial do Postgres passa por ele. Mesmo contrato de
    retorno de `snowflake_load.load`: um dict por spec com `rows` e `bytes`.
    """
    cursor = connection.cursor()
    require_schemas(cursor, (schema,))

    resumo = {}
    for spec in SPECS:
        nome_spec = spec["name"]
        alvo = qualified(nome_spec, schema)
        caminho = os.path.join(stage_dir, f"{nome_spec}.parquet")
        if not os.path.exists(caminho):
            raise PostgresLoadError(
                f"parquet ausente: {caminho}. Rode `make warehouse-export` antes de carregar."
            )

        colunas = _describe(duck_connection, spec["sql"].strip())
        nomes_coluna = [nome for nome, _ in colunas]

        # DROP + CREATE, nao `create or replace`: Postgres nao tem essa sintaxe para
        # tabela. CASCADE derruba dependencias (views/FKs) — nao ha nenhuma esperada aqui,
        # STAGE e espelho isolado por desenho.
        cursor.execute(f"drop table if exists {alvo} cascade")
        cursor.execute(ddl_for(spec, colunas, schema))

        colunas_sql = ", ".join(nomes_coluna)
        linhas = duck_connection.execute(
            f"select {colunas_sql} from read_parquet('{caminho}')"
        ).fetchall()
        with cursor.copy(f"copy {alvo} ({colunas_sql}) from stdin") as copy:
            for linha in linhas:
                copy.write_row(linha)

        cursor.execute(f"select count(*) from {alvo}")
        carregadas = cursor.fetchone()[0]
        resumo[nome_spec] = {"rows": carregadas, "bytes": os.path.getsize(caminho)}

    connection.commit()
    cursor.close()
    return resumo


def verify(connection, stage_dir: str, expected: dict, schema: str = STAGE) -> list[str]:
    """Reconfere a contagem de cada tabela contra o que o export declarou. Mesma disciplina
    de `snowflake_load.verify` — devolve a lista de divergencias, vazia quando tudo bate."""
    cursor = connection.cursor()
    problemas = []
    for spec in SPECS:
        nome_spec = spec["name"]
        try:
            cursor.execute(f"select count(*) from {qualified(nome_spec, schema)}")
            destino = cursor.fetchone()[0]
        except Exception as exc:
            problemas.append(f"{nome_spec}: nao foi possivel contar no destino ({exc})")
            continue
        origem = expected.get(nome_spec, {}).get("rows")
        if origem is None:
            problemas.append(f"{nome_spec}: o export nao declarou contagem")
        elif origem != destino:
            problemas.append(f"{nome_spec}: origem {origem} != destino {destino}")
    cursor.close()
    return problemas
