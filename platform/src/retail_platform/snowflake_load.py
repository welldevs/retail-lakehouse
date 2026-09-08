"""Transporte do recorte para o Snowflake: DDL, PUT em stage interno, COPY INTO.

Separado de `snowflake_export.py` pelo mesmo criterio que separa `land.py` de
`oltp_reference.py`: um modulo PRODUZ o artefato, o outro o MOVE. Testar o recorte nao
deveria exigir credencial, e mover bytes nao deveria exigir DuckDB.

STAGE INTERNO, NAO EXTERNAL. Um Snowflake gerenciado nao enxerga um MinIO em localhost:
`COPY INTO` a partir de um external stage exigiria S3 real mais uma storage integration
com IAM. O stage interno de tabela (`@%TABELA`) inverte o sentido — quem empurra os bytes
e este processo, que enxerga os dois lados — e nao pede infraestrutura nenhuma. Para ~1 MB
de parquet e a escolha obviamente proporcional. Gatilho para trocar por external stage:
o RAW passar a viver num S3 de verdade, ou o volume crescer a ponto de o upload pela
maquina local virar o gargalo.

CREDENCIAL: nenhuma linha deste modulo le, guarda ou transporta segredo. Tudo vem de
`~/.snowflake/config.toml` pelo nome da conexao, que o proprio conector resolve —
inclusive `private_key_file`. Autenticacao por PAR DE CHAVES, nao por senha: e o metodo
que o Snowflake recomenda para acesso programatico, e o unico que serve para uma DAG, que
nao tem como abrir navegador nem digitar senha.
"""

from __future__ import annotations

import os

from .snowflake_export import SPECS, STAGE_SCHEMA, _describe, ddl_for

DEFAULT_CONNECTION = os.environ.get("RETAIL_SNOWFLAKE_CONNECTION", "spark_retail")
DEFAULT_DATABASE = os.environ.get("RETAIL_SNOWFLAKE_DATABASE", "RETAIL")

# O PAPEL DO CARREGADOR, e nao o da conexao. A conexao de ~/.snowflake/config.toml aponta
# para o papel administrativo porque e ela que roda o bootstrap; a carga diaria sobrescreve
# para RETAIL_LOADER. Sem isto, criar tres papeis e continuar carregando como ACCOUNTADMIN
# — que foi o estado ate aqui: o controle existia, estava verificado, e nao era usado.
DEFAULT_LOAD_ROLE = os.environ.get("RETAIL_SNOWFLAKE_LOAD_ROLE", "RETAIL_LOADER")

# O warehouse (compute) e objeto SEPARADO do database, com grant proprio. Enquanto tudo
# rodava como ACCOUNTADMIN isso passou despercebido, porque administrador enxerga todo
# warehouse; o primeiro `load` vestindo RETAIL_LOADER falhou com "No active warehouse
# selected" — a conexao pedia COMPUTE_WH, o papel nao tinha USAGE, e o Snowflake apenas
# deixa a sessao sem compute em vez de recusar a conexao.
DEFAULT_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# Schemas que o dbt vai escrever. Criados aqui porque `dbt run` nao cria schema com
# comentario nem garante ordem; e DDL de infraestrutura, nao de modelo.
SCHEMAS = {
    STAGE_SCHEMA: "Espelho 1:1 do recorte do Silver. Sem regra de negocio. Full refresh.",
    "GOLD": "Modelo dimensional conformado: DIM_* e FACT_*. Escrito por dbt-snowflake.",
    "MART": "Tabelas por pergunta, com o grao declarado no cabecalho de cada modelo.",
}


class SnowflakeLoadError(Exception):
    """O destino recusou o DDL, o upload ou a copia."""


def connect(connection_name: str = DEFAULT_CONNECTION, role: str | None = None):
    """Conexao pelo nome definido em ~/.snowflake/config.toml. Importa sob demanda.

    `role` sobrescreve o papel da conexao — usado por check_isolation para abrir uma sessao
    por papel sem precisar de um arquivo de configuracao por papel.
    """
    try:
        import snowflake.connector
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise SnowflakeLoadError(
            "snowflake-connector-python nao esta instalado neste venv. "
            "Rode `make venv` para reinstalar as dependencias da plataforma."
        ) from exc

    extra = {"role": role} if role else {}
    try:
        return snowflake.connector.connect(connection_name=connection_name, **extra)
    except Exception as exc:
        raise SnowflakeLoadError(
            f"nao foi possivel conectar pela conexao '{connection_name}' de "
            f"~/.snowflake/config.toml: {exc}"
        ) from exc


def require_warehouse(cursor) -> None:
    """Confere que a sessao tem compute. Roda ANTES de qualquer consulta a dado.

    `current_warehouse()` e funcao de sessao e nao precisa de warehouse para responder — e
    por isso serve de sonda. Quando o papel nao tem USAGE no warehouse pedido pela conexao,
    o Snowflake nao recusa a conexao: entrega uma sessao sem compute, e o erro so aparece
    na primeira consulta a dado, apontando para a sessao em vez de para o grant.
    """
    atual = cursor.execute("select current_warehouse()").fetchone()[0]
    if not atual:
        raise SnowflakeLoadError(
            "a sessao esta sem warehouse ativo. O papel desta conexao provavelmente nao "
            "tem `usage` no warehouse — rode `make warehouse-bootstrap`, que concede "
            f"usage on warehouse {DEFAULT_WAREHOUSE} aos tres papeis."
        )


def require_schemas(cursor, database: str, schemas=(STAGE_SCHEMA,)) -> None:
    """Confere que o bootstrap ja rodou. NAO cria nada — e essa a diferenca.

    Confere APENAS os schemas de que o chamador precisa, e o default e so o STAGE. Conferir
    os tres reprovaria a carga por um motivo certo: `information_schema.schemata` devolve
    apenas o que o papel enxerga, e RETAIL_LOADER nao enxerga GOLD nem MART — que e
    precisamente o isolamento pedido. Uma verificacao mais ampla que a necessidade
    transforma o controle funcionando em falha de execucao.

    Antes de os papeis serem vestidos, `load` fazia `create database if not exists` e
    `create schema if not exists`. Funcionava porque tudo rodava como ACCOUNTADMIN, e era
    justamente o sintoma: o carregador podia criar database. RETAIL_LOADER nao pode, e nao
    deve — criar infraestrutura e trabalho de uma vez so, com privilegio administrativo, e
    ja e o que `bootstrap` faz.

    Conferir em vez de criar tambem melhora o erro: "rode make warehouse-bootstrap" diz o
    que fazer, e "Insufficient privileges to operate on database" nao.
    """
    try:
        linhas = cursor.execute(
            f"select schema_name from {database}.information_schema.schemata"
        ).fetchall()
    except Exception as exc:
        raise SnowflakeLoadError(
            f"o database {database} nao existe ou este papel nao o enxerga ({exc}). "
            f"Rode `make warehouse-bootstrap` uma vez por conta antes de carregar."
        ) from exc

    existentes = {linha[0].upper() for linha in linhas}
    faltando = [nome for nome in schemas if nome.upper() not in existentes]
    if faltando:
        raise SnowflakeLoadError(
            f"schema ausente em {database}: {', '.join(faltando)}. "
            f"Rode `make warehouse-bootstrap` uma vez por conta antes de carregar."
        )


# O que cada papel PODE e NAO PODE ler. Escrito como matriz para ser executavel: um grant
# nao verificado e uma intencao, nao um controle.
ISOLATION_MATRIX = {
    "RETAIL_READER": {
        "allow": ["{db}.MART.MART_CUSTOMER_BASE"],
        "deny": ["{db}.GOLD.DIM_CUSTOMER", "{db}.STAGE.STG_CUSTOMER"],
    },
    "RETAIL_TRANSFORMER": {
        "allow": ["{db}.STAGE.STG_CUSTOMER", "{db}.GOLD.DIM_CUSTOMER"],
        "deny": [],
    },
    "RETAIL_LOADER": {
        "allow": ["{db}.STAGE.STG_CUSTOMER"],
        "deny": ["{db}.MART.MART_CUSTOMER_BASE", "{db}.GOLD.DIM_CUSTOMER"],
    },
}


def check_isolation(connect_as_role, database: str = DEFAULT_DATABASE) -> list[str]:
    """Prova a matriz acima abrindo uma sessao por papel. Devolve as violacoes.

    `USE SECONDARY ROLES NONE` NAO E DETALHE — e o que torna este teste valido.
    Contas Snowflake modernas vem com DEFAULT_SECONDARY_ROLES = ('ALL'), e nesse modo a
    sessao ativa TODOS os papeis do usuario alem do primario. Medido nesta conta: com
    secondary roles ativos, RETAIL_READER lia GOLD e STAGE sem problema, porque o mesmo
    usuario tambem tem ACCOUNTADMIN — e `show grants to role RETAIL_READER` continuava
    mostrando apenas MART, ou seja, a configuracao parecia certa e o comportamento nao era.
    Uma verificacao de RBAC feita da sessao de um admin sem desligar isso passa por
    engano, sempre.

    Em producao o isolamento de verdade vem de um USUARIO DE SERVICO por papel, sem
    ACCOUNTADMIN. Aqui ha um usuario so, entao desligar os papeis secundarios e a forma de
    provar que os grants estao corretos.
    """
    problemas = []
    for papel, esperado in ISOLATION_MATRIX.items():
        try:
            connection = connect_as_role(papel)
        except Exception as exc:
            problemas.append(f"{papel}: nao foi possivel abrir sessao ({exc})")
            continue
        cursor = connection.cursor()
        try:
            cursor.execute("use secondary roles none")
            for alvo in esperado["allow"]:
                tabela = alvo.format(db=database)
                try:
                    cursor.execute(f"select count(*) from {tabela}").fetchone()
                except Exception:
                    problemas.append(f"{papel}: DEVERIA ler {tabela} e foi negado")
            for alvo in esperado["deny"]:
                tabela = alvo.format(db=database)
                try:
                    cursor.execute(f"select count(*) from {tabela}").fetchone()
                    problemas.append(f"{papel}: NAO deveria ler {tabela} e leu")
                except Exception:
                    pass
        finally:
            cursor.close()
            connection.close()
    return problemas


def qualified(table: str, database: str = DEFAULT_DATABASE) -> str:
    return f"{database}.{STAGE_SCHEMA}.{table}"


def transfer_statements(table: str, parquet_path: str, database: str = DEFAULT_DATABASE) -> list[str]:
    """Os dois comandos que movem um parquet para uma tabela STAGE.

    Funcao propria, e nao duas f-strings dentro do laco de `load`, para que a suite possa
    conferi-los SEM Snowflake. As duas decisoes abaixo custaram uma execucao real cada uma,
    e nenhuma falharia de forma obvia se voltasse atras.
    """
    alvo = qualified(table, database)

    # STAGE DE TABELA SEMPRE QUALIFICADO. `@%TABELA` resolve contra o schema CORRENTE da
    # sessao, e `create schema` do Snowflake TROCA o schema corrente — medido: ensure_schemas
    # termina em MART, e o PUT passava a procurar RETAIL.MART.%STG_PRODUCT_PRICE, que nao
    # existe. Qualificar torna o comando independente do estado da sessao, que e o que se
    # quer num carregador.
    stage = f"@{database}.{STAGE_SCHEMA}.%{table}"

    return [
        # OVERWRITE = TRUE: o stage de tabela guarda o arquivo entre execucoes, e sem isso
        # um COPY seguinte leria o arquivo antigo junto do novo e dobraria as linhas.
        # AUTO_COMPRESS = FALSE: o parquet ja sai comprimido em zstd do DuckDB; deixar o
        # PUT envolver tudo num gzip so faria o Snowflake desembrulhar duas vezes.
        f"put file://{os.path.abspath(parquet_path)} {stage} "
        f"overwrite = true auto_compress = false",

        # MATCH_BY_COLUMN_NAME dispensa a ordem das colunas bater entre o parquet e a
        # tabela. Sem isso, acrescentar uma coluna no meio do recorte carregaria dado na
        # coluna errada em silencio — o unico erro desta camada que nenhum teste a jusante
        # pegaria, porque o resultado continua sendo um numero plausivel.
        f"copy into {alvo} from {stage} "
        # USE_LOGICAL_TYPE = TRUE. Sem isto, TODO timestamp atravessa a fronteira 56
        # MILHOES DE ANOS no futuro, e nada reprova.
        #
        # O DuckDB anota a unidade do timestamp so no LogicalType moderno do parquet
        # (Timestamp(timeUnit=microseconds)) e deixa o ConvertedType legado em NONE. O
        # leitor do Snowflake ignora o LogicalType por padrao, cai no legado, nao acha
        # unidade nenhuma e assume MILISSEGUNDOS: 1,787e15 microssegundos viram 1,787e15
        # milissegundos, ou seja o ano 56.648.666.
        #
        # POR QUE ISTO SO APARECEU AGORA: era a primeira vez que um TIMESTAMP cruzava a
        # fronteira. Ate o Marco 6 o recorte inteiro so tinha DATE (ingestion_date,
        # snapshot_date, valid_from), e DATE viaja como date32 sem ambiguidade de unidade.
        #
        # E POR QUE NADA PEGOU: a reconferencia do carregador compara CONTAGEM de linhas, e
        # a contagem estava certa. O dbt construiu os 166 nos em verde — as duracoes viraram
        # numeros grandes, nao nulos nem erros, e um funil feito de `count_if(x is not
        # null)` continua exato quando `x` esta 56 milhoes de anos deslocado. Quem apontou
        # foi ler o mart de SLA e ver 80.000.060 minutos de separacao. Por isso existe
        # agora assert_order_milestones_are_plausible_against_the_order_date, que reprova
        # se um marco cair fora da janela do proprio pedido.
        f"file_format = (type = parquet use_logical_type = true) "
        f"match_by_column_name = case_insensitive "
        f"on_error = abort_statement",
    ]


# Tres papeis, um por VERBO do pipeline — nao um por pessoa. Quem carrega nao modela, quem
# modela nao le o RAW, e quem consome nao escreve em lugar nenhum. E o que torna "camada
# governada" uma afirmacao verificavel em vez de um slogan: sem os grants, o Snowflake aqui
# seria um DuckDB caro.
ROLES = {
    "RETAIL_LOADER": "Loads the Silver cut into STAGE. Does not read GOLD or MART.",
    "RETAIL_TRANSFORMER": "Reads STAGE and writes GOLD and MART. This is dbt's role.",
    "RETAIL_READER": "Read-only on MART. This is the BI and third-party role.",
}


def bootstrap(
    cursor,
    database: str,
    grant_to_user: str | None = None,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> list[str]:
    """Database, schemas, papeis e grants. Idempotente, e separado de `load` de proposito.

    Criar papel exige ACCOUNTADMIN e e infraestrutura de uma vez so; misturar isso na carga
    diaria significaria pedir ACCOUNTADMIN todo dia para mover parquet. O carregador roda
    com RETAIL_LOADER depois disto.
    """
    feito = []

    def executa(sql, descricao):
        cursor.execute(sql)
        feito.append(descricao)

    # O recorte inteiro tem ~146 mil linhas e o `dbt build` fecha em ~12 s. X-Small com
    # suspensao rapida e a escolha proporcional; o default de 300 s cobraria cinco minutos
    # de compute ocioso a cada execucao, o que numa conta trial e a diferenca entre
    # demonstrar o fluxo varias vezes e nao poder.
    executa(
        f"alter warehouse {warehouse} set warehouse_size = 'XSMALL' "
        f"auto_suspend = 60 auto_resume = true",
        f"warehouse {warehouse}: xsmall, auto_suspend 60s",
    )

    executa(f"create database if not exists {database}", f"database {database}")
    for nome, comentario in SCHEMAS.items():
        executa(
            f"create schema if not exists {database}.{nome} comment = '{comentario}'",
            f"schema {nome}",
        )
    for nome, comentario in ROLES.items():
        executa(f"create role if not exists {nome} comment = '{comentario}'", f"role {nome}")
        # Papel customizado pendurado em SYSADMIN — convencao do proprio Snowflake, e aqui
        # ela deixou de ser cosmetica. Assim que a POSSE das tabelas passou para os papeis
        # do projeto e os papeis secundarios foram desligados, ACCOUNTADMIN parou de ver os
        # objetos em information_schema: ele nao herdava esses papeis. Sem esta linha,
        # vestir o papel custaria a visibilidade administrativa da conta — e o isolamento
        # nao muda, porque heranca sobe (SYSADMIN passa a ver MART), nunca desce
        # (RETAIL_READER continua sem GOLD).
        executa(f"grant role {nome} to role SYSADMIN", f"{nome} -> SYSADMIN")

    # USAGE no database e pre-requisito de qualquer coisa; sem ele o papel nem enxerga os
    # schemas que tem permissao de ler. USAGE no WAREHOUSE e o outro pre-requisito, e e
    # facil de esquecer porque ACCOUNTADMIN nunca precisa dele: os tres papeis leem e
    # escrevem DADO, e dado sem compute nao se move. Sem esta linha, `load` morre com "No
    # active warehouse selected" — erro que aponta para a sessao, nao para o grant que
    # falta.
    for nome in ROLES:
        executa(f"grant usage on database {database} to role {nome}", f"usage db -> {nome}")
        executa(f"grant usage on warehouse {warehouse} to role {nome}", f"usage wh -> {nome}")

    # LOADER: escreve STAGE, e so.
    executa(f"grant usage on schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: usage stage")
    executa(f"grant create table on schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: create table")
    executa(f"grant all on all tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: tabelas")
    executa(f"grant all on future tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_LOADER", "loader: tabelas futuras")

    # POSSE, nao so privilegio. `ddl_for` emite `create or replace table`, e substituir uma
    # tabela exige OWNERSHIP sobre a que ja existe — `grant all` concede todos os
    # privilegios APLICAVEIS e posse nao e um deles. Numa conta nova isto e inocuo (o
    # LOADER cria as tabelas e ja nasce dono); numa conta onde a carga rodou como
    # ACCOUNTADMIN antes, e o que permite parar de rodar. `copy current grants` preserva o
    # select do TRANSFORMER, que sem isso seria revogado junto com a posse.
    executa(
        f"grant ownership on all tables in schema {database}.{STAGE_SCHEMA} "
        f"to role RETAIL_LOADER copy current grants",
        "loader: posse das tabelas stage",
    )

    # TRANSFORMER: LE o stage (nunca escreve nele) e escreve GOLD e MART.
    executa(f"grant usage on schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: usage stage")
    executa(f"grant select on all tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: select stage")
    executa(f"grant select on future tables in schema {database}.{STAGE_SCHEMA} to role RETAIL_TRANSFORMER", "transformer: select stage futuro")
    # `grant all on SCHEMA` da privilegio no schema (usage, create table), NAO nas tabelas
    # que ja existem dentro dele — elas pertencem a quem as criou. Sem os dois grants de
    # tabela abaixo, o TRANSFORMER pode criar tabelas novas em GOLD e nao consegue LER as
    # que ja estao la. Isso passou despercebido ate check_isolation reprovar.
    for camada in ("GOLD", "MART"):
        executa(f"grant all on schema {database}.{camada} to role RETAIL_TRANSFORMER",
                f"transformer: schema {camada}")
        executa(f"grant all on all tables in schema {database}.{camada} to role RETAIL_TRANSFORMER",
                f"transformer: tabelas {camada}")
        executa(f"grant all on future tables in schema {database}.{camada} to role RETAIL_TRANSFORMER",
                f"transformer: tabelas futuras {camada}")
        # POSSE. Mesma razao do STAGE, um nivel acima: o dbt materializa com
        # `create or replace table`, e substituir exige OWNERSHIP sobre a tabela existente.
        # Medido: com `grant all` e sem posse, o `dbt build` reprovou 8 modelos com
        # "must have OWNERSHIP granted on TABLE". Inocuo numa conta nova, onde o proprio
        # TRANSFORMER cria e ja nasce dono. `copy current grants` preserva o select do
        # READER em MART — sem ele, vestir o papel silenciaria o consumidor.
        for objeto in ("tables", "views"):
            executa(
                f"grant ownership on all {objeto} in schema {database}.{camada} "
                f"to role RETAIL_TRANSFORMER copy current grants",
                f"transformer: posse de {objeto} em {camada}",
            )

    # READER: so MART, so leitura. Nao enxerga STAGE nem GOLD — quem consome nao precisa
    # saber como o numero foi feito, e nao deveria poder ler o intermediario.
    executa(f"grant usage on schema {database}.MART to role RETAIL_READER", "reader: usage mart")
    executa(f"grant select on all tables in schema {database}.MART to role RETAIL_READER", "reader: select mart")
    executa(f"grant select on future tables in schema {database}.MART to role RETAIL_READER", "reader: select mart futuro")

    if grant_to_user:
        for nome in ROLES:
            executa(f"grant role {nome} to user {grant_to_user}", f"{nome} -> {grant_to_user}")

        # SEM ISTO, VESTIR O PAPEL NAO SIGNIFICA NADA. Contas Snowflake modernas vem com
        # DEFAULT_SECONDARY_ROLES = ('ALL'): a sessao ativa TODOS os papeis do usuario alem
        # do primario. Medido nesta conta antes da mudanca:
        #   current_role() = ACCOUNTADMIN
        #   current_secondary_roles() = ORGADMIN, RETAIL_READER, RETAIL_TRANSFORMER, RETAIL_LOADER
        # Ou seja: conectar como RETAIL_LOADER continuaria carregando ACCOUNTADMIN por
        # baixo, e todo teste de permissao passaria por engano. Desligar no USUARIO faz o
        # papel primario valer para toda conexao — a do dbt inclusive, que nao tem onde
        # rodar um `use secondary roles none`.
        executa(
            f"alter user {grant_to_user} set default_secondary_roles = ()",
            f"papeis secundarios desligados para {grant_to_user}",
        )

    return feito


def load(connection, duck_connection, stage_dir: str, database: str = DEFAULT_DATABASE) -> dict:
    """Cria as tabelas STAGE e carrega os parquet de stage_dir. Devolve o resumo.

    `duck_connection` entra so para DESCREVER o recorte — o schema das tabelas vem da
    propria query, nunca de um DDL escrito a mao ao lado (ver snowflake_export). Nenhuma
    linha de dado passa pelo DuckDB aqui: os bytes ja estao no parquet.
    """
    cursor = connection.cursor()
    require_warehouse(cursor)
    require_schemas(cursor, database)

    resumo = {}
    for spec in SPECS:
        nome = spec["name"]
        caminho = os.path.join(stage_dir, f"{nome}.parquet")
        if not os.path.exists(caminho):
            raise SnowflakeLoadError(
                f"parquet ausente: {caminho}. Rode `make warehouse-export` antes de carregar."
            )

        colunas = _describe(duck_connection, spec["sql"].strip())
        cursor.execute(ddl_for(spec, colunas, database))
        for comando in transfer_statements(nome, caminho, database):
            cursor.execute(comando)

        alvo = qualified(nome, database)
        carregadas = cursor.execute(f"select count(*) from {alvo}").fetchone()[0]
        resumo[nome] = {"rows": carregadas, "bytes": os.path.getsize(caminho)}

    cursor.close()
    return resumo


def verify(connection, stage_dir: str, expected: dict, database: str = DEFAULT_DATABASE) -> list[str]:
    """Reconfere a contagem de cada tabela contra o que o export declarou.

    Mesma disciplina de `verify-landing`: carregar sem reconferir e confiar no transporte.
    Devolve a lista de divergencias — vazia quando tudo bate.
    """
    cursor = connection.cursor()
    problemas = []
    for spec in SPECS:
        nome = spec["name"]
        alvo = qualified(nome, database)
        try:
            destino = cursor.execute(f"select count(*) from {alvo}").fetchone()[0]
        except Exception as exc:
            problemas.append(f"{nome}: nao foi possivel contar no destino ({exc})")
            continue
        origem = expected.get(nome, {}).get("rows")
        if origem is None:
            problemas.append(f"{nome}: o export nao declarou contagem")
        elif origem != destino:
            problemas.append(f"{nome}: origem {origem} != destino {destino}")
    cursor.close()
    return problemas
