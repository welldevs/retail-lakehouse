"""Consulta ao Silver.

O ARQUIVO retail.duckdb NAO CONTEM DADO. Ele guarda quatro VIEWs que apontam para o
parquet no object storage — o dado real vive em s3://<lakehouse>/silver/. Isso e
deliberado: o object storage e a verdade, e o .duckdb e estado local descartavel
(esta no .gitignore e `make clean-duckdb` o apaga sem perda).

A consequencia pratica e que abrir o arquivo com um cliente DuckDB qualquer FALHA com
`NoSuchBucket`: a sessao nova nao sabe o endpoint nem a credencial, e o DuckDB tenta a
AWS de verdade. Duas saidas, ambas oferecidas aqui:

  - `connect()` devolve uma conexao ja configurada, para uso programatico;
  - `create_persistent_secret()` grava um secret do DuckDB no perfil do usuario
    (~/.duckdb/stored_secrets), depois do que QUALQUER cliente DuckDB da maquina abre o
    arquivo e consulta as views sem configurar nada.
"""

from __future__ import annotations

from urllib.parse import urlparse

SECRET_NAME = "retail_minio"
DEFAULT_DB = "platform/dbt/retail.duckdb"


def _endpoint_host(endpoint: str) -> str:
    """O DuckDB quer host:porta SEM esquema; o boto3 quer a URL completa.

    O teste de ausencia de "://" nao e defensivismo: urlparse("minio:9000") interpreta
    `minio` como ESQUEMA e `9000` como caminho, devolvendo "9000". Um S3_ENDPOINT escrito
    sem esquema — forma perfeitamente razoavel — chegaria ao DuckDB so com a porta.
    """
    if "://" not in endpoint:
        return endpoint
    return urlparse(endpoint).netloc or endpoint


def _apply_s3_settings(connection, config) -> None:
    connection.execute("install httpfs; load httpfs;")
    connection.execute(f"set s3_endpoint='{_endpoint_host(config.endpoint)}'")
    connection.execute(f"set s3_access_key_id='{config.access_key}'")
    connection.execute(f"set s3_secret_access_key='{config.secret_key}'")
    connection.execute("set s3_use_ssl=false")
    connection.execute("set s3_url_style='path'")


def connect(config, database: str = DEFAULT_DB, read_only: bool = True):
    """Conexao ao .duckdb com httpfs e S3 configurados. Importa duckdb sob demanda."""
    import duckdb

    connection = duckdb.connect(database, read_only=read_only)
    _apply_s3_settings(connection, config)
    return connection


def connect_lakehouse(config):
    """Conexao EM MEMORIA com views sobre o parquet do object storage.

    Nao toca o arquivo .duckdb, e essa e a razao de existir: o DuckDB e single-writer, e um
    cliente com o arquivo aberto em escrita (DBeaver abre assim por padrao) bloqueia
    qualquer outro processo — inclusive leitores. Consultar o Silver nao deveria depender
    de ninguem ter fechado uma janela.

    Tambem nao depende de o dbt ter rodado nesta maquina: as views sao derivadas do que
    esta no bucket, que e a verdade.
    """
    import duckdb

    connection = duckdb.connect(":memory:")
    _apply_s3_settings(connection, config)

    prefix = f"s3://{config.lakehouse_bucket}/silver/"
    client = config.client()
    files, directories = set(), set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.lakehouse_bucket, Prefix="silver/"):
        for item in page.get("Contents") or []:
            rest = item["Key"][len("silver/"):]
            if not rest.endswith(".parquet"):
                continue
            head, _, tail = rest.partition("/")
            # `silver/x.parquet` vira uma view sobre o arquivo; `silver/x/…/y.parquet`
            # vira uma view sobre a arvore particionada inteira.
            (files if not tail else directories).add(head)

    for name in sorted(files):
        connection.execute(
            f'create view "{name[:-len(".parquet")]}" as '
            f"select * from read_parquet('{prefix}{name}')"
        )
    for name in sorted(directories):
        connection.execute(
            f'create view "{name}" as select * from '
            f"read_parquet('{prefix}{name}/**/*.parquet', hive_partitioning = 1)"
        )
    return connection


def create_persistent_secret(config) -> str:
    """Grava o secret S3 no perfil do usuario e devolve onde ficou.

    Depois disto, `duckdb platform/dbt/retail.duckdb` funciona direto. O secret guarda a
    credencial em ~/.duckdb — aceitavel para o MinIO local de desenvolvimento, e a razao
    de o valor vir do .env em vez de estar escrito no codigo.
    """
    import duckdb

    connection = duckdb.connect()
    connection.execute("install httpfs; load httpfs;")
    connection.execute(
        f"""
        create or replace persistent secret {SECRET_NAME} (
            type      s3,
            key_id    '{config.access_key}',
            secret    '{config.secret_key}',
            endpoint  '{_endpoint_host(config.endpoint)}',
            use_ssl   false,
            url_style 'path'
        )
        """
    )
    row = connection.execute(
        "select storage from duckdb_secrets() where name = ?", [SECRET_NAME]
    ).fetchone()
    connection.close()
    return row[0] if row else "desconhecido"
