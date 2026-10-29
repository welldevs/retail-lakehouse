"""Querying Silver.

The retail.duckdb FILE CONTAINS NO DATA. It holds four VIEWs pointing at the parquet in
the object storage — the real data lives in s3://<lakehouse>/silver/. This is
deliberate: the object storage is the truth, and the .duckdb is disposable local state
(it is in .gitignore and `make clean-duckdb` deletes it without loss).

The practical consequence is that opening the file with an arbitrary DuckDB client FAILS
with `NoSuchBucket`: the new session knows neither the endpoint nor the credentials, and
DuckDB tries real AWS. Two ways out, both offered here:

  - `connect()` returns an already-configured connection, for programmatic use;
  - `create_persistent_secret()` writes a DuckDB secret to the user's profile
    (~/.duckdb/stored_secrets), after which ANY DuckDB client on the machine opens the
    file and queries the views without configuring anything.
"""

from __future__ import annotations

from urllib.parse import urlparse

SECRET_NAME = "retail_minio"
DEFAULT_DB = "platform/dbt/retail.duckdb"


def _endpoint_host(endpoint: str) -> str:
    """DuckDB wants host:port WITHOUT a scheme; boto3 wants the full URL.

    The check for a missing "://" is not defensiveness: urlparse("minio:9000") parses
    `minio` as the SCHEME and `9000` as the path, returning "9000". An S3_ENDPOINT written
    without a scheme — a perfectly reasonable form — would reach DuckDB with only the port.
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
    """Connection to the .duckdb with httpfs and S3 configured. Imports duckdb on demand."""
    import duckdb

    connection = duckdb.connect(database, read_only=read_only)
    _apply_s3_settings(connection, config)
    return connection


def connect_lakehouse(config):
    """IN-MEMORY connection with views over the object storage's parquet.

    It does not touch the .duckdb file, and that is its reason to exist: DuckDB is
    single-writer, and a client with the file open for writing (DBeaver opens it that way
    by default) blocks every other process — readers included. Querying Silver should not
    depend on someone having closed a window.

    It also does not depend on dbt having run on this machine: the views are derived from
    what is in the bucket, which is the truth.
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
            # `silver/x.parquet` becomes a view over the file; `silver/x/…/y.parquet`
            # becomes a view over the whole partitioned tree.
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
    """Writes the S3 secret to the user's profile and returns where it was stored.

    After this, `duckdb platform/dbt/retail.duckdb` works directly. The secret stores the
    credentials in ~/.duckdb — acceptable for the local development MinIO, and the reason
    the value comes from .env instead of being written in the code.
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
    return row[0] if row else "unknown"
