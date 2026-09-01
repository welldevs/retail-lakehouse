"""A sessao Spark com o catalogo Iceberg do projeto. UM lugar, dois consumidores.

    scripts/spike_spark_iceberg.py   o experimento que decidiu se o Spark entrava
    jobs/spark/stock_ledger.py       o job de producao

POR QUE O EXPERIMENTO IMPORTA DAQUI, EM VEZ DE GUARDAR A PROPRIA COPIA. A tentacao era
congelar a configuracao dentro do spike, para que a evidencia registrada descrevesse
exatamente o que foi medido. Seria pior: no dia em que a configuracao de producao mudasse,
o spike continuaria APROVANDO uma configuracao que ninguem usa mais. Importando daqui,
`make spike-spark-iceberg` vira uma conferencia viva — ele mede o que o job vai usar.

E o mesmo argumento do CONTRACT do painel e do DDL do STAGE: o documento e derivado da
coisa, entao nao ha um segundo lugar onde a verdade mora.
"""

from __future__ import annotations

import os

CATALOGO = os.environ.get("ICEBERG_CATALOG_NAME", "retail")


def sessao(app_name: str, *, master: str = "local[*]"):
    """Sessao com o catalogo apontado para o MESMO Postgres que o pyiceberg escreve.

    Cada propriedade e uma escolha, e as tres que decidem:

    `catalog-impl=JdbcCatalog` — o `SqlCatalog` do pyiceberg grava em duas tabelas cujo
    formato e o que o JdbcCatalog Java espera. As duas implementacoes sao independentes;
    que elas conversem foi MEDIDO no Marco A, e nao presumido.

    `io-impl=S3FileIO` e nao S3A — o pyiceberg escreveu os caminhos como `s3://` com o
    PyArrowFileIO. O S3A do Hadoop pediria `s3a://` e nao acharia nada: o metadado ja esta
    escrito e nao vai mudar de esquema para agradar o leitor. Como efeito colateral, a
    imagem nao precisa do `hadoop-aws` nem do bundle do AWS SDK v1.

    `local[*]` — driver e executor no mesmo JVM. O que este projeto demonstra com Spark e
    interoperabilidade e FORMA do job, nunca escala: o gatilho de volume nao disparou e isso
    esta medido. Um cluster de mentira nao demonstraria nem uma coisa nem outra.
    """
    from pyspark.sql import SparkSession

    prefixo = f"spark.sql.catalog.{CATALOGO}"
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(prefixo, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefixo}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{prefixo}.uri", os.environ["ICEBERG_CATALOG_JDBC"])
        .config(f"{prefixo}.jdbc.user", os.environ.get("ICEBERG_CATALOG_USER", "oltp"))
        .config(f"{prefixo}.jdbc.password",
                os.environ.get("ICEBERG_CATALOG_PASSWORD", "oltp"))
        .config(f"{prefixo}.warehouse",
                os.environ.get("ICEBERG_WAREHOUSE", "s3://retail-lakehouse/iceberg"))
        .config(f"{prefixo}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefixo}.s3.endpoint",
                os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config(f"{prefixo}.s3.path-style-access", "true")
        .config(f"{prefixo}.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config(f"{prefixo}.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(f"{prefixo}.client.region", os.environ.get("AWS_REGION", "us-east-1"))
        # UTC em todo lugar, como no resto da plataforma: `cast(picked_at as date)` do
        # DuckDB ja resolveu o dia; a sessao nao pode desloca-lo de novo.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
