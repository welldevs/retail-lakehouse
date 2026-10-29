"""The Spark session with the project's Iceberg catalog. ONE place, two consumers.

    scripts/check_spark_iceberg.py   the experiment that decided whether Spark came in
    jobs/spark/stock_ledger.py       the production job

WHY THE EXPERIMENT IMPORTS FROM HERE INSTEAD OF KEEPING ITS OWN COPY. The temptation was to
freeze the configuration inside the spike, so the recorded evidence would describe exactly
what was measured. That would be worse: the day the production configuration changed, the
spike would keep APPROVING a configuration nobody uses anymore. By importing from here,
`make check-spark-iceberg` becomes a live check — it measures what the job will use.

It is the same argument as the dashboard CONTRACT and the STAGE DDL: the document is
derived from the thing, so there is no second place where the truth lives.

(`CATALOGO` and `sessao` keep their historical names because both consumers import them.)
"""

from __future__ import annotations

import os

CATALOGO = os.environ.get("ICEBERG_CATALOG_NAME", "retail")


def sessao(app_name: str, *, master: str = "local[*]"):
    """Session with the catalog pointed at the SAME Postgres pyiceberg writes to.

    Every property is a choice; these are the three that matter:

    `catalog-impl=JdbcCatalog` — pyiceberg's `SqlCatalog` writes to two tables whose format
    is what the Java JdbcCatalog expects. The two implementations are independent; that
    they talk to each other was MEASURED in Milestone A, not assumed.

    `io-impl=S3FileIO`, not S3A — pyiceberg wrote the paths as `s3://` with PyArrowFileIO.
    Hadoop's S3A would ask for `s3a://` and find nothing: the metadata is already written
    and will not change scheme to please the reader. As a side effect, the image needs
    neither `hadoop-aws` nor the AWS SDK v1 bundle.

    `local[*]` — driver and executor in the same JVM. What this project demonstrates with
    Spark is interoperability and the SHAPE of the job, never scale: the volume trigger did
    not fire and that is measured. A fake cluster would demonstrate neither one.
    """
    from pyspark.sql import SparkSession

    prefix = f"spark.sql.catalog.{CATALOGO}"
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{prefix}.uri", os.environ["ICEBERG_CATALOG_JDBC"])
        .config(f"{prefix}.jdbc.user", os.environ.get("ICEBERG_CATALOG_USER", "oltp"))
        .config(f"{prefix}.jdbc.password",
                os.environ.get("ICEBERG_CATALOG_PASSWORD", "oltp"))
        .config(f"{prefix}.warehouse",
                os.environ.get("ICEBERG_WAREHOUSE", "s3://retail-lakehouse/iceberg"))
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint",
                os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config(f"{prefix}.s3.path-style-access", "true")
        .config(f"{prefix}.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config(f"{prefix}.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(f"{prefix}.client.region", os.environ.get("AWS_REGION", "us-east-1"))
        # UTC everywhere, as in the rest of the platform: DuckDB's `cast(picked_at as date)`
        # already resolved the day; the session must not shift it again.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
