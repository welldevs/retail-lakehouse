"""Configuration read from the environment. No credentials in the code.

Reads a .env file if it exists, so the CLI works without depending on the Makefile. The
parser is deliberately minimal (stdlib): the platform does not need python-dotenv.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_RAW_BUCKET = "retail-raw"
DEFAULT_LAKEHOUSE_BUCKET = "retail-lakehouse"
# MinIO ignores the region, but boto3 requires one to sign the request.
DEFAULT_REGION = "us-east-1"


class ConfigError(Exception):
    """Environment without enough information to talk to the object storage."""


def load_dotenv(path: str = ".env") -> None:
    """Loads KEY=VALUE from a .env into the environment, without overwriting what exists.

    A variable already present in the environment wins over the file: that way the
    Makefile, Airflow or a manual export always has the last word over the development .env.
    """
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


@dataclass(frozen=True)
class Config:
    endpoint: str
    access_key: str
    secret_key: str
    raw_bucket: str
    lakehouse_bucket: str
    region: str = DEFAULT_REGION

    def client(self):
        """S3 client pointed at the configured endpoint.

        Imports boto3 here, not at the top of the module, so that reading the manifest and
        validating a partition do not require boto3 to be installed.
        """
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )


def from_env(dotenv: str = ".env") -> Config:
    """Builds the configuration from the environment. Accepts AWS_* or MINIO_ROOT_* credentials."""
    load_dotenv(dotenv)

    access = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD")
    if not access or not secret:
        raise ConfigError(
            "missing credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or "
            "MINIO_ROOT_USER/MINIO_ROOT_PASSWORD (see .env.example)."
        )

    return Config(
        endpoint=os.environ.get("S3_ENDPOINT", DEFAULT_ENDPOINT),
        access_key=access,
        secret_key=secret,
        raw_bucket=os.environ.get("RAW_BUCKET", DEFAULT_RAW_BUCKET),
        lakehouse_bucket=os.environ.get("LAKEHOUSE_BUCKET", DEFAULT_LAKEHOUSE_BUCKET),
        region=os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION),
    )
