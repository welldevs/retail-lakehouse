"""Configuracao lida do ambiente. Nenhuma credencial no codigo.

Le um arquivo .env se existir, para que o CLI funcione sem depender do Makefile. O
parser e deliberadamente minimo (stdlib): a plataforma nao precisa de python-dotenv.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_RAW_BUCKET = "retail-raw"
DEFAULT_LAKEHOUSE_BUCKET = "retail-lakehouse"
# MinIO ignora a regiao, mas boto3 exige uma para assinar a requisicao.
DEFAULT_REGION = "us-east-1"


class ConfigError(Exception):
    """Ambiente sem informacao suficiente para falar com o object storage."""


def load_dotenv(path: str = ".env") -> None:
    """Carrega KEY=VALUE de um .env para o ambiente, sem sobrescrever o que ja existe.

    Variavel ja presente no ambiente ganha do arquivo: assim o Makefile, o Airflow ou um
    export manual sempre tem a ultima palavra sobre o .env de desenvolvimento.
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
        """Cliente S3 apontado para o endpoint configurado.

        Importa boto3 aqui, e nao no topo do modulo, para que ler o manifesto e validar
        uma particao nao exija boto3 instalado.
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
    """Monta a configuracao do ambiente. Aceita credencial AWS_* ou MINIO_ROOT_*."""
    load_dotenv(dotenv)

    access = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD")
    if not access or not secret:
        raise ConfigError(
            "credencial ausente: defina AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY ou "
            "MINIO_ROOT_USER/MINIO_ROOT_PASSWORD (veja .env.example)."
        )

    return Config(
        endpoint=os.environ.get("S3_ENDPOINT", DEFAULT_ENDPOINT),
        access_key=access,
        secret_key=secret,
        raw_bucket=os.environ.get("RAW_BUCKET", DEFAULT_RAW_BUCKET),
        lakehouse_bucket=os.environ.get("LAKEHOUSE_BUCKET", DEFAULT_LAKEHOUSE_BUCKET),
        region=os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION),
    )
