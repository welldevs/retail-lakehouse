"""Precedencia de configuracao. Sem rede.

A regra "ambiente ganha do .env" nao e detalhe de estilo: e o que faz o container do
Airflow funcionar. O .env do repositorio diz S3_ENDPOINT=http://localhost:9000, e dentro da
rede do compose o endpoint e http://minio:9000. Se o arquivo sobrepusesse o ambiente, o
compose nao teria como corrigir isso e todo acesso ao object storage falharia de dentro do
container.
"""

from __future__ import annotations

import os
import pathlib
import re
import tempfile
import unittest

from retail_platform.config import ConfigError, Config, from_env, load_dotenv
from retail_platform.query import _endpoint_host


class EnvIsolated(unittest.TestCase):
    """Cada teste comeca com o ambiente limpo das variaveis que o config le."""

    KEYS = (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
        "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
        "S3_ENDPOINT", "RAW_BUCKET", "LAKEHOUSE_BUCKET",
    )

    def setUp(self):
        self._saved = {k: os.environ.pop(k, None) for k in self.KEYS}

        def restore():
            for key, value in self._saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.addCleanup(restore)

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def write_dotenv(self, body: str) -> str:
        path = os.path.join(self._tmp.name, ".env")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(body)
        return path


class TestDotenv(EnvIsolated):
    def test_environment_wins_over_the_file(self):
        """A regra que faz o compose poder corrigir o endpoint dentro da rede."""
        os.environ["S3_ENDPOINT"] = "http://minio:9000"
        load_dotenv(self.write_dotenv("S3_ENDPOINT=http://localhost:9000\n"))
        self.assertEqual(os.environ["S3_ENDPOINT"], "http://minio:9000")

    def test_file_fills_what_the_environment_lacks(self):
        load_dotenv(self.write_dotenv("RAW_BUCKET=do-arquivo\n"))
        self.assertEqual(os.environ["RAW_BUCKET"], "do-arquivo")

    def test_ignores_comments_blank_lines_and_quotes(self):
        load_dotenv(self.write_dotenv(
            '# comentario\n\nRAW_BUCKET="entre-aspas"\nsem_igual\n'
        ))
        self.assertEqual(os.environ["RAW_BUCKET"], "entre-aspas")

    def test_missing_file_is_not_an_error(self):
        load_dotenv(os.path.join(self._tmp.name, "nao-existe"))  # nao levanta


class TestFromEnv(EnvIsolated):
    def test_accepts_minio_credentials(self):
        os.environ["MINIO_ROOT_USER"] = "u"
        os.environ["MINIO_ROOT_PASSWORD"] = "p"
        config = from_env(dotenv=os.path.join(self._tmp.name, "ausente"))
        self.assertEqual((config.access_key, config.secret_key), ("u", "p"))

    def test_aws_credentials_take_precedence(self):
        os.environ["MINIO_ROOT_USER"] = "minio"
        os.environ["MINIO_ROOT_PASSWORD"] = "minio"
        os.environ["AWS_ACCESS_KEY_ID"] = "aws"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "aws-secret"
        config = from_env(dotenv=os.path.join(self._tmp.name, "ausente"))
        self.assertEqual(config.access_key, "aws")

    def test_missing_credentials_is_a_clear_error(self):
        with self.assertRaisesRegex(ConfigError, "credencial ausente"):
            from_env(dotenv=os.path.join(self._tmp.name, "ausente"))

    def test_defaults_when_only_credentials_are_set(self):
        os.environ["MINIO_ROOT_USER"] = "u"
        os.environ["MINIO_ROOT_PASSWORD"] = "p"
        config = from_env(dotenv=os.path.join(self._tmp.name, "ausente"))
        self.assertEqual(config.endpoint, "http://localhost:9000")
        self.assertEqual(config.raw_bucket, "retail-raw")


class TestEndpointHost(unittest.TestCase):
    """O boto3 quer a URL completa; o DuckDB quer host:porta sem esquema. Uma unica
    variavel no .env serve os dois porque a conversao acontece aqui."""

    def test_strips_the_scheme(self):
        self.assertEqual(_endpoint_host("http://localhost:9000"), "localhost:9000")
        self.assertEqual(_endpoint_host("https://minio:9000"), "minio:9000")

    def test_passes_through_a_bare_host_port(self):
        self.assertEqual(_endpoint_host("minio:9000"), "minio:9000")


class TodaVariavelDeAmbienteEDeclarada(unittest.TestCase):
    """Uma variavel que o codigo le e que nenhum arquivo declara e configuracao invisivel:
    existe, muda comportamento, e ninguem que clone o repositorio descobre que existe.

    Este teste varre o codigo real atras de `os.environ[...]` / `os.environ.get(...)` /
    `os.getenv(...)` e exige que cada nome apareca em `.env.example` (ainda que comentado,
    como sobrescrita opcional) ou em `infra/docker-compose.yml`. Nao confere o VALOR — o
    default mora no codigo, e duplica-lo aqui criaria o segundo lugar onde ele vive.

    Visto vermelho: apagar a linha de `KAFKA_ORDERS_TOPIC` do `.env.example` reprova.
    """

    # `jobs/` entrou na Fase 7 junto com o job Spark, e a ausencia dele era uma brecha do
    # tipo que este teste existe para nao ter: uma arvore de codigo que le ambiente e que a
    # varredura nao alcanca nao produz falso negativo barulhento, produz silencio.
    RAIZES = ("platform/src", "streamlit", "orchestration/airflow/dags", "scripts", "jobs")

    # Definidas pelo ambiente de execucao, nunca pelo repositorio.
    DO_SISTEMA = {"HOME", "PATH", "PWD", "USER", "SNOWFLAKE_HOME", "PYTHONPATH", "TZ"}

    def _repo(self):
        return pathlib.Path(__file__).resolve().parents[2]

    def test_toda_variavel_lida_aparece_no_env_example_ou_no_compose(self):
        padrao = re.compile(
            r"""(?:environ\.get\(|environ\[|getenv\()\s*["']([A-Z][A-Z0-9_]*)["']"""
        )
        repo = self._repo()
        lidas: dict[str, str] = {}
        for raiz in self.RAIZES:
            for arquivo in sorted((repo / raiz).rglob("*.py")):
                if "__pycache__" in str(arquivo):
                    continue
                for nome in padrao.findall(arquivo.read_text()):
                    lidas.setdefault(nome, str(arquivo.relative_to(repo)))

        declarado = (repo / ".env.example").read_text()
        declarado += (repo / ".env.snowflake.example").read_text()
        declarado += (repo / "infra" / "docker-compose.yml").read_text()

        ausentes = sorted(
            f"{nome} (lida em {onde})"
            for nome, onde in lidas.items()
            if nome not in self.DO_SISTEMA and nome not in declarado
        )
        self.assertEqual(
            ausentes,
            [],
            "variaveis lidas pelo codigo e declaradas em lugar nenhum:\n  "
            + "\n  ".join(ausentes),
        )


if __name__ == "__main__":
    unittest.main()
