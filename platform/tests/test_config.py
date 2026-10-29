"""Configuration precedence. No network.

The rule "the environment wins over .env" is not a style detail: it is what makes the
Airflow container work. The repository's .env says S3_ENDPOINT=http://localhost:9000, and
inside the compose network the endpoint is http://minio:9000. If the file overrode the
environment, compose would have no way to fix that and every object-storage access from
inside the container would fail.
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
    """Each test starts with the environment cleared of the variables config reads."""

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
        """The rule that lets compose fix the endpoint inside the network."""
        os.environ["S3_ENDPOINT"] = "http://minio:9000"
        load_dotenv(self.write_dotenv("S3_ENDPOINT=http://localhost:9000\n"))
        self.assertEqual(os.environ["S3_ENDPOINT"], "http://minio:9000")

    def test_file_fills_what_the_environment_lacks(self):
        load_dotenv(self.write_dotenv("RAW_BUCKET=from-file\n"))
        self.assertEqual(os.environ["RAW_BUCKET"], "from-file")

    def test_ignores_comments_blank_lines_and_quotes(self):
        load_dotenv(self.write_dotenv(
            '# comment\n\nRAW_BUCKET="in-quotes"\nno_equals\n'
        ))
        self.assertEqual(os.environ["RAW_BUCKET"], "in-quotes")

    def test_missing_file_is_not_an_error(self):
        load_dotenv(os.path.join(self._tmp.name, "does-not-exist"))  # does not raise


class TestFromEnv(EnvIsolated):
    def test_accepts_minio_credentials(self):
        os.environ["MINIO_ROOT_USER"] = "u"
        os.environ["MINIO_ROOT_PASSWORD"] = "p"
        config = from_env(dotenv=os.path.join(self._tmp.name, "missing"))
        self.assertEqual((config.access_key, config.secret_key), ("u", "p"))

    def test_aws_credentials_take_precedence(self):
        os.environ["MINIO_ROOT_USER"] = "minio"
        os.environ["MINIO_ROOT_PASSWORD"] = "minio"
        os.environ["AWS_ACCESS_KEY_ID"] = "aws"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "aws-secret"
        config = from_env(dotenv=os.path.join(self._tmp.name, "missing"))
        self.assertEqual(config.access_key, "aws")

    def test_missing_credentials_is_a_clear_error(self):
        with self.assertRaisesRegex(ConfigError, "missing credentials"):
            from_env(dotenv=os.path.join(self._tmp.name, "missing"))

    def test_defaults_when_only_credentials_are_set(self):
        os.environ["MINIO_ROOT_USER"] = "u"
        os.environ["MINIO_ROOT_PASSWORD"] = "p"
        config = from_env(dotenv=os.path.join(self._tmp.name, "missing"))
        self.assertEqual(config.endpoint, "http://localhost:9000")
        self.assertEqual(config.raw_bucket, "retail-raw")


class TestEndpointHost(unittest.TestCase):
    """boto3 wants the full URL; DuckDB wants host:port without a scheme. A single
    variable in .env serves both because the conversion happens here."""

    def test_strips_the_scheme(self):
        self.assertEqual(_endpoint_host("http://localhost:9000"), "localhost:9000")
        self.assertEqual(_endpoint_host("https://minio:9000"), "minio:9000")

    def test_passes_through_a_bare_host_port(self):
        self.assertEqual(_endpoint_host("minio:9000"), "minio:9000")


class EveryEnvironmentVariableIsDeclared(unittest.TestCase):
    """A variable the code reads and no file declares is invisible configuration: it
    exists, changes behavior, and nobody who clones the repository finds out it exists.

    This test scans the real code for `os.environ[...]` / `os.environ.get(...)` /
    `os.getenv(...)` and requires each name to appear in `.env.example` (even commented
    out, as an optional override) or in `infra/docker-compose.yml`. It does not check the
    VALUE — the default lives in the code, and duplicating it here would create a second
    place where it lives.

    Seen red: deleting the `KAFKA_ORDERS_TOPIC` line from `.env.example` fails it.
    """

    # `jobs/` came in with Phase 7 along with the Spark job, and its absence was a gap of
    # the kind this test exists to avoid: a code tree that reads the environment and that
    # the scan does not reach produces no noisy false negative, it produces silence.
    ROOTS = ("platform/src", "streamlit", "orchestration/airflow/dags", "scripts", "jobs")

    # Set by the runtime environment, never by the repository.
    FROM_SYSTEM = {"HOME", "PATH", "PWD", "USER", "SNOWFLAKE_HOME", "PYTHONPATH", "TZ"}

    def _repo(self):
        return pathlib.Path(__file__).resolve().parents[2]

    def test_every_variable_read_appears_in_env_example_or_compose(self):
        pattern = re.compile(
            r"""(?:environ\.get\(|environ\[|getenv\()\s*["']([A-Z][A-Z0-9_]*)["']"""
        )
        repo = self._repo()
        read_vars: dict[str, str] = {}
        for root in self.ROOTS:
            for path in sorted((repo / root).rglob("*.py")):
                if "__pycache__" in str(path):
                    continue
                for name in pattern.findall(path.read_text()):
                    read_vars.setdefault(name, str(path.relative_to(repo)))

        declared = (repo / ".env.example").read_text()
        declared += (repo / ".env.snowflake.example").read_text()
        declared += (repo / "infra" / "docker-compose.yml").read_text()

        missing = sorted(
            f"{name} (read in {where})"
            for name, where in read_vars.items()
            if name not in self.FROM_SYSTEM and name not in declared
        )
        self.assertEqual(
            missing,
            [],
            "variables read by the code and declared nowhere:\n  "
            + "\n  ".join(missing),
        )


if __name__ == "__main__":
    unittest.main()
