"""A promessa de zero dependencias de runtime e verificada, nao apenas declarada.

Percorre a AST de todos os modulos do pacote e dos testes e falha se aparecer qualquer
import que nao seja da biblioteca padrao. Sem isto, requirements.txt e
[project.dependencies] seriam apenas afirmacoes.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

import ine_callejero_source

PACKAGE_ROOT = os.path.dirname(os.path.abspath(ine_callejero_source.__file__))
TESTS_ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TESTS_ROOT)

LOCAL_MODULES = {"ine_callejero_source", "tests"}


def python_files(directory: str) -> list[str]:
    found = []
    for base, _, names in os.walk(directory):
        if "__pycache__" in base:
            continue
        found.extend(os.path.join(base, n) for n in names if n.endswith(".py"))
    return sorted(found)


def top_level_imports(path: str) -> set:
    with open(path, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=path)
    names: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # import relativo: proprio pacote
                continue
            if node.module:
                names.add(node.module.split(".")[0])
    return names


class NoRuntimeDependenciesTest(unittest.TestCase):
    def test_pacote_importa_apenas_stdlib(self):
        permitido = set(sys.stdlib_module_names) | LOCAL_MODULES
        for path in python_files(PACKAGE_ROOT):
            with self.subTest(modulo=os.path.relpath(path, PACKAGE_ROOT)):
                externos = top_level_imports(path) - permitido
                self.assertEqual(
                    externos,
                    set(),
                    f"{path} importa pacote de terceiros: {sorted(externos)}. "
                    f"Se for intencional, declare em requirements.txt e em "
                    f"[project.dependencies] do pyproject.toml.",
                )

    def test_testes_importam_apenas_stdlib(self):
        permitido = set(sys.stdlib_module_names) | LOCAL_MODULES
        for path in python_files(TESTS_ROOT):
            with self.subTest(modulo=os.path.relpath(path, TESTS_ROOT)):
                externos = top_level_imports(path) - permitido
                self.assertEqual(externos, set(), f"{path} importa {sorted(externos)}")

    def test_encontrou_modulos_para_analisar(self):
        # Protege contra o teste passar por nao ter olhado nada.
        self.assertGreaterEqual(len(python_files(PACKAGE_ROOT)), 6)
        self.assertGreaterEqual(len(python_files(TESTS_ROOT)), 6)

    def test_pyproject_declara_dependencies_vazio(self):
        path = os.path.join(PROJECT_ROOT, "pyproject.toml")
        self.assertTrue(os.path.exists(path), f"pyproject.toml ausente em {PROJECT_ROOT}")
        import tomllib

        with open(path, "rb") as handle:
            config = tomllib.load(handle)
        self.assertEqual(
            config["project"].get("dependencies", []),
            [],
            "pyproject declara dependencias de runtime",
        )

    def test_metadata_instalado_nao_declara_dependencia(self):
        import importlib.metadata as metadata

        try:
            requires = metadata.distribution("ine-callejero-source").requires
        except metadata.PackageNotFoundError:
            requires = None  # nao instalado: o teste do pyproject ja cobre a fonte
        self.assertIn(requires, (None, []), f"metadata declara dependencias: {requires}")

    def test_requirements_txt_nao_tem_nenhum_pacote(self):
        path = os.path.join(PROJECT_ROOT, "requirements.txt")
        self.assertTrue(os.path.exists(path), f"requirements.txt ausente em {PROJECT_ROOT}")
        with open(path, "r", encoding="utf-8") as handle:
            linhas = [
                linha.strip()
                for linha in handle
                if linha.strip() and not linha.strip().startswith("#")
            ]
        self.assertEqual(
            linhas, [], f"requirements.txt lista pacotes de runtime: {linhas}"
        )


class ProjectMetadataConsistencyTest(unittest.TestCase):
    """A versao e declarada em dois lugares; elas nao podem divergir em silencio."""

    def _pyproject(self) -> dict:
        import tomllib

        with open(os.path.join(PROJECT_ROOT, "pyproject.toml"), "rb") as handle:
            return tomllib.load(handle)

    def test_versao_do_pacote_bate_com_o_pyproject(self):
        self.assertEqual(
            ine_callejero_source.__version__,
            self._pyproject()["project"]["version"],
            "__init__.py e pyproject.toml declaram versoes diferentes",
        )

    def test_console_script_aponta_para_o_ponto_de_entrada_real(self):
        alvo = self._pyproject()["project"]["scripts"]["ine-callejero-source"]
        modulo, funcao = alvo.split(":")
        import importlib

        self.assertTrue(
            callable(getattr(importlib.import_module(modulo), funcao)),
            f"console script aponta para {alvo}, que nao e chamavel",
        )

    def test_pacote_declarado_no_pyproject_existe_em_src(self):
        where = self._pyproject()["tool"]["setuptools"]["packages"]["find"]["where"]
        self.assertEqual(where, ["src"])
        self.assertTrue(
            os.path.isdir(os.path.join(PROJECT_ROOT, "src", "ine_callejero_source"))
        )


if __name__ == "__main__":
    unittest.main()
