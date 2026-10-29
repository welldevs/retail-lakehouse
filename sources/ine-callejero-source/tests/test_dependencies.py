"""The zero-runtime-dependency promise is verified, not just declared.

Walks the AST of every package and test module and fails if any import that is not from
the standard library appears. Without this, requirements.txt and [project.dependencies]
would be mere claims.
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
            if node.level:  # relative import: own package
                continue
            if node.module:
                names.add(node.module.split(".")[0])
    return names


class NoRuntimeDependenciesTest(unittest.TestCase):
    def test_package_imports_only_stdlib(self):
        allowed = set(sys.stdlib_module_names) | LOCAL_MODULES
        for path in python_files(PACKAGE_ROOT):
            with self.subTest(module=os.path.relpath(path, PACKAGE_ROOT)):
                external = top_level_imports(path) - allowed
                self.assertEqual(
                    external,
                    set(),
                    f"{path} imports a third-party package: {sorted(external)}. "
                    f"If intentional, declare it in requirements.txt and in "
                    f"[project.dependencies] of pyproject.toml.",
                )

    def test_tests_import_only_stdlib(self):
        allowed = set(sys.stdlib_module_names) | LOCAL_MODULES
        for path in python_files(TESTS_ROOT):
            with self.subTest(module=os.path.relpath(path, TESTS_ROOT)):
                external = top_level_imports(path) - allowed
                self.assertEqual(external, set(), f"{path} imports {sorted(external)}")

    def test_found_modules_to_analyze(self):
        # Guards against the test passing because it looked at nothing.
        self.assertGreaterEqual(len(python_files(PACKAGE_ROOT)), 6)
        self.assertGreaterEqual(len(python_files(TESTS_ROOT)), 6)

    def test_pyproject_declares_empty_dependencies(self):
        path = os.path.join(PROJECT_ROOT, "pyproject.toml")
        self.assertTrue(os.path.exists(path), f"pyproject.toml missing in {PROJECT_ROOT}")
        import tomllib

        with open(path, "rb") as handle:
            config = tomllib.load(handle)
        self.assertEqual(
            config["project"].get("dependencies", []),
            [],
            "pyproject declares runtime dependencies",
        )

    def test_installed_metadata_declares_no_dependency(self):
        import importlib.metadata as metadata

        try:
            requires = metadata.distribution("ine-callejero-source").requires
        except metadata.PackageNotFoundError:
            requires = None  # not installed: the pyproject test already covers the source
        self.assertIn(requires, (None, []), f"metadata declares dependencies: {requires}")

    def test_requirements_txt_has_no_package(self):
        path = os.path.join(PROJECT_ROOT, "requirements.txt")
        self.assertTrue(os.path.exists(path), f"requirements.txt missing in {PROJECT_ROOT}")
        with open(path, "r", encoding="utf-8") as handle:
            lines = [
                line.strip()
                for line in handle
                if line.strip() and not line.strip().startswith("#")
            ]
        self.assertEqual(
            lines, [], f"requirements.txt lists runtime packages: {lines}"
        )


class ProjectMetadataConsistencyTest(unittest.TestCase):
    """The version is declared in two places; they must not silently diverge."""

    def _pyproject(self) -> dict:
        import tomllib

        with open(os.path.join(PROJECT_ROOT, "pyproject.toml"), "rb") as handle:
            return tomllib.load(handle)

    def test_package_version_matches_pyproject(self):
        self.assertEqual(
            ine_callejero_source.__version__,
            self._pyproject()["project"]["version"],
            "__init__.py and pyproject.toml declare different versions",
        )

    def test_console_script_points_to_the_real_entry_point(self):
        target = self._pyproject()["project"]["scripts"]["ine-callejero-source"]
        module, function = target.split(":")
        import importlib

        self.assertTrue(
            callable(getattr(importlib.import_module(module), function)),
            f"console script points to {target}, which is not callable",
        )

    def test_package_declared_in_pyproject_exists_in_src(self):
        where = self._pyproject()["tool"]["setuptools"]["packages"]["find"]["where"]
        self.assertEqual(where, ["src"])
        self.assertTrue(
            os.path.isdir(os.path.join(PROJECT_ROOT, "src", "ine_callejero_source"))
        )


if __name__ == "__main__":
    unittest.main()
