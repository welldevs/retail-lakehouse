"""The documentation checked as code. No network.

WHY THIS SUITE EXISTS. The 2026-09-01 review found, in the documentation alone: a
directory tree that announced 7 marts where there were 8 and left out 8 of the 14 seeds, a
coverage table that said 131 tests where there were 376, and the SAME section claiming 29
and 16 for the same module in two paragraphs. None of that breaks anything — and that is
exactly why it survived six phases.

What a machine can check is here. What it cannot (whether the text is RIGHT) is still
human reading; these tests only guarantee that the text does not point at things that do
not exist.

It deliberately does NOT check test counts written in prose: the fix there was to stop
writing the number, not to automate copying it. See ARCHITECTURE.md, "Test coverage".
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _tracked(pattern: str) -> list[pathlib.Path]:
    output = subprocess.run(
        ["git", "ls-files", pattern], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    return [ROOT / line for line in output.splitlines() if line]


class StructureTreeTest(unittest.TestCase):
    """The README tree names, path by path, what exists in the repository.

    It is the map someone reads before opening any file, so a wrong path in it costs more
    than a wrong comment: it sends the reader looking for something that is not there.
    """

    def _tree_paths(self) -> list[str]:
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        block = re.search(r"## Structure\n\n```\n(.*?)\n```", text, re.S)
        self.assertIsNotNone(block, "the '## Structure' block is gone from the README")
        stack: list[str] = []
        paths: list[str] = []
        for line in block.group(1).splitlines():
            found = re.match(r"^([│ ]*)(?:├── |└── )(\S+)", line)
            if not found:
                continue
            depth = len(found.group(1)) // 4
            stack = stack[:depth] + [found.group(2).rstrip("/")]
            paths.append("/".join(stack))
        return paths

    def test_every_tree_path_exists(self):
        paths = self._tree_paths()
        self.assertGreater(len(paths), 15, "the tree shrank — the parser probably broke")

        # data/ and its children are the only exception: the tree itself marks them
        # "outside version control" (.gitignore line 36) because they are extraction
        # scratch, never source code. A clean checkout never has them before running the
        # pipeline — CI is what exposed this, since it only passed locally where the
        # pipeline had already run at some point.
        untracked = {p for p in paths if p == "data" or p.startswith("data/")}
        regular = [p for p in paths if p not in untracked]

        missing = sorted(p for p in regular if not (ROOT / p).exists())
        self.assertEqual(
            missing, [],
            "the README tree names paths that do not exist:\n  " + "\n  ".join(missing),
        )

        not_ignored = sorted(
            p for p in untracked
            if subprocess.run(["git", "check-ignore", "-q", p + "/"], cwd=ROOT).returncode != 0
        )
        self.assertEqual(
            not_ignored, [],
            "the tree marks these paths as outside version control, "
            "but .gitignore does not cover them:\n  " + "\n  ".join(not_ignored),
        )


class RelativeLinksTest(unittest.TestCase):
    """A broken Markdown link fails nowhere: it just does not open.

    Checks only RELATIVE links. External URLs are left out: checking them would need the
    network, and the suite is offline by repository invariant.
    """

    def test_every_relative_link_resolves(self):
        broken = []
        for path in _tracked("*.md"):
            base = path.parent
            text = path.read_text(encoding="utf-8")
            for target in re.findall(r"\]\(([^)\s]+)\)", text):
                target = target.split("#")[0]
                if not target or target.startswith(("http://", "https://", "mailto:", "s3://")):
                    continue
                if not (base / target).exists():
                    broken.append(f"{path.relative_to(ROOT)} -> {target}")
        self.assertEqual(
            sorted(broken), [],
            "relative links that do not resolve:\n  " + "\n  ".join(sorted(broken)),
        )


class SectionNameReferenceTest(unittest.TestCase):
    """`[DOC.md § "Title"](DOC.md)` must point at a heading that exists there.

    WHY THIS WAS NOT COVERED. `RelativeLinksTest` checks that the FILE exists, and all of
    these references point at files that exist — so it passed. What nobody checked was the
    LABEL: the named section could have been renamed, or moved to another file, and the
    link would stay "valid" while taking the reader to the wrong document.

    This stopped being hypothetical in Phase 7, when 17 narrative sections moved from
    ARCHITECTURE.md to DECISIONS.md. Five references had to be redirected — and two others
    looked broken until I found out my own checker only looked at level-2 headings. The
    test looks at ALL levels.

    THE COMPARISON IS BY PREFIX on purpose: references abbreviate ("Phase 2" points at
    "Phase 2: analytical layer on Snowflake"). Requiring the full heading would force
    repeating long phrases inside running text, and the abbreviation introduces no
    ambiguity — no two sections have headings that start the same way.
    """

    PATTERN = re.compile(r'\[([A-Za-z_.\/-]*\.md) § "([^"]+)"\]')

    def _headings(self, path: pathlib.Path) -> set[str]:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return set()
        return {
            line.lstrip("#").strip()
            for line in text.split("\n")
            if re.match(r"^#{1,6}\s", line)
        }

    def test_every_section_cited_by_name_exists_in_the_cited_document(self):
        problems = []
        for path in _tracked("*.md"):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for target, heading in self.PATTERN.findall(text):
                destination = (path.parent / target).resolve()
                if not destination.exists():
                    # RelativeLinksTest already covers a missing file; here, only the label.
                    continue
                headings = self._headings(destination)
                if not any(h == heading or h.startswith(heading) or heading in h
                           for h in headings):
                    problems.append(
                        f"{path.relative_to(ROOT)} cites "
                        f'{target} § "{heading}", which does not exist there'
                    )
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


class LineCeilingTest(unittest.TestCase):
    """README and ARCHITECTURE must not go back to growing without limit.

    SIZE IS, IN ITSELF, A COMPLEXITY QUESTION. Before Phase 7 the README had 1,435 lines and
    ARCHITECTURE 2,475, and much of it was the SAME explanation in two places, ageing at
    different rates. The narrative went to DECISIONS.md and both dropped to less than half.

    THE CEILING IS NOT AN AESTHETIC GOAL, and the number was not picked by taste: it is
    today's size with a narrow margin. The intended effect is not to force cuts — it is to
    make GROWING a decision rather than an accumulation. Whoever needs more room changes the
    number here, and that change shows up in the diff.

    WHERE TEXT SHOULD GO when the ceiling gets tight: history to DECISIONS.md, future scope
    to BACKLOG.md, and a Source's details to its own README.

    THE README CEILING WENT FROM 700 TO 760 on 2026-09-02, and the reason is written down
    because the test itself says growing must be a decision and not an accumulation: the
    section "The fourteen closing questions" came in, an INDEX of one line per question
    pointing at where the evidence lives. It does not duplicate explanation — it is the
    opposite, and it exists so that a reader can check whether the documentation backs what
    it claims without reading 3,000 lines.

    BOTH CEILINGS DROPPED on 2026-09-14, in a review aimed at external readers (the
    repository started being attached as a technical showcase): the README directory tree
    was condensed to top level, and ARCHITECTURE lost three postmortem subsections (RBAC,
    `warehouse_load`, homonyms in the population) and a second history table that
    duplicated what `DECISIONS.md` already narrates per phase — nothing was lost, only moved
    to the file that already exists to keep history.
    """

    CEILINGS = {
        "README.md": 680,
        "ARCHITECTURE.md": 560,
    }

    def test_no_state_document_exceeds_its_ceiling(self):
        for name, ceiling in self.CEILINGS.items():
            with self.subTest(name):
                lines = len((ROOT / name).read_text(encoding="utf-8").splitlines())
                self.assertLessEqual(
                    lines, ceiling,
                    f"{name} has {lines} lines (ceiling {ceiling}). History goes to "
                    f"DECISIONS.md, future scope to BACKLOG.md.",
                )

    def test_the_four_root_documents_exist_and_have_distinct_roles(self):
        """The structure the project declares: state, history, scope, execution."""
        for name in ("README.md", "ARCHITECTURE.md", "DECISIONS.md", "BACKLOG.md"):
            with self.subTest(name):
                self.assertTrue((ROOT / name).exists(), f"{name} does not exist")


class AnchorsResolveTest(unittest.TestCase):
    """`](#section)` and `](DOC.md#section)` must point at a heading that exists.

    WHY THIS IS SEPARATE FROM `RelativeLinksTest`. That one checks that the FILE exists, and
    a link with a broken anchor points at a file that exists — so it passes, and the reader
    lands at the top of the document without knowing they are in the wrong place. A whole
    index can rot this way without any test complaining, and that was the concrete risk of
    Phase 7, when 17 sections changed files and a new index was born with 11 anchors.

    THE SLUG RULE IS GITHUB'S, and an earlier version of this checker was WRONG: it
    collapsed repeated spaces, and GitHub does not. That produced five false positives in a
    correct document — a wrong checker costs more than no checker, because it teaches people
    to ignore the result.
    """

    @staticmethod
    def _slug(heading: str) -> str:
        text = heading.lower()
        text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
        return text.strip().replace(" ", "-")

    def _headings(self, path: pathlib.Path) -> set[str]:
        """Headings PLUS explicit HTML anchors.

        `docs/indicators/operations.md` is generated and uses `<a id="key"></a>` before each
        indicator, because the indicator key is more stable than its title. A checker that
        only looked at headings would fail 22 correct anchors — and a checker that fails
        what is right is worse than no checker, because it teaches people to ignore the
        result. It already happened in this project, with a slug that collapsed repeated
        spaces when GitHub does not.
        """
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return set()
        headings = {self._slug(line.lstrip("#").strip())
                    for line in text.split("\n") if re.match(r"^#{1,6}\s", line)}
        explicit = set(re.findall(r'<a\s+id="([^"]+)"', text))
        return headings | explicit

    def test_every_anchor_points_at_an_existing_heading(self):
        pattern = re.compile(r"\]\(([^)\s]*)#([^)\s]+)\)")
        problems = []
        for path in _tracked("*.md"):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for target, anchor in pattern.findall(text):
                if target.startswith(("http://", "https://")):
                    continue
                destination = path if not target else (path.parent / target).resolve()
                if not destination.exists():
                    continue  # a missing file is RelativeLinksTest's business
                if anchor not in self._headings(destination):
                    problems.append(
                        f"{path.relative_to(ROOT)} -> {target or path.name}#{anchor}")
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


class NoTrackedJunkTest(unittest.TestCase):
    """A run log committed by mistake gets in nobody's way, and that is why it stays for years.

    `platform/logs/dbt.log` was there since the first commit: 16 lines of an aborted dbt
    invocation, referenced nowhere. dbt writes `logs/` RELATIVE to the cwd, not to
    `--project-dir`, so running dbt from inside `platform/` recreates the directory.
    """

    def test_no_log_or_run_artifact_is_tracked(self):
        forbidden = []
        for path in _tracked("*"):
            relative = path.relative_to(ROOT).as_posix()
            if re.search(r"(^|/)(logs?/|.*\.log$|\.duckdb$|__pycache__/)", relative):
                forbidden.append(relative)
        self.assertEqual(
            sorted(forbidden), [],
            "tracked run artifacts:\n  " + "\n  ".join(sorted(forbidden)),
        )


class DashboardScreenshotsTest(unittest.TestCase):
    """A published dashboard screenshot is a claim about the data, so it must show THIS data
    and must have been read.

    On 2026-10-30 a review trusted shots cut at 1000 px and missed ten defects below the fold,
    and `docs/screens/dashboard-inventory-en.png` was still showing 206,523 orders from a model
    two regenerations old (CR-018). `make dashboard-shots` measures every tab and refuses a
    truncated one; this test refuses shots of another seal, a shot outside the manifest, and
    a set nobody approved after reading it (`make dashboard-shots-approve`).
    """

    SHOTS = ROOT / "docs" / "screens" / "dashboard"

    def _manifest(self) -> dict:
        import json

        path = self.SHOTS / "MANIFEST.json"
        self.assertTrue(path.exists(), "docs/screens/dashboard/MANIFEST.json is gone: "
                        "run make dashboard-shots")
        return json.loads(path.read_text(encoding="utf-8"))

    def test_shots_show_the_sealed_capture(self):
        sealed = re.search(r"`capture_id` = `([0-9a-f]{64})`",
                           (ROOT / "docs" / "FREEZE.md").read_text(encoding="utf-8"))
        self.assertIsNotNone(sealed, "docs/FREEZE.md no longer states the capture_id")
        self.assertEqual(
            self._manifest()["capture_id"], sealed.group(1),
            "the dashboard screenshots show another seal: the data was regenerated after "
            "they were taken. Run make dashboard-shots, READ every PNG, then approve.",
        )

    def test_shots_were_read_before_publishing(self):
        manifest = self._manifest()
        self.assertTrue(manifest.get("reviewed_at") and manifest.get("reviewed_note"),
                        "the screenshots were never approved: open every PNG, then run "
                        "make dashboard-shots-approve NOTE=\"what you checked\"")
        self.assertGreaterEqual(manifest["reviewed_at"], manifest["taken_at"][:19],
                                "approved before the current shots were taken")

    def test_no_shot_outside_the_manifest(self):
        listed = {entry["file"] for entry in self._manifest()["files"]}
        present = {path.name for path in self.SHOTS.glob("*.png")}
        self.assertEqual(present, listed, "screenshots outside the manifest (or missing)")
        stray = sorted(path.name for path in (ROOT / "docs" / "screens").glob("dashboard*.png"))
        self.assertEqual(stray, [], "dashboard screenshots outside docs/screens/dashboard/ "
                         "escape this check — move them in or delete them")


class MakefileExplainsItselfTest(unittest.TestCase):
    """`make help` is the repository's only interface. A target outside it does not exist
    in practice, and a script without a target is a file nobody knows how to run.

    Found in the 2026-09-01 review: five real targets missing from help (`logs`,
    `stream-logs`, `orders-oltp-init`, `orders-projection-init`, `iceberg-metadata`) and
    four `derive_*` scripts with no target at all — precisely the ones that derive the
    TRACKED seeds, meaning the provenance of four CSVs lived only in the docstring of a file
    the README did not say how to run.
    """

    def _makefile(self) -> str:
        return (ROOT / "Makefile").read_text(encoding="utf-8")

    def test_every_target_appears_in_make_help(self):
        text = self._makefile()
        targets = set(re.findall(r"^([a-z][a-z0-9_-]*):", text, re.M)) - {"help"}
        block = re.search(r"^help:\n(.*?)(?=\n[^\t\n])", text, re.S | re.M)
        self.assertIsNotNone(block, "the `help` target is gone from the Makefile")
        cited = set(re.findall(r'@echo "  ([a-z][a-z0-9_-]*)', block.group(1)))
        missing = sorted(targets - cited)
        self.assertEqual(
            missing, [],
            "targets that exist and `make help` does not cite:\n  " + "\n  ".join(missing),
        )

    def test_every_script_has_a_makefile_target(self):
        text = self._makefile()
        orphans = sorted(
            path.name for path in _tracked("scripts/*")
            if path.name not in text
        )
        self.assertEqual(
            orphans, [],
            "scripts without a Makefile target — no documented way to run them:\n  "
            + "\n  ".join(orphans),
        )

    def test_every_makefile_variable_is_used(self):
        """A variable that is defined and never referenced is configuration that promises an
        effect and has none — worse than absent, because it looks like it exists."""
        text = self._makefile()
        dead = sorted(
            name for name in set(re.findall(r"^([A-Z][A-Z0-9_]*)\s*[?:]?=", text, re.M))
            if f"$({name})" not in text
        )
        self.assertEqual(dead, [], "Makefile variables never referenced: " + ", ".join(dead))

    def test_every_target_cited_in_the_docs_exists(self):
        """A `make X` in prose is an instruction: if the target does not exist, whoever
        follows the text hits an error. That is how `warehouse-evidence` survived three
        reviews after being renamed (CR-009). DECISIONS.md is left out on purpose: it is
        dated history, and the name a target had back then is the truth of that time."""
        targets = set(re.findall(r"^([a-z][a-z0-9_-]*):", self._makefile(), re.M))
        documents = [ROOT / "README.md", ROOT / "ARCHITECTURE.md",
                     ROOT / ".env.example", ROOT / ".env.snowflake.example",
                     *_tracked("docs/*.md"), *_tracked("docs/**/*.md"),
                     *_tracked("streamlit/*.md")]
        broken = sorted({
            f"{doc.relative_to(ROOT)}: make {target}"
            for doc in set(documents) if doc.exists()
            # `make warehouse-snowflake-*` is a family wildcard, not a target.
            for target in re.findall(r"`make ([a-z][a-z0-9_-]*)(?![*\w-])",
                                     doc.read_text(encoding="utf-8"))
            if target not in targets
        })
        self.assertEqual(broken, [],
                         "cited targets that do not exist in the Makefile:\n  " + "\n  ".join(broken))


if __name__ == "__main__":
    unittest.main()
