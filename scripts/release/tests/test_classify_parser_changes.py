"""Regression cases for selecting native builds without delaying report fixes."""

from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "classify_parser_changes", ROOT / "scripts/release/classify_parser_changes.py"
)
SCOPE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCOPE)


class ParserScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-parser-scope-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Scope test")
        self.git("config", "user.email", "scope@example.invalid")
        self.write("README.md")
        self.write("crates/alopex-sql/nim-sql-parser/src/parser.nim")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(
            [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgsign=false",
                *args,
            ],
            cwd=self.repo,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()

    def write(self, path, content="fixture\n"):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "--quiet", "-m", "fixture")
        return self.git("rev-parse", "HEAD")

    def required(self, head):
        return SCOPE.needs_native_parser(self.base, head, self.repo)

    def test_report_publication_and_demo_changes_skip_native_builds(self):
        for path in (
            "scripts/release/verify-release/report.py",
            "scripts/release/verify-release/recheck_embedded.py",
            "scripts/release/tests/test_embedded_recheck.py",
            "scripts/release/tests/test_public_scenarios.py",
            "scripts/release/verify_python_vector_api.py",
            ".github/workflows/public-release-verification.yml",
            ".github/workflows/publish-public-report.yml",
            "docs/release-pipeline-responsibilities.md",
        ):
            self.write(path)
        self.assertFalse(self.required(self.commit()))

    def test_each_native_dependency_requires_all_hosts(self):
        for path in (
            "Cargo.toml",
            "scripts/build-nim-parser.sh",
            "scripts/release/parser_asset_manifest.py",
            "scripts/release/tests/test_parser_asset_manifest.py",
            "scripts/release/tests/host_tools.py",
            ".github/actions/setup-nim-parser-toolchain/action.yml",
            "crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION",
            "crates/alopex-sql/nim-sql-parser/nimble.lock",
            ".github/workflows/release-process.yml",
            "scripts/release/classify_parser_changes.py",
            "scripts/release/tests/test_classify_parser_changes.py",
        ):
            with self.subTest(path=path):
                self.write(path)
                head = self.commit()
                self.assertTrue(self.required(head))
                self.base = head

    def test_parser_source_edit_requires_all_hosts(self):
        self.write("crates/alopex-sql/nim-sql-parser/src/parser.nim", "changed\n")
        self.assertTrue(self.required(self.commit()))

    def test_mixed_report_and_parser_change_requires_all_hosts(self):
        self.write("scripts/release/verify-release/report.py")
        self.write("crates/alopex-sql/nim-sql-parser/src/new.nim")
        self.assertTrue(self.required(self.commit()))

    def test_rename_out_of_parser_tree_is_not_missed(self):
        self.git("mv", "crates/alopex-sql/nim-sql-parser/src/parser.nim", "old.txt")
        self.assertTrue(self.required(self.commit()))

    def test_rename_into_parser_tree_is_not_missed(self):
        self.git("mv", "README.md", "crates/alopex-sql/nim-sql-parser/new.nim")
        self.assertTrue(self.required(self.commit()))

    def test_deleted_parser_input_requires_all_hosts(self):
        self.git("rm", "crates/alopex-sql/nim-sql-parser/src/parser.nim")
        self.assertTrue(self.required(self.commit()))

    def test_newline_in_unrelated_filename_is_not_an_extra_path(self):
        self.write("scripts/release/report\nCargo.toml")
        self.assertFalse(self.required(self.commit()))

    def test_empty_diff_runs_all_hosts(self):
        self.assertTrue(self.required(self.base))

    def test_missing_or_invalid_history_runs_all_hosts(self):
        for base, head in (
            ("", self.base),
            ("0" * 40, self.base),
            (self.base, "f" * 40),
            ("--help", self.base),
            (self.base, self.base[:8]),
        ):
            with self.subTest(base=base, head=head):
                self.assertTrue(SCOPE.needs_native_parser(base, head, self.repo))

    def test_unreadable_repository_runs_all_hosts(self):
        self.assertTrue(
            SCOPE.needs_native_parser(self.base, self.base, self.repo / "missing")
        )

    def test_failed_diff_runs_all_hosts(self):
        with mock.patch.object(
            SCOPE.subprocess,
            "check_output",
            side_effect=subprocess.CalledProcessError(1, ["git", "diff"]),
        ):
            self.assertTrue(self.required(self.base))

    def test_shallow_checkout_needs_only_the_base_commit(self):
        self.write("scripts/release/verify-release/report.py")
        head = self.commit()
        shallow = self.repo / "shallow"
        subprocess.run(
            ["git", "clone", "--quiet", "--depth=1", self.repo.as_uri(), str(shallow)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.assertTrue(SCOPE.needs_native_parser(self.base, head, shallow))
        subprocess.run(
            ["git", "fetch", "--quiet", "--no-tags", "--depth=1", "origin", self.base],
            cwd=shallow,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.assertFalse(SCOPE.needs_native_parser(self.base, head, shallow))

    def test_workflow_selects_both_native_jobs_and_keeps_version_trigger(self):
        workflow = (ROOT / ".github/workflows/release-process.yml").read_text()
        self.assertEqual(workflow.count("- 'Cargo.toml'"), 2)
        contract = workflow.split("\n  contract:\n", 1)[1].split("\n  parser-host:", 1)[
            0
        ]
        self.assertIn("fetch-depth: 1", contract)
        self.assertIn('git fetch --no-tags --depth=1 origin "${base_sha}"', contract)
        self.assertIn("echo 'parser=true'", contract)
        self.assertIn("parser: ${{ steps.parser-scope.outputs.parser }}", contract)
        self.assertIn('"${PR_BASE_SHA}"', contract)
        self.assertIn('"${GITHUB_SHA}" >> "${GITHUB_OUTPUT}"', contract)
        self.assertIn("scripts/release/classify_parser_changes.py", contract)
        for job, following, dependencies in (
            ("parser-host", "parser-manifest", "[contract]"),
            ("parser-manifest", "state-model", "[contract, parser-host]"),
        ):
            block = workflow.split(f"\n  {job}:\n", 1)[1].split(
                f"\n  {following}:\n", 1
            )[0]
            self.assertIn(f"needs: {dependencies}", block)
            self.assertIn("if: needs.contract.outputs.parser == 'true'", block)


if __name__ == "__main__":
    unittest.main()
