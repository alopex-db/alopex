#!/usr/bin/env python3
"""Behavior contracts for parser-building pull-request workflows."""

from __future__ import annotations

from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_DIR = REPOSITORY_ROOT / ".github/workflows"
EXACT_SETUP = "uses: ./.github/actions/setup-nim-parser-toolchain"
FLOATING_SETUP = "uses: jiro4989/setup-nim-action@"


class CiWorkflowContractTests(unittest.TestCase):
    def workflow(self, name: str) -> str:
        return (WORKFLOW_DIR / name).read_text(encoding="utf-8")

    def test_every_non_linux_host_build_uses_the_exact_local_toolchain(self) -> None:
        for workflow_name in ("alopex-py.yml", "alopex-cli.yml"):
            with self.subTest(workflow=workflow_name):
                workflow = self.workflow(workflow_name)
                self.assertNotIn(FLOATING_SETUP, workflow)
                self.assertEqual(workflow.count(EXACT_SETUP), 1)

                setup_offset = workflow.index(
                    "name: Setup exact Nim parser toolchain (macOS/Windows)"
                )
                build_offset = workflow.index(
                    "name: Build Nim SQL parser library (macOS/Windows)"
                )
                self.assertLess(setup_offset, build_offset)

                setup_block = workflow[setup_offset:build_offset]
                self.assertIn("if: runner.os != 'Linux'", setup_block)
                self.assertIn(EXACT_SETUP, setup_block)

    def test_parser_toolchain_owners_trigger_every_consumer_workflow(self) -> None:
        required_paths = (
            ".github/actions/setup-nim-parser-toolchain/**",
            "scripts/build-nim-parser.sh",
            "crates/alopex-sql/**",
        )
        for workflow_name in ("alopex-py.yml", "alopex-cli.yml"):
            with self.subTest(workflow=workflow_name):
                workflow = self.workflow(workflow_name)
                for required_path in required_paths:
                    self.assertGreaterEqual(
                        workflow.count(required_path),
                        2,
                        f"{required_path} must trigger push and pull_request",
                    )

    def test_signal_harness_captures_status_and_rejects_unexpected_exit(self) -> None:
        workflow = self.workflow("alopex-cli.yml")
        signal_step = workflow.split(
            "- name: Test Ctrl-C signal handling", maxsplit=1
        )[1].split("# S3 compatibility test with MinIO", maxsplit=1)[0]

        self.assertIn("trap cleanup_signal_test EXIT", signal_step)
        self.assertIn('rm -f -- "$SQL_FILE"', signal_step)
        self.assertIn(
            "set +e\n          wait \"$PID\" 2>/dev/null\n"
            "          EXIT_CODE=$?\n          set -e",
            signal_step,
        )
        self.assertNotIn('[[ "$EXIT_CODE" -eq 1 ]]', signal_step)
        self.assertIn('echo "FAIL: Unexpected exit code: $EXIT_CODE" >&2', signal_step)
        self.assertIn("exit 1", signal_step)


if __name__ == "__main__":
    unittest.main()
