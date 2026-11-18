#!/usr/bin/env python3
"""Behavior contracts for parser-building pull-request workflows."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_DIR = REPOSITORY_ROOT / ".github/workflows"
EXACT_SETUP = "uses: ./.github/actions/setup-nim-parser-toolchain"
FLOATING_SETUP = "uses: jiro4989/setup-nim-action@"


class CiWorkflowContractTests(unittest.TestCase):
    def workflow(self, name: str) -> str:
        return (WORKFLOW_DIR / name).read_text(encoding="utf-8")

    def trusted_audit_python(self, step_name: str) -> str:
        workflow = self.workflow("trusted-security-audit.yml")
        step = workflow.split(f"- name: {step_name}\n", maxsplit=1)[1]
        block = step.split("python3 - <<'PY'\n", maxsplit=1)[1].split(
            "\n          PY", maxsplit=1
        )[0]
        return textwrap.dedent(block)

    def run_trusted_audit_python(
        self, step_name: str, directory: Path, head: str
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-c", self.trusted_audit_python(step_name)],
            cwd=directory,
            env={**os.environ, "PR_HEAD_SHA": head, "CARGO_NET_OFFLINE": "true"},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_trusted_audit_stages_only_regular_dependency_data(self) -> None:
        stage = "Fetch and stage PR dependency data"
        boundary = "Reject local dependencies outside PR data"

        def commit_fixture(root: Path) -> str:
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(["git", "add", "-A"], cwd=root, check=True)
            subprocess.run(
                ["git", "-c", "user.name=Audit Test", "-c", "user.email=audit@example.invalid", "commit", "-qm", "fixture"],
                cwd=root,
                check=True,
            )
            return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Cargo.toml").write_text(
                '[package]\nname = "inside"\nversion = "0.1.0"\nedition = "2021"\n',
                encoding="utf-8",
            )
            (root / "Cargo.lock").write_text(
                'version = 3\n\n[[package]]\nname = "inside"\nversion = "0.1.0"\n',
                encoding="utf-8",
            )
            (root / "src").mkdir()
            (root / "src" / "lib.rs").touch()
            (root / "scripts").mkdir()
            (root / "scripts" / "untrusted.sh").write_text("exit 1\n", encoding="utf-8")
            (root / ".cargo").mkdir()
            (root / ".cargo" / "config.toml").write_text("[build]\n", encoding="utf-8")
            head = commit_fixture(root)
            result = self.run_trusted_audit_python(stage, root, head)
            self.assertEqual(result.returncode, 0, result.stderr)
            staged = root / "audit-input"
            self.assertEqual((staged / "Cargo.toml").read_bytes(), (root / "Cargo.toml").read_bytes())
            self.assertEqual((staged / "Cargo.lock").read_bytes(), (root / "Cargo.lock").read_bytes())
            self.assertFalse((staged / "scripts" / "untrusted.sh").exists())
            self.assertFalse((staged / ".cargo" / "config.toml").exists())
            result = self.run_trusted_audit_python(boundary, root, head)
            self.assertEqual(result.returncode, 0, result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "real.toml").write_text('[package]\nname = "inside"\nversion = "0.1.0"\n')
            (root / "Cargo.toml").symlink_to("real.toml")
            (root / "Cargo.lock").write_text('version = 3\n')
            (root / "src").mkdir()
            (root / "src" / "lib.rs").touch()
            head = commit_fixture(root)
            result = self.run_trusted_audit_python(stage, root, head)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("dependency input is not a regular file", result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Cargo.toml").write_text(
                '[package]\nname = "inside"\nversion = "0.1.0"\nedition = "2021"\n'
                '[dependencies]\noutside = { path = "../outside" }\n'
            )
            (root / "Cargo.lock").write_text('version = 3\n')
            (root / "src").mkdir()
            (root / "src" / "lib.rs").touch()
            head = commit_fixture(root)
            result = self.run_trusted_audit_python(stage, root, head)
            self.assertEqual(result.returncode, 0, result.stderr)
            outside = root / "outside"
            (outside / "src").mkdir(parents=True)
            (outside / "src" / "lib.rs").touch()
            (outside / "Cargo.toml").write_text(
                '[package]\nname = "outside"\nversion = "0.1.0"\nedition = "2021"\n'
            )
            subprocess.run(
                ["cargo", "generate-lockfile", "--offline", "--manifest-path", str(root / "audit-input" / "Cargo.toml")],
                cwd=root,
                capture_output=True,
                text=True,
                check=True,
            )
            result = self.run_trusted_audit_python(boundary, root, head)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("local dependency manifest escapes PR data", result.stderr)

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
        self.assertIn("seq 1 100", signal_step)
        self.assertNotIn("seq 1 1000", signal_step)
        self.assertNotIn("seq 1 5000", signal_step)
        self.assertNotIn("seq 1 10000", signal_step)
        self.assertNotIn("seq 1 20000", signal_step)
        self.assertNotIn("seq 1 50000", signal_step)
        self.assertIn('echo "FAIL: Unexpected exit code: $EXIT_CODE" >&2', signal_step)
        self.assertIn("exit 1", signal_step)

    def test_exact_toolchain_pins_the_immutable_nimble_registry_snapshot(self) -> None:
        action = (WORKFLOW_DIR.parent / "actions/setup-nim-parser-toolchain/action.yml").read_text(
            encoding="utf-8"
        )
        revision = "b79eaaa3fc65fc473bc9e803445f8f7aef7112a2"
        self.assertIn(f"package_list_revision='{revision}'", action)
        self.assertIn(
            "raw.githubusercontent.com/nim-lang/packages/${package_list_revision}/packages.json",
            action,
        )
        self.assertIn("cp \"${pinned_metadata}\" \"${nimble_dir}/packages_temp.json\"", action)

    def test_v08_gate_exposes_just_built_parser_dll_on_windows(self) -> None:
        workflow = self.workflow("ci.yml")
        stage_offset = workflow.index("name: Stage just-built parser for v0.8 surfaces")
        gate_offset = workflow.index("name: Run v0.8 release gate", stage_offset)
        windows_path_step = workflow[stage_offset:gate_offset]
        self.assertIn("name: Add Nim library to PATH (Windows)", windows_path_step)
        self.assertIn("if: runner.os == 'Windows'", windows_path_step)
        self.assertIn(
            'echo "${GITHUB_WORKSPACE}/${NIM_SQL_PARSER_DIR}" >> "$GITHUB_PATH"',
            windows_path_step,
        )

    def test_v08_gate_uses_the_just_built_parser_instead_of_vendor(self) -> None:
        workflow = self.workflow("ci.yml")
        gate = workflow.split("  v08-release-gate:", maxsplit=1)[1].split(
            "\n  ci-success:", maxsplit=1
        )[0]

        self.assertIn('ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD: "1"', gate)
        self.assertIn(
            "NIM_SQL_PARSER_LIB_DIR: ${{ github.workspace }}/crates/alopex-sql/nim-sql-parser",
            gate,
        )
        self.assertIn("name: Stage just-built parser for v0.8 surfaces", gate)
        self.assertIn('(cd "${local_dir}" && sha256sum -c SHA256SUMS)', gate)
        self.assertNotIn('reviewed_dir="${NIM_SQL_PARSER_DIR}/vendor/', gate)

    def test_local_lane_ci_supplies_python_and_rootless_parser_runtime(self) -> None:
        makefile = (REPOSITORY_ROOT / "Makefile").read_text(encoding="utf-8")
        runner = (REPOSITORY_ROOT / "scripts/run-lane-ci.sh").read_text(
            encoding="utf-8"
        )
        parser_build = (REPOSITORY_ROOT / "scripts/build-nim-parser.sh").read_text(
            encoding="utf-8"
        )
        python_build = (REPOSITORY_ROOT / "crates/alopex-py/build.rs").read_text(
            encoding="utf-8"
        )

        self.assertIn("lane-ci test-lane-ci:", makefile)
        self.assertIn("bash scripts/run-lane-ci.sh", makefile)
        self.assertIn('"numpy<2"', runner)
        self.assertIn('export PYO3_PYTHON="${venv_python}"', runner)
        self.assertIn("LD_LIBRARY_PATH", runner)
        self.assertIn('parser_backend="${NIM_PARSER_BACKEND:-docker}"', runner)
        self.assertIn("--userns=keep-id", parser_build)
        self.assertIn('token.starts_with("-Wl,-rpath,")', python_build)

    def test_security_audit_is_fail_closed_with_a_guarded_exception(self) -> None:
        workflow = self.workflow("ci.yml")
        audit_job = workflow.split("  security-audit:", maxsplit=1)[1].split(
            "\n  build:", maxsplit=1
        )[0]

        self.assertNotIn("continue-on-error", audit_job)
        self.assertNotIn("RUSTSEC-2026-0194", audit_job)
        self.assertNotIn("RUSTSEC-2026-0195", audit_job)
        tree_guard = audit_job.index(
            "cargo tree --locked --workspace --all-features --target all --prefix none --format '{p}'"
        )
        audit_step = audit_job.index("name: Run RustSec audit")
        self.assertLess(tree_guard, audit_step)
        self.assertIn("RUSTSEC-2026-0235 is reachable", audit_job)
        self.assertIn(
            "cargo install cargo-audit --version 0.22.2 --locked", audit_job
        )
        self.assertIn(
            "cargo audit --file Cargo.lock --ignore RUSTSEC-2026-0235", audit_job
        )
        self.assertNotIn("rustsec/audit-check@", audit_job)

    def test_trusted_security_audit_uses_base_controller_and_pr_data_only(self) -> None:
        workflow = self.workflow("trusted-security-audit.yml")
        self.assertIn("pull_request_target:", workflow)
        self.assertIn("branches: [main]", workflow)
        self.assertIn("types: [opened, synchronize, reopened, ready_for_review, edited]", workflow)
        self.assertIn("contents: read", workflow)
        self.assertNotIn("pull-requests: write", workflow)
        self.assertIn("name: Trusted Security Audit", workflow)
        self.assertNotIn("needs: [scope]", workflow)
        self.assertNotIn("classify-change-scope", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertEqual(workflow.count("uses: actions/checkout@"), 1)
        self.assertIn('git fetch --no-tags --depth=1 origin "refs/pull/${PR_NUMBER}/head"', workflow)
        self.assertIn('"$(git rev-parse FETCH_HEAD)" == "${PR_HEAD_SHA}"', workflow)
        self.assertIn('if path != "Cargo.lock" and parts[-1] != "Cargo.toml":', workflow)
        self.assertIn('mode not in (b"100644", b"100755")', workflow)
        self.assertIn('name: Reject local dependencies outside PR data', workflow)
        self.assertIn('if package["source"] is not None:', workflow)
        self.assertIn('if not manifest.is_relative_to(root) or not manifest.is_file():', workflow)
        self.assertIn('manifest-path: audit-input/Cargo.toml', workflow)
        self.assertIn('arguments: --all-features --locked --config deny.toml', workflow)
        self.assertIn('uses: EmbarkStudios/cargo-deny-action@3c6349835b2b7b196a839186cb8b78e02f7b5f25', workflow)
        self.assertIn('working-directory: audit-input', workflow)
        self.assertIn("cargo tree --locked --workspace --all-features --target all --prefix none --format '{p}'", workflow)
        self.assertIn('cargo audit --file Cargo.lock --ignore RUSTSEC-2026-0235', workflow)
        self.assertNotIn("continue-on-error", workflow)

    def test_clippy_uses_the_release_msrv_toolchain(self) -> None:
        workflow = self.workflow("ci.yml")
        clippy_job = workflow.split("  clippy:", maxsplit=1)[1].split(
            "\n  test:", maxsplit=1
        )[0]

        self.assertIn("uses: dtolnay/rust-toolchain@1.90.0", clippy_job)
        self.assertNotIn("uses: dtolnay/rust-toolchain@stable", clippy_job)
        self.assertIn(
            "cargo clippy --all-targets --all-features -- -D warnings",
            clippy_job,
        )

    def test_release_verdict_reuses_approved_ci_and_checks_delivery_only(self) -> None:
        workflow = self.workflow("release.yml")
        approval = workflow.split("  ci-gate:", maxsplit=1)[1].split(
            "\n  build-release:", maxsplit=1
        )[0]

        self.assertIn("Approved source evidence", approval)
        self.assertIn("gh run list --workflow ci.yml", approval)
        self.assertNotIn("origin/main", approval)
        self.assertNotIn("git merge-base --is-ancestor", approval)
        for forbidden in (
            "cargo test",
            "cargo clippy",
            "maturin",
            "test-nim-parser.sh",
            "v07_gate.sh",
            "verify-v08-surfaces.sh",
        ):
            self.assertNotIn(forbidden, approval)

    def test_release_process_changes_have_a_short_dedicated_lane(self) -> None:
        process = self.workflow("release-process.yml")
        parity = self.workflow("parity-harness.yml")
        ci = self.workflow("ci.yml")
        compatibility = self.workflow("compatibility.yml")
        stress = self.workflow("stress-tests.yml")

        for path in (
            ".github/workflows/**",
            "scripts/release/**",
            "formal/release-report/**",
        ):
            self.assertIn(path, process)
            self.assertIn(path, compatibility)
        scope = (REPOSITORY_ROOT / "scripts/ci/classify-change-scope.sh").read_text()
        self.assertIn("bash scripts/ci/classify-change-scope.sh", ci)
        for path in (
            ".github/workflows/*",
            "scripts/release/*",
            "formal/release-report/*",
        ):
            self.assertIn(path, scope)
        self.assertIn("name: Change scope", ci)
        self.assertIn("needs: [scope]", ci)
        self.assertIn("needs.scope.outputs.production == 'true'", ci)
        self.assertIn("Release-process-only change", ci)
        self.assertIn("paths-ignore:", compatibility)
        self.assertIn("python -m unittest discover -s scripts/release/tests", process)
        self.assertNotIn("cargo test", process)
        self.assertNotIn("pull_request:", stress)
        self.assertNotIn("release-process-contract:", ci)
        self.assertIn("scripts/parity/*", scope)
        self.assertGreaterEqual(compatibility.count("scripts/parity/**"), 2)
        self.assertGreaterEqual(parity.count("scripts/parity/**"), 2)
        self.assertIn("scripts.parity.test_compat_fixture_contract", parity)
        self.assertNotIn("cargo test", parity)

    def test_workflows_do_not_clone_an_unpinned_chirps_checkout(self) -> None:
        for workflow_path in WORKFLOW_DIR.glob("*.yml"):
            with self.subTest(workflow=workflow_path.name):
                workflow = workflow_path.read_text(encoding="utf-8")
                self.assertNotIn("checkout-chirps.sh", workflow)
                self.assertNotIn("Checkout Chirps dependency", workflow)

        self.assertFalse(
            (WORKFLOW_DIR.parent / "scripts/checkout-chirps.sh").exists()
        )


if __name__ == "__main__":
    unittest.main()
