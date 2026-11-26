"""Small receipt/selection negatives; never builds or loads Alopex."""
import copy
import os
import shutil
import stat
import tempfile
from pathlib import Path
import unittest
import json
import contextlib
import io
from unittest import mock
import subprocess
import time
import windows_overflow_acceptance as acceptance

from windows_overflow_acceptance import (
    STAGES, PHASES, PILOT_PHASES, OLD_WHEEL_NAME, OLD_WHEEL_SHA256, EvidenceError,
    pilot_projected_seconds, projected_full_time, validate_pilot_admission,
    public_result, publish,
    require_runner, validate_python_report,
    validate_pytest, validate_rust_log,
    native_git_bash, retry_readonly,
)


class AcceptanceGuardTests(unittest.TestCase):
    def test_pilot_and_full_gate_physical_volumes_before_acquisition(self):
        workflow = (acceptance.REPO / ".github/workflows/compatibility.yml").read_text()
        pilot = workflow.split("  windows-overflow-pilot:\n", 1)[1].split(
            "  windows-overflow-public:\n", 1)[0]
        full = workflow.split("  windows-overflow-public:\n", 1)[1].split(
            "  historical-parser:\n", 1)[0]
        for job in (pilot, full):
            gate = job.index("- name: Admit physical volumes before checkout and tool setup")
            checkout = job.index("- uses: actions/checkout@v4")
            python_setup = job.index("- uses: actions/setup-python@v5")
            prepare = job.index("windows_overflow_acceptance.py prepare")
            rust_setup = job.index("- uses: dtolnay/rust-toolchain@1.96.0")
            nim_setup = job.index("windows_overflow_acceptance.py toolchain")
            self.assertLess(gate, checkout)
            self.assertLess(checkout, python_setup)
            self.assertLess(python_setup, prepare)
            self.assertLess(prepare, rust_setup)
            self.assertLess(rust_setup, nim_setup)
            self.assertIn("$free -lt 8000000000", job[gate:checkout])
            self.assertIn("timeout-minutes: 5", job[rust_setup:nim_setup])
            self.assertIn("id: checkout", job[checkout:python_setup])
            self.assertIn("id: python_setup", job[python_setup:prepare])
            guarded_tail = job[nim_setup:]
            guard = "if: always() && steps.checkout.outcome == 'success' && steps.python_setup.outcome == 'success'"
            self.assertEqual(guarded_tail.count(guard), 3)

    def test_physical_admission_records_private_volumes_and_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = object.__new__(acceptance.Acceptance)
            owner.output = Path(directory)
            volumes = [{"volume_id": 1, "roots": ["private-root"],
                        "total_bytes": 12, "free_bytes": 8}]
            with mock.patch.object(owner, "volume_roots", return_value=[Path(directory)]), \
                 mock.patch.object(acceptance.storage, "physical_volumes",
                                   return_value=volumes):
                owner.check_physical(8, "admission.private.json")
                self.assertEqual(json.loads((owner.output / "admission.private.json").read_text())
                                 ["volumes"], volumes)
                with self.assertRaises(EvidenceError) as caught:
                    owner.check_physical(9, "refusal.private.json")
                self.assertEqual(caught.exception.code, "PHYSICAL_SPACE")
            with mock.patch.object(owner, "volume_roots", return_value=[Path(directory)]), \
                 mock.patch.object(acceptance.storage, "physical_volumes",
                                   side_effect=PermissionError("private-root")):
                with self.assertRaises(EvidenceError) as caught:
                    owner.check_physical(8, "missing.private.json")
                self.assertEqual(caught.exception.code, "VOLUME_COVERAGE")

    def test_command_watchdog_maps_physical_refusal_to_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "plan.json").write_text(json.dumps({
                "initial_allocated_bytes": 0, "estimated_peak_additional_bytes": 500_000_000,
                "job_started_unix": 0}))
            owner = object.__new__(acceptance.Acceptance)
            owner.suite = "windows-overflow-pilot"
            owner.phase = "pilot"
            owner.phase_started = 0
            owner.output = root
            owner.work = root / "work"
            owner.target = root / "target"
            owner.env = {"RUSTFLAGS": "-D warnings"}
            process = mock.Mock()
            process.wait.return_value = 1

            marker = "PHYSICAL_SPACE"

            def refusal(command, **kwargs):
                self.assertIn("--physical-min-free-bytes", command)
                self.assertIn("--volume-root", command)
                kwargs["stdout"].write(marker + "\n")
                kwargs["stdout"].flush()
                return process

            for marker in ("PHYSICAL_SPACE", "STOP_FAILURE"):
                with mock.patch.object(acceptance.storage, "check",
                                       return_value={"allocated_bytes": 0}), \
                     mock.patch.object(acceptance.subprocess, "Popen", side_effect=refusal), \
                     mock.patch.object(owner, "check_physical"), \
                     mock.patch.object(owner, "check_time_budget"):
                    with self.assertRaises(EvidenceError) as caught:
                        owner.run("physical-stop", ["pilot-command"])
                self.assertEqual(caught.exception.code, marker)

    def test_evidence_certificate_has_exact_cross_platform_checkout_bytes(self):
        attributes = (acceptance.REPO / ".gitattributes").read_text(encoding="utf-8")
        self.assertIn("/.github/ops/native-evidence-cert.pem text eol=lf", attributes.splitlines())
        self.assertEqual(acceptance.digest(acceptance.EVIDENCE_CERT),
                         acceptance.EVIDENCE_CERT_SHA256)

    def test_full_prepare_rejects_missing_or_stale_pilot_admission(self):
        source = {"sha": "a" * 40, "tree": "b" * 40,
                  "workflow_sha256": "c" * 64, "driver_sha256": "d" * 64}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "admission.json"
            with self.assertRaises(EvidenceError):
                validate_pilot_admission(path, source)
            receipt = {"schema": "alopex.windows-overflow-pilot-admission/v1",
                       "pilot_run_id": "123", "pilot_run_attempt": "1",
                       "source_sha": source["sha"], "source_tree": source["tree"],
                       "workflow_sha256": source["workflow_sha256"],
                       "driver_sha256": source["driver_sha256"],
                       "artifact_sha256": "e" * 64, "admitted": True}
            path.write_text(json.dumps(receipt))
            self.assertEqual(validate_pilot_admission(path, source), receipt)
            receipt["source_sha"] = "f" * 40
            path.write_text(json.dumps(receipt))
            with self.assertRaises(EvidenceError) as caught:
                validate_pilot_admission(path, source)
            self.assertEqual(caught.exception.code, "PILOT")

    def test_stalled_pilot_command_stops_owned_tree_at_twenty_minutes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "plan.json").write_text(json.dumps({
                "initial_allocated_bytes": 0, "estimated_peak_additional_bytes": 500_000_000,
                "job_started_unix": 0}))
            owner = object.__new__(acceptance.Acceptance)
            owner.suite = "windows-overflow-pilot"
            owner.phase = "pilot"
            owner.phase_started = 0
            owner.output = root
            owner.work = root / "work"
            owner.target = root / "target"
            owner.env = {"RUSTFLAGS": "-D warnings"}
            process = mock.Mock()
            process.wait.side_effect = subprocess.TimeoutExpired("pilot", 0.1)
            with (mock.patch.object(acceptance.storage, "check",
                                    return_value={"allocated_bytes": 0}),
                  mock.patch.object(acceptance.subprocess, "Popen", return_value=process),
                  mock.patch.object(owner, "check_physical"),
                  mock.patch.object(acceptance.time, "time", side_effect=[0, 0, 1200, 1200, 1200]),
                  mock.patch.object(owner, "stop_process_tree") as stop):
                with self.assertRaises(EvidenceError) as caught:
                    owner.run("stall", ["pilot-command"])
            self.assertEqual(caught.exception.code, "TIME_BUDGET")
            stop.assert_called_once_with(process)

    def test_toolchain_log_cap_stops_process_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "plan.json").write_text(json.dumps({
                "initial_allocated_bytes": 0, "estimated_peak_additional_bytes": 2_000_000_000,
                "job_started_unix": time.time()}))
            owner = object.__new__(acceptance.Acceptance)
            owner.suite = "windows-overflow-pilot"
            owner.phase = "toolchain"
            owner.phase_started = time.time()
            owner.output = root
            owner.work = root / "work"
            owner.target = root / "target"
            owner.env = {"RUSTFLAGS": "-D warnings"}
            process = mock.Mock()
            def overflow(timeout=None):
                with (root / "nim-toolchain.log").open("r+b") as stream:
                    stream.truncate(acceptance.MAX_PRIVATE_BYTES + 1)
                raise subprocess.TimeoutExpired("toolchain", 0.1)
            process.wait.side_effect = overflow
            with (mock.patch.object(acceptance.storage, "check",
                                    return_value={"allocated_bytes": 0}),
                  mock.patch.object(acceptance.subprocess, "Popen", return_value=process),
                  mock.patch.object(owner, "check_physical"),
                  mock.patch.object(owner, "stop_process_tree") as stop):
                with self.assertRaises(EvidenceError) as caught:
                    owner.run("nim-toolchain", ["toolchain-command"],
                              additional=2_000_000_000, read_output=False)
            self.assertEqual(caught.exception.code, "EVIDENCE")
            stop.assert_called_once_with(process)

    def test_full_projection_stops_at_exact_margin_and_rejects_bad_clock(self):
        self.assertLess(projected_full_time(0, 0, "prepare", 0), 6_600)
        self.assertLess(projected_full_time(0, 1_319, "normal", 0), 6_600)
        self.assertEqual(projected_full_time(0, 1_320, "normal", 0), 6_600)
        with self.assertRaises(EvidenceError):
            projected_full_time(2, 1, "normal", 1)

    def test_pilot_global_stop_reserves_cleanup_and_publication(self):
        owner = object.__new__(acceptance.Acceptance)
        owner.suite = "windows-overflow-pilot"
        owner.phase = "pilot"
        owner.phase_started = 1_499
        with mock.patch.object(acceptance.time, "time", return_value=1_499):
            owner.check_time_budget({"job_started_unix": 0})
        with mock.patch.object(acceptance.time, "time", return_value=1_500):
            with self.assertRaises(EvidenceError) as caught:
                owner.check_time_budget({"job_started_unix": 0})
        self.assertEqual(caught.exception.code, "TIME_BUDGET")
        self.assertEqual(30 * 60 - acceptance.PILOT_JOB_EARLY_STOP_SECONDS, 300)

    def test_pilot_projection_requires_all_probes_and_preserves_legacy_reserve(self):
        units = ("toolchain", "parser", "runtime-venv", "pytest-install", "normal-build", "normal-package",
                 "ordinary-case", "rust-build", "rust-case", "instrumented-build",
                 "instrumented-package", "python-case",
                 "legacy-venv", "legacy-download", "legacy-install", "legacy-import")
        timings = {name: 1.0 for name in units}
        self.assertGreaterEqual(pilot_projected_seconds(timings), 35 * 60)
        with self.assertRaises(EvidenceError):
            pilot_projected_seconds(dict(timings, **{"rust-case": 0.0}))
        with self.assertRaises(EvidenceError):
            pilot_projected_seconds({k: v for k, v in timings.items() if k != "python-case"})
        timings["rust-case"] = 90.0
        self.assertGreater(pilot_projected_seconds(timings) + 600, 6_600)

    def test_pilot_public_receipt_requires_source_probe_and_cleanup(self):
        sha = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for phase in PILOT_PHASES:
                (output / f"phase-{phase}.json").write_text(json.dumps({
                    "phase": phase, "status": "passed"}))
            source = {"sha": sha, "tree": "b" * 40,
                      "workflow_sha256": "c" * 64, "driver_sha256": "d" * 64,
                      "run_id": "123", "attempt": "1"}
            (output / "source.json").write_text(json.dumps(source))
            (output / "cleanup.json").write_text(json.dumps({
                "target_removed": True, "work_removed": True}))
            pilot = {"source": source, "budget_admitted": True,
                     "probe_counts": {"ordinary": 1, "rust": 1, "python": 1,
                                      "old_wheel": 1}}
            (output / "pilot.json").write_text(json.dumps(pilot))
            public = public_result(output, sha, "windows-overflow-pilot")
            self.assertEqual(public["outcome"], "passed")
            self.assertEqual(public["case"], "WINDOWS_OVERFLOW_PILOT")
            self.assertEqual(public["source_tree"], "b" * 40)
            pilot["probe_counts"]["python"] = 0
            (output / "pilot.json").write_text(json.dumps(pilot))
            self.assertEqual(public_result(output, sha, "windows-overflow-pilot")[
                "error_code"], "PILOT")

    def test_public_output_does_not_follow_a_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "outside").mkdir()
            try:
                (root / "output").symlink_to(root / "outside", target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("symlink creation unavailable")
            with self.assertRaises(EvidenceError):
                publish(root / "output", "a" * 40)
            self.assertEqual(list((root / "outside").iterdir()), [])

    def test_manual_workflow_uploads_only_fixed_public_and_encrypted_files(self):
        workflow = (acceptance.REPO / ".github/workflows/compatibility.yml").read_text()
        pilot_job = workflow.split("  windows-overflow-pilot:", 1)[1].split(
            "  windows-overflow-public:", 1)[0]
        self.assertLess(pilot_job.index("Start bounded pilot clock"),
                        pilot_job.index("actions/checkout@v4"))
        self.assertLess(pilot_job.index("windows_overflow_acceptance.py prepare"),
                        pilot_job.index("windows_overflow_acceptance.py toolchain"))
        self.assertLess(pilot_job.index("windows_overflow_acceptance.py toolchain"),
                        pilot_job.index("windows_overflow_acceptance.py parser"))
        self.assertNotIn("uses: ./.github/actions/setup-nim-parser-toolchain", pilot_job)
        self.assertNotIn("-m unittest", pilot_job)
        self.assertNotIn("CARGO_TARGET_DIR:", pilot_job)
        self.assertLess(pilot_job.index("windows_overflow_acceptance.py pilot"),
                        pilot_job.index("windows_overflow_acceptance.py cleanup"))
        self.assertEqual(workflow.count("inputs.acceptance_suite == 'full'"), 3)
        job = workflow.split("  windows-overflow-public:", 1)[1].split(
            "  historical-parser:", 1)[0]
        self.assertLess(job.index("Start full-job clock before checkout"),
                        job.index("actions/checkout@v4"))
        self.assertLess(job.index("windows_pilot_attestation.py"),
                        job.index("windows_overflow_acceptance.py prepare"))
        self.assertLess(job.index("windows_overflow_acceptance.py prepare"),
                        job.index("windows_overflow_acceptance.py toolchain"))
        self.assertLess(job.index("windows_overflow_acceptance.py toolchain"),
                        job.index("windows_overflow_acceptance.py parser"))
        self.assertNotIn("uses: ./.github/actions/setup-nim-parser-toolchain", job)
        self.assertNotIn("-m unittest", job)
        self.assertNotIn("CARGO_TARGET_DIR:", job)
        upload = job.split("      - name: Upload encrypted diagnostics and validated public result", 1)[1]
        self.assertIn("artifacts/windows-overflow-public/public.json", upload)
        self.assertIn("artifacts/windows-overflow-public/private-evidence.cms", upload)
        self.assertNotIn("**/", upload)
        self.assertNotIn("*.log", upload)
        self.assertNotIn("*.jsonl", upload)
        self.assertLess(job.index("run: python scripts/compatibility/windows_overflow_acceptance.py publish"),
                        job.index("      - name: Upload encrypted diagnostics and validated public result"))

    def test_command_failure_keeps_private_exception_out_of_actions_log(self):
        with tempfile.TemporaryDirectory() as directory:
            stderr = io.StringIO()
            with (mock.patch.object(acceptance, "REPO", Path(directory)),
                  mock.patch.object(acceptance, "Acceptance",
                                    side_effect=RuntimeError("C:\\secret\\private.log")),
                  mock.patch.object(acceptance.sys, "argv", ["acceptance", "normal"]),
                  contextlib.redirect_stderr(stderr)):
                self.assertEqual(acceptance.main(), 1)
            self.assertEqual(stderr.getvalue().strip(), "WINDOWS_OVERFLOW_NORMAL")
            private = Path(directory) / "artifacts/windows-overflow-public/phase-normal.private.log"
            self.assertIn("C:\\secret", private.read_text())

    def test_public_result_preserves_distinct_physical_failure_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for code in ("PHYSICAL_SPACE", "VOLUME_COVERAGE"):
                (output / "phase-prepare.json").write_text(json.dumps({
                    "phase": "prepare", "status": "failed", "error_code": code}))
                public = public_result(output, "a" * 40, "windows-overflow-pilot")
                self.assertEqual(public["error_code"], code)
                self.assertEqual(set(public), {"schema", "case", "outcome",
                                               "error_code", "source_sha"})

    def test_public_result_allowlists_success_and_fixed_failure_codes(self):
        sha = "a" * 40
        openssl = shutil.which("openssl")
        if openssl is None:
            self.skipTest("OpenSSL unavailable for local CMS verification")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for phase in PHASES:
                (output / f"phase-{phase}.json").write_text(
                    json.dumps({"phase": phase, "status": "passed"}))
            source = {"sha": sha, "private_path": "C:\\private\\do-not-upload"}
            (output / "source.json").write_text(json.dumps(source))
            (output / "result.json").write_text(json.dumps({
                "status": "passed", "source": source, "rust_boundary_cases": 24,
                "python_boundary_cases": 24, "ordinary_python_cases": 15,
                "legacy_container_processes": 4, "legacy_live_wal_processes": 6,
                "legacy_wheel_sha256": OLD_WHEEL_SHA256,
                "raw_exception": "C:\\private\\do-not-upload"}))
            (output / "legacy-wheel-identity.json").write_text(json.dumps({
                "filename": OLD_WHEEL_NAME, "sha256": OLD_WHEEL_SHA256}))
            (output / "cleanup.json").write_text(json.dumps({
                "target_removed": True, "work_removed": True,
                "private_path": "C:\\private\\do-not-upload"}))
            (output / "phase-legacy.json").write_text(json.dumps({
                "phase": "legacy", "status": "failed", "secret": "C:\\private"}))
            self.assertEqual(public_result(output, sha)["error_code"], "LEGACY")
            (output / "phase-legacy.json").write_text(json.dumps({
                "phase": "legacy", "status": "passed"}))
            self.assertTrue(publish(output, sha, openssl))
            public = (output / "public.json").read_text()
            self.assertNotIn("private", public)
            self.assertEqual(set(json.loads(public)), {
                "schema", "case", "outcome", "error_code", "source_sha",
                "ciphertext_sha256", "old_wheel_sha256"})
            self.assertEqual({k: v for k, v in json.loads(public).items()
                              if k != "ciphertext_sha256"}, {
                "schema": "alopex.windows-overflow-public/v1",
                "case": "WINDOWS_OVERFLOW_PUBLIC", "outcome": "passed",
                "error_code": None, "source_sha": sha,
                "old_wheel_sha256": OLD_WHEEL_SHA256})
            self.assertTrue((output / "private-evidence.cms").is_file())
            self.assertFalse((output / "source.json").exists())
            self.assertNotIn("C:\\private", public)
            self.assertIsNone(public_result(output, "invalid")["source_sha"])

    def test_git_bash_is_selected_without_path_fallback_to_wsl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Git/cmd").mkdir(parents=True)
            (root / "Git/bin").mkdir()
            git = root / "Git/cmd/git.exe"
            git.touch()
            bash = root / "Git/bin/bash.exe"
            bash.touch()
            self.assertEqual(native_git_bash(str(git)), bash.resolve())
            # A missing Git Bash must fail even if an unrelated bash is on PATH.
            bash.unlink()
            with self.assertRaisesRegex(RuntimeError, "not fall back to WSL"):
                native_git_bash(str(git))

    def test_readonly_pack_cleanup_and_unrelated_error_propagation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pack = root / "pack.idx"
            pack.write_bytes(b"tiny pack")
            pack.chmod(stat.S_IREAD)
            denied = PermissionError("readonly pack")
            retry_readonly(os.unlink, str(pack), (PermissionError, denied, None))
            self.assertFalse(pack.exists())
            pack.write_bytes(b"writable")
            with self.assertRaises(PermissionError):
                retry_readonly(os.unlink, str(pack), (PermissionError, denied, None))
            self.assertTrue(pack.exists())
            io_error = OSError("other I/O error")
            with self.assertRaisesRegex(OSError, "other I/O"):
                retry_readonly(os.unlink, str(pack), (OSError, io_error, None))

    def test_owned_tree_with_readonly_git_pack_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "owned"
            pack = root / ".git/objects/pack/small.idx"
            pack.parent.mkdir(parents=True)
            pack.write_bytes(b"pack")
            pack.chmod(stat.S_IREAD)
            shutil.rmtree(root, onerror=retry_readonly)
            self.assertFalse(root.exists())

    def test_manual_host_is_required(self):
        env = {"RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_EVENT_NAME": "workflow_dispatch",
               "ALOPEX_ACCEPTANCE_SUITE": "windows-overflow"}
        require_runner("win32", env)
        require_runner("win32", dict(env, ALOPEX_ACCEPTANCE_SUITE="windows-overflow-pilot"))
        for platform, changes in (("linux", {}), ("win32", {"RUNNER_ENVIRONMENT": "self-hosted"}),
                                  ("win32", {"GITHUB_EVENT_NAME": "pull_request"}),
                                  ("win32", {"ALOPEX_ACCEPTANCE_SUITE": "full"})):
            with self.subTest(platform=platform, changes=changes), self.assertRaises(RuntimeError):
                require_runner(platform, dict(env, **changes))

    def test_python_receipt_cannot_hide_missing_failed_or_duplicate_cases(self):
        cases = [{"case": f"{surface}-{mode}-{stage}", "status": "passed",
                  "fixture_bytes": 512, "reopen_cycles": 2}
                 for surface in ("kv", "sql") for mode in ("error", "exit") for stage in STAGES]
        good = {"status": "passed", "native_sha256": "a" * 64, "cases": cases}
        validate_python_report(good, "a" * 64)
        bad_reports = []
        for field, value in (("status", "failed"), ("fixture_bytes", 1_048_577), ("reopen_cycles", 1)):
            bad = copy.deepcopy(good)
            bad["cases"][0][field] = value
            bad_reports.append(bad)
        bad_reports += [dict(good, cases=cases[:-1]), dict(good, cases=cases[:-1] + [cases[0]]),
                        dict(good, native_sha256="b" * 64), dict(good, status="failed")]
        for index, bad in enumerate(bad_reports):
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                validate_python_report(bad, "a" * 64)

    def test_rust_summary_alone_cannot_claim_acceptance(self):
        lines = [f"test {surface}::{mode}_{stage} ... ok" for surface in ("kv", "sql")
                 for mode in ("error", "exit") for stage in STAGES]
        summary = "test result: ok. 24 passed; 0 failed; 0 ignored; 1 filtered out; finished in 1s\n"
        validate_rust_log("\n".join(lines) + "\n" + summary)
        for bad in (summary, "\n".join(lines[:-1]) + "\n" + summary,
                    "\n".join(lines[:-1] + [lines[0]]) + "\n" + summary,
                    "\n".join(lines) + "\n" + summary.replace("0 ignored", "1 ignored")):
            with self.subTest(log=bad[:40]), self.assertRaises(RuntimeError):
                validate_rust_log(bad)

    def test_pytest_skips_and_errors_are_not_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.xml"
            cases = [f'<testcase name="case{i}" />' for i in range(15)]
            path.write_text("<testsuite>" + "".join(cases) + "</testsuite>")
            validate_pytest(path)
            for child in ("skipped", "failure", "error"):
                path.write_text("<testsuite>" + "".join(cases[:-1]) +
                                f'<testcase name="case14"><{child}/></testcase></testsuite>')
                with self.subTest(child=child), self.assertRaises(RuntimeError):
                    validate_pytest(path)
            path.write_text("<testsuite>" + "".join(cases[:-1]) + "</testsuite>")
            with self.assertRaises(RuntimeError):
                validate_pytest(path)

    def test_build_parser_prefers_host_localappdata_temp_over_same_drive_work_tmp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            local_app_data = root / "LocalAppData"
            host_temp = local_app_data / "Temp"
            host_temp.mkdir(parents=True)
            work = root / "work"
            (work / "tmp").mkdir(parents=True)
            owner = object.__new__(acceptance.Acceptance)
            owner.work = work
            with mock.patch.dict(os.environ, {"LOCALAPPDATA": str(local_app_data)}, clear=False):
                self.assertEqual(owner.parser_temp_dir(), host_temp.resolve())

    def test_windows_jobs_and_driver_keep_test_profile_at_opt_level_zero(self):
        workflow = (acceptance.REPO / ".github/workflows/compatibility.yml").read_text()
        pilot = workflow.split("  windows-overflow-pilot:\n", 1)[1].split(
            "  windows-overflow-public:\n", 1)[0]
        full = workflow.split("  windows-overflow-public:\n", 1)[1].split(
            "  historical-parser:\n", 1)[0]
        self.assertIn("CARGO_PROFILE_TEST_OPT_LEVEL: 0", pilot)
        self.assertIn("CARGO_PROFILE_TEST_OPT_LEVEL: 0", full)
        with tempfile.TemporaryDirectory() as directory:
            env = {
                "RUNNER_ENVIRONMENT": "github-hosted",
                "GITHUB_EVENT_NAME": "workflow_dispatch",
                "ALOPEX_ACCEPTANCE_SUITE": "windows-overflow-pilot",
                "RUNNER_TEMP": directory,
                "PATH": "",
            }
            with mock.patch.object(acceptance.sys, "platform", "win32"), \
                 mock.patch.dict(os.environ, env, clear=False):
                owner = acceptance.Acceptance("pilot")
            self.assertEqual(owner.env["CARGO_PROFILE_TEST_OPT_LEVEL"], "0")

    def test_stop_process_tree_accepts_taskkill_128_when_process_exited_and_fails_if_alive(self):
        process = mock.Mock()
        process.pid = 4321
        process.poll.return_value = None
        process.wait.return_value = 1
        with mock.patch.object(acceptance.subprocess, "run",
                               return_value=mock.Mock(returncode=128)):
            acceptance.Acceptance.stop_process_tree(process)
        process.wait.assert_called_once_with(timeout=20)

        stuck = mock.Mock()
        stuck.pid = 4322
        stuck.poll.return_value = None
        stuck.wait.side_effect = subprocess.TimeoutExpired("stuck", 20)
        with mock.patch.object(acceptance.subprocess, "run",
                               return_value=mock.Mock(returncode=128)):
            with self.assertRaises(EvidenceError) as caught:
                acceptance.Acceptance.stop_process_tree(stuck)
        self.assertEqual(caught.exception.code, "STOP_FAILURE")

    def test_run_passes_timing_output_flag_to_storage_wrapper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "plan.json").write_text(json.dumps({
                "initial_allocated_bytes": 0, "estimated_peak_additional_bytes": 500_000_000,
                "job_started_unix": 0}))
            owner = object.__new__(acceptance.Acceptance)
            owner.suite = "windows-overflow-pilot"
            owner.phase = "pilot"
            owner.phase_started = 0
            owner.output = root
            owner.work = root / "work"
            owner.target = root / "target"
            owner.env = {"RUSTFLAGS": "-D warnings"}
            seen = []
            process = mock.Mock()
            process.wait.return_value = 0

            def capture(command, **kwargs):
                seen.append(command)
                return process

            timing = root / "pilot-ordinary-case-timing.private.json"
            with mock.patch.object(acceptance.storage, "check",
                                   return_value={"allocated_bytes": 0}), \
                 mock.patch.object(acceptance.subprocess, "Popen", side_effect=capture), \
                 mock.patch.object(owner, "check_physical"), \
                 mock.patch.object(owner, "check_time_budget"):
                owner.run("pilot-ordinary-case", ["pytest"], timing_output=timing)
            self.assertIn("--timing-output", seen[0])
            self.assertIn(str(timing), seen[0])


if __name__ == "__main__":
    unittest.main()
