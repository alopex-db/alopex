"""Small receipt/selection negatives; never builds or loads Alopex."""
import copy
import tempfile
from pathlib import Path
import unittest

from windows_overflow_acceptance import (
    STAGES, require_runner, validate_python_report, validate_pytest, validate_rust_log,
)


class AcceptanceGuardTests(unittest.TestCase):
    def test_manual_host_is_required(self):
        env = {"RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_EVENT_NAME": "workflow_dispatch",
               "ALOPEX_ACCEPTANCE_SUITE": "windows-overflow"}
        require_runner("win32", env)
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


if __name__ == "__main__":
    unittest.main()
