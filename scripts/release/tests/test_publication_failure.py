import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import publication_failure as failure
import publication_record as contract
import test_report_pair as fixtures
import test_report_publish_shell as git_fixtures


class PublicationFailureTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.ReportPairTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.output = self.case.root / "bundle"

    def prepare(self, **overrides):
        args = dict(root=self.case.root / "download", artifact="release-verification-123-1",
                    run_id="123", attempt=2, version="0.8.17", commit="a" * 40,
                    stage="docs_ingestion", output=self.output)
        args.update(overrides)
        return failure.prepare(**args)

    def test_all_original_outcomes_and_bytes_survive_publication_failure(self):
        for outcome in ("success", "failure", "incomplete"):
            with self.subTest(outcome=outcome):
                self.case.payload["outcome"] = outcome
                self.case.payload["failure_stage"] = None if outcome == "success" else "availability"
                self.case.payload["steps"][0]["status"] = outcome
                self.case.write_pair()
                before = {p.name: p.read_bytes() for p in self.case.source.iterdir()}
                record = self.prepare(output=self.case.root / outcome)
                loaded, blobs = failure.load_bundle(self.case.root / outcome)
                self.assertEqual(loaded, record)
                self.assertEqual(record["verification"]["outcome"], outcome)
                self.assertEqual(record["publication_attempt"], 2)
                self.assertEqual(record["verification"]["producing_attempt"], 1)
                self.assertEqual(len(blobs), 4)
                for ext in ("json", "md"):
                    self.assertEqual(blobs[contract.original_stem(record) + "." + ext], before["v0.8.17." + ext])
                self.assertEqual(before, {p.name: p.read_bytes() for p in self.case.source.iterdir()})

    def test_missing_or_rejected_original_is_unavailable_not_success(self):
        for bad in ("missing", "rejected"):
            with self.subTest(bad=bad):
                if bad == "rejected":
                    self.case.results.write_text("not JSON")
                record = self.prepare(root=self.case.root / bad if bad == "missing" else self.case.root / "download",
                                      stage="artifact_download", output=self.case.root / bad / "bundle")
                self.assertIsNone(record["verification"])
                self.assertIn(b"unavailable or rejected", contract.render(record))
                self.assertEqual(len(failure.load_bundle(self.case.root / bad / "bundle")[1]), 2)

    def test_invalid_publication_identity_is_rejected(self):
        for changes in ({"attempt": 0}, {"attempt": True}, {"artifact": "release-verification-123-3"},
                        {"run_id": "../123"}, {"version": "../0.8.17"}, {"stage": "success"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.prepare(**changes)
        self.assertFalse(self.output.exists())

    def test_altered_original_or_failure_markdown_is_rejected(self):
        record = self.prepare()
        for name in (contract.original_stem(record) + ".json", contract.stem(record) + ".md"):
            path = self.output / name
            old = path.read_bytes()
            path.write_bytes(old + b"\n")
            with self.assertRaises(ValueError):
                failure.load_bundle(self.output)
            path.write_bytes(old)

    def test_extra_file_and_partial_bundle_are_rejected(self):
        record = self.prepare()
        extra = self.output / "extra.json"
        extra.write_text("{}")
        with self.assertRaises(ValueError):
            failure.load_bundle(self.output)
        extra.unlink()
        (self.output / (contract.original_stem(record) + ".md")).unlink()
        with self.assertRaises(ValueError):
            failure.load_bundle(self.output)

    def test_confirmation_requires_all_four_public_files(self):
        self.prepare()
        _, blobs = failure.load_bundle(self.output)
        def fetch(url, **kwargs):
            path = urllib.parse.urlparse(url).path.split("/main/", 1)[1]
            return io.BytesIO(blobs[path])
        with patch.object(failure.urllib.request, "urlopen", side_effect=fetch) as read:
            failure.confirm(self.output, attempts=1, interval=0)
            self.assertEqual(read.call_count, 4)

    def test_403_missing_or_different_public_bytes_never_confirm(self):
        self.prepare()
        errors = [urllib.error.HTTPError("https://example.invalid", 403, "Forbidden", {}, None),
                  urllib.error.HTTPError("https://example.invalid", 404, "Not Found", {}, None)]
        for error in errors:
            with self.subTest(error=error.code), patch.object(failure.urllib.request, "urlopen", side_effect=error), \
                    contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
                failure.confirm(self.output, attempts=1, interval=0)
        with patch.object(failure.urllib.request, "urlopen", return_value=io.BytesIO(b"wrong")), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
            failure.confirm(self.output, attempts=1, interval=0)

    def test_actual_workflow_records_first_failed_phase(self):
        workflow = (Path(__file__).resolve().parents[3] / ".github/workflows/public-release-verification.yml").read_text()
        block = workflow.split("      - name: Record publication failure stage\n", 1)[1].split("\n  publish-vector-benchmark:", 1)[0]
        script = "\n".join(line[10:] for line in block.split("        run: |\n", 1)[1].splitlines())
        phases = [("DOWNLOAD", "artifact_download"), ("SELECTED", "report_validation"),
                  ("VALIDATED", "report_validation"), ("RETAINED", "source_retention"),
                  ("INGESTED", "docs_ingestion"), ("CLEANED", "branch_cleanup")]
        for index, (phase, stage) in enumerate(phases):
            with self.subTest(phase=phase):
                output = self.case.root / (phase + ".txt")
                env = dict(os.environ, GITHUB_OUTPUT=str(output))
                env.update({name: "success" if pos < index else "failure" if pos == index else "skipped"
                            for pos, (name, _) in enumerate(phases)})
                subprocess.run(["bash", "-eu", "-c", script], env=env, check=True, capture_output=True)
                self.assertEqual(output.read_text(), "stage=" + stage + "\n")

    def test_actual_retain_command_reuses_identical_branch_and_rejects_changes(self):
        repo = git_fixtures.ReportPublisherTests()
        repo.setUp()
        self.addCleanup(repo.doCleanups)
        self.prepare()
        command = [sys.executable, str(Path(failure.__file__)), "retain", "--root", str(self.output),
                   "--github-output", str(repo.root / "output")]
        def run():
            return subprocess.run(command, cwd=repo.repo, env=repo.env, capture_output=True, text=True)
        result = run()
        self.assertEqual(result.returncode, 0, result.stderr)
        original = repo.git("rev-parse", "HEAD").stdout
        repo.git("checkout", "--detach", repo.base)
        self.assertEqual(run().returncode, 0)
        record, blobs = failure.load_bundle(self.output)
        record["stage"] = "source_retention"
        prefix = contract.stem(record)
        (self.output / (prefix + ".json")).write_text(json.dumps(record))
        (self.output / (prefix + ".md")).write_bytes(contract.render(record))
        conflict = run()
        self.assertNotEqual(conflict.returncode, 0)
        self.assertIn("refusing to overwrite", conflict.stderr)
        branch = "report/verify-release-failure-v0.8.17-123-2"
        self.assertEqual(repo.git("rev-parse", branch).stdout, original)


if __name__ == "__main__":
    unittest.main()
