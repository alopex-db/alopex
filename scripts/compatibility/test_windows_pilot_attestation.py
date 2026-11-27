"""Offline source and artifact admission negatives; no network or build."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error
import zipfile

import windows_pilot_attestation as admission


SHA = "a" * 40
TREE = "b" * 40
WORKFLOW_HASH = "c" * 64
DRIVER_HASH = "d" * 64
REPO = "asopitech/alopex"
RUN_ID = 123
ATTEMPT = 2


class PilotAttestationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.archive = Path(self.temp.name) / "pilot.zip"
        self.cms = b"encrypted diagnostic bytes"
        self.public = {
            "schema": admission.SCHEMA, "case": "WINDOWS_OVERFLOW_PILOT",
            "outcome": "passed", "error_code": None, "source_sha": SHA,
            "source_tree": TREE, "workflow_sha256": WORKFLOW_HASH,
            "driver_sha256": DRIVER_HASH, "run_id": str(RUN_ID),
            "run_attempt": str(ATTEMPT), "budget_admitted": True,
            "ciphertext_sha256": hashlib.sha256(self.cms).hexdigest(),
        }
        self.run = {
            "id": RUN_ID, "run_attempt": ATTEMPT, "event": "workflow_dispatch",
            "status": "completed", "conclusion": "success", "head_sha": SHA,
            "path": admission.WORKFLOW, "head_branch": "release/v0.8.17",
            "repository": {"full_name": REPO},
            "head_repository": {"full_name": REPO},
            "workflow_id": 42,
        }
        self.workflow = {"id": 42, "path": admission.WORKFLOW}
        self.make_archive()

    def make_archive(self, extra=None):
        with zipfile.ZipFile(self.archive, "w") as bundle:
            bundle.writestr("public.json", json.dumps(self.public))
            bundle.writestr("private-evidence.cms", self.cms)
            if extra:
                bundle.writestr(extra, b"unsafe")
        self.artifacts = {"total_count": 1, "artifacts": [{
            "name": f"windows-overflow-pilot-{SHA}-{RUN_ID}-{ATTEMPT}",
            "id": 99, "expired": False,
            "size_in_bytes": self.archive.stat().st_size,
            "archive_download_url": f"https://api.github.com/repos/{REPO}/actions/artifacts/99/zip",
            "workflow_run": {"id": RUN_ID, "head_sha": SHA},
            "digest": "sha256:" + admission.sha256_file(self.archive),
        }]}

    def check(self):
        return admission.verify(self.run, self.workflow, self.artifacts,
                                self.archive, repository=REPO, source_sha=SHA,
                                source_tree=TREE, workflow_sha256=WORKFLOW_HASH,
                                driver_sha256=DRIVER_HASH, run_id=RUN_ID,
                                run_attempt=ATTEMPT)

    def rejects(self, code):
        with self.assertRaises(admission.AdmissionError) as caught:
            self.check()
        self.assertEqual(caught.exception.code, code)

    def test_accepts_exact_source_artifact_and_ciphertext(self):
        self.run["path"] = admission.WORKFLOW + "@refs/heads/release/v0.8.17"
        receipt = self.check()
        self.run["path"] = admission.WORKFLOW + "@release/v0.8.17"
        self.check()
        self.assertTrue(receipt["admitted"])
        self.assertEqual(receipt["artifact_sha256"], admission.sha256_file(self.archive))
        self.assertEqual(set(receipt), {"schema", "pilot_run_id", "pilot_run_attempt",
                                        "source_sha", "source_tree", "workflow_sha256",
                                        "driver_sha256", "artifact_sha256", "admitted"})

    def test_rejects_wrong_run_metadata(self):
        for key, value in (("event", "push"), ("head_sha", "f" * 40),
                           ("run_attempt", 3), ("conclusion", "failure"),
                           ("path", ".github/workflows/other.yml")):
            with self.subTest(key=key):
                original = self.run[key]
                self.run[key] = value
                self.rejects("RUN")
                self.run[key] = original

    def test_rejects_wrong_workflow_and_repository(self):
        self.workflow["path"] = ".github/workflows/other.yml"
        self.rejects("WORKFLOW")
        self.workflow["path"] = admission.WORKFLOW
        self.run["head_repository"]["full_name"] = "other/alopex"
        self.rejects("RUN")

    def test_rejects_wrong_artifact_url_and_head_sha(self):
        artifact = self.artifacts["artifacts"][0]
        artifact["archive_download_url"] = "https://other.example/artifact.zip"
        self.rejects("ARTIFACT")
        artifact["archive_download_url"] = f"https://api.github.com/repos/{REPO}/actions/artifacts/99/zip"
        artifact["workflow_run"]["head_sha"] = "f" * 40
        self.rejects("ARTIFACT")

    def test_rejects_archive_digest_and_extras(self):
        self.artifacts["artifacts"][0]["digest"] = "sha256:" + "0" * 64
        self.rejects("ARTIFACT_DIGEST")
        self.make_archive("private.log")
        self.rejects("ARCHIVE")

    def test_rejects_public_identity_failure_and_raw_fields(self):
        for key, value in (("source_tree", "e" * 40),
                           ("workflow_sha256", "e" * 64),
                           ("driver_sha256", "e" * 64),
                           ("outcome", "failed"), ("budget_admitted", False),
                           ("ciphertext_sha256", "0" * 64)):
            with self.subTest(key=key):
                original = self.public[key]
                self.public[key] = value
                self.make_archive()
                self.rejects("PUBLIC")
                self.public[key] = original
        self.public["private_path"] = "C:\\secret\\diagnostics"
        self.make_archive()
        self.rejects("PUBLIC")

    def test_main_mocks_http_and_prints_only_fixed_codes(self):
        output = Path(self.temp.name) / "admission.json"
        args = ["--repository", REPO, "--run-id", str(RUN_ID),
                "--run-attempt", str(ATTEMPT), "--output", str(output)]
        identity = {"source_sha": SHA, "source_tree": TREE,
                    "workflow_sha256": WORKFLOW_HASH, "driver_sha256": DRIVER_HASH}
        def copy_archive(artifact, token, destination):
            destination.write_bytes(self.archive.read_bytes())
        with mock.patch.dict("os.environ", {"GITHUB_TOKEN": "test-token"}), \
             mock.patch.object(admission, "checkout_identity", return_value=identity), \
             mock.patch.object(admission, "download_artifact", side_effect=copy_archive), \
             mock.patch.object(admission, "github_json",
                               side_effect=[self.run, self.workflow, self.artifacts]) as api, \
             contextlib.redirect_stdout(io.StringIO()) as stdout:
            self.assertEqual(admission.main(args), 0)
        self.assertEqual(stdout.getvalue(), "WINDOWS_PILOT_ADMITTED\n")
        self.assertEqual(api.call_count, 3)
        self.assertTrue(json.loads(output.read_text())["admitted"])
        output.unlink()
        with mock.patch.dict("os.environ", {"GITHUB_TOKEN": "test-token"}), \
             mock.patch.object(admission, "checkout_identity", return_value=identity), \
             mock.patch.object(admission, "github_json",
                               side_effect=RuntimeError("C:\\private\\api-response")), \
             contextlib.redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(admission.main(args), 1)
        self.assertEqual(stderr.getvalue(), "WINDOWS_PILOT_API_OR_IO\n")
        self.assertFalse(output.exists())

    def test_main_rejects_metadata_before_download(self):
        output = Path(self.temp.name) / "admission.json"
        args = ["--repository", REPO, "--run-id", str(RUN_ID),
                "--run-attempt", str(ATTEMPT), "--output", str(output)]
        identity = {"source_sha": SHA, "source_tree": TREE,
                    "workflow_sha256": WORKFLOW_HASH, "driver_sha256": DRIVER_HASH}
        self.run["conclusion"] = "failure"
        with mock.patch.dict("os.environ", {"GITHUB_TOKEN": "test-token"}), \
             mock.patch.object(admission, "checkout_identity", return_value=identity), \
             mock.patch.object(admission, "github_json",
                               side_effect=[self.run, self.workflow, self.artifacts]), \
             mock.patch.object(admission, "download_artifact") as download, \
             contextlib.redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(admission.main(args), 1)
        download.assert_not_called()
        self.assertEqual(stderr.getvalue(), "WINDOWS_PILOT_RUN\n")
        self.assertFalse(output.exists())

    def test_malformed_run_identifier_never_echoes_input(self):
        secret_like = "C:\\private\\token=do-not-print"
        args = ["--repository", REPO, "--run-id", secret_like,
                "--run-attempt", str(ATTEMPT),
                "--output", str(Path(self.temp.name) / "admission.json")]
        with mock.patch.dict("os.environ", {"GITHUB_TOKEN": "test-token"}), \
             contextlib.redirect_stderr(io.StringIO()) as stderr, \
             contextlib.redirect_stdout(io.StringIO()) as stdout:
            self.assertEqual(admission.main(args), 1)
        self.assertEqual(stderr.getvalue(), "WINDOWS_PILOT_INPUT\n")
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn(secret_like, stderr.getvalue())

    def test_download_accepts_only_signed_documented_artifact_redirect(self):
        artifact = self.artifacts["artifacts"][0]
        destination = Path(self.temp.name) / "download.zip"
        signed = "https://pipelines.actions.githubusercontent.com/asset?sig=test"

        class Response(io.BytesIO):
            status = 200

        class Opener:
            def __init__(self):
                self.requests = []

            def open(self, request, timeout):
                self.requests.append(request)
                if len(self.requests) == 1:
                    raise urllib.error.HTTPError(request.full_url, 302, "Found",
                                                 {"Location": signed}, None)
                return Response(b"zip data")

        opener = Opener()
        with mock.patch.object(admission.urllib.request, "build_opener", return_value=opener):
            admission.download_artifact(artifact, "private-token", destination)
        self.assertEqual(destination.read_bytes(), b"zip data")
        self.assertIn("Bearer private-token", opener.requests[0].headers.values())
        self.assertNotIn("Bearer private-token", opener.requests[1].headers.values())

        self.assertTrue(admission._signed_artifact_url(
            "https://productionresultssa1.blob.core.windows.net/container/blob"
            "?sv=2024-01-01&sp=r&se=2026-12-01&sig=example"))
        self.assertFalse(admission._signed_artifact_url(
            "https://productionresultssa1.blob.core.windows.net/container/blob?sig=example"))

        destination.unlink()
        for signed in ("https://evil.example/asset?sig=test",
                       "http://pipelines.actions.githubusercontent.com/asset?sig=test",
                       "https://pipelines.actions.githubusercontent.com/asset"):
            with self.subTest(signed=signed):
                opener = Opener()
                with mock.patch.object(admission.urllib.request, "build_opener", return_value=opener):
                    with self.assertRaises(admission.AdmissionError) as caught:
                        admission.download_artifact(artifact, "private-token", destination)
                self.assertEqual(caught.exception.code, "DOWNLOAD_REDIRECT")
                self.assertFalse(destination.exists())

    def test_download_stops_at_byte_limit(self):
        artifact = self.artifacts["artifacts"][0]
        destination = Path(self.temp.name) / "download.zip"
        signed = "https://pipelines.actions.githubusercontent.com/asset?sig=test"

        class Response(io.BytesIO):
            status = 200

        class Opener:
            count = 0

            def open(self, request, timeout):
                self.count += 1
                if self.count == 1:
                    raise urllib.error.HTTPError(request.full_url, 302, "Found",
                                                 {"Location": signed}, None)
                return Response(b"123456")

        with mock.patch.object(admission.urllib.request, "build_opener", return_value=Opener()), \
             mock.patch.object(admission, "MAX_ARCHIVE", 5):
            with self.assertRaises(admission.AdmissionError) as caught:
                admission.download_artifact(artifact, "private-token", destination)
        self.assertEqual(caught.exception.code, "DOWNLOAD_SIZE")


if __name__ == "__main__":
    unittest.main()
