"""Public-only Stage-2 upload admission and host-budget checks."""

import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request
import zipfile

import stress_public_case as public_case
import stress_upload_boundary as boundary

SOURCE = "a" * 40
RUN_ID = 123
ARTIFACT_ID = 456


def archive_for(members: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return output.getvalue()


def metadata(case_id: str, archive_bytes: bytes):
    digest = "sha256:" + hashlib.sha256(archive_bytes).hexdigest()
    run = {"id": RUN_ID, "run_attempt": 2,
           "path": ".github/workflows/stress-tests.yml", "head_branch": "main",
           "head_sha": SOURCE,
           "repository": {"id": boundary.REPOSITORY_ID,
                          "full_name": boundary.REPOSITORY},
           "head_repository": {"id": boundary.REPOSITORY_ID}}
    artifact = {"id": ARTIFACT_ID, "name": f"{case_id}-public",
                "expired": False, "digest": digest,
                "size_in_bytes": len(archive_bytes),
                "archive_download_url":
                f"{boundary.API}/actions/artifacts/{ARTIFACT_ID}/zip",
                "workflow_run": {"id": RUN_ID, "head_sha": SOURCE,
                                 "repository_id": boundary.REPOSITORY_ID,
                                 "head_repository_id": boundary.REPOSITORY_ID}}
    return run, artifact, digest


def fixture(root: Path):
    workspace = root / "checkout"
    evidence = workspace / "target/stress-public-artifacts/sql_parser"
    evidence.mkdir(parents=True)
    (evidence / "execution.json").write_text(json.dumps({
        "schema": "alopex.stress-execution/v1", "case_id": "sql_parser",
        "source_commit": SOURCE, "outcome": "passed", "duration_ms": 1200,
        "fuzz_executions": 1000,
    }))
    result = public_case.assemble(workspace=workspace, case_id="sql_parser",
                                  source=SOURCE, outcome="passed")
    output = public_case.write_result(workspace=workspace, case_id="sql_parser",
                                      result=result)
    return workspace, evidence, output


class UploadBoundaryTests(unittest.TestCase):
    def receipt_fixture(self, root: Path, members=None):
        workspace, _, output = fixture(root)
        archive_bytes = archive_for(members or {"sql_parser.json": output.read_bytes()})
        run, artifact, digest = metadata("sql_parser", archive_bytes)
        private = root / "stress-private"
        private.mkdir(mode=0o700)
        return workspace, output, private, archive_bytes, run, artifact, digest

    def verify_mock_receipt(self, workspace, private, archive_bytes, run,
                            artifact, digest):
        with patch.object(boundary, "fetch_metadata", side_effect=[run, artifact]), \
             patch.object(boundary, "fetch_zip", return_value=archive_bytes):
            return boundary.verify_receipt(
                workspace=workspace, private=private, case_id="sql_parser",
                source=SOURCE, run_id=RUN_ID, run_attempt=2,
                artifact_id=ARTIFACT_ID, artifact_digest=digest, token="test-token")

    def test_receipt_proves_exact_public_bytes_and_is_private(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, output, private, archive_bytes, run, artifact, digest = \
                self.receipt_fixture(Path(directory))
            receipt = self.verify_mock_receipt(
                workspace, private, archive_bytes, run, artifact, digest)
            value = json.loads(receipt.read_text())
            self.assertEqual(value["public_sha256"],
                             hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertEqual(value["zip_sha256"], digest.removeprefix("sha256:"))
            self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)

    def test_receipt_rejects_wrong_run_artifact_or_digest(self):
        for mutation in ("run", "artifact", "digest"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                workspace, _, private, body, run, artifact, digest = \
                    self.receipt_fixture(Path(directory))
                if mutation == "run":
                    run["id"] += 1
                elif mutation == "artifact":
                    artifact["workflow_run"]["id"] += 1
                else:
                    artifact["digest"] = "sha256:" + "0" * 64
                with self.assertRaises(ValueError):
                    self.verify_mock_receipt(workspace, private, body,
                                             run, artifact, digest)
                self.assertEqual(list(private.iterdir()), [])

    def test_receipt_accepts_bound_ref_path_and_rejects_other_ref(self):
        for path in (".github/workflows/stress-tests.yml@main",
                     ".github/workflows/stress-tests.yml@refs/heads/main"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                workspace, _, private, body, run, artifact, digest = \
                    self.receipt_fixture(Path(directory))
                run["path"] = path
                self.verify_mock_receipt(workspace, private, body,
                                         run, artifact, digest)
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, private, body, run, artifact, digest = \
                self.receipt_fixture(Path(directory))
            run["path"] = ".github/workflows/stress-tests.yml@refs/heads/other"
            with self.assertRaises(ValueError):
                self.verify_mock_receipt(workspace, private, body,
                                         run, artifact, digest)

    def test_receipt_rejects_altered_missing_or_extra_zip_bytes(self):
        for kind in ("altered", "missing", "extra"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                workspace, _, output = fixture(root)
                members = {"sql_parser.json": output.read_bytes()}
                if kind == "altered":
                    members["sql_parser.json"] += b"x"
                elif kind == "missing":
                    members = {"other.json": output.read_bytes()}
                else:
                    members["private.log"] = b"private"
                body = archive_for(members)
                run, artifact, digest = metadata("sql_parser", body)
                private = root / "stress-private"
                private.mkdir(mode=0o700)
                with self.assertRaises(ValueError):
                    self.verify_mock_receipt(workspace, private, body,
                                             run, artifact, digest)

    def test_receipt_rejects_oversized_zip_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, private, body, run, artifact, digest = \
                self.receipt_fixture(Path(directory))
            artifact["size_in_bytes"] = boundary.MAX_ZIP_BYTES + 1
            with self.assertRaises(ValueError):
                self.verify_mock_receipt(workspace, private, body,
                                         run, artifact, digest)

    def test_zip_fetch_never_redirects_bearer_token(self):
        test = self
        redirect = HTTPError(f"{boundary.API}/actions/artifacts/456/zip", 302,
                             "redirect", {"Location": "https://signed.example/artifact"},
                             None)
        class SignedResponse:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def read(self, size):
                test.assertLessEqual(size, boundary.MAX_ZIP_BYTES + 1)
                return b"zip"
        class SignedOpener:
            def open(self, request, timeout):
                test.assertIsNone(request.get_header("Authorization"))
                return response
        response = SignedResponse()
        with patch.object(boundary, "api_request", side_effect=redirect), \
             patch.object(boundary, "build_opener", return_value=SignedOpener()):
            self.assertEqual(boundary.fetch_zip(ARTIFACT_ID, "token"), b"zip")

    def test_zip_fetch_rejects_oversized_body_and_http_redirect(self):
        redirect = HTTPError(f"{boundary.API}/actions/artifacts/456/zip", 302,
                             "redirect", {"Location": "https://signed.example/artifact"},
                             None)
        class SignedResponse:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def read(self, _size):
                return b"xxxx"
        class SignedOpener:
            def open(self, _request, _timeout=None, **_kwargs):
                return SignedResponse()
        with patch.object(boundary, "api_request", side_effect=redirect), \
             patch.object(boundary, "build_opener", return_value=SignedOpener()), \
             patch.object(boundary, "MAX_ZIP_BYTES", 3):
            with self.assertRaises(ValueError):
                boundary.fetch_zip(ARTIFACT_ID, "token")
        with self.assertRaises(ValueError):
            boundary.HttpsRedirect().redirect_request(
                Request("https://signed.example/artifact"), None, 302,
                "redirect", {}, "http://insecure.example/artifact")

    def test_admits_exact_public_case_only(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, output = fixture(Path(directory))
            self.assertEqual(boundary.verify_output(
                workspace=workspace, case_id="sql_parser", source=SOURCE), output)

    def test_rejects_raw_fields_extra_file_and_source_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, evidence, output = fixture(Path(directory))
            original = output.read_bytes()
            altered = json.loads(original)
            altered["host_path"] = "/private/sensitive"
            output.write_text(json.dumps(altered))
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            output.write_bytes(original)
            duplicate = original.replace(b'"case_id":"sql_parser"',
                                         b'"case_id":"/private/sensitive","case_id":"sql_parser"')
            output.write_bytes(duplicate)
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            output.write_bytes(original)
            (output.parent / "raw.log").write_text("private")
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            (output.parent / "raw.log").unlink()
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source="b" * 40)
            (evidence / "unexpected.json").symlink_to(evidence / "execution.json")
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)

    def test_reserves_space_before_upload_and_checks_after(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "stress-private"
            private.mkdir(mode=0o700)
            with patch.object(boundary.storage, "inventory",
                              return_value={"allocated_bytes": 38_500_000_000}), \
                 patch.object(boundary.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(boundary.release_storage, "tool_roots",
                              return_value=[]):
                self.assertEqual(boundary.measure(
                    workspace=root, runner_temp=root, private=private,
                    case_id="perf", stage="before"), 38_500_000_000)
            with patch.object(boundary.storage, "inventory",
                              return_value={"allocated_bytes": 39_500_000_000}), \
                 patch.object(boundary.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(boundary.release_storage, "tool_roots",
                              return_value=[]):
                with self.assertRaises(ValueError):
                    boundary.measure(workspace=root, runner_temp=root, private=private,
                                     case_id="stress", stage="before")
                self.assertEqual(boundary.measure(
                    workspace=root, runner_temp=root, private=private,
                    case_id="perf", stage="after"), 39_500_000_000)

    def test_cli_error_is_fixed_without_private_path(self):
        result = subprocess.run([
            sys.executable, str(Path(boundary.__file__)),
            "--case", "sensitive-machine-path", "--stage", "before",
        ], capture_output=True, text=True, env={"GITHUB_SHA": "invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(), "STRESS_UPLOAD_BOUNDARY_REJECTED")
        self.assertNotIn("sensitive-machine-path", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
