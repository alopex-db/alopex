"""Bounded historical perf baseline intake checks."""

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import stress_legacy_baseline as legacy


def metadata():
    run = {
        "id": legacy.RUN_ID,
        "path": ".github/workflows/stress-tests.yml",
        "event": "schedule",
        "status": "completed",
        "conclusion": "success",
        "head_sha": legacy.SOURCE_SHA,
        "repository": {"id": legacy.REPOSITORY_ID, "full_name": "alopex-db/alopex"},
        "head_repository": {"id": legacy.REPOSITORY_ID},
    }
    artifact = {
        "id": legacy.ARTIFACT_ID,
        "name": "perf-baselines",
        "expired": False,
        "digest": "sha256:" + legacy.ZIP_SHA256,
        "size_in_bytes": 593,
        "archive_download_url": f"{legacy.API}/actions/artifacts/{legacy.ARTIFACT_ID}/zip",
        "workflow_run": {
            "id": legacy.RUN_ID,
            "head_sha": legacy.SOURCE_SHA,
            "repository_id": legacy.REPOSITORY_ID,
            "head_repository_id": legacy.REPOSITORY_ID,
        },
    }
    return run, artifact


def baseline_zip():
    files = {
        name: json.dumps({"metrics": {"sample": 1.25}}).encode()
        for name in legacy.BASELINES
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name, body in files.items():
            archive.writestr(name, body)
    return stream.getvalue(), files


class LegacyBaselineTests(unittest.TestCase):
    def test_accepts_only_exact_successful_scheduled_source_and_artifact(self):
        run, artifact = metadata()
        legacy.verify_metadata(run, artifact)
        for changed_run, changed_artifact in (
            ({**run, "head_sha": "0" * 40}, artifact),
            ({**run, "event": "workflow_dispatch"}, artifact),
            (run, {**artifact, "expired": True}),
            (run, {**artifact, "digest": "sha256:" + "0" * 64}),
            (run, {**artifact, "workflow_run": {**artifact["workflow_run"], "id": 1}}),
        ):
            with self.subTest(change=(changed_run, changed_artifact)):
                with self.assertRaises(ValueError):
                    legacy.verify_metadata(changed_run, changed_artifact)

    def test_extracts_only_two_finite_baselines(self):
        body, files = baseline_zip()
        with patch.object(legacy, "ZIP_SHA256", hashlib.sha256(body).hexdigest()):
            self.assertEqual(legacy.extract_baselines(body), files)
            with self.assertRaises(ValueError):
                legacy.extract_baselines(body + b"extra")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy.write_baselines(root, files)
            destination = root / "target/stress-baselines"
            self.assertEqual({p.name for p in destination.iterdir()}, set(files))
            self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
            with self.assertRaises(ValueError):
                legacy.write_baselines(root, files)

    def test_rejects_unsafe_zip_members(self):
        files = baseline_zip()[1]
        files[next(iter(files))] = b'{"metrics":{"sample":NaN}}'
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            for name, body in files.items():
                archive.writestr(name, body)
        body = stream.getvalue()
        with patch.object(legacy, "ZIP_SHA256", hashlib.sha256(body).hexdigest()):
            with self.assertRaises(ValueError):
                legacy.extract_baselines(body)

    def test_api_request_disables_bearer_redirects(self):
        captured = []
        class Opener:
            def open(self, request, timeout):
                captured.append((request, timeout))
                return io.BytesIO(b"{}")
        def build(handler):
            self.assertIsInstance(handler, legacy.NoRedirect)
            return Opener()
        with patch.object(legacy, "build_opener", side_effect=build):
            with legacy.api_request(f"{legacy.API}/actions/runs/{legacy.RUN_ID}", "secret"):
                pass
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0].get_header("Authorization"), "Bearer secret")
        with self.assertRaises(ValueError):
            legacy.api_request("https://example.invalid/redirect", "secret")


if __name__ == "__main__":
    unittest.main()
