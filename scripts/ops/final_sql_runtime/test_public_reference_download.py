"""Public reference transport uses authenticated metadata and no raw upload."""
from contextlib import contextmanager
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import public_reference_download as download
from scripts.ops.final_sql_runtime import test_reference_public as fixtures


class Response:
    def __init__(self, status, body=b"", headers=None):
        self.status = status
        self.stream = io.BytesIO(body)
        self.headers = headers or {}

    def getheader(self, name):
        return self.headers.get(name)

    def read(self, size):
        return self.stream.read(size)


class PublicReferenceDownloadTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.PublicReferenceTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.destination = Path(temp.name).resolve() / "private"
        self.destination.mkdir(mode=0o700)
        self.host = "reviewed.blob.core.windows.net"
        self.requests = []
        self.body = {fixture.pins[subject]["artifact_id"]:
                     fixture.wrappers[subject].read_bytes()
                     for subject in fixture.pins}

    @contextmanager
    def transport(self, url, headers, timeout):
        self.requests.append((url, dict(headers)))
        parts = url.split("/")
        if "/actions/runs/" in url:
            run_id = int(parts[-1])
            subject = next(subject for subject, pin in self.fixture.pins.items()
                           if pin["run_id"] == run_id)
            response = Response(200, json.dumps(self.fixture.runs[subject]).encode())
        elif url.endswith("/zip"):
            artifact_id = int(parts[-2])
            response = Response(302, headers={"Location":
                f"https://{self.host}/{artifact_id}?signature=fake"})
        elif url.startswith("https://api.github.com/"):
            artifact_id = int(parts[-1])
            subject = next(subject for subject, pin in self.fixture.pins.items()
                           if pin["artifact_id"] == artifact_id)
            response = Response(200, json.dumps(self.fixture.artifacts[subject]).encode())
        else:
            artifact_id = int(parts[-1].split("?")[0])
            body = self.body[artifact_id]
            response = Response(200, body, {"Content-Length": str(len(body))})
        yield response

    def call(self):
        return download.retrieve(
            pins=self.fixture.pins, destination=self.destination,
            private_roots=("/nonexistent-private-root",),
            trusted_download_hosts={"reviewed.blob.core.windows.net"},
            token="synthetic-token", transport=self.transport)

    def test_exact_public_builds_admit_without_retaining_wrappers(self):
        paths = self.call()
        self.assertEqual(set(paths), {"before", "published"})
        self.assertEqual({entry.name for entry in self.destination.iterdir()}, {"admitted"})
        self.assertTrue(all(path.is_file() for path in paths.values()))
        self.assertEqual(len(self.requests), 8)
        for url, headers in self.requests:
            if url.startswith("https://api.github.com/"):
                self.assertEqual(headers["Authorization"], "Bearer synthetic-token")
            else:
                self.assertNotIn("Authorization", headers)

    def test_wrong_metadata_stops_before_download(self):
        self.fixture.artifacts["published"]["workflow_run"]["head_sha"] = "e" * 40
        with self.assertRaises(ValueError):
            self.call()
        self.assertEqual(len(self.requests), 4)
        self.assertFalse(list(self.destination.iterdir()))

    def test_unreviewed_redirect_and_short_body_clean_private_copy(self):
        self.host = "unreviewed.example"
        with self.assertRaisesRegex(ValueError, "ARTIFACT_REDIRECT_HOST"):
            self.call()
        self.assertFalse(list(self.destination.iterdir()))
        self.host = "reviewed.blob.core.windows.net"
        artifact_id = self.fixture.pins["published"]["artifact_id"]
        self.body[artifact_id] = self.body[artifact_id][:-1]
        with self.assertRaisesRegex(ValueError, "ARTIFACT_DOWNLOAD_SIZE"):
            self.call()
        self.assertFalse(list(self.destination.iterdir()))

    def test_second_directory_creation_failure_cleans_first(self):
        original = Path.mkdir
        def fail_admitted(path, *args, **kwargs):
            if path == self.destination / "admitted":
                raise OSError("synthetic storage failure")
            return original(path, *args, **kwargs)
        with patch.object(Path, "mkdir", fail_admitted):
            with self.assertRaisesRegex(ValueError, "PUBLIC_TRANSPORT"):
                self.call()
        self.assertFalse(list(self.destination.iterdir()))

    def test_changed_zip_bytes_fail_and_remove_downloads(self):
        artifact_id = self.fixture.pins["published"]["artifact_id"]
        altered = bytearray(self.body[artifact_id])
        altered[-8] ^= 1
        self.body[artifact_id] = bytes(altered)
        with self.assertRaisesRegex(ValueError, "PUBLIC_ZIP_BYTES"):
            self.call()
        self.assertFalse(list(self.destination.iterdir()))

    def test_download_cleanup_failure_removes_admitted_products(self):
        original = shutil.rmtree
        def fail_after_removing_downloads(path, *args, **kwargs):
            original(path, *args, **kwargs)
            if path == self.destination / "downloads":
                raise OSError("synthetic cleanup failure")
        with patch.object(download.shutil, "rmtree", fail_after_removing_downloads):
            with self.assertRaisesRegex(ValueError, "PUBLIC_CLEANUP"):
                self.call()
        self.assertFalse(list(self.destination.iterdir()))


if __name__ == "__main__":
    unittest.main()
