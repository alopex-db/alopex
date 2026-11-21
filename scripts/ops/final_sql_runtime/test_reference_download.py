"""Offline HTTP and filesystem boundary tests for hosted reference retrieval."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from . import reference_download as download


NOW = datetime(2026, 11, 21, tzinfo=timezone.utc)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def wrapper(name, data):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(name, data)
    return output.getvalue()


class FakeResponse:
    def __init__(self, status, payload=b"", headers=None):
        self.status = status
        self.payload = io.BytesIO(payload)
        self.headers = headers or {}

    def getheader(self, name):
        return self.headers.get(name)

    def read(self, size):
        return self.payload.read(size)


class ReferenceDownloadTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.destination = Path(temp.name) / "private"
        self.destination.mkdir(mode=0o700)
        self.descriptors = {}
        self.pins = {}
        self.metadata = {}
        self.payloads = {}
        self.run = {"id": 42, "head_sha": "a" * 40,
                    "repository": {"id": 7}, "workflow_id": 8,
                    "run_attempt": 1, "event": "workflow_dispatch",
                    "status": "completed", "conclusion": "success"}
        for number, subject in enumerate(("before", "postgresql", "published"), 1):
            original = b"original " + subject.encode()
            payload = wrapper(subject + ".zip", original)
            self.payloads[number] = payload
            self.pins[subject] = {"zip_sha256": sha(original),
                                  "zip_size_bytes": len(original)}
            self.descriptors[subject] = {
                "run_id": 42, "head_sha": "a" * 40,
                "repository_id": 7, "workflow_id": 8, "artifact_id": number,
                "artifact_name": "reference-" + subject,
                "artifact_digest": sha(payload), "artifact_size_bytes": len(payload),
                "inner_name": subject + ".zip"}
            self.metadata[number] = {
                "id": number, "name": "reference-" + subject,
                "digest": "sha256:" + sha(payload), "size_in_bytes": len(payload),
                "expired": False, "created_at": "2026-11-20T00:00:00Z",
                "expires_at": "2026-11-22T00:00:00Z",
                "workflow_run": {"id": 42, "head_sha": "a" * 40}}
        self.redirect_host = "reviewed.blob.core.windows.net"
        self.redirect_status = 302
        self.signed_status = 200
        self.requests = []

    @contextmanager
    def transport(self, url, headers, timeout):
        self.requests.append((url, dict(headers)))
        if url.endswith("/actions/runs/42"):
            response = FakeResponse(200, json.dumps(self.run).encode())
        elif url.endswith("/zip"):
            number = int(url.split("/")[-2])
            response = FakeResponse(self.redirect_status, headers={
                "Location": f"https://{self.redirect_host}/{number}?signature=fake"})
        elif url.startswith("https://api.github.com/"):
            number = int(url.split("/")[-1])
            response = FakeResponse(200, json.dumps(self.metadata[number]).encode())
        else:
            number = int(url.split("/")[-1].split("?")[0])
            response = FakeResponse(self.signed_status, self.payloads[number], {
                "Content-Length": str(len(self.payloads[number]))})
        yield response

    def call(self):
        return download.retrieve_and_admit(
            repository="alopex-db/alopex", descriptors=self.descriptors,
            original_pins=self.pins, destination=self.destination,
            trusted_download_hosts={"reviewed.blob.core.windows.net"},
            token="test-secret", transport=self.transport, now=NOW)

    def test_exact_three_streamed_with_no_redirect_auth(self):
        admitted = self.call()
        self.assertEqual(set(admitted), {"before", "postgresql", "published"})
        self.assertEqual(set(self.destination.iterdir()), set(admitted.values()))
        self.assertEqual(len(self.requests), 10)
        for url, headers in self.requests:
            if url.startswith("https://api.github.com/"):
                self.assertEqual(headers["Authorization"], "Bearer test-secret")
            else:
                self.assertNotIn("Authorization", headers)

    def test_untrusted_redirect_rejected_without_request(self):
        self.redirect_host = "untrusted.example"
        with self.assertRaisesRegex(ValueError, "ARTIFACT_REDIRECT_HOST"):
            self.call()
        self.assertEqual(len(self.requests), 5)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_metadata_rejected_before_download(self):
        self.metadata[2]["workflow_run"]["id"] = 99
        with self.assertRaisesRegex(ValueError, "ARTIFACT_METADATA"):
            self.call()
        self.assertEqual(len(self.requests), 4)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_later_short_download_cleans_all_wrappers(self):
        self.payloads[3] = self.payloads[3][:-1]
        with self.assertRaisesRegex(ValueError, "ARTIFACT_DOWNLOAD_SIZE"):
            self.call()
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_later_digest_failure_cleans_all_outputs(self):
        altered = bytearray(self.payloads[3])
        altered[35] ^= 1
        self.payloads[3] = bytes(altered)
        with self.assertRaisesRegex(ValueError, "WRAPPER_DIGEST"):
            self.call()
        self.assertEqual(list(self.destination.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
