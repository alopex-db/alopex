"""Offline API and archive rejection checks for the live acquisition producer."""
import copy
import hashlib
import io
import json
from pathlib import Path
import struct
import tempfile
import unittest
import zipfile

from . import acquire, qualification


def digest(data):
    return hashlib.sha256(data).hexdigest()


def zipped(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files:
            archive.writestr(name, data)
    return output.getvalue()


class AcquisitionProducerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name)
        self.parent.chmod(0o700)
        self.directory = self.parent / "asset-acquisition"
        self.job_id = "12345"
        source = "a" * 39 + "b"
        tree = "c" * 39 + "d"
        harness = "e" * 39 + "f"
        server = b"server fixture"
        wheel = zipped([("alopex/module.so", b"fixture")])
        library = b"library fixture"
        harness_file = zipped([("harness.txt", b"fixture")])
        receipt = json.dumps({"source_commit": source, "platform": "linux/amd64",
                              "source": {"commit": source, "tree": tree, "profile": "release"},
                              "artifacts": {"bin/alopex-server": digest(server),
                                            "wheels/alopex-0.8.17-cp38-abi3-linux_x86_64.whl": digest(wheel),
                                            "lib/libalopex_sql_parser.so": digest(library)}}).encode()
        inventory = [("server", "bin/alopex-server", server),
                     ("wheel", "wheels/alopex-0.8.17-cp38-abi3-linux_x86_64.whl", wheel),
                     ("library", "lib/libalopex_sql_parser.so", library),
                     ("receipt", "receipts/build.json", receipt),
                     ("harness", "harness.tar", harness_file)]
        manifest = {"schema": "alopex.test-env-assets/v1", "platform": "linux/amd64",
                    "source_commit": source, "harness_commit": harness,
                    "expanded_limit_bytes": 100000,
                    "files": [{"role": role, "path": path, "size": len(data),
                               "sha256": digest(data)} for role, path, data in inventory]}
        manifest_bytes = json.dumps(manifest).encode()
        result = {"schema": "alopex.native-assets/v1", "status": "passed",
                  "source": {"commit": source, "tree": tree, "profile": "release"},
                  "assets_digest": digest(manifest_bytes),
                  "performance_qualification": False}
        self.files = [("result.json", json.dumps(result).encode()),
                      ("assets/assets.json", manifest_bytes)]
        self.files += [("assets/" + path, data) for _, path, data in inventory]
        self.zip = zipped(self.files)
        verifier_path = Path(acquire.__file__).resolve().parents[2] / "test-env/common.py"
        self.pin = {"asset_run_id": 88, "asset_run_head": "1" * 39 + "2",
                    "artifact_id": 77, "artifact_sha256": digest(self.zip),
                    "zip_sha256": digest(self.zip),
                    "asset_zip_size_bytes": len(self.zip),
                    "assets_digest": digest(manifest_bytes), "source_commit": source,
                    "source_tree": tree, "harness_commit": harness,
                    "server_sha256": digest(server), "wheel_sha256": digest(wheel),
                    "asset_file_digests": {role: digest(data) for role, _, data in inventory},
                    "common_verifier_sha256": digest(verifier_path.read_bytes())}
        self.pin.update({name: {"operator_sha256": {"worker.py": "f" * 64}}.get(name, "f" * 64)
                         for name in qualification.PIN_FIELDS - set(self.pin)})
        self.lock = {"schema": "alopex.sql-four-case-pins/v1", "pin": self.pin}
        self.run = {"id": 88, "status": "completed", "conclusion": "success",
                    "event": "workflow_dispatch", "run_attempt": 1,
                    "head_sha": self.pin["asset_run_head"]}
        self.jobs = {"total_count": 1, "jobs": [{"run_id": 88,
                     "head_sha": self.pin["asset_run_head"], "status": "completed",
                     "conclusion": "success", "steps": [{"status": "completed",
                     "conclusion": "success"}]}]}
        self.listing = {"total_count": 1, "artifacts": [{"id": 77,
                        "name": "native-product-assets", "expired": False,
                        "digest": "sha256:" + self.pin["zip_sha256"],
                        "size_in_bytes": len(self.zip), "workflow_run": {
                        "id": 88, "head_sha": self.pin["asset_run_head"]}}]}

    def fetch(self, path, *, binary=False, timeout=30, max_bytes=None):
        self.assertIn(timeout, (30, 180))
        if binary:
            self.assertEqual(path, "/actions/artifacts/77/zip")
            self.assertEqual(max_bytes, self.pin["asset_zip_size_bytes"])
            return self.zip
        return copy.deepcopy({"/actions/runs/88": self.run,
                            "/actions/runs/88/jobs?per_page=100": self.jobs,
                            "/actions/runs/88/artifacts?per_page=100": self.listing}[path])

    def acquire(self):
        return acquire.acquire(self.lock, job_id=self.job_id,
                               directory=self.directory, fetch=self.fetch)

    def test_success_writes_private_verified_bytes(self):
        proof = self.acquire()
        self.assertEqual(proof["zip_size_bytes"], len(self.zip))
        self.assertEqual((self.directory / "candidate.original.zip").read_bytes(), self.zip)
        for path in self.directory.rglob("*"):
            self.assertEqual(path.stat().st_mode & 0o777, 0o700 if path.is_dir() else 0o600)

    def test_rejects_remote_identity_and_bytes_without_output(self):
        changes = [(self.run, "run_attempt", 2), (self.run, "head_sha", "0" * 40),
                   (self.run, "event", "push"), (self.run, "conclusion", "failure"),
                   (self.jobs["jobs"][0], "head_sha", "0" * 40),
                   (self.jobs["jobs"][0]["steps"][0], "conclusion", "failure"),
                   (self.listing["artifacts"][0], "size_in_bytes", len(self.zip) + 1),
                   (self.listing["artifacts"][0], "digest", "sha256:" + "0" * 64)]
        for target, key, value in changes:
            with self.subTest(key=key, value=value):
                old = target[key]
                target[key] = value
                with self.assertRaises(ValueError):
                    self.acquire()
                self.assertFalse(self.directory.exists())
                target[key] = old
        self.zip += b"wrong"
        with self.assertRaises(ValueError):
            self.acquire()
        self.assertFalse(self.directory.exists())

    def test_rejects_untrusted_verifier_and_nonexact_role_pins(self):
        self.pin["common_verifier_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "ASSET_MANIFEST"):
            self.acquire()
        self.assertFalse(self.directory.exists())
        verifier_path = Path(acquire.__file__).resolve().parents[2] / "test-env/common.py"
        self.pin["common_verifier_sha256"] = digest(verifier_path.read_bytes())
        self.pin["asset_file_digests"]["library"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "ASSET_MANIFEST"):
            self.acquire()
        self.assertFalse(self.directory.exists())
        self.pin["asset_file_digests"].pop("receipt")
        with self.assertRaisesRegex(ValueError, "ASSET_IDENTITY"):
            self.acquire()
        self.assertFalse(self.directory.exists())

    def test_rejects_unsafe_archive_and_cleans_partial_files(self):
        for files in (self.files + [("../escape", b"x")],
                      self.files + [("private-evidence.cms", b"private")],
                      self.files + [("assets/extra", b"unexpected")]):
            with self.subTest(member=files[-1][0]):
                self.zip = zipped(files)
                self.pin["zip_sha256"] = self.pin["artifact_sha256"] = digest(self.zip)
                self.pin["asset_zip_size_bytes"] = len(self.zip)
                self.listing["artifacts"][0]["digest"] = "sha256:" + digest(self.zip)
                self.listing["artifacts"][0]["size_in_bytes"] = len(self.zip)
                with self.assertRaises(ValueError):
                    self.acquire()
                self.assertFalse(self.directory.exists())
        # A DEFLATED ZIP declares an oversized expanded member while its
        # original bytes remain below the ZIP limit.
        candidate = bytearray(zipped(self.files + [("assets/large", b"x" * 1024)]))
        marker = candidate.rfind(b"PK\x01\x02")
        self.assertGreater(marker, 0)
        struct.pack_into("<I", candidate, marker + 24, acquire.EXPANDED_LIMIT + 1)
        self.zip = bytes(candidate)
        self.assertLess(len(self.zip), 60_000_000)
        self.pin["zip_sha256"] = self.pin["artifact_sha256"] = digest(self.zip)
        self.pin["asset_zip_size_bytes"] = len(self.zip)
        self.listing["artifacts"][0]["digest"] = "sha256:" + digest(self.zip)
        self.listing["artifacts"][0]["size_in_bytes"] = len(self.zip)
        with self.assertRaisesRegex(ValueError, "ASSET_ARCHIVE"):
            self.acquire()
        self.assertFalse(self.directory.exists())
        self.zip = zipped([(name, b"bad manifest" if name == "assets/assets.json" else data)
                           for name, data in self.files])
        self.pin["zip_sha256"] = self.pin["artifact_sha256"] = digest(self.zip)
        self.pin["asset_zip_size_bytes"] = len(self.zip)
        self.listing["artifacts"][0]["digest"] = "sha256:" + digest(self.zip)
        self.listing["artifacts"][0]["size_in_bytes"] = len(self.zip)
        with self.assertRaises(ValueError):
            self.acquire()
        self.assertFalse(self.directory.exists())


if __name__ == "__main__":
    unittest.main()
