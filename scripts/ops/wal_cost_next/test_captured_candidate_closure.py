"""Synthetic file-input contracts; no real Ubuntu or container qualification."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from captured_candidate_closure import captured_candidate_closure
from offline_closure_adapter import ExactBaseDpkg
from resolve_builder_lock import BASE, PLATFORM, LockError


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class CapturedCandidateClosureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.proof = root / "proof"
        self.capture = root / "capture"
        self.proof.mkdir()
        self.capture.mkdir()
        self.script = root / "resolver.sh"
        self.script.write_bytes(b"synthetic resolver")
        self.index_path = self.capture / "Packages"
        self.base = {"base": BASE, "platform": PLATFORM,
                     "packages": {"make": "4.3", "gawk": "5.2", "libc6-dev": "2.39",
                                  "linux-libc-dev": "6.8", "binutils": "2.42"}}
        self.dependencies = ", ".join(["make (>= 4.3)"] * 131)
        self.uris = ("'https://archive.ubuntu.com/ubuntu/pool/main/g/gcc/"
                     "gcc_14.2_amd64.deb' gcc_14.2_amd64.deb 100 ignored-md5\n")
        self._write_capture()
        self.comparator = object.__new__(ExactBaseDpkg)
        self.comparator.identity = "d" * 64
        self.calls = []

        def compare(actual, operator, required):
            self.calls.append((actual, operator, required))
            return True

        self.comparator.compare = compare

    def _write_capture(self):
        index = ("Package: gcc\nVersion: 14.2\nArchitecture: amd64\n"
                 "Filename: pool/main/g/gcc/gcc_14.2_amd64.deb\nSize: 100\n"
                 "SHA256: " + "a" * 64 + "\nInstalled-Size: 200\n"
                 "Depends: " + self.dependencies + "\n\n").encode()
        self.index_path.write_bytes(index)
        (self.capture / "apt-uris.txt").write_text(self.uris)
        auth = {"base": BASE, "platform": PLATFORM, "signature_verified": True,
                "apt_update_complete": True, "apt_no_remove": True,
                "apt_no_recommends": True, "resolver_sha256": sha(self.script.read_bytes()),
                "base_inventory_sha256": sha(canonical(self.base)),
                "apt_uris_sha256": sha(self.uris.encode()), "apt_uri_count": 1,
                "indexes": [{"name": "noble-main",
                             "url": "https://archive.ubuntu.com/ubuntu/dists/noble/InRelease",
                             "repo_base_url": "https://archive.ubuntu.com/ubuntu",
                             "inrelease_sha256": "c" * 64,
                             "signer_fingerprint": "D" * 40,
                             "packages_sha256": sha(index)}]}
        (self.proof / "base.json").write_text(json.dumps(self.base))
        (self.proof / "authentication.json").write_text(json.dumps(auth))
        (self.proof / "indexes.json").write_text(json.dumps(
            {"noble-main": str(self.index_path)}))
        (self.proof / "proof.sha256").write_text(sha(canonical(auth)) + "\n")

    def run_candidate(self):
        return captured_candidate_closure(self.proof, self.capture,
                                          self.script, self.comparator)

    def test_131_exact_base_comparisons_are_candidate_only(self):
        result = self.run_candidate()
        self.assertEqual(result["schema"], "alopex.trace-builder-closure-candidate/v1")
        self.assertEqual(result["approval"], "unapproved")
        self.assertIs(result["formal_lock"], False)
        self.assertEqual(result["exact_base_dpkg"],
                         {"image_id": "d" * 64, "comparisons": 131})
        self.assertEqual(self.calls, [("4.3", ">=", "4.3")] * 131)

    def test_rejects_changed_proof(self):
        (self.proof / "proof.sha256").write_text("0" * 64)
        with self.assertRaisesRegex(LockError, "candidate proof digest"):
            self.run_candidate()
        self.assertEqual(self.calls, [])

    def test_rejects_unsatisfied_dependency(self):
        self.comparator.compare = lambda *_: False
        with self.assertRaisesRegex(LockError, "unsatisfied Depends"):
            self.run_candidate()

    def test_rejects_malformed_comparator_result(self):
        self.comparator.compare = lambda *_: 1
        with self.assertRaisesRegex(LockError, "malformed exact base dpkg result"):
            self.run_candidate()

    def test_rejects_index_outside_capture(self):
        (self.proof / "indexes.json").write_text(json.dumps(
            {"noble-main": str(self.script)}))
        with self.assertRaisesRegex(LockError, "candidate index outside capture"):
            self.run_candidate()


if __name__ == "__main__":
    unittest.main()
