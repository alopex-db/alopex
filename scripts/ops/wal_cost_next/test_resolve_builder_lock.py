"""Small offline contract fixtures. They do not assert a real Ubuntu signature."""
import copy
import hashlib
import json
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from resolve_builder_lock import BASE, PLATFORM, LockError, make_lock


class BuilderLockTests(unittest.TestCase):
    def setUp(self):
        self.base = {"base": BASE, "platform": PLATFORM,
                     "packages": {"make": "4.3", "gawk": "5.2", "libc6-dev": "2.39",
                                  "linux-libc-dev": "6.8", "binutils": "2.42"}}
        self.index = ("Package: gcc\nVersion: 14.2\nArchitecture: amd64\n"
                      "Filename: pool/main/g/gcc/gcc_14.2_amd64.deb\nSize: 100\n"
                      "SHA256: " + "a" * 64 + "\nInstalled-Size: 200\n"
                      "Depends: make, gawk | mawk\n\n").encode()
        self.uri = ("'https://archive.ubuntu.com/ubuntu/pool/main/g/gcc/"
                    "gcc_14.2_amd64.deb' gcc_14.2_amd64.deb 100 ignored-md5\n")
        self.resolver = "b" * 64
        self.auth = {"base": BASE, "platform": PLATFORM, "signature_verified": True,
                     "apt_update_complete": True, "apt_no_remove": True,
                     "apt_no_recommends": True, "resolver_sha256": self.resolver,
                     "apt_uris_sha256": hashlib.sha256(self.uri.encode()).hexdigest(),
                     "apt_uri_count": 1,
                     "base_inventory_sha256": hashlib.sha256(json.dumps(
                         self.base, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                     "indexes": [{"name": "noble-main", "url": "https://archive.ubuntu.com/ubuntu/dists/noble/InRelease",
                                  "repo_base_url": "https://archive.ubuntu.com/ubuntu",
                                  "inrelease_sha256": "c" * 64,
                                  "signer_fingerprint": "D" * 40,
                                  "packages_sha256": hashlib.sha256(self.index).hexdigest()}]}

    def resolve(self, *, base=None, auth=None, index=None, uri=None):
        auth = auth or self.auth
        uri = uri or self.uri
        auth = copy.deepcopy(auth)
        auth["apt_uris_sha256"] = hashlib.sha256(uri.encode()).hexdigest()
        auth["apt_uri_count"] = len(uri.splitlines())
        proof_hash = hashlib.sha256(json.dumps(
            auth, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with patch("resolve_builder_lock.APPROVED_PROOF_SHA256", proof_hash), \
             patch("resolve_builder_lock.APPROVED_SIGNERS", frozenset({"D" * 40})):
            return make_lock(base or self.base, auth,
                             {"noble-main": index or self.index}, uri, self.resolver)

    def test_lock_binds_base_index_uri_and_closure(self):
        lock = self.resolve()
        self.assertEqual(lock["packages"][0]["sha256"], "a" * 64)
        self.assertEqual(lock["budgets"]["download_bytes"], 100)
        self.assertEqual(lock["packages"][0]["action"], "install")

    def test_rejects_unproven_index(self):
        with self.assertRaisesRegex(LockError, "signed index proof"):
            self.resolve(index=self.index.replace(b"make", b"mawk"))

    def test_rejects_unapproved_proof_and_wrong_signer_format(self):
        with self.assertRaisesRegex(LockError, "unapproved signed-index proof"):
            make_lock(self.base, self.auth, {"noble-main": self.index},
                      self.uri, self.resolver)
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["signer_fingerprint"] = "D" * 64
        with self.assertRaisesRegex(LockError, "signed index proof"):
            self.resolve(auth=auth)
        auth["indexes"][0]["signer_fingerprint"] = "E" * 40
        with self.assertRaisesRegex(LockError, "signed index proof"):
            self.resolve(auth=auth)

    def test_rejects_http_and_unlisted_host(self):
        for uri in (self.uri.replace("https://", "http://"),
                    self.uri.replace("archive.ubuntu.com", "example.com")):
            with self.subTest(uri=uri), self.assertRaisesRegex(LockError, "APT URI host"):
                self.resolve(uri=uri)

    def test_rejects_unexpected_apt_line_and_encoded_path_escape(self):
        with self.assertRaisesRegex(LockError, "unexpected APT output line"):
            self.resolve(uri=self.uri + "unexpected package line\n")
        with self.assertRaisesRegex(LockError, "APT filename"):
            self.resolve(uri=self.uri.replace("gcc_14.2_amd64.deb'", "gcc%2fescape_14.2_amd64.deb'"))

    def test_rejects_missing_dependency(self):
        index = self.index.replace(b"make, gawk | mawk", b"missing-package")
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["packages_sha256"] = hashlib.sha256(index).hexdigest()
        with self.assertRaisesRegex(LockError, "unsatisfied Depends"):
            self.resolve(index=index, auth=auth)

    def test_rejects_wrong_repository_path_and_ambiguous_version(self):
        with self.assertRaisesRegex(LockError, "missing/ambiguous signed package"):
            self.resolve(uri=self.uri.replace("/ubuntu/pool/", "/other/pool/"))
        second = self.index.replace(b"Version: 14.2", b"Version: 14.3")
        index = self.index + second
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["packages_sha256"] = hashlib.sha256(index).hexdigest()
        with self.assertRaisesRegex(LockError, "missing/ambiguous signed package"):
            self.resolve(index=index, auth=auth)

    def test_rejects_version_constraint(self):
        index = self.index.replace(b"make, gawk | mawk", b"make (>= 9.9)")
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["packages_sha256"] = hashlib.sha256(index).hexdigest()
        with patch("resolve_builder_lock.version_satisfies", return_value=False):
            with self.assertRaisesRegex(LockError, "unsatisfied Depends"):
                self.resolve(index=index, auth=auth)

    def test_rejects_identity_and_authentication_drift(self):
        base = copy.deepcopy(self.base)
        base["base"] = "ubuntu@sha256:" + "0" * 64
        with self.assertRaisesRegex(LockError, "base/platform"):
            self.resolve(base=base)
        auth = copy.deepcopy(self.auth)
        auth["signature_verified"] = False
        with self.assertRaisesRegex(LockError, "authentication evidence"):
            self.resolve(auth=auth)

    def test_rejects_duplicate_package_and_budget(self):
        with self.assertRaisesRegex(LockError, "APT filename"):
            self.resolve(uri=self.uri + self.uri)
        index = self.index.replace(b"Size: 100", b"Size: 450000001")
        uri = self.uri.replace(" 100 ", " 450000001 ")
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["packages_sha256"] = hashlib.sha256(index).hexdigest()
        with self.assertRaisesRegex(LockError, "download budget"):
            self.resolve(index=index, uri=uri, auth=auth)


if __name__ == "__main__":
    unittest.main()
