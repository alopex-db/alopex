"""Candidate closure never grants production trust or emits a formal lock."""
import copy
import hashlib
import json
import sys
from pathlib import Path
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parent))
from candidate_closure import SCHEMA, candidate_closure
from resolve_builder_lock import BASE, PLATFORM, LockError, make_lock


class CandidateClosureTests(unittest.TestCase):
    def setUp(self):
        self.base = {"base": BASE, "platform": PLATFORM,
                     "packages": {"make": "4.3", "gawk": "5.2", "libc6-dev": "2.39",
                                  "linux-libc-dev": "6.8", "binutils": "2.42"}}
        self.index = ("Package: gcc\nVersion: 14.2\nArchitecture: amd64\n"
                      "Filename: pool/main/g/gcc/gcc_14.2_amd64.deb\nSize: 100\n"
                      "SHA256: " + "a" * 64 + "\nInstalled-Size: 200\n"
                      "Depends: make (>= 4.3)\n\n").encode()
        self.uris = ("'https://archive.ubuntu.com/ubuntu/pool/main/g/gcc/"
                     "gcc_14.2_amd64.deb' gcc_14.2_amd64.deb 100 ignored-md5\n")
        self.resolver = "b" * 64
        self.auth = {"base": BASE, "platform": PLATFORM,
                     "signature_verified": True, "apt_update_complete": True,
                     "apt_no_remove": True, "apt_no_recommends": True,
                     "resolver_sha256": self.resolver,
                     "base_inventory_sha256": self.sha(self.canonical(self.base)),
                     "apt_uris_sha256": self.sha(self.uris.encode()),
                     "apt_uri_count": 1,
                     "indexes": [{"name": "noble-main",
                                  "url": "https://archive.ubuntu.com/ubuntu/dists/noble/InRelease",
                                  "repo_base_url": "https://archive.ubuntu.com/ubuntu",
                                  "inrelease_sha256": "c" * 64,
                                  "signer_fingerprint": "D" * 40,
                                  "packages_sha256": self.sha(self.index)}]}

    @staticmethod
    def sha(raw):
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def canonical(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    def candidate(self, *, auth=None, index=None, compare=None):
        auth = self.auth if auth is None else auth
        return candidate_closure(self.base, auth, self.sha(self.canonical(auth)),
                                 {"noble-main": self.index if index is None else index},
                                 self.uris, self.resolver,
                                 compare if compare is not None else Mock(return_value=True))

    def test_candidate_binds_versioned_closure_without_approving_formal_lock(self):
        compare = Mock(return_value=True)
        result = self.candidate(compare=compare)
        self.assertEqual(result["schema"], SCHEMA)
        self.assertEqual(result["approval"], "unapproved")
        self.assertIs(result["formal_lock"], False)
        self.assertEqual(result["packages"][0]["sha256"], "a" * 64)
        self.assertEqual(result["budgets"]["download_bytes"], 100)
        compare.assert_called_once_with("4.3", ">=", "4.3")
        with self.assertRaisesRegex(LockError, "unapproved signed-index proof"):
            make_lock(self.base, self.auth, {"noble-main": self.index},
                      self.uris, self.resolver)

    def test_rejects_changed_proof_and_signed_index(self):
        with self.assertRaisesRegex(LockError, "candidate proof digest"):
            candidate_closure(self.base, self.auth, "0" * 64,
                              {"noble-main": self.index}, self.uris,
                              self.resolver, Mock(return_value=True))
        with self.assertRaisesRegex(LockError, "signed index proof"):
            self.candidate(index=self.index.replace(b"make", b"mawk"))

    def test_rejects_unsatisfied_dependency_and_unexpected_signer_format(self):
        with self.assertRaisesRegex(LockError, "unsatisfied Depends"):
            self.candidate(compare=Mock(return_value=False))
        auth = copy.deepcopy(self.auth)
        auth["indexes"][0]["signer_fingerprint"] = "D" * 64
        with self.assertRaisesRegex(LockError, "signed index proof"):
            self.candidate(auth=auth)

    def test_rejects_bad_apt_uri_binding(self):
        auth = copy.deepcopy(self.auth)
        auth["apt_uris_sha256"] = "0" * 64
        with self.assertRaisesRegex(LockError, "APT output proof"):
            self.candidate(auth=auth)

    def test_epoch_in_apt_filename_is_bound_to_signed_version(self):
        index = self.index.replace(b"Version: 14.2", b"Version: 1:14.2")
        uris = self.uris.replace("gcc_14.2_amd64.deb 100",
                                 "gcc_1%3a14.2_amd64.deb 100")
        auth = copy.deepcopy(self.auth)
        auth["apt_uris_sha256"] = self.sha(uris.encode())
        auth["indexes"][0]["packages_sha256"] = self.sha(index)
        result = candidate_closure(self.base, auth, self.sha(self.canonical(auth)),
                                   {"noble-main": index}, uris, self.resolver,
                                   Mock(return_value=True))
        self.assertEqual(result["packages"][0]["version"], "1:14.2")
        self.assertEqual(result["packages"][0]["filename"],
                         "pool/main/g/gcc/gcc_14.2_amd64.deb")

        bad_uris = uris.replace("gcc_1%3a14.2_amd64.deb 100",
                                "gcc_2%3a14.2_amd64.deb 100")
        auth["apt_uris_sha256"] = self.sha(bad_uris.encode())
        with self.assertRaisesRegex(LockError, "APT filename/version"):
            candidate_closure(self.base, auth, self.sha(self.canonical(auth)),
                              {"noble-main": index}, bad_uris, self.resolver,
                              Mock(return_value=True))


if __name__ == "__main__":
    unittest.main()
