"""Boundary checks for the offline, exact-base dpkg adapter."""
import json
import hashlib
import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from offline_closure_adapter import ExactBaseDpkg, make_lock_with_exact_base_dpkg
from resolve_builder_lock import BASE, LockError
import resolve_builder_lock

IDENTITY = "a" * 64
IMAGE_ID = "sha256:" + "b" * 64


class ExactBaseDpkgTests(unittest.TestCase):
    def setUp(self):
        self.live = {"State": {"Running": True, "Paused": False}, "Image": IMAGE_ID}
        self.image = {"Id": IMAGE_ID, "Os": "linux", "Architecture": "amd64",
                      "RepoDigests": [BASE]}
        self.docker = Mock(prefix=["/usr/bin/docker", "--context", "selected"])
        self.docker.call.return_value = json.dumps([self.image])
        self.monitor = Mock()
        self.monitor.run.return_value = (0, "", "")

    def adapter(self):
        with patch("offline_closure_adapter.inspect_resolver", return_value=self.live):
            return ExactBaseDpkg(self.docker, self.monitor, IDENTITY, {}, "token")

    def test_uses_only_exact_base_container_dpkg(self):
        adapter = self.adapter()
        with patch("offline_closure_adapter.inspect_resolver", return_value=self.live):
            self.assertTrue(adapter.compare("1:2.39-0ubuntu8.9", ">=", "2.39"))
            self.monitor.run.return_value = (1, "", "")
            self.assertFalse(adapter.compare("1.0~rc1", ">=", "1.0"))
        command = self.monitor.run.call_args_list[0].args[0]
        self.assertEqual(command, [*self.docker.prefix, "exec", "--user=root",
                                   IDENTITY, "/usr/bin/dpkg", "--compare-versions",
                                   "1:2.39-0ubuntu8.9", ">=", "2.39"])
        self.assertNotIn("/usr/bin/dpkg", command[:2])

    def test_refuses_stopped_or_different_base(self):
        for live, image in (({"State": {"Running": False}, "Image": IMAGE_ID}, self.image),
                            (self.live, {**self.image, "RepoDigests": []}),
                            ({**self.live, "Image": "sha256:" + "c" * 64}, self.image)):
            with self.subTest(live=live, image=image):
                self.docker.call.return_value = json.dumps([image])
                with patch("offline_closure_adapter.inspect_resolver", return_value=live):
                    with self.assertRaisesRegex(LockError, "selected base resolver unavailable"):
                        ExactBaseDpkg(self.docker, self.monitor, IDENTITY, {}, "token")
        self.monitor.run.assert_not_called()

    def test_rechecks_identity_and_rejects_failed_comparison(self):
        adapter = self.adapter()
        for result in ((127, "", ""), (0, "unexpected", ""), (0, "", "error")):
            with self.subTest(result=result):
                self.monitor.run.return_value = result
                with patch("offline_closure_adapter.inspect_resolver", return_value=self.live):
                    with self.assertRaisesRegex(LockError, "comparison failed"):
                        adapter.compare("1.0", "=", "1.0")
        with patch("offline_closure_adapter.inspect_resolver", return_value={
                "State": {"Running": False}, "Image": IMAGE_ID}):
            before = self.monitor.run.call_count
            with self.assertRaisesRegex(LockError, "selected base resolver unavailable"):
                adapter.compare("1.0", "=", "1.0")
            self.assertEqual(self.monitor.run.call_count, before)

    def test_rejects_untrusted_arguments_before_exec(self):
        adapter = self.adapter()
        for args in (("-1", "=", "1"), ("1", ";", "1"), ("1", "=", "1;id")):
            with self.subTest(args=args), self.assertRaisesRegex(LockError, "invalid dpkg comparison"):
                adapter.compare(*args)
        self.monitor.run.assert_not_called()

    def test_unapproved_proof_still_fails_closed_and_restores_comparator(self):
        adapter = self.adapter()
        original = resolve_builder_lock.version_satisfies
        with self.assertRaisesRegex(LockError, "base/platform identity"):
            make_lock_with_exact_base_dpkg({}, {}, {}, "", "", adapter)
        self.assertIs(resolve_builder_lock.version_satisfies, original)
        base = {"base": BASE, "platform": "linux/amd64", "packages": {}}
        auth = {"base": BASE, "platform": "linux/amd64", "signature_verified": True,
                "apt_update_complete": True, "apt_no_remove": True,
                "apt_no_recommends": True, "resolver_sha256": "f" * 64}
        with self.assertRaisesRegex(LockError, "unapproved signed-index proof"):
            make_lock_with_exact_base_dpkg(base, auth, {}, "", "f" * 64, adapter)
        self.assertIs(resolve_builder_lock.version_satisfies, original)

    def test_versioned_closure_calls_base_dpkg_and_restores_validator(self):
        adapter = self.adapter()
        base = {"base": BASE, "platform": "linux/amd64",
                "packages": {"make": "4.3", "gawk": "5.2", "libc6-dev": "2.39",
                             "linux-libc-dev": "6.8", "binutils": "2.42"}}
        index = ("Package: gcc\nVersion: 14.2\nArchitecture: amd64\n"
                 "Filename: pool/main/g/gcc/gcc_14.2_amd64.deb\nSize: 100\n"
                 "SHA256: " + "a" * 64 + "\nInstalled-Size: 200\n"
                 "Depends: make (>= 4.3)\n\n").encode()
        uris = ("'https://archive.ubuntu.com/ubuntu/pool/main/g/gcc/"
                "gcc_14.2_amd64.deb' gcc_14.2_amd64.deb 100 ignored-md5\n")
        resolver = "b" * 64
        auth = {"base": BASE, "platform": "linux/amd64", "signature_verified": True,
                "apt_update_complete": True, "apt_no_remove": True,
                "apt_no_recommends": True, "resolver_sha256": resolver,
                "apt_uris_sha256": hashlib.sha256(uris.encode()).hexdigest(),
                "apt_uri_count": 1,
                "base_inventory_sha256": hashlib.sha256(json.dumps(
                    base, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                "indexes": [{"name": "noble-main",
                             "url": "https://archive.ubuntu.com/ubuntu/dists/noble/InRelease",
                             "repo_base_url": "https://archive.ubuntu.com/ubuntu",
                             "inrelease_sha256": "c" * 64,
                             "signer_fingerprint": "D" * 40,
                             "packages_sha256": hashlib.sha256(index).hexdigest()}]}
        proof = hashlib.sha256(json.dumps(
            auth, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        original = resolve_builder_lock.version_satisfies
        with patch("resolve_builder_lock.APPROVED_PROOF_SHA256", proof), \
             patch("resolve_builder_lock.APPROVED_SIGNERS", frozenset({"D" * 40})), \
             patch("offline_closure_adapter.inspect_resolver", return_value=self.live):
            lock = make_lock_with_exact_base_dpkg(
                base, auth, {"noble-main": index}, uris, resolver, adapter)
        self.assertEqual(lock["budgets"]["download_bytes"], 100)
        self.assertIs(resolve_builder_lock.version_satisfies, original)
        self.assertEqual(self.monitor.run.call_args.args[0][-5:],
                         ["/usr/bin/dpkg", "--compare-versions", "4.3", ">=", "4.3"])


if __name__ == "__main__":
    unittest.main()
