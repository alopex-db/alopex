"""Offline parser seed admits only fixed package shape and owned state."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "release"))
import issue681_hosted_parser_seed as seed
import issue681_hosted_acquire_source as source
import parser_asset_manifest as manifest


class HostedParserSeedTest(unittest.TestCase):
    def orchestration_fixture(self, root):
        parent = root / "input"
        parent.mkdir(mode=0o700)
        home = root / "git-home"
        home.mkdir(mode=0o700)
        return parent, source._git_env(home)

    def fixture(self, root):
        checkout = root / "checkout"
        (checkout / "src/nested").mkdir(parents=True)
        (checkout / "npeg.nimble").write_text('version = "1.3.0"\n')
        (checkout / "src/npeg.nim").write_text("proc parse() = discard\n")
        (checkout / "src/nested/extra.nim").write_text("discard\n")
        (checkout / "src/ignored.txt").write_text("ignored\n")
        output = root / "seed"
        (output / "pkgs2").mkdir(parents=True)
        pin = {**seed.PINS["npeg"], "source_files": 2}
        return checkout, output, pin

    def test_package_copy_writes_only_nim_sources_and_fixed_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkout, output, pin = self.fixture(Path(tmp))
            package = seed._copy_package(checkout, output, "npeg", pin)
            files = sorted(p.relative_to(package).as_posix()
                           for p in package.rglob("*") if p.is_file())
            self.assertEqual(files, ["nimblemeta.json", "npeg.nimble",
                                     "src/nested/extra.nim", "src/npeg.nim"])
            metadata = json.loads((package / "nimblemeta.json").read_bytes())
            self.assertEqual(metadata["metaData"]["files"],
                             ["/npeg.nimble", "/src/nested/extra.nim",
                              "/src/npeg.nim"])
            self.assertEqual(metadata["metaData"]["vcsRevision"],
                             seed.PINS["npeg"]["commit"])

    def test_symlink_under_source_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkout, output, pin = self.fixture(Path(tmp))
            (checkout / "src/escape.nim").symlink_to(checkout / "npeg.nimble")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                seed._copy_package(checkout, output, "npeg", pin)

    def test_registry_bytes_are_pinned_before_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checkout, output, _ = self.fixture(root)
            raw = b"[]\n"
            (checkout / "packages.json").write_bytes(raw)
            with mock.patch.object(seed, "REGISTRY_BYTES", len(raw)), \
                 mock.patch.object(seed, "REGISTRY_SHA256",
                                   hashlib.sha256(raw).hexdigest()):
                seed._copy_registry(checkout, output)
                self.assertEqual((output / "packages_official.json").read_bytes(),
                                 raw)
                self.assertEqual((output / "packages_temp.json").read_bytes(), raw)
            (output / "packages_official.json").unlink()
            (output / "packages_temp.json").unlink()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                seed._copy_registry(checkout, output)
            self.assertFalse((output / "packages_official.json").exists())

    def test_package_revision_and_registry_pins_are_literal(self):
        self.assertEqual(seed.PINS["npeg"]["commit"],
                         "409f6796d0e880b3f0222c964d1da7de6e450811")
        self.assertEqual(seed.PINS["msgpack4nim"]["commit"],
                         "f4cc097ca9694f17feced9f82994f583ef7911fe")
        self.assertEqual(seed.PINS["registry"]["commit"],
                         "b79eaaa3fc65fc473bc9e803445f8f7aef7112a2")
        self.assertEqual(seed.REGISTRY_BYTES, 977_981)
        self.assertEqual(seed.REGISTRY_SHA256,
                         "e42c394337184b2fe52f65538ea3dc93fb8840efb77ec775261ebd1f7169d329")

    def test_assembled_seed_passes_actual_manifest_verifier_then_rejects_mutations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            checkout, output, npeg_pin = self.fixture(root)
            npeg = seed._copy_package(checkout, output, "npeg", npeg_pin)
            second = root / "msgpack-checkout"
            (second / "src").mkdir(parents=True)
            (second / "msgpack4nim.nimble").write_text('version = "0.4.4"\n')
            (second / "src/msgpack4nim.nim").write_text("discard\n")
            msgpack_pin = {**seed.PINS["msgpack4nim"], "source_files": 1}
            msgpack = seed._copy_package(second, output, "msgpack4nim",
                                         msgpack_pin)
            registry = b"[]\n"
            (checkout / "packages.json").write_bytes(registry)
            with mock.patch.object(seed, "REGISTRY_BYTES", len(registry)), \
                 mock.patch.object(seed, "REGISTRY_SHA256",
                                   hashlib.sha256(registry).hexdigest()):
                seed._copy_registry(checkout, output)
            packages = {}
            for name, package in (("npeg", npeg), ("msgpack4nim", msgpack)):
                packages[name] = {
                    **manifest.REQUIRED_PACKAGES[name],
                    **manifest.tree_identity(package, name,
                                             manifest.package_path_ignored),
                }
            metadata = {name: {
                "sha256": hashlib.sha256(registry).hexdigest(),
                "size": len(registry),
                "source_revision": manifest.REQUIRED_PACKAGE_LIST_REVISION,
            } for name in seed.PACKAGE_FILES}
            package_args = ["npeg=1.3.0=" + str(npeg),
                            "msgpack4nim=0.4.4=" + str(msgpack)]
            metadata_args = [name + "=" + str(output / name)
                             for name in seed.PACKAGE_FILES]
            with mock.patch.object(manifest, "REQUIRED_PACKAGES", packages), \
                 mock.patch.object(manifest, "REQUIRED_REGISTRY_METADATA",
                                   metadata):
                self.assertEqual(len(manifest.package_identities(package_args)), 2)
                self.assertEqual(len(manifest.metadata_identities(metadata_args)), 2)
                source_file = npeg / "src/npeg.nim"
                source_file.write_text("changed\n")
                with self.assertRaises(manifest.ParserAssetError):
                    manifest.package_identities(package_args)
                source_file.write_text("proc parse() = discard\n")
                nimble_metadata = npeg / "nimblemeta.json"
                value = json.loads(nimble_metadata.read_bytes())
                value["metaData"]["vcsRevision"] = "0" * 40
                nimble_metadata.write_text(json.dumps(value))
                with self.assertRaises(manifest.ParserAssetError):
                    manifest.package_identities(package_args)
                (output / seed.PACKAGE_FILES[0]).write_bytes(b"changed")
                with self.assertRaises(manifest.ParserAssetError):
                    manifest.metadata_identities(metadata_args)

    def test_same_job_orchestration_keeps_only_verified_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent, git_env = self.orchestration_fixture(Path(tmp).resolve())
            calls = []

            def fake_checkout(path, pin, env, guard, log):
                self.assertEqual(env, git_env)
                path.mkdir(mode=0o700)
                calls.append(pin["commit"])
                guard()

            with mock.patch.object(seed.source, "_trusted_git"), \
                 mock.patch.object(seed, "_checkout", side_effect=fake_checkout), \
                 mock.patch.object(seed, "_copy_package"), \
                 mock.patch.object(seed, "_copy_registry"), \
                 mock.patch.object(seed, "_verify_seed") as verify:
                output = seed.acquire_seed(parent, lambda: calls.append("guard"),
                                           git_env)
            self.assertEqual(output, parent / "seed")
            self.assertFalse((parent / "checkouts").exists())
            self.assertEqual(len([x for x in calls if x == "guard"]), 7)
            self.assertEqual(len([x for x in calls if x != "guard"]), 3)
            verify.assert_called_once()

    def test_git_failure_cleans_owned_seed_and_checkouts(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent, git_env = self.orchestration_fixture(Path(tmp).resolve())

            def fail_checkout(path, pin, env, guard, log):
                path.mkdir(mode=0o700)
                raise RuntimeError("private failure")

            with mock.patch.object(seed.source, "_trusted_git"), \
                 mock.patch.object(seed, "_checkout", side_effect=fail_checkout):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    seed.acquire_seed(parent, lambda: None, git_env)
            self.assertFalse((parent / "seed").exists())
            self.assertFalse((parent / "checkouts").exists())
            self.assertEqual((parent / "seed.private.log").read_text(),
                             "git:RuntimeError\n")

    def test_preexisting_log_is_not_appended_after_admission_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent, git_env = self.orchestration_fixture(Path(tmp).resolve())
            log = parent / "seed.private.log"
            log.write_bytes(b"earlier invocation\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                seed.acquire_seed(parent, lambda: None, git_env)
            self.assertEqual(log.read_bytes(), b"earlier invocation\n")

    def test_racing_seed_directory_is_not_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent, git_env = self.orchestration_fixture(Path(tmp).resolve())
            original = Path.mkdir

            def racing_mkdir(path, *args, **kwargs):
                if path == parent / "seed":
                    original(path, mode=0o700)
                    raise FileExistsError("another invocation")
                return original(path, *args, **kwargs)

            with mock.patch.object(seed.source, "_trusted_git"), \
                 mock.patch.object(Path, "mkdir", racing_mkdir):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    seed.acquire_seed(parent, lambda: None, git_env)
            self.assertTrue((parent / "seed").is_dir())
            self.assertFalse((parent / "checkouts").exists())

    def test_sealed_git_checkout_checks_exact_commit_and_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository = root / "repository"
            repository.mkdir(mode=0o700)

            def git(*args):
                result = subprocess.run([str(seed.source.GIT), *args],
                                        cwd=repository, capture_output=True,
                                        timeout=10)
                self.assertEqual(result.returncode, 0)
                return result.stdout.decode().strip()

            git("init", "-q")
            (repository / "source.nim").write_text("discard\n")
            git("add", "source.nim")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "-qm", "fixture")
            commit = git("rev-parse", "HEAD")
            tree = git("rev-parse", "HEAD^{tree}")
            home = root / "home"
            home.mkdir(mode=0o700)
            env = seed.source._git_env(home)
            log = root / "git.private.log"
            pin = {"url": str(repository), "commit": commit, "tree": tree,
                   "max_bytes": 10_000_000}
            guards = []
            with log.open("wb") as output:
                seed._checkout(root / "accepted", pin, env,
                               lambda: guards.append(True), output)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    seed._checkout(root / "rejected", {**pin, "tree": "0" * 40},
                                   env, lambda: None, output)
            self.assertGreater(len(guards), 4)


if __name__ == "__main__":
    unittest.main()
