"""The selected job accepts only one exact offline Cargo vendor input."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_vendor_consume as consume
from scripts.performance.tests import test_issue681_public_vendor_bundle as fixtures


class HostedVendorConsumeTests(unittest.TestCase):
    def fixture(self, root):
        builder = fixtures.PublicVendorBundleTest("test_deterministic_roundtrip_and_archive_provenance")
        self.addCleanup(builder.doCleanups)
        vendor, cache, manifest, locks = builder.fixture(
            root, executable=True, nested_pair=True)
        private = root / "private"
        private.mkdir(mode=0o700)
        zip_path = private / "vendor.zip"
        consume.bundle.write_bundle(
            vendor, manifest, (cache,), zip_path, locks,
            forbidden_roots=(b"/private/test-host",))
        sources = {}
        for arm in ("before", "candidate"):
            source = root / arm
            source.mkdir(mode=0o700)
            (source / "Cargo.lock").write_bytes(locks[arm])
            sources[arm] = source
        home = root / "home"
        home.mkdir(mode=0o700)
        cargo_home = home / "cargo"
        cargo_home.mkdir(mode=0o700)
        guard = object.__new__(consume.capacity.HostStorageGuard)
        return zip_path, sources, home, cargo_home, guard

    def admitted(self, root):
        zip_path, sources, home, cargo_home, guard = self.fixture(root)
        pins = (
            mock.patch.object(consume, "ZIP_BYTES", zip_path.stat().st_size),
            mock.patch.object(consume, "ZIP_SHA256",
                              hashlib.sha256(zip_path.read_bytes()).hexdigest()),
            mock.patch.object(Path, "home", return_value=home),
        )
        for patcher in pins:
            patcher.start()
            self.addCleanup(patcher.stop)
        return zip_path, sources, cargo_home, guard

    def test_exact_bundle_stages_verified_vendor_and_cargo_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path, sources, cargo_home, guard = self.admitted(Path(tmp).resolve())
            calls = []
            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", side_effect=lambda: calls.append(1)):
                result = consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(result["cargo_home"], cargo_home)
            self.assertEqual((result["vendor_root"] / "demo-1.0.0/src/lib.rs")
                             .read_bytes(), b"pub fn demo() {}\n")
            self.assertEqual((result["vendor_root"] / "demo-1.0.0/src/lib.rs")
                             .stat().st_mode & 0o777, 0o700)
            self.assertEqual((result["vendor_root"] / "demo-1.0.0/Cargo.toml")
                             .stat().st_mode & 0o777, 0o600)
            config = tomllib.loads((cargo_home / "config.toml").read_text())
            self.assertEqual(config["source"]["crates-io"]["replace-with"],
                             "vendored-sources")
            self.assertEqual(config["source"]["vendored-sources"]["directory"],
                             str(result["vendor_root"]))
            self.assertGreater(len(calls), 4)
            self.assertEqual(list(cargo_home.glob(".vendor.*")), [])

    def test_extracted_vendor_resolves_in_offline_cargo(self):
        cargo = shutil.which("cargo")
        if cargo is None:
            self.skipTest("Cargo is unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            zip_path, sources, cargo_home, guard = self.admitted(root)
            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", return_value=None):
                consume.install_vendor(zip_path, sources, cargo_home, guard)
            consumer = root / "consumer"
            (consumer / "src").mkdir(parents=True)
            (consumer / "Cargo.toml").write_text(
                "[package]\nname='vendor-consumer'\nversion='0.1.0'\n"
                "edition='2021'\n[dependencies]\ndemo='=1.0.0'\n")
            (consumer / "src/lib.rs").write_text("pub fn use_demo() {}\n")
            environment = dict(os.environ, CARGO_HOME=str(cargo_home),
                               CARGO_NET_OFFLINE="true")
            result = subprocess.run(
                [cargo, "metadata", "--offline", "--format-version", "1"],
                cwd=consumer, env=environment, stdin=subprocess.DEVNULL,
                capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0,
                             msg=result.stderr.decode(errors="replace")[:300])
            packages = json.loads(result.stdout)["packages"]
            self.assertIn("demo", {item["name"] for item in packages})

    def test_changed_zip_and_lock_are_rejected_without_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path, sources, cargo_home, guard = self.admitted(Path(tmp).resolve())
            original = zip_path.read_bytes()
            zip_path.write_bytes(original + b"changed")
            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", return_value=None):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(list(cargo_home.iterdir()), [])
            zip_path.write_bytes(original)
            (sources["before"] / "Cargo.lock").write_bytes(b"changed")
            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", return_value=None):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(list(cargo_home.iterdir()), [])

    def test_guard_failure_during_extraction_removes_owned_partial(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path, sources, cargo_home, guard = self.admitted(Path(tmp).resolve())

            def fail_after_partial():
                if list(cargo_home.glob(".vendor.*")):
                    raise RuntimeError("private capacity failure")

            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", side_effect=fail_after_partial):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(list(cargo_home.iterdir()), [])

    def test_path_swap_after_verifier_cannot_change_pinned_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            zip_path, sources, cargo_home, guard = self.admitted(root)
            verify = consume.bundle.verify_bundle

            def replace_after_verify(path, locks, *, forbidden_roots):
                manifest = verify(path, locks, forbidden_roots=forbidden_roots)
                path.rename(root / "original.zip")
                path.write_bytes(b"replacement")
                return manifest

            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", return_value=None), \
                 mock.patch.object(consume.bundle, "verify_bundle",
                                   side_effect=replace_after_verify):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(list(cargo_home.iterdir()), [])

    def test_nested_symlink_swap_cannot_redirect_extraction(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            zip_path, sources, cargo_home, guard = self.admitted(root)
            outside = root / "outside"
            outside.mkdir(mode=0o700)
            switched = []

            def guard_and_swap():
                if switched:
                    return
                for partial in cargo_home.glob(".vendor.*"):
                    nested = partial / "demo-1.0.0/src"
                    if (nested / "extra.rs").is_file():
                        nested.rename(partial / "demo-1.0.0/src-moved")
                        nested.symlink_to(outside)
                        switched.append(1)

            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", side_effect=guard_and_swap):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual(switched, [1])
            self.assertEqual(list(outside.iterdir()), [])
            self.assertEqual(list(cargo_home.iterdir()), [])

    def test_competing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path, sources, cargo_home, guard = self.admitted(Path(tmp).resolve())
            promote = consume.archive_tools._promote_noreplace

            def compete(partial, output):
                output.mkdir(mode=0o700)
                (output / "competitor").write_bytes(b"keep")
                return promote(partial, output)

            with mock.patch.object(consume.capacity.HostStorageGuard,
                                   "__call__", return_value=None), \
                 mock.patch.object(consume.archive_tools, "_promote_noreplace",
                                   side_effect=compete):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    consume.install_vendor(zip_path, sources, cargo_home, guard)
            self.assertEqual((cargo_home / "vendor/competitor").read_bytes(),
                             b"keep")
            self.assertFalse((cargo_home / "config.toml").exists())
            self.assertEqual(list(cargo_home.glob(".vendor.*")), [])


if __name__ == "__main__":
    unittest.main()
