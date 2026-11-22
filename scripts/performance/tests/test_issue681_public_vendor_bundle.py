"""Public Cargo vendor ZIP accepts only authenticated, fixed-shape input."""

import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest import mock
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_vendor_bundle as bundle


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


class PublicVendorBundleTest(unittest.TestCase):
    def fixture(self, root, *, patch_pins=True):
        vendor = root / "vendor"
        package = vendor / "demo-1.0.0"
        (package / "src").mkdir(parents=True)
        files = {"Cargo.toml": b"[package]\nname='demo'\nversion='1.0.0'\n",
                 "src-file.rs": b"pub fn sibling() {}\n",
                 "src/lib.rs": b"pub fn demo() {}\n"}
        for name, raw in files.items():
            (package / name).write_bytes(raw)
        archive_raw = io.BytesIO()
        with tarfile.open(fileobj=archive_raw, mode="w:gz") as archive:
            for name, raw in (*files.items(), (".gitignore", b"target\n")):
                info = tarfile.TarInfo("demo-1.0.0/" + name)
                info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))
        checksum = bundle.digest(archive_raw.getvalue())
        cache = root / "cache"
        cache.mkdir()
        (cache / "demo-1.0.0.crate").write_bytes(archive_raw.getvalue())
        metadata = {"$comment": bundle.CHECKSUM_COMMENT,
                    "files": {name: bundle.digest(raw)
                              for name, raw in files.items()},
                    "package": checksum}
        (package / ".cargo-checksum.json").write_text(json.dumps(metadata))
        locks = {}
        for arm in ("before", "candidate"):
            locks[arm] = ("version = 4\n\n[[package]]\nname = 'demo'\n"
                          "version = '1.0.0'\nsource = '" + bundle.REGISTRY +
                          "'\nchecksum = '" + checksum + "'\n").encode()
        pins = {arm: {**bundle.source.PINS[arm],
                      "lock": bundle.digest(locks[arm])}
                for arm in ("before", "candidate")}
        rows = {}
        for path in sorted(vendor.rglob("*")):
            if path.is_file():
                raw = path.read_bytes()
                rows[path.relative_to(vendor).as_posix()] = (len(raw),
                                                              bundle.digest(raw))
        tree = hashlib.sha256()
        for name, (size, file_digest) in sorted(rows.items()):
            tree.update(json.dumps([name, size, file_digest],
                                   separators=(",", ":")).encode() + b"\n")
        manifest = {
            "schema": bundle.SCHEMA, "release_version": "0.8.17",
            "sources": {arm: {"commit": pins[arm]["commit"],
                              "tree": pins[arm]["tree"],
                              "cargo_lock_sha256": bundle.digest(locks[arm])}
                        for arm in ("before", "candidate")},
            "common_example_sha256": pins["before"]["example"],
            "package_count": 1,
            "packages": [{"crate": "demo", "version": "1.0.0",
                          "archive_sha256": checksum}],
            "vendor_file_count": len(rows),
            "vendor_content_bytes": sum(value[0] for value in rows.values()),
            "vendor_tree_sha256": tree.hexdigest(),
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_bytes(canonical(manifest))
        if patch_pins:
            patcher = mock.patch.object(bundle.source, "PINS", pins)
            patcher.start()
            self.addCleanup(patcher.stop)
        return vendor, cache, manifest_path, locks

    def test_deterministic_roundtrip_and_archive_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor, cache, manifest, locks = self.fixture(root)
            first, second = root / "first.zip", root / "second.zip"
            digest1 = bundle.write_bundle(
                vendor, manifest, (cache,), first, locks,
                forbidden_roots=(b"/private/test-host",))
            digest2 = bundle.write_bundle(
                vendor, manifest, (cache,), second, locks,
                forbidden_roots=(b"/private/test-host",))
            self.assertEqual(digest1, digest2)
            self.assertEqual(bundle.verify_bundle(
                first, locks, forbidden_roots=(b"/private/test-host",))
                ["package_count"], 1)

    def test_extra_or_traversal_member_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor, cache, manifest, locks = self.fixture(root)
            output = root / "bundle.zip"
            bundle.write_bundle(vendor, manifest, (cache,), output, locks,
                                forbidden_roots=(b"/private/test-host",))
            with zipfile.ZipFile(output, "a") as archive:
                archive.writestr(bundle._zip_info("../escape"), b"bad")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                bundle.verify_bundle(output, locks,
                                     forbidden_roots=(b"/private/test-host",))

    def test_changed_source_byte_cannot_create_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor, cache, manifest, locks = self.fixture(root)
            (vendor / "demo-1.0.0/src/lib.rs").write_bytes(b"changed\n")
            output = root / "bundle.zip"
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                bundle.write_bundle(vendor, manifest, (cache,), output, locks,
                                    forbidden_roots=(b"/private/test-host",))
            self.assertFalse(output.exists())
            self.assertFalse(output.with_name(output.name + ".partial").exists())

    def test_real_source_pins_reject_synthetic_locks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor, cache, manifest, locks = self.fixture(root, patch_pins=False)
            output = root / "bundle.zip"
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                bundle.write_bundle(vendor, manifest, (cache,), output, locks,
                                    forbidden_roots=(b"/private/test-host",))
            self.assertFalse(output.exists())

    def test_reordered_members_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor, cache, manifest, locks = self.fixture(root)
            output = root / "bundle.zip"
            bundle.write_bundle(vendor, manifest, (cache,), output, locks,
                                forbidden_roots=(b"/private/test-host",))
            reordered = root / "reordered.zip"
            with zipfile.ZipFile(output) as original, zipfile.ZipFile(
                    reordered, "w") as changed:
                infos = original.infolist()
                for info in [infos[0], *reversed(infos[1:])]:
                    changed.writestr(info, original.read(info))
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                bundle.verify_bundle(reordered, locks,
                                     forbidden_roots=(b"/private/test-host",))


if __name__ == "__main__":
    unittest.main()
