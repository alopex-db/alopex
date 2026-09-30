from __future__ import annotations

import importlib.util
import io
import json
import struct
import tarfile
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/release/publish_crate_archive.py"


def load_module():
    spec = importlib.util.spec_from_file_location("publish_crate_archive", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PublishCrateArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_module()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.archive = self.root / "alopex-core-0.8.14.crate"
        with tarfile.open(self.archive, "w:gz") as bundle:
            content = b'[package]\nname = "alopex-core"\nversion = "0.8.14"\n'
            info = tarfile.TarInfo("alopex-core-0.8.14/Cargo.toml")
            info.size = len(content)
            bundle.addfile(info, io.BytesIO(content))
        self.metadata = {"name": "alopex-core", "vers": "0.8.14"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_request_body_binds_metadata_to_exact_crate_bytes(self) -> None:
        body = self.module.request_body(self.metadata, self.archive)
        metadata_size = struct.unpack("<I", body[:4])[0]
        metadata = json.loads(body[4 : 4 + metadata_size])
        archive_size = struct.unpack("<I", body[4 + metadata_size : 8 + metadata_size])[0]

        self.assertEqual(metadata, self.metadata)
        self.assertEqual(body[8 + metadata_size :], self.archive.read_bytes())
        self.assertEqual(archive_size, self.archive.stat().st_size)

    def test_request_body_rejects_identity_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "identity"):
            self.module.request_body({"name": "wrong", "vers": "0.8.14"}, self.archive)


if __name__ == "__main__":
    unittest.main()
