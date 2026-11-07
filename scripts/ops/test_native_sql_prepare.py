import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

from common import Failure, digest, verify_assets
from native_sql_matrix import plan, prepare_cell


class PrepareCellTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "server").write_bytes(b"fixture server")
        with zipfile.ZipFile(self.source / "fixture.whl", "w") as archive:
            archive.writestr("fixture.py", "# fixture\n")
        self.harness = self.root / "combined.tar"
        with tarfile.open(self.harness, "w") as archive:
            data = b"# fixture\n"
            item = tarfile.TarInfo("docs/scripts/performance/sql_resources.py")
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
        (self.source / "harness.tar").write_bytes(self.harness.read_bytes())
        receipt = {"schema": "alopex.performance-build/v1", "source": {"commit": "a" * 40},
                   "server_sha256": digest(self.source / "server"),
                   "python_wheel_sha256": digest(self.source / "fixture.whl")}
        (self.source / "receipt.json").write_text(json.dumps(receipt))
        files = []
        for role, name in (("server", "server"), ("wheel", "fixture.whl"),
                           ("receipt", "receipt.json"), ("harness", "harness.tar")):
            path = self.source / name
            files.append({"role": role, "path": name, "size": path.stat().st_size, "sha256": digest(path)})
        self.manifest = self.source / "assets.json"
        self.manifest.write_text(json.dumps({"schema": "alopex.test-env-assets/v1", "platform": "fixture-platform",
                                            "source_commit": "a" * 40, "harness_commit": "b" * 40,
                                            "expanded_limit_bytes": 100000, "files": files}))
        self.record = {"sha256": digest(self.harness), "docs_subdirectory": "docs", "product_subdirectory": "product",
                       "docs_files": {"fixture": "c" * 64}, "product_files": {"fixture": "d" * 64}}
        self.operators = {}
        for role in ("measurement_controller", "measurement_worker"):
            path = self.root / (role + ".py")
            path.write_text("# fixture\n")
            self.operators[role] = {"path": path, "sha256": digest(path)}

    def prepare(self):
        return prepare_cell(self.manifest, self.root / "input", self.harness, self.record, plan()[0], self.operators)

    def test_preserves_product_bytes_and_binds_measurement_inputs(self):
        original = self.manifest.read_bytes()
        path, config = self.prepare()
        manifest = verify_assets(path, "fixture-platform")
        for name in ("server", "fixture.whl", "receipt.json"):
            self.assertEqual((path.parent / name).read_bytes(), (self.source / name).read_bytes())
        self.assertEqual(self.manifest.read_bytes(), original)
        self.assertEqual(config["source"], {"commit": "a" * 40})
        self.assertEqual(json.loads((path.parent / "measurement.json").read_text()), config)
        self.assertTrue({"measurement_controller", "measurement_worker", "measurement_config"}.issubset(
            {item["role"] for item in manifest["files"]}))

    def test_altered_product_rejected_before_staging(self):
        (self.source / "server").write_bytes(b"tampered")
        with self.assertRaises(Failure): self.prepare()
        self.assertFalse((self.root / "input").exists())

    def test_altered_operator_rejected(self):
        self.operators["measurement_controller"]["path"].write_text("changed")
        with self.assertRaises(ValueError): self.prepare()

    def test_altered_combined_harness_rejected(self):
        self.harness.write_bytes(b"changed")
        with self.assertRaises(ValueError): self.prepare()

    def test_existing_cell_never_overwritten(self):
        self.prepare()
        with self.assertRaises(ValueError): self.prepare()


if __name__ == "__main__":
    unittest.main()
