"""Preflight the frozen PR 819 diagnostic plan before remote measurement."""

import unittest
import hashlib
import json
from pathlib import Path
import tempfile
from unittest.mock import patch
import zipfile

import hosted_native_begin as hosted
import native_begin_matrix as matrix
import native_begin_supervisor as supervisor


class TargetedControllerTests(unittest.TestCase):
    def test_source_and_schema_are_fixed(self):
        self.assertEqual(hosted.BEFORE_COMMIT,
                         "ce88bbfadd8030ff5cd67262a72f915e50e5b8da")
        self.assertEqual(hosted.BEFORE_TREE,
                         "8cc4513c4654c95cfe8c0dd7ea8162f4ce6345a5")
        self.assertEqual(hosted.CANDIDATE_COMMIT,
                         "1ceaf881dfc0e38a80ab88113032bbcbcbc831cc")
        self.assertEqual(hosted.CANDIDATE_TREE,
                         "e996099a852903b1e436167ddd9805cdcf223cdf")

    def test_frozen_plan_and_probe_inventory(self):
        cells = matrix.plan()
        fixtures = ((0, 0), (0, 1), (100, 2), (0, 2))
        self.assertEqual(len(cells), 24)
        self.assertEqual(len({cell["id"] for cell in cells}), 24)
        self.assertEqual({(cell["rows"], cell["indexes"], cell["trial"], cell["subject"])
                          for cell in cells},
                         {(rows, indexes, trial, subject)
                          for rows, indexes in fixtures
                          for trial in range(3)
                          for subject in ("before", "candidate")})
        self.assertEqual([cell["subject"] for cell in cells[:6]],
                         ["before", "candidate", "candidate", "before", "before", "candidate"])
        self.assertEqual(sum(len(supervisor.probes(cell["rows"], cell["indexes"]))
                             for cell in cells), 30)
        self.assertEqual(supervisor.TARGET_PROBES,
                         {(0, 0): (("ro", "noop"), ("rw", "query")),
                          (0, 1): (("ro", "query"),),
                          (100, 2): (("ro", "query"),),
                          (0, 2): (("rw", "query"),)})
        for rows, indexes in ((0, 3), (100, 1), (1000, 2)):
            with self.assertRaises(ValueError):
                supervisor.probes(rows, indexes)

    def test_wheel_only_source_and_bytes_are_verified(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wheel = root / "base.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("alopex/_alopex.abi3.so", b"extension")
            library = root / "parser.so"
            library.write_bytes(b"parser")
            digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            source = {"commit": hosted.BEFORE_COMMIT, "tree": hosted.BEFORE_TREE,
                      "profile": "release"}
            receipt = root / "build.json"
            receipt.write_text(json.dumps({"schema": "alopex.performance-wheel-build/v1",
                                           "source": source, "python_wheel_sha256": digest(wheel),
                                           "parser_sha256": digest(library)}))
            files = [dict(role=role, path=path.name, size=path.stat().st_size,
                          sha256=digest(path)) for role, path in
                     (("wheel", wheel), ("library", library), ("receipt", receipt))]
            manifest = root / "assets.json"
            value = dict(schema="alopex.native-wheel-assets/v1", platform="linux/amd64",
                         source_commit=source["commit"], source_tree=source["tree"], files=files)
            manifest.write_text(json.dumps(value))
            self.assertEqual(supervisor.verify_begin_assets(manifest), value)
            harness = root / "source-harness.tar"
            harness.write_bytes(b"fixed harness")
            operators = {role: {"path": path, "sha256": digest(path)} for role, path in
                         (("measurement_controller", Path(supervisor.__file__)),
                          ("measurement_worker", Path(__file__).with_name("native_begin_probe.py")))}
            prepared, _ = matrix.prepare_cell(manifest, root / "prepared", matrix.plan()[0],
                                              operators, harness)
            self.assertEqual({item["role"] for item in supervisor.verify_begin_assets(prepared)["files"]},
                             {"wheel", "library", "receipt", "harness", "measurement_controller",
                              "measurement_worker", "measurement_config"})
            library.write_bytes(b"substitution")
            with self.assertRaises(ValueError):
                supervisor.verify_begin_assets(manifest)

    def test_begin_only_asset_adapter_is_restored(self):
        import envctl
        original = envctl.verify_assets

        def inspect(profile, budget, config, **checks):
            self.assertIs(envctl.verify_assets, supervisor.verify_begin_assets)
            self.assertEqual(set(checks), {"verify", "recovered"})
            return "inspected"

        with patch("native_sql_measure_container.execute_cell", side_effect=inspect):
            self.assertEqual(matrix.execute_cell({}, None, {}), "inspected")
        self.assertIs(envctl.verify_assets, original)


if __name__ == "__main__":
    unittest.main()
