import copy
import hashlib
import io
import stat
import tarfile
import tempfile
from pathlib import Path
import json
import sys
import types
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import docs_harness, matrix, worker


class MatrixTests(unittest.TestCase):
    def cells(self):
        return [dict(row, status="passed", result_checksum=worker.result_digest(row["case"]),
                     trial_median_seconds=.001) for row in matrix.plan()]

    def test_plan_and_complete_comparison(self):
        rows = matrix.plan()
        self.assertEqual(len(rows), 48)
        self.assertEqual(len({row["id"] for row in rows}), 48)
        for case in matrix.CASES:
            for trial in range(3):
                self.assertEqual(tuple(row["subject"] for row in rows if row["case"] == case and row["trial"] == trial),
                                 matrix.SUBJECTS if trial % 2 == 0 else matrix.SUBJECTS[::-1])
        summary = matrix.summarize(self.cells())
        self.assertEqual(len(summary["cases"]), 4)
        self.assertFalse(summary["performance_acceptance"])

    def test_missing_duplicate_extra_and_mismatched_cells(self):
        rows = self.cells()
        bads = [rows[:-1], rows[:-1] + [rows[0]], rows + [dict(rows[0], case="insert-commit")]]
        for field, value in (("engine", "postgresql"), ("result_checksum", "f" * 64),
                             ("trial_median_seconds", 0), ("status", "failed")):
            bad = copy.deepcopy(rows)
            bad[0][field] = value
            bads.append(bad)
        for bad in bads:
            with self.subTest(bad=bad[0]), self.assertRaises(ValueError):
                matrix.summarize(bad)

    def test_harness_closure_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docs = root / "docs.tar"
            docs_records = {}
            with tarfile.open(docs, "w") as archive:
                for name in ("sql_resources.py", "subject_identity.py", "server-sql-isolated-v0816.py"):
                    data = b"pinned harness\n"
                    docs_records[name] = (len(data), hashlib.sha256(data).hexdigest())
                    item = tarfile.TarInfo("scripts/performance/" + name)
                    item.size = len(data)
                    archive.addfile(item, io.BytesIO(data))
            product = root / "product"
            records = {}
            for name in ("parity_v0811_measure.py", "parity_v0811_gate.py", "fixtures/sql-v0.8.11-curated.json"):
                path = product / name
                path.parent.mkdir(parents=True, exist_ok=True)
                data = b"fixture\n"
                path.write_bytes(data)
                records[name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            with patch.object(docs_harness, "FILES", docs_records):
                result = matrix.compose_harness(docs, product, records, root / "bundle.tar")
            self.assertEqual(len(result["docs_files"]), 3)
            self.assertEqual(stat.S_IMODE((root / "bundle.tar").stat().st_mode), 0o600)
            (product / "parity_v0811_measure.py").write_text("changed")
            with patch.object(docs_harness, "FILES", docs_records), self.assertRaises(ValueError):
                matrix.compose_harness(docs, product, records, root / "bad.tar")
            (product / "parity_v0811_measure.py").write_bytes(b"fixture\n")
            with tarfile.open(docs, "w") as archive:
                for name in docs_records:
                    data = b"changed harness\n" if name == "sql_resources.py" else b"pinned harness\n"
                    item = tarfile.TarInfo("scripts/performance/" + name)
                    item.size = len(data)
                    archive.addfile(item, io.BytesIO(data))
            with patch.object(docs_harness, "FILES", docs_records), self.assertRaisesRegex(
                    ValueError, "docs harness identity mismatch"):
                matrix.compose_harness(docs, product, records, root / "changed.tar")
            with tarfile.open(docs, "w") as archive:
                data = b"only observer"
                item = tarfile.TarInfo("scripts/performance/sql_resources.py")
                item.size = len(data)
                archive.addfile(item, io.BytesIO(data))
            with patch.object(docs_harness, "FILES", docs_records), self.assertRaises(ValueError):
                matrix.compose_harness(docs, product, records, root / "missing.tar")

    def test_prepare_cell_requires_independent_subject_pin_before_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"}
            files = []
            for role in ("server", "wheel", "receipt"):
                data = json.dumps({"source": source}).encode() if role == "receipt" else role.encode()
                (root / role).write_bytes(data)
                files.append({"role": role, "path": role, "size": len(data),
                              "sha256": hashlib.sha256(data).hexdigest()})
            manifest = {"platform": "linux", "source_commit": source["commit"],
                        "source_tree": source["tree"], "files": files}
            path = root / "assets.json"
            path.write_text(json.dumps(manifest))
            pin = {"subject": "before", "product_subject": "before", "source": source,
                   "assets_digest": hashlib.sha256(path.read_bytes()).hexdigest(),
                   "platform": "linux", "files": files}
            cell = next(row for row in matrix.plan() if row["subject"] == "before")
            self.assertEqual(matrix.verify_subject_pin(path, manifest, {"source": source}, cell, pin), source)
            common = types.ModuleType("common")
            common.verify_assets = lambda *_: manifest
            common.contained = lambda base, name: base / name
            common.digest = lambda item: hashlib.sha256(item.read_bytes()).hexdigest()
            common.private_dir = lambda item: item.mkdir(parents=True, exist_ok=True)
            common.atomic_json = lambda item, value: item.write_text(json.dumps(value))
            bads = [dict(pin, subject="candidate"),
                    dict(pin, assets_digest="0" * 64),
                    dict(pin, source=dict(source, tree="c" * 40)),
                    dict(pin, files=[dict(files[0], sha256="0" * 64), *files[1:]])]
            for bad in bads:
                target = root / "unused"
                with self.subTest(bad=bad), patch.dict(sys.modules, {"common": common}), self.assertRaises(ValueError):
                    matrix.prepare_cell(path, target, root / "harness.tar", {}, cell, {}, bad,
                                        kind="cell")
                self.assertFalse(target.exists())
            (root / "receipt").write_text(json.dumps({"source": dict(source, commit="c" * 40)}))
            with patch.dict(sys.modules, {"common": common}), self.assertRaises(ValueError):
                matrix.prepare_cell(path, root / "unused", root / "harness.tar", {}, cell, {}, pin,
                                    kind="cell")
            with patch.dict(sys.modules, {"common": common}), self.assertRaises(ValueError):
                matrix.prepare_cell(path, root / "unused", root / "harness.tar", {}, cell, {}, pin,
                                    kind="preflight")
            pg = next(row for row in matrix.plan() if row["subject"] == "postgresql")
            pg_pin = dict(pin, subject="postgresql", product_subject="published",
                          reference_image_digest="d" * 64,
                          published_client_pin=dict(pin, subject="published", product_subject="published"))
            self.assertEqual(matrix.verify_subject_pin(path, manifest, {"source": source}, pg, pg_pin), source)
            for bad in (dict(pg_pin, product_subject="postgresql"),
                        {key: value for key, value in pg_pin.items() if key != "reference_image_digest"},
                        dict(pg_pin, published_client_pin=dict(pg_pin["published_client_pin"], assets_digest="0" * 64))):
                with self.assertRaises(ValueError):
                    matrix.verify_subject_pin(path, manifest, {"source": source}, pg, bad)


if __name__ == "__main__":
    unittest.main()
