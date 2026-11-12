import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import native_sql_matrix as matrix


class MatrixTests(unittest.TestCase):
    def test_exact_48_cells_and_alternating_order(self):
        plan = matrix.plan()
        self.assertEqual(len(plan), 48)
        self.assertEqual(len({row["id"] for row in plan}), 48)
        for case in matrix.CASES:
            for trial in range(3):
                cells = [r for r in plan if r["case"] == case and r["trial"] == trial]
                expected = matrix.SUBJECTS if trial % 2 == 0 else tuple(reversed(matrix.SUBJECTS))
                self.assertEqual(tuple(row["subject"] for row in cells), expected)

    def test_five_percent_improvement_boundary(self):
        self.assertTrue(matrix.compare([100] * 3, [95] * 3)["improvement_proven"])
        self.assertFalse(matrix.compare([100] * 3, [95.001] * 3)["improvement_proven"])

    def test_five_percent_regression_boundary(self):
        self.assertEqual(matrix.compare([100] * 3, [105] * 3)["status"], "within_tolerance")
        self.assertEqual(matrix.compare([100] * 3, [105.001] * 3)["status"], "regression")

    def test_increase_within_tolerance_remains_visible(self):
        result = matrix.compare([100] * 3, [103] * 3)
        self.assertTrue(result["increase"])
        self.assertFalse(result["improvement_proven"])

    def test_noise_uses_trial_mad_and_never_widens_tolerance(self):
        result = matrix.compare([97, 100, 103], [90, 90, 90])
        self.assertEqual(result["noise"], 9)
        self.assertEqual(result["status"], "pending_noise")
        self.assertFalse(result["improvement_proven"])

    def test_every_paired_trial_must_improve(self):
        self.assertFalse(matrix.compare([100, 100, 80], [90, 90, 90])["improvement_proven"])

    def test_improvement_must_exceed_noise(self):
        self.assertTrue(matrix.compare([100, 100, 100], [94, 95, 96])["improvement_proven"])
        result = matrix.compare([120, 120, 120], [112, 114, 116])
        self.assertEqual(result["noise"], 6)
        self.assertEqual(result["status"], "within_tolerance")
        self.assertFalse(result["improvement_proven"])

    def test_bad_trial_counts_and_values_fail(self):
        for values in ([1, 1], [1, 1, 1, 1], [1, 0, 1], [1, float("nan"), 1], [1, float("inf"), 1], [1, True, 1]):
            with self.subTest(values=values), self.assertRaises(ValueError): matrix.compare(values, [1, 1, 1])

    def cells(self):
        return [dict(row, status="passed", result_checksum="a" * 64, input_checksum="b" * 64,
                     trial_median_seconds=.001) for row in matrix.plan()]

    def test_complete_matrix_is_measured_without_claiming_release_acceptance(self):
        result = matrix.summarize(self.cells())
        self.assertEqual(len(result["cases"]), 4)
        self.assertTrue(all(row["status"] == "measured" for row in result["cases"]))
        self.assertFalse(result["performance_acceptance"])

    def test_missing_and_duplicate_cells_fail(self):
        rows = self.cells()
        with self.assertRaises(ValueError): matrix.summarize(rows[:-1])
        rows[-1] = rows[0]
        with self.assertRaises(ValueError): matrix.summarize(rows)

    def test_failure_and_missing_observations_never_pass(self):
        rows = self.cells(); rows[0]["status"] = "failed"
        self.assertEqual(matrix.summarize(rows)["cases"][0]["status"], "failed_cell")

    def test_cross_subject_result_mismatch_fails(self):
        rows = self.cells(); rows[0]["result_checksum"] = "c" * 64
        with self.assertRaises(ValueError): matrix.summarize(rows)

    def test_write_case_is_excluded(self):
        rows = self.cells()
        rows.append(dict(rows[0], case="insert-commit"))
        with self.assertRaises(ValueError): matrix.summarize(rows)


class HarnessBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.docs = self.root / "docs.tar"
        with tarfile.open(self.docs, "w") as output:
            for name in ("sql_resources.py", "subject_identity.py"):
                data = b"# fixed fixture\n"; member = tarfile.TarInfo("scripts/performance/" + name)
                member.size = len(data); output.addfile(member, io.BytesIO(data))
        self.product = self.root / "product"; self.product.mkdir(); self.files = {}
        for name in ("parity_v0811_measure.py", "parity_v0811_gate.py", "fixtures/sql-v0.8.11-curated.json"):
            path = self.product / name; path.parent.mkdir(exist_ok=True)
            data = b"{}\n"; path.write_bytes(data)
            self.files[name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    def test_inputs_remain_identical_in_composed_archive(self):
        path = self.root / "combined.tar"
        result = matrix.compose_harness(self.docs, self.product, self.files, path)
        with tarfile.open(path) as archive:
            for name, digest in result["product_files"].items():
                data = archive.extractfile("product/scripts/performance/" + name).read()
                self.assertEqual(data, (self.product / name).read_bytes())
                self.assertEqual(hashlib.sha256(data).hexdigest(), digest)
            self.assertTrue(all(m.uid == m.gid == 0 and m.mtime == 0 for m in archive.getmembers()))

    def test_changed_product_bytes_are_rejected(self):
        (self.product / "parity_v0811_measure.py").write_text("changed")
        with self.assertRaises(ValueError): matrix.compose_harness(self.docs, self.product, self.files, self.root / "combined.tar")

    def test_nonregular_archive_member_is_rejected(self):
        with tarfile.open(self.docs, "a") as output:
            member = tarfile.TarInfo("scripts/performance/link"); member.type = tarfile.SYMTYPE; member.linkname = "../../outside"
            output.addfile(member)
        with self.assertRaises(ValueError): matrix.compose_harness(self.docs, self.product, self.files, self.root / "combined.tar")


if __name__ == "__main__":
    unittest.main()
