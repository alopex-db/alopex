"""Regression tests for the matrix's failure propagation (stdlib only)."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.compatibility.run import assert_complete
from scripts.compatibility.storage import allocated_bytes


class MatrixAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.results = {
            "api-result.json": {
                "no_new_regressions": True,
                "rust_api": {"identical": True, "cases": 10},
                "surface_errors": [],
                "behavior": {"baseline_passed": 258, "regressions": []},
            },
            "native/result.json": {
                "direct_same_database_open": True,
                "second_target_reopen_pass": True,
                "sql_vectors_bits_equal": True,
                "raw_get_vector_bits_equal": True,
                "old_existing_hnsw_indexes_recreated_by_harness": False,
            },
            "arrow/report.json": {
                "cases": [
                    {
                        "representation": name,
                        "pass": True,
                        "dimensions": [
                            {
                                "dimension": d,
                                "hnsw_query_pass": True,
                                "new_db_reopen_pass": True,
                            }
                            for d in [3, 384]
                        ],
                    }
                    for name in [
                        "list_f32",
                        "list_f64",
                        "fixed_f32",
                        "fixed_f64",
                        "json_utf8",
                    ]
                ]
            },
            "python-from-python-result.json": {
                "logical_roundtrip_pass": True,
                "new_write_update_pass": True,
            },
            "python15-python-direct-result.json": {
                "direct_open_pass": True,
                "index_recreated": False,
                "second_reopen_pass": True,
            },
        }

    def write(self):
        for name, result in self.results.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(result))

    def test_all_required_cases_pass(self):
        self.write()
        self.assertEqual(len(assert_complete(self.root, rust=False)), 5)

    def test_one_successful_arrow_format_cannot_hide_failed_formats(self):
        self.results["arrow/report.json"]["cases"][1]["pass"] = False
        self.write()
        with self.assertRaises(AssertionError):
            assert_complete(self.root, rust=False)

    def test_missing_or_duplicate_dimension_fails(self):
        self.results["arrow/report.json"]["cases"][0]["dimensions"][1]["dimension"] = 3
        self.write()
        with self.assertRaises(AssertionError):
            assert_complete(self.root, rust=False)

    def test_missing_format_fails(self):
        self.results["arrow/report.json"]["cases"].pop()
        self.write()
        with self.assertRaises(AssertionError):
            assert_complete(self.root, rust=False)

    def test_index_recreation_cannot_count_as_direct_compatibility(self):
        self.results["native/result.json"][
            "old_existing_hnsw_indexes_recreated_by_harness"
        ] = True
        self.write()
        with self.assertRaises(AssertionError):
            assert_complete(self.root, rust=False)

    def test_missing_rust_evidence_does_not_count_as_full_matrix(self):
        self.write()
        with self.assertRaises(FileNotFoundError):
            assert_complete(self.root)

    def test_raw_vector_mismatch_fails(self):
        self.results["native/result.json"]["raw_get_vector_bits_equal"] = False
        self.write()
        with self.assertRaises(AssertionError):
            assert_complete(self.root, rust=False)

    def test_inventory_counts_hardlinks_and_nested_roots_once(self):
        import os

        payload = self.root / "data"
        payload.write_bytes(b"payload" * 1000)
        os.link(payload, self.root / "hardlink")
        total = allocated_bytes([self.root])
        self.assertEqual(total, allocated_bytes([self.root, payload]))
        self.assertEqual(
            total, self.root.stat().st_blocks * 512 + payload.stat().st_blocks * 512
        )


if __name__ == "__main__":
    unittest.main()
