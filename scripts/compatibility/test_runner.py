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


class SplitCaseAcceptanceTests(unittest.TestCase):
    def test_missing_native_case_cannot_pass(self):
        from scripts.compatibility.run import aggregate_native

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                aggregate_native(Path(directory))

    def test_wrong_native_values_or_case_identity_fail(self):
        from scripts.compatibility.run import aggregate_native, NATIVE_CASES

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case in NATIVE_CASES:
                path = root / "native" / case / "result.json"
                path.parent.mkdir(parents=True)
                path.write_text(
                    json.dumps(
                        dict(
                            case=case,
                            direct_same_database_open=True,
                            second_target_reopen_pass=True,
                            values_match_seed=True,
                            index_recreated=False,
                            rows=4
                            if case == "columnar"
                            else 1
                            if case == "raw-3"
                            else 5,
                        )
                    )
                )
            self.assertEqual(len(aggregate_native(root)["cases"]), 4)
            path = root / "native/columnar/result.json"
            result = json.loads(path.read_text())
            result["values_match_seed"] = False
            path.write_text(json.dumps(result))
            with self.assertRaises(AssertionError):
                aggregate_native(root)
            result["values_match_seed"] = True
            result["case"] = "raw-3"
            path.write_text(json.dumps(result))
            with self.assertRaises(AssertionError):
                aggregate_native(root)

    def test_missing_arrow_dimension_cannot_pass(self):
        from scripts.compatibility.run import aggregate_arrow

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                aggregate_arrow(Path(directory))

    def test_api_allowlist_excludes_volume_and_replaced_cases(self):
        from scripts.compatibility.api import CONFIG

        selected = CONFIG["selected_cases"]
        self.assertEqual(len(selected), len(set(selected)))
        self.assertTrue(all("::" in node for node in selected))
        self.assertFalse(set(selected) & set(CONFIG["excluded_cases"]))
        self.assertFalse(set(selected) & set(CONFIG["test_replacements"]))
        self.assertFalse(any("34250" in node for node in selected))
        for spec in CONFIG["test_replacements"].values():
            self.assertEqual(
                sum(node.endswith(spec["replacement"]) for node in selected), 1
            )


class LogicalFixtureLifetimeTests(unittest.TestCase):
    """Exercise the real scheduler with byte-sized stand-ins for old WAL DBs."""

    def exercise(self, root, python_only=False, fail_at=None):
        import shutil
        from scripts.compatibility.run import run_logical_cases

        calls, removed = [], []

        def callback(language, label, owner, mode, *arguments):
            calls.append(label)
            if mode == "export":
                self.assertEqual(owner, "old")
                source = arguments[0] if language == "python" else label
                self.assertFalse(list(root.glob("*-db")), "previous source still live")
                path = root / (source + "-source-db")
                path.mkdir()
                (path / "wal").write_bytes(b"old-wal")
                (root / (source + "-bundle.json")).write_text("{}")
            else:
                self.assertEqual(owner, "candidate")
                source = arguments[-1]
                source_db = root / (source + "-source-db")
                self.assertEqual((source_db / "wal").read_bytes(), b"old-wal")
                if mode == "direct":
                    path = root / (source + "-" + language + "-direct-db")
                    if language == "python":
                        shutil.copytree(source_db, path)
                    self.assertEqual((path / "wal").read_bytes(), b"old-wal")
                else:
                    self.assertEqual(mode, "import")
                    if language == "python":
                        self.assertEqual(arguments[0], label)
                    # Both real importers create <label>-destination-db.
                    path = root / (label + "-destination-db")
                    path.mkdir()
                (path / "wal").write_bytes(b"new-wal")
                self.assertEqual(
                    {p.name for p in root.glob("*-db")},
                    {source_db.name, path.name},
                    "completed reader DB overlaps the next reader",
                )
                self.assertEqual((source_db / "wal").read_bytes(), b"old-wal")
            (root / (label + "-result.json")).write_text('{"pass": true}')
            (root / (label + ".log")).write_text("case evidence")
            if label == fail_at:
                raise RuntimeError("injected reader failure")

        run_logical_cases(
            root,
            lambda *args: callback("python", *args),
            None if python_only else lambda *args: callback("rust", *args),
            removed,
        )
        return calls, removed

    def test_full_matrix_bounds_live_databases_and_keeps_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls, removed = self.exercise(root)
            self.assertEqual(calls, [
                "logical-old", "python15-python-direct", "python-from-python",
                "rust-from-python-direct", "rust-from-python", "rust15",
                "rust15-python-direct", "python-from-rust",
                "rust-from-rust-direct", "rust-from-rust",
            ])
            self.assertEqual(len(removed), 10)
            self.assertEqual(len(set(removed)), 10)
            self.assertFalse(list(root.glob("*-db")))
            self.assertEqual(len(list(root.glob("*-result.json"))), 10)
            self.assertEqual(len(list(root.glob("*.log"))), 10)
            self.assertEqual(len(list(root.glob("*-bundle.json"))), 2)

    def test_python_only_keeps_both_python_consumers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls, removed = self.exercise(root, python_only=True)
            self.assertEqual(calls, [
                "logical-old", "python15-python-direct", "python-from-python",
            ])
            self.assertEqual(len(removed), 3)
            self.assertFalse(list(root.glob("*-db")))

    def test_failure_stops_schedule_and_final_cleanup_keeps_failure_evidence(self):
        from scripts.compatibility.run import remove_databases

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "injected reader failure"):
                self.exercise(root, fail_at="rust-from-python-direct")
            self.assertEqual({p.name for p in root.glob("*-db")}, {
                "python15-source-db", "python15-rust-direct-db",
            })
            self.assertFalse((root / "rust15-bundle.json").exists())
            removed = []
            remove_databases(root, removed, *root.glob("*-db"))
            self.assertEqual(len(removed), 2)
            self.assertTrue((root / "rust-from-python-direct.log").exists())
            self.assertTrue((root / "rust-from-python-direct-result.json").exists())

    def test_cleanup_failure_is_not_silently_discounted(self):
        from unittest.mock import patch
        from scripts.compatibility.run import remove_databases

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "owned-db"
            path.mkdir()
            removed = []
            with patch("scripts.compatibility.run.shutil.rmtree", side_effect=OSError("busy")):
                with self.assertRaisesRegex(OSError, "busy"):
                    remove_databases(root, removed, path)
            self.assertEqual(removed, [])
            self.assertTrue(path.exists())
