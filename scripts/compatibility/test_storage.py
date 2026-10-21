"""Tiny native inventory contracts; no load test or benchmark."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import storage


class StorageTest(unittest.TestCase):
    def test_unique_files_nested_roots_and_hard_links(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name)
            file = root / "data"
            file.write_bytes(b"x" * 4097)
            before = storage.allocated_bytes([root])
            self.assertGreaterEqual(before, 4097)
            os.link(file, root / "alias")
            # Directory allocation can grow after the link. Counting both roots
            # must still produce the exact same inventory as counting just one.
            self.assertEqual(storage.allocated_bytes([root, file]),
                             storage.allocated_bytes([root]))
            file_bytes = (storage.allocation_size(file) if os.name == "nt"
                          else file.stat().st_blocks * 512)
            self.assertEqual(storage.allocated_bytes([file, root / "alias"]), file_bytes)

    def test_measurement_failure_is_not_zero(self):
        with patch.object(storage, "inventory", side_effect=PermissionError("unreadable")):
            with self.assertRaises(PermissionError):
                storage.check()

    def test_stop_threshold_is_inclusive(self):
        with patch.object(storage, "inventory", return_value={"allocated_bytes": 40}):
            with self.assertRaises(RuntimeError):
                storage.check(limit=40)
            self.assertEqual(storage.check(limit=41)["allocated_bytes"], 40)

    @unittest.skipUnless(os.name == "nt", "Windows native allocation API")
    def test_windows_missing_path_reports_failure(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            with self.assertRaises(FileNotFoundError):
                storage.allocation_size(Path(name) / "missing")


if __name__ == "__main__":
    unittest.main()
