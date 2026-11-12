"""Tiny native inventory contracts; no load test or benchmark."""
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
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

    def test_windows_native_identity_deduplicates_hard_links_even_with_zero_stat_ino(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name)
            first, alias, second = (root / part for part in ("first", "alias", "second"))
            first.write_bytes(b"x")
            os.link(first, alias)
            second.write_bytes(b"y")
            measured = {
                first: ((7, b"\x01" * 16), 4096),
                alias: ((7, b"\x01" * 16), 4096),
                second: ((7, b"\x02" * 16), 8192),
            }
            measured = {path.resolve(): value for path, value in measured.items()}
            original_lstat = Path.lstat

            def zero_inode(path, *args, **kwargs):
                info = original_lstat(path, *args, **kwargs)
                if path in measured:
                    return SimpleNamespace(st_dev=info.st_dev, st_ino=0,
                                           st_mode=info.st_mode, st_blocks=info.st_blocks)
                return info

            with patch.object(storage, "WINDOWS", True), patch.object(
                storage, "native_file_info", side_effect=lambda path: measured[path], create=True
            ) as native, patch.object(Path, "lstat", zero_inode):
                self.assertEqual(storage.allocated_bytes([first, alias, second]), 12288)
                self.assertEqual(native.call_count, 3)

    def test_windows_native_identity_failure_stops_measurement(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            file = Path(name) / "object"
            file.write_bytes(b"x")
            with patch.object(storage, "WINDOWS", True), patch.object(
                storage, "native_file_info", side_effect=OSError("identity unavailable"),
                create=True,
            ):
                with self.assertRaisesRegex(OSError, "identity unavailable"):
                    storage.allocated_bytes([file])

    def test_windows_concurrent_deletion_is_skipped(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            file = Path(name) / "object"
            file.write_bytes(b"x")
            with patch.object(storage, "WINDOWS", True), patch.object(
                storage, "native_file_info", side_effect=FileNotFoundError(), create=True
            ):
                self.assertEqual(storage.allocated_bytes([file]), 0)

    @unittest.skipUnless(os.name == "nt", "Windows native file API")
    def test_windows_native_hard_link_identity(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            first = Path(name) / "first"
            alias = Path(name) / "alias"
            first.write_bytes(b"x" * 4097)
            os.link(first, alias)
            self.assertEqual(storage.native_file_info(first), storage.native_file_info(alias))

    @unittest.skipUnless(os.name == "nt", "Windows native allocation API")
    def test_windows_missing_path_reports_failure(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            with self.assertRaises(FileNotFoundError):
                storage.allocation_size(Path(name) / "missing")


if __name__ == "__main__":
    unittest.main()
