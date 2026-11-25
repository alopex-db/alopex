"""Tiny native inventory contracts; no load test or benchmark."""
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import redirect_stderr
import io
import subprocess
import sys

import storage


class StorageTest(unittest.TestCase):
    def test_physical_volumes_deduplicates_nested_roots_and_future_target(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name).resolve()
            usage = SimpleNamespace(total=1000, used=300, free=700)
            with patch.object(storage.shutil, "disk_usage", return_value=usage) as measure:
                result = storage.physical_volumes([root, root / "future" / "output"])
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["roots"], [str(root), str(root / "future" / "output")])
            self.assertEqual((result[0]["total_bytes"], result[0]["free_bytes"]), (1000, 700))
            measure.assert_called_once_with(root)

    def test_physical_volumes_rejects_relative_missing_unreadable_and_symlink(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name).resolve()
            with self.assertRaises(storage.VolumeCoverageError):
                storage.physical_volumes([Path("relative")])
            with self.assertRaises(storage.VolumeCoverageError):
                storage.physical_volumes([])
            with self.assertRaises(storage.VolumeCoverageError):
                storage.physical_volumes([root / ".." / "other"])
            link = root / "link"
            link.symlink_to(root, target_is_directory=True)
            with self.assertRaises(storage.VolumeCoverageError):
                storage.physical_volumes([link / "future"])
            original = Path.lstat

            def unreadable(path, *args, **kwargs):
                if path == root:
                    raise PermissionError("private path")
                return original(path, *args, **kwargs)

            with patch.object(Path, "lstat", unreadable):
                with self.assertRaises(PermissionError):
                    storage.physical_volumes([root / "future"])
            with patch.object(Path, "lstat", side_effect=FileNotFoundError()):
                with self.assertRaises(storage.VolumeCoverageError):
                    storage.physical_volumes([root / "missing"])

    def test_physical_volumes_measures_distinct_mocked_volumes(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name).resolve()
            first, second = root / "first", root / "second"
            first.mkdir()
            second.mkdir()
            original = Path.lstat

            def distinct(path, *args, **kwargs):
                info = original(path, *args, **kwargs)
                if path == second:
                    return SimpleNamespace(st_mode=info.st_mode, st_dev=info.st_dev + 1)
                return info

            def usage(path):
                return SimpleNamespace(total=1000, used=100 if path == first else 200,
                                       free=900 if path == first else 800)

            with patch.object(Path, "lstat", distinct), patch.object(
                storage.shutil, "disk_usage", side_effect=usage
            ) as measure:
                result = storage.physical_volumes([first, second])
            self.assertEqual([item["free_bytes"] for item in result], [900, 800])
            self.assertEqual(measure.call_count, 2)

    def test_physical_gate_fails_closed_and_logs_private_detail(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            log = Path(name) / "private.jsonl"
            with patch.object(storage, "inventory", return_value={"allocated_bytes": 1}), patch.object(
                storage, "physical_volumes", return_value=[{"free_bytes": 4, "roots": ["private"]}]
            ):
                with self.assertRaisesRegex(storage.PhysicalSpaceError, "PHYSICAL_SPACE"):
                    storage.check(log, physical_min_free_bytes=5, volume_roots=["private"])
            self.assertIn("private", log.read_text())
            with patch.object(storage, "inventory", return_value={"allocated_bytes": 1}), patch.object(
                storage, "physical_volumes", side_effect=PermissionError("private location")
            ):
                with self.assertRaisesRegex(storage.VolumeCoverageError, "VOLUME_COVERAGE"):
                    storage.check(log, physical_min_free_bytes=5, volume_roots=["private"])
            self.assertIn("private location", log.read_text())

    def test_physical_gate_accepts_threshold_and_rejects_invalid_capacity(self):
        with patch.object(storage, "inventory", return_value={"allocated_bytes": 1}), patch.object(
            storage, "physical_volumes", return_value=[{"free_bytes": 5, "roots": ["private"]}]
        ):
            self.assertEqual(storage.check(physical_min_free_bytes=5,
                                           volume_roots=["private"])["allocated_bytes"], 1)
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            root = Path(name).resolve()
            with patch.object(storage.shutil, "disk_usage",
                              return_value=SimpleNamespace(total=0, free=0)):
                with self.assertRaises(storage.VolumeCoverageError):
                    storage.physical_volumes([root])

    def test_physical_cli_emits_only_fixed_failure_marker(self):
        with tempfile.TemporaryDirectory(prefix="alopex-storage-test-") as name:
            log = Path(name) / "private.jsonl"
            with patch.object(sys, "argv", ["storage.py", "--log", str(log),
                                            "--physical-min-free-bytes", "5",
                                            "--volume-root", str(Path(name) / "future")]), patch.object(
                storage, "inventory", return_value={"allocated_bytes": 1}
            ), patch.object(storage, "physical_volumes",
                            side_effect=PermissionError("private location")):
                output = io.StringIO()
                with redirect_stderr(output), self.assertRaises(SystemExit) as stopped:
                    storage.main()
            self.assertEqual(stopped.exception.code, 1)
            self.assertEqual(output.getvalue().strip(), "VOLUME_COVERAGE")
            self.assertIn("private location", log.read_text())

    def test_physical_cli_checks_before_command_and_on_watchdog_sample(self):
        args = SimpleNamespace(log=Path("private.jsonl"), root=[],
                               additional_bytes=100, command=["--", "child"],
                               physical_min_free_bytes=5, volume_root=["planned"])
        child = SimpleNamespace(returncode=0)
        polls = iter([None, 0])
        child.poll = lambda: next(polls)
        child.wait = lambda timeout: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("child", timeout))
        with patch.object(storage, "check", return_value={"allocated_bytes": 1}) as check, patch.object(
            storage.subprocess, "Popen", return_value=child
        ):
            with self.assertRaises(SystemExit) as stopped:
                storage._main_with_args(args)
        self.assertEqual(stopped.exception.code, 0)
        self.assertEqual(check.call_count, 3)
        for call in check.call_args_list:
            self.assertEqual(call.kwargs["physical_min_free_bytes"], 5)
            self.assertEqual(call.kwargs["volume_roots"], ["planned"])

    def test_failed_windows_tree_stop_has_distinct_fixed_code(self):
        args = SimpleNamespace(log=Path("private.jsonl"), root=[],
                               additional_bytes=100, command=["--", "child"],
                               physical_min_free_bytes=5, volume_root=["planned"])
        child = SimpleNamespace(pid=123)
        child.poll = lambda: None
        child.wait = lambda timeout: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("child", timeout))
        with patch.object(storage, "WINDOWS", True), patch.object(
            storage, "check", side_effect=[{"allocated_bytes": 1},
                                           storage.PhysicalSpaceError("PHYSICAL_SPACE")]
        ), patch.object(storage.subprocess, "Popen", return_value=child), patch.object(
            storage.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "taskkill")
        ):
            with self.assertRaisesRegex(storage.StopFailureError, "STOP_FAILURE"):
                storage._main_with_args(args)

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
                                           st_mode=info.st_mode)
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
