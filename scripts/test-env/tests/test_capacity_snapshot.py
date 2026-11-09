"""Small deterministic directory-change regressions for capacity monitoring."""
import errno
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from capacity import Budget, allocated_bytes
from common import Failure


class CompleteTraversal(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.original_lstat = Path.lstat

    def expected(self, *paths):
        return sum(self.original_lstat(path).st_blocks * 512 for path in paths)

    def test_deleted_entry_restarts_all_counting_and_includes_replacement(self):
        stable = self.root / 'stable'
        stable.write_bytes(b'x' * 4096)
        transient = self.root / 'checkpoint.meta'
        transient.write_bytes(b'x')
        replacement = self.root / 'replacement'
        stable_reads = []
        original_iterdir = Path.iterdir

        def ordered(path):
            if path == self.root and transient.exists():
                return iter((transient, stable))  # Count stable before the failure.
            return original_iterdir(path)

        def observed(path, *args, **kwargs):
            if path == stable:
                stable_reads.append(path)
            if path == transient:
                transient.unlink()
                replacement.write_bytes(b'y' * 8192)
                raise FileNotFoundError(errno.ENOENT, 'entry replaced')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'iterdir', ordered), patch.object(Path, 'lstat', observed):
            measured = allocated_bytes([self.root])
        self.assertEqual(len(stable_reads), 2)
        self.assertEqual(measured, self.expected(self.root, stable, replacement))

    def test_directory_removed_during_listing_restarts_from_inventory_roots(self):
        stable = self.root / 'stable'
        stable.write_bytes(b'x' * 4096)
        transient = self.root / 'temporary'
        transient.mkdir()
        original_iterdir = Path.iterdir
        root_reads = []

        def observed(path):
            if path == self.root:
                root_reads.append(path)
            if path == transient:
                transient.rmdir()
                raise FileNotFoundError(errno.ENOENT, 'directory removed')
            return original_iterdir(path)

        with patch.object(Path, 'iterdir', observed):
            measured = allocated_bytes([self.root])
        self.assertEqual(len(root_reads), 2)
        self.assertEqual(measured, self.expected(self.root, stable))

    def test_repeated_disappearance_fails_after_three_complete_attempts(self):
        transient = self.root / 'checkpoint.meta'
        transient.write_bytes(b'x')
        reads = []

        def observed(path, *args, **kwargs):
            if path == transient:
                reads.append(path)
                raise FileNotFoundError(errno.ENOENT, 'persistent churn')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed), self.assertRaises(Failure) as caught:
            Budget({'roots': [self.root]}).used()
        self.assertEqual(caught.exception.code, 'MEASUREMENT')
        self.assertEqual(len(reads), 3)

    def test_inventory_root_disappearance_is_not_retried(self):
        root = self.root / 'inventory'
        root.mkdir()
        reads = []

        def observed(path, *args, **kwargs):
            if path == root:
                reads.append(path)
                root.rmdir()
                raise FileNotFoundError(errno.ENOENT, 'root removed')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed), self.assertRaises(Failure) as caught:
            allocated_bytes([root])
        self.assertEqual(caught.exception.code, 'MEASUREMENT')
        self.assertEqual(len(reads), 1)

    def test_permission_and_io_errors_fail_without_retry(self):
        target = self.root / 'file'
        target.write_bytes(b'x')
        for error in (PermissionError(errno.EACCES, 'denied'), OSError(errno.EIO, 'I/O error')):
            reads = []

            def observed(path, *args, **kwargs):
                if path == target:
                    reads.append(path)
                    raise error
                return self.original_lstat(path, *args, **kwargs)

            with self.subTest(error=error.errno), patch.object(Path, 'lstat', observed):
                with self.assertRaises(OSError) as caught:
                    allocated_bytes([self.root])
                self.assertEqual(caught.exception.errno, error.errno)
                self.assertEqual(len(reads), 1)

    def test_generator_inventory_is_preserved_across_restart(self):
        first, second = (self.root / name for name in ('first', 'second'))
        first.mkdir()
        second.mkdir()
        target = first / 'checkpoint.meta'
        target.write_bytes(b'x')
        payload = second / 'payload'
        payload.write_bytes(b'y' * 4096)
        disappeared = False

        def observed(path, *args, **kwargs):
            nonlocal disappeared
            if path == target and not disappeared:
                disappeared = True
                raise FileNotFoundError(errno.ENOENT, 'transient entry lookup')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed):
            measured = allocated_bytes(root for root in (first, second))
        self.assertEqual(measured, self.expected(first, second, target, payload))


if __name__ == '__main__':
    unittest.main()
