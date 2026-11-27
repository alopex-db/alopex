"""Selected hosted build budget must fail closed without exposing host data."""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_build_guard as guard


class HostStorageGuardTest(unittest.TestCase):
    def test_complete_scan_admits_then_checks_growth(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            device = root.stat().st_dev
            samples = iter(((20_000_000_000, {device: 20_000_000_000}),
                            (27_000_000_000, {device: 12_000_000_000}),
                            (28_000_000_000, {device: 11_000_000_000})))
            check = guard.HostStorageGuard(root, scanner=lambda: next(samples))
            check()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_CAPACITY$"):
                check()

    def test_baseline_reserve_and_missing_device_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            device = root.stat().st_dev
            with self.assertRaisesRegex(ValueError, "^PUBLIC_CAPACITY$"):
                guard.HostStorageGuard(root, scanner=lambda: (
                    30_000_000_000, {device: 20_000_000_000}))
            samples = iter(((20_000_000_000, {device: 20_000_000_000}),
                            (20_000_000_000, {device + 1: 20_000_000_000})))
            check = guard.HostStorageGuard(root, scanner=lambda: next(samples))
            with self.assertRaisesRegex(ValueError, "^PUBLIC_CAPACITY$"):
                check()

    def test_scan_exception_is_fixed_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            device = root.stat().st_dev
            calls = 0

            def scanner():
                nonlocal calls
                calls += 1
                if calls == 1:
                    return 20_000_000_000, {device: 20_000_000_000}
                raise OSError("private mount path")

            check = guard.HostStorageGuard(root, scanner=scanner)
            with self.assertRaisesRegex(ValueError, "^PUBLIC_CAPACITY$"):
                check()

    def test_unmounted_secondary_device_cannot_reduce_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            device = root.stat().st_dev
            samples = iter(((20_000_000_000,
                             {device: 20_000_000_000, device + 1: 20_000_000_000}),
                            (19_000_000_000, {device: 20_000_000_000})))
            check = guard.HostStorageGuard(root, scanner=lambda: next(samples))
            with self.assertRaisesRegex(ValueError, "^PUBLIC_CAPACITY$"):
                check()

    def test_min_interval_throttles_frequent_scans_and_force_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            device = root.stat().st_dev
            calls = 0

            def scanner():
                nonlocal calls
                calls += 1
                return 20_000_000_000, {device: 20_000_000_000}

            check = guard.HostStorageGuard(
                root, scanner=scanner, min_interval_seconds=60.0)
            self.assertEqual(calls, 1)
            for _ in range(50):
                check()
            self.assertEqual(calls, 1)
            check(force=True)
            self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
