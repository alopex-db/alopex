"""Storage and isolation checks for the server-only no-mount controller."""

import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'test-env'))
from capacity import Budget, allocated_bytes
from hosted_native_assets_no_mount import (
    StableAllocatedMeasure, run_counted_command, validate_storage_boundary,
)


class ServerNoMountTests(unittest.TestCase):
    def test_transient_inventory_error_retries_but_persistent_error_stops(self):
        values = iter((FileNotFoundError(), FileNotFoundError(), 500))
        def measure(_):
            value = next(values)
            if isinstance(value, BaseException):
                raise value
            return value
        meter = StableAllocatedMeasure(measure)
        self.assertEqual(meter([Path('/tmp')]), 500)
        self.assertEqual(meter.summary()['transient_retries'], 2)
        with self.assertRaises(FileNotFoundError):
            StableAllocatedMeasure(lambda _: (_ for _ in ()).throw(FileNotFoundError()))(
                [Path('/tmp')])

    def test_scratch_must_be_counted_and_within_40gb_envelope(self):
        planned = {'prepare': 1_000_000_000, 'tools': 4_000_000_000,
                   'server': 10_000_000_000, 'collect': 1_000_000_000}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            counted = root / 'counted'
            counted.mkdir()
            scratch = counted / 'alopex-native-server-build'
            validate_storage_boundary(scratch, [counted], planned, 2_000_000_000)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [root / 'other'], planned, 2_000_000_000)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [counted], planned, 25_000_000_000)
            scratch.symlink_to(root)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [counted], planned, 2_000_000_000)

    @unittest.skipIf(os.name == 'nt', 'POSIX process group needed')
    def test_owned_child_is_paused_during_storage_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            meter = StableAllocatedMeasure(allocated_bytes)
            budget = Budget({'roots': [str(root)], 'stop_bytes': 40_000_000,
                             'reserve_bytes': 1_000_000, 'poll_seconds': 0.03,
                             'planned_bytes': {'server': 10_000_000}}, measure=meter)
            budget.before('server')
            script = ("from pathlib import Path\nimport time,sys\nroot=Path(sys.argv[1])\n"
                      "for _ in range(50):\n"
                      " (root/'scratch').write_bytes(b'x'*4096)\n"
                      " time.sleep(0.01)\n")
            run_counted_command([sys.executable, '-c', script, str(root)], budget=budget,
                                meter=meter, seconds=5, log=root / 'child.log',
                                env=dict(os.environ), poll_seconds=0.03)
            self.assertGreater(meter.paused_checks, 0)

    def test_controller_has_no_mount_or_privilege_command(self):
        source = (Path(__file__).with_name('hosted_native_assets_no_mount.py')).read_text()
        self.assertNotIn('"sudo"', source)
        self.assertNotIn('"mkfs.ext4"', source)
        self.assertNotIn('"umount"', source)


if __name__ == '__main__':
    unittest.main()
