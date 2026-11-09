import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test-env"))
from capacity import Budget, allocated_bytes
from hosted_native_wheel_no_mount import (
    StableAllocatedMeasure, parser_request, run_counted_command, validate_storage_boundary,
)


class StorageBoundaryTests(unittest.TestCase):
    def test_transient_disappearance_retries_and_persistent_failure_stops(self):
        values = iter((FileNotFoundError(), FileNotFoundError(), 500))
        def measure(_):
            value = next(values)
            if isinstance(value, BaseException):
                raise value
            return value
        meter = StableAllocatedMeasure(measure)
        self.assertEqual(meter([Path("/tmp")]), 500)
        self.assertEqual(meter.summary()["transient_retries"], 2)
        self.assertEqual(meter.summary()["samples"], 1)
        with self.assertRaises(FileNotFoundError):
            StableAllocatedMeasure(lambda _: (_ for _ in ()).throw(FileNotFoundError()))(
                [Path("/tmp")])

    @unittest.skipIf(os.name == "nt", "process groups require POSIX signals")
    def test_owned_churning_child_is_paused_for_storage_checks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            meter = StableAllocatedMeasure(allocated_bytes)
            budget = Budget({"roots": [str(root)], "stop_bytes": 40_000_000,
                             "reserve_bytes": 1_000_000, "poll_seconds": 0.03,
                             "planned_bytes": {"wheel": 10_000_000}}, measure=meter)
            budget.before("wheel")
            script = ("from pathlib import Path\nimport time, sys\nroot=Path(sys.argv[1])\n"
                      "for _ in range(50):\n"
                      "    (root/'scratch').write_bytes(b'x'*4096)\n"
                      "    time.sleep(0.01)\n")
            run_counted_command([sys.executable, "-c", script, str(root)], budget=budget,
                                meter=meter, seconds=5, log=root / "child.log",
                                env=dict(os.environ), poll_seconds=0.03)
            self.assertGreater(meter.paused_checks, 0)
            self.assertLess(meter.maximum, 40_000_000)

    def test_hash_pinned_public_parser_asset_url_policy(self):
        request = parser_request(
            "https://api.github.com/repos/alopex-db/alopex/releases/assets/123")
        self.assertEqual(request.get_header("Accept"), "application/octet-stream")
        with self.assertRaises(ValueError):
            parser_request("https://example.com/parser.tar.gz")

    def test_counted_private_scratch_and_negative_boundaries(self):
        planned = {"prepare": 1_000_000_000, "tools": 4_000_000_000,
                   "wheel": 10_000_000_000, "collect": 1_000_000_000}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            counted = root / "counted"
            counted.mkdir()
            scratch = counted / "alopex-native-build"
            validate_storage_boundary(scratch, [counted], planned, 2_000_000_000)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [root / "other"], planned, 2_000_000_000)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [counted], planned, 25_000_000_000)
            scratch.symlink_to(root)
            with self.assertRaises(ValueError):
                validate_storage_boundary(scratch, [counted], planned, 2_000_000_000)


if __name__ == "__main__":
    unittest.main()
