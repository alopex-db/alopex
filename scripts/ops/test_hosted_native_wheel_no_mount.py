import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from hosted_native_wheel_no_mount import HostUsageMeter, parser_request, validate_storage_boundary


class StorageBoundaryTests(unittest.TestCase):
    def test_counter_tracks_peak_and_rejects_changed_volume(self):
        spaces = iter((SimpleNamespace(total=1000, free=1000),
                       SimpleNamespace(total=1000, free=900),
                       SimpleNamespace(total=1000, free=950),
                       SimpleNamespace(total=999, free=850)))
        meter = HostUsageMeter([Path("/tmp")], allocated=lambda _: 100,
                               usage=lambda _: next(spaces), device=lambda _: 1)
        self.assertEqual(meter.used([Path("/tmp")]), 200)
        self.assertEqual(meter.used([Path("/tmp")]), 200)
        self.assertEqual(meter.summary()["observed_peak_additional_bytes"], 100)
        with self.assertRaises(ValueError):
            meter.used([Path("/tmp")])

    def test_counter_sums_distinct_filesystems_and_rejects_root_change(self):
        roots = [Path("/first"), Path("/second")]
        spaces = {roots[0]: iter((1000, 900)), roots[1]: iter((1000, 850))}
        meter = HostUsageMeter(roots, allocated=lambda _: 200,
                               usage=lambda root: SimpleNamespace(total=1000, free=next(spaces[root])),
                               device=lambda root: roots.index(root))
        self.assertEqual(meter.used(roots), 450)
        with self.assertRaises(ValueError):
            meter.used(roots[::-1])

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
