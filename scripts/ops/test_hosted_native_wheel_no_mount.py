import tempfile
import unittest
from pathlib import Path

from hosted_native_wheel_no_mount import parser_request, validate_storage_boundary


class StorageBoundaryTests(unittest.TestCase):
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
