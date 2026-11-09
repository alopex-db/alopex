"""Preflight the six-cell private self-smaps diagnostic source."""

import base64
import hashlib
import unittest
import zlib

import hosted_native_begin as hosted
import native_begin_matrix as matrix
import native_begin_probe as worker
import native_begin_supervisor as supervisor


class MappingControllerTests(unittest.TestCase):
    def test_source_and_frozen_plan(self):
        self.assertEqual(hosted.CANDIDATE_COMMIT,
                         "1ceaf881dfc0e38a80ab88113032bbcbcbc831cc")
        self.assertEqual(hosted.CANDIDATE_TREE,
                         "e996099a852903b1e436167ddd9805cdcf223cdf")
        cells = matrix.plan()
        self.assertEqual(len(cells), 6)
        self.assertEqual(len({cell["id"] for cell in cells}), 6)
        self.assertEqual([(cell["trial"], cell["subject"]) for cell in cells],
                         [(0, "before"), (0, "candidate"), (1, "candidate"),
                          (1, "before"), (2, "before"), (2, "candidate")])
        self.assertTrue(all((cell["rows"], cell["indexes"]) == (0, 2) for cell in cells))
        self.assertEqual(supervisor.TARGET_PROBES, {(0, 2): (("rw", "query"),)})
        self.assertEqual(supervisor.probes(0, 2),
                         [{"mode": "rw", "operation": "query", "samples": 1, "repeat": 0}])
        for fixture in ((0, 0), (0, 1), (100, 2)):
            with self.assertRaises(ValueError):
                supervisor.probes(*fixture)

    def test_smaps_encoding_bounds_and_integrity(self):
        raw = b"maps and permission evidence\n"
        encoded = worker.encode_smaps(raw)
        self.assertEqual(encoded["smaps_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(zlib.decompress(base64.b64decode(encoded["smaps_zlib_base64"])), raw)
        for invalid in (b"", b"a" * 2_000_001, b"\xff"):
            with self.assertRaises((RuntimeError, UnicodeDecodeError)):
                worker.encode_smaps(invalid)


if __name__ == "__main__":
    unittest.main()
