"""Preflight the frozen PR 819 diagnostic plan before remote measurement."""

import unittest

import hosted_native_begin as hosted
import native_begin_matrix as matrix
import native_begin_supervisor as supervisor


class TargetedControllerTests(unittest.TestCase):
    def test_source_and_schema_are_fixed(self):
        self.assertEqual(hosted.BEFORE_COMMIT,
                         "ce88bbfadd8030ff5cd67262a72f915e50e5b8da")
        self.assertEqual(hosted.BEFORE_TREE,
                         "8cc4513c4654c95cfe8c0dd7ea8162f4ce6345a5")
        self.assertEqual(hosted.CANDIDATE_COMMIT,
                         "1ceaf881dfc0e38a80ab88113032bbcbcbc831cc")
        self.assertEqual(hosted.CANDIDATE_TREE,
                         "e996099a852903b1e436167ddd9805cdcf223cdf")

    def test_frozen_plan_and_probe_inventory(self):
        cells = matrix.plan()
        fixtures = ((0, 0), (0, 1), (100, 2), (0, 2))
        self.assertEqual(len(cells), 24)
        self.assertEqual(len({cell["id"] for cell in cells}), 24)
        self.assertEqual({(cell["rows"], cell["indexes"], cell["trial"], cell["subject"])
                          for cell in cells},
                         {(rows, indexes, trial, subject)
                          for rows, indexes in fixtures
                          for trial in range(3)
                          for subject in ("before", "candidate")})
        self.assertEqual([cell["subject"] for cell in cells[:6]],
                         ["before", "candidate", "candidate", "before", "before", "candidate"])
        self.assertEqual(sum(len(supervisor.probes(cell["rows"], cell["indexes"]))
                             for cell in cells), 30)
        self.assertEqual(supervisor.TARGET_PROBES,
                         {(0, 0): (("ro", "noop"), ("rw", "query")),
                          (0, 1): (("ro", "query"),),
                          (100, 2): (("ro", "query"),),
                          (0, 2): (("rw", "query"),)})
        for rows, indexes in ((0, 3), (100, 1), (1000, 2)):
            with self.assertRaises(ValueError):
                supervisor.probes(rows, indexes)


if __name__ == "__main__":
    unittest.main()
