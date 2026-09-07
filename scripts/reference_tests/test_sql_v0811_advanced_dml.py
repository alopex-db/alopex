import unittest

from scripts.reference_tests.sql_v0811_advanced_dml import CORPUS


class SqlV0811AdvancedDmlTests(unittest.TestCase):
    def test_fixed_corpus_covers_each_v0811_advanced_dml_contract(self):
        self.assertEqual(set(CORPUS["references"]), {"postgresql", "duckdb"})
        self.assertEqual(
            {case["id"] for case in CORPUS["cases"]},
            {"returning", "on-conflict", "update-from", "delete-using", "merge"},
        )
        for reference in CORPUS["references"].values():
            self.assertEqual(len(reference["commit"]), 40)
        for case in CORPUS["cases"]:
            self.assertTrue(case["setup"])
            self.assertTrue(case["statement"])
            self.assertTrue(case["verify"])


if __name__ == "__main__":
    unittest.main()
