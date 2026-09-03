import json
import unittest
from pathlib import Path

from scripts.reference_tests.sql_v0811_differential import CORPUS, normalize_rows


class SqlV0811DifferentialTests(unittest.TestCase):
    def test_reference_revisions_and_case_contracts_are_complete(self):
        self.assertEqual(set(CORPUS["references"]), {"sqlite", "postgresql", "duckdb", "datafusion"})
        for reference in CORPUS["references"].values():
            self.assertEqual(len(reference["commit"]), 40)
            self.assertTrue(reference["source"])
            self.assertTrue(reference["license"])
        for case in [*CORPUS["cases"], *CORPUS["error_cases"]]:
            self.assertTrue(case["id"])
            self.assertTrue(case["contracts"])

    def test_normalization_preserves_column_and_row_order(self):
        rows = normalize_rows([{"id": 2, "value": None}, {"id": 1, "value": 1.25}])
        self.assertEqual(rows, [{"id": 2, "value": None}, {"id": 1, "value": 1.25}])

    def test_legacy_json_fixtures_are_honestly_classified(self):
        fixture_dir = (
            Path(__file__).resolve().parents[2]
            / "crates/alopex-sql/tests/fixtures"
        )
        fixtures = sorted(fixture_dir.glob("*_reference.json"))
        self.assertEqual(len(fixtures), 9)
        for path in fixtures:
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                payload["test_provenance"]["relationship"],
                "handwritten-regression",
                path.name,
            )
            self.assertNotIn("verified_with", payload, path.name)


if __name__ == "__main__":
    unittest.main()
