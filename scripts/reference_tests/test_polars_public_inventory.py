import copy
import json
import unittest
from pathlib import Path

from scripts.reference_tests.polars_public_inventory import (
    EXPECTED_MATERIALIZED_ROWS,
    materialize,
)


LEDGER = Path(__file__).parents[2] / "docs/parity/polars-v1.43.2.json"


class PolarsPublicInventoryTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(LEDGER.read_text(encoding="utf-8"))

    def test_materializes_exact_public_inventory(self):
        self.assertEqual(len(materialize(self.payload)), EXPECTED_MATERIALIZED_ROWS)

    def test_rejects_missing_unknown_and_duplicate_claims(self):
        missing = copy.deepcopy(self.payload)
        missing["claims"] = [
            claim for claim in missing["claims"] if claim["api"] != "DataFrame.height"
        ]
        with self.assertRaisesRegex(RuntimeError, "unmapped Alopex/Polars overlaps"):
            materialize(missing)

        unknown = copy.deepcopy(self.payload)
        claim = copy.deepcopy(unknown["claims"][0])
        claim["api"] = "DataFrame.not_a_polars_api"
        unknown["claims"].append(claim)
        with self.assertRaisesRegex(RuntimeError, "unknown Polars claims"):
            materialize(unknown)

        duplicate = copy.deepcopy(self.payload)
        duplicate["claims"].append(copy.deepcopy(duplicate["claims"][0]))
        with self.assertRaisesRegex(RuntimeError, "duplicate claim"):
            materialize(duplicate)

    def test_rejects_missing_performance_evidence(self):
        broken = copy.deepcopy(self.payload)
        claim = next(
            claim for claim in broken["claims"] if claim["api"] == "DataFrame.height"
        )
        claim.pop("performance_evidence")
        with self.assertRaisesRegex(RuntimeError, "missing performance evidence"):
            materialize(broken)


if __name__ == "__main__":
    unittest.main()
