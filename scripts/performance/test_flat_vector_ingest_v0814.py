"""Unit checks for the #448 performance acceptance boundary."""

import unittest

from scripts.performance.flat_vector_ingest_v0814 import (
    VECTOR_COUNT,
    WINDOW_SIZE,
    median_growth,
)


class FlatVectorIngestContractTests(unittest.TestCase):
    def test_median_growth_uses_complete_window_sequences(self):
        rows = [
            [
                {"us_per_vector": 1.0},
                *({"us_per_vector": 1.5} for _ in range(VECTOR_COUNT // WINDOW_SIZE - 2)),
                {"us_per_vector": 2.0},
            ]
            for _ in range(3)
        ]

        self.assertEqual(median_growth(rows, "us_per_vector"), 2.0)

    def test_median_growth_rejects_missing_windows(self):
        with self.assertRaisesRegex(ValueError, "complete window sequence"):
            median_growth([[{"us_per_vector": 1.0}]], "us_per_vector")


if __name__ == "__main__":
    unittest.main()
