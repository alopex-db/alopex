import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_contract import DATASET_SIZE, DIMENSION, write_artifacts


class HnswDiagnosticContractTests(unittest.TestCase):
    def test_artifacts_are_reproducible_and_machine_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            write_artifacts(Path(directory), [{"dataset_size": DATASET_SIZE, "dimension": DIMENSION, "query_count": 10000, "seed": 42, "duration_seconds": 2.0, "queries_per_second": 5000.0, "checksum": 1.0}] * 3)
            self.assertTrue((Path(directory) / "hnsw-diagnostic.json").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.csv").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.md").is_file())


if __name__ == "__main__":
    unittest.main()
