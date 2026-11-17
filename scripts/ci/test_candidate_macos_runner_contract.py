"""Keep candidate workflow routing aligned with runner admission."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_REF = "refs/heads/integration/v0.8.17-candidate"


class CandidateMacosRunnerContractTests(unittest.TestCase):
    def test_workflows_and_runner_admit_same_candidate(self):
        for path, count in (
            (".github/workflows/ci.yml", 4),
            (".github/workflows/alopex-py.yml", 1),
            ("scripts/ci/prepare-temporary-macos-runner.sh", 1),
        ):
            with self.subTest(path=path):
                source = (ROOT / path).read_text()
                self.assertEqual(source.count(CANDIDATE_REF), count)
                self.assertNotIn("refs/heads/integration/v0.8.16-candidate", source)


if __name__ == "__main__":
    unittest.main()
