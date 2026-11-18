"""Offline boundary tests for the unimplemented owned container handoff."""
import tempfile
from pathlib import Path
import unittest

from . import stage_pair
from .test_qualification import fixture, JOB


class StagePairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "pair"
        self.output.mkdir(mode=0o700)
        for name in ("on", "off"):
            (self.output / name).mkdir(mode=0o700)
        _, self.proof, self.receipt = fixture()
        self.config = {
            "schema": "alopex.sql-four-case-pair/v1", "engine": "alopex-server",
            "case": "select-all", "trial": 0, "modes": ["on", "off"],
            "source": {"commit": self.receipt["source_commit"],
                       "tree": self.receipt["source_tree"], "profile": "release"},
            "assets_digest": self.receipt["assets_digest"],
            "same_job_id": JOB, "fixture_sha256": "a" * 64}

    def test_valid_binding_still_cannot_start_unowned_server(self):
        stage_pair.validate_stage_binding(
            self.config, self.output, self.proof, self.receipt)
        with self.assertRaisesRegex(ValueError, "^PAIR_STAGE$"):
            stage_pair.stage_pair(self.config, self.output, self.proof, self.receipt)

    def test_rejects_source_and_job_drift(self):
        self.config["source"]["commit"] = "b" * 40
        with self.assertRaisesRegex(ValueError, "^PAIR_STAGE$"):
            stage_pair.validate_stage_binding(
                self.config, self.output, self.proof, self.receipt)
        self.config["source"]["commit"] = self.receipt["source_commit"]
        self.receipt["job_id"] = "999"
        with self.assertRaisesRegex(ValueError, "^PAIR_STAGE$"):
            stage_pair.validate_stage_binding(
                self.config, self.output, self.proof, self.receipt)

    def test_rejects_policy_and_output_drift(self):
        self.proof["profile"]["security"]["lsm_profile"] = "unconfined"
        with self.assertRaisesRegex(ValueError, "^PAIR_STAGE$"):
            stage_pair.validate_stage_binding(
                self.config, self.output, self.proof, self.receipt)
        self.proof["profile"]["security"]["lsm_profile"] = "docker-default"
        self.output.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "^PAIR_STAGE$"):
            stage_pair.validate_stage_binding(
                self.config, self.output, self.proof, self.receipt)


if __name__ == "__main__":
    unittest.main()
