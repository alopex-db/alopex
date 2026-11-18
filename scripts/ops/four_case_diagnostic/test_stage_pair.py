"""Offline source-pinned staging checks; no container is started."""
import hashlib
import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock

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

    def test_extra_bundle_requires_exact_named_sources(self):
        root = Path(self.temp.name).resolve()
        repo, runner_temp = root / "repo", root / "runner-temp"
        repo.mkdir()
        runner_temp.mkdir()
        package_root = repo / "scripts/ops"
        docs_root, product_root = runner_temp / "alopex-712-docs", repo
        docs_root.mkdir(mode=0o700)
        package = {}
        for name in ("__init__", "adapter", "log_parser", "operation", "oracle",
                     "runner", "worker", "supervisor", "container_pair_entrypoint"):
            path = package_root / "four_case_diagnostic" / f"{name}.py"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"# {name}\n")
            package[f"four_case_diagnostic/{name}.py"] = hashlib.sha256(
                path.read_bytes()).hexdigest()
        for harness in (docs_root, product_root):
            path = harness / "scripts/performance/subject_identity.py"
            path.parent.mkdir(parents=True)
            path.write_text("# pinned harness\n")
        harness_digest = hashlib.sha256(
            (docs_root / "scripts/performance/subject_identity.py").read_bytes()).hexdigest()
        manifest = {"package_files": package,
                    "docs_files": {"subject_identity.py": harness_digest},
                    "product_files": {"subject_identity.py": harness_digest}}
        bundle = root / "bundle"
        files = stage_pair._extra_bundle(
            manifest, self.config, self.proof, {"platform": "linux/amd64"},
            bundle, Mock(), repo=repo, runner_temp=runner_temp)
        self.assertEqual(len(files), 11)
        self.assertEqual(json.loads((bundle / "stage.json").read_text())["files"], files)
        (package_root / "four_case_diagnostic/worker.py").write_text("# changed\n")
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            stage_pair._extra_bundle(
                manifest, self.config, self.proof, {"platform": "linux/amd64"},
                root / "changed", Mock(), repo=repo, runner_temp=runner_temp)
        manifest["docs_files"] = {"../escape.py": harness_digest}
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            stage_pair._extra_bundle(
                manifest, self.config, self.proof, {"platform": "linux/amd64"},
                root / "escape", Mock(), repo=repo, runner_temp=runner_temp)
        manifest["docs_files"] = {"subject_identity.py": harness_digest}
        docs_root.rename(runner_temp / "docs-unqualified")
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            stage_pair._extra_bundle(
                manifest, self.config, self.proof, {"platform": "linux/amd64"},
                root / "missing-docs", Mock(), repo=repo, runner_temp=runner_temp)
        (runner_temp / "docs-unqualified").rename(docs_root)
        docs_root.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            stage_pair._extra_bundle(
                manifest, self.config, self.proof, {"platform": "linux/amd64"},
                root / "open-docs", Mock(), repo=repo, runner_temp=runner_temp)


if __name__ == "__main__":
    unittest.main()
