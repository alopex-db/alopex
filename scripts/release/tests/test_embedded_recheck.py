import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

DIRECTORY = Path(__file__).resolve().parents[1] / "verify-release"
sys.path.insert(0, str(DIRECTORY))
SPEC = importlib.util.spec_from_file_location(
    "recheck_embedded", DIRECTORY / "recheck_embedded.py"
)
RECHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECHECK)


class RecheckTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "schema": RECHECK.report.SCHEMA,
            "version": "0.8.16",
            "suite": "scenarios",
            "started_at": "start",
            "completed_at": "end",
            "outcome": "failure",
            "failure_stage": "v0.8.16 Embedded API 全シナリオ",
            "identity": {
                "run_id": "123",
                "run_attempt": 1,
                "commit": "d" * 40,
                "tag": "alopex-py-v0.8.16",
                "run_url": "https://github.com/alopex-db/alopex/actions/runs/123",
                "responsibility": "test",
            },
            "verifier_commit": "b" * 40,
            "steps": [
                {
                    "name": f"scenario-{i}",
                    "status": "success",
                    "duration_ms": i,
                    "log_excerpt": [str(i)],
                }
                for i in range(16)
            ]
            + [
                {
                    "name": "v0.8.16 Embedded API 全シナリオ",
                    "status": "failure",
                    "log_excerpt": ["failed golden assertion"],
                }
            ],
        }

    def validate(self, payload=None, digest=None):
        payload = payload or self.payload
        data = json.dumps(payload).encode()
        return RECHECK.validate_baseline(
            payload, data, digest or hashlib.sha256(data).hexdigest(), "123", 1
        )

    def test_accepts_exactly_one_failed_embedded_scenario(self):
        self.assertEqual(self.validate(), "v0.8.16 Embedded API 全シナリオ")

    def test_rejects_tampered_baseline_digest(self):
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.validate(digest="0" * 64)

    def test_rejects_other_failed_or_incomplete_scenarios(self):
        payload = copy.deepcopy(self.payload)
        payload["steps"][0]["status"] = "failure"
        payload["failure_stage"] = "scenario-0"
        with self.assertRaisesRegex(ValueError, "only the Embedded"):
            self.validate(payload)

    def test_rejects_wrong_producing_attempt(self):
        payload = copy.deepcopy(self.payload)
        payload["identity"]["run_attempt"] = 2
        with self.assertRaisesRegex(ValueError, "run/attempt"):
            self.validate(payload)

    def test_composition_preserves_passing_values_and_records_both_origins(self):
        before = copy.deepcopy(self.payload)
        context = {
            "run_id": "456",
            "run_attempt": 1,
            "run_url": "https://github.com/alopex-db/alopex/actions/runs/456",
            "verifier_commit": "c" * 40,
        }
        result = RECHECK.compose(self.payload, "f" * 64, context)
        self.assertEqual(self.payload, before)
        self.assertEqual(result["identity"]["run_id"], "456")
        self.assertEqual(result["resumed_from"]["run_id"], "123")
        self.assertEqual(result["resumed_from"]["sha256"], "f" * 64)
        self.assertEqual(result["outcome"], "incomplete")
        for prior, retained in zip(before["steps"][:16], result["steps"][:16]):
            evidence = retained.pop("evidence")
            self.assertEqual(evidence["run_id"], "123")
            self.assertEqual(retained, prior)

    def test_repeated_recheck_preserves_original_passing_origin(self):
        payload = copy.deepcopy(self.payload)
        payload["steps"][0]["evidence"] = {
            "run_id": "100",
            "run_attempt": 1,
            "run_url": "https://github.com/alopex-db/alopex/actions/runs/100",
            "verifier_commit": "a" * 40,
        }
        context = {
            "run_id": "456",
            "run_attempt": 1,
            "run_url": "https://github.com/alopex-db/alopex/actions/runs/456",
            "verifier_commit": "c" * 40,
        }
        result = RECHECK.compose(payload, "f" * 64, context)
        self.assertEqual(result["steps"][0]["evidence"]["run_id"], "100")
        self.assertEqual(result["steps"][-1]["evidence"]["run_id"], "456")
        self.assertEqual(result["steps"][-1]["status"], "incomplete")
        self.assertEqual(result["steps"][-1]["log_excerpt"], [])
        self.assertIsNone(result["steps"][-1]["duration_ms"])

    def test_rejects_changed_passing_scenario_or_product_inputs(self):
        for path in [
            "scripts/demo/v08/demo_sql_v08.py",
            "scripts/release/verify_python_vector_api.py",
            "crates/alopex-core/src/lib.rs",
        ]:
            with (
                self.subTest(path=path),
                patch.object(RECHECK, "git", return_value=path),
            ):
                with self.assertRaisesRegex(
                    ValueError, "inputs or product source changed"
                ):
                    RECHECK.validate_input_changes("old", "new")

    def test_only_description_count_may_change_in_shared_runner(self):
        with patch.object(
            RECHECK,
            "git",
            side_effect=[
                "scripts/release/verify-release/run.sh",
                "10シナリオ",
                "11シナリオ",
            ],
        ):
            RECHECK.validate_input_changes("old", "new")
        with patch.object(
            RECHECK,
            "git",
            side_effect=[
                "scripts/release/verify-release/run.sh",
                "command old",
                "command new",
            ],
        ):
            with self.assertRaisesRegex(ValueError, "commands changed"):
                RECHECK.validate_input_changes("old", "new")

    def test_focused_and_full_publishers_share_data_only_transport(self):
        workflows = DIRECTORY.parents[2] / ".github/workflows"
        for name in [
            "public-release-verification.yml",
            "public-release-embedded-recheck.yml",
        ]:
            text = (workflows / name).read_text()
            self.assertIn("uses: ./.github/workflows/publish-public-report.yml", text)
            self.assertIn(
                "artifact_name: ${{ needs.verify.outputs.report_artifact }}", text
            )
        publisher = (workflows / "publish-public-report.yml").read_text()
        self.assertIn("ref: main", publisher)
        self.assertIn('origin["head_sha"] == payload["verifier_commit"]', publisher)
        self.assertNotIn("DOCS_REPO_TOKEN", publisher)


if __name__ == "__main__":
    unittest.main()
