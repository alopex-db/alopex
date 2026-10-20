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

    def test_rejects_changed_passing_scenario_or_product_inputs(self):
        for path in [
            "scripts/demo/v08/demo_sql_v08.py",
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

    def test_focused_publisher_matches_primary_publication_contract(self):
        workflows = DIRECTORY.parents[2] / ".github/workflows"
        primary = (
            (workflows / "public-release-verification.yml")
            .read_text()
            .split("  publish:\n", 1)[1]
            .split("  publish-vector-benchmark:\n", 1)[0]
        )
        focused = (
            (workflows / "public-release-embedded-recheck.yml")
            .read_text()
            .split("  publish:\n", 1)[1]
        )
        primary = primary.replace(" && inputs.baseline_run_id == ''", "")
        self.assertEqual(primary, focused)


if __name__ == "__main__":
    unittest.main()
