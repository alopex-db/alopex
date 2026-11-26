"""Focused negative admission checks for parity storage reservation."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.performance.parity_storage_reservation import (
    SCHEMA, STAGES, Failure, reserve,
)


SHA = "a" * 40
RUN_ID = 123
RUN_ATTEMPT = 2


class ParityStorageReservationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.profile_path = self.root / "profile.json"
        self.profile = {
            "schema": SCHEMA,
            "source_commit": SHA,
            "run_id": RUN_ID,
            "run_attempt": RUN_ATTEMPT,
            "suite": "full",
            "paired_invocations": 288,
            "stages": {name: {"input_units": 1, "observed_units": 1,
                              "observed_seconds": 1,
                              "expected_seconds": 1, "conservative_seconds": 2}
                       for name in STAGES},
            "margin_seconds": 60,
            "storage": {
                "roots": [str(self.root)],
                "runtime_storage_roots": [str(self.root)],
                "stop_bytes": 40_000_000_000,
                "reserve_bytes": 1024,
                "planned_run_bytes": 1024,
                "poll_seconds": 1,
            },
        }

    def write_profile(self):
        self.profile_path.write_text(json.dumps(self.profile), encoding="utf-8")
        self.profile_path.chmod(0o600)

    def reserve(self):
        return reserve(self.profile_path, source_sha=SHA, git_head=SHA,
                       run_id=RUN_ID, run_attempt=RUN_ATTEMPT,
                       selected_suite="full")

    def test_exact_source_and_complete_budget_reserve_storage(self):
        self.write_profile()
        receipt = self.reserve()
        self.assertEqual(receipt["paired_invocations"], 288)
        self.assertEqual((receipt["run_id"], receipt["run_attempt"]),
                         (RUN_ID, RUN_ATTEMPT))
        self.assertGreaterEqual(receipt["allocated_bytes"], 0)

    def test_other_run_or_retry_cannot_reuse_profile(self):
        self.write_profile()
        with self.assertRaises(Failure) as other_run:
            reserve(self.profile_path, source_sha=SHA, git_head=SHA,
                    run_id=RUN_ID + 1, run_attempt=RUN_ATTEMPT,
                    selected_suite="full")
        self.assertEqual(other_run.exception.code, "IDENTITY")
        with self.assertRaises(Failure) as retry:
            reserve(self.profile_path, source_sha=SHA, git_head=SHA,
                    run_id=RUN_ID, run_attempt=RUN_ATTEMPT + 1,
                    selected_suite="full")
        self.assertEqual(retry.exception.code, "IDENTITY")

    def test_profile_suite_must_match_selected_workflow_suite(self):
        self.write_profile()
        with self.assertRaises(Failure) as mismatch:
            reserve(self.profile_path, source_sha=SHA, git_head=SHA,
                    run_id=RUN_ID, run_attempt=RUN_ATTEMPT,
                    selected_suite="curated")
        self.assertEqual(mismatch.exception.code, "INPUT")

    def test_stale_source_or_missing_workload_stops_before_measurement(self):
        self.write_profile()
        with self.assertRaises(Failure) as stale:
            reserve(self.profile_path, source_sha="b" * 40, git_head="b" * 40,
                    run_id=RUN_ID, run_attempt=RUN_ATTEMPT,
                    selected_suite="full")
        self.assertEqual(stale.exception.code, "IDENTITY")
        del self.profile["stages"]["paired"]
        self.write_profile()
        with self.assertRaises(Failure) as incomplete:
            self.reserve()
        self.assertEqual(incomplete.exception.code, "INPUT")

    def test_overlapping_roots_and_overlong_budget_fail_closed(self):
        child = self.root / "child"
        child.mkdir()
        self.profile["storage"]["roots"].append(str(child))
        self.write_profile()
        with self.assertRaises(Failure) as overlap:
            self.reserve()
        self.assertEqual(overlap.exception.code, "MEASUREMENT")
        self.profile["storage"]["roots"].pop()
        self.profile["stages"]["paired"]["conservative_seconds"] = 21600
        self.write_profile()
        with self.assertRaises(Failure) as overlong:
            self.reserve()
        self.assertEqual(overlong.exception.code, "CAPACITY")

    def test_world_readable_profile_and_unreserved_growth_fail_closed(self):
        self.write_profile()
        self.profile_path.chmod(0o644)
        with self.assertRaises(Failure) as exposed:
            self.reserve()
        self.assertEqual(exposed.exception.code, "INPUT")
        self.profile["storage"]["planned_run_bytes"] = 40_000_000_000
        self.write_profile()
        with self.assertRaises(Failure) as unreserved:
            self.reserve()
        self.assertEqual(unreserved.exception.code, "CAPACITY")

    def test_expected_time_must_cover_observed_pilot_rate(self):
        paired = self.profile["stages"]["paired"]
        paired.update(input_units=288, observed_units=24, observed_seconds=60,
                      expected_seconds=719, conservative_seconds=720)
        self.write_profile()
        with self.assertRaises(Failure) as underestimated:
            self.reserve()
        self.assertEqual(underestimated.exception.code, "CAPACITY")
        paired["expected_seconds"] = 720
        self.write_profile()
        receipt = self.reserve()
        self.assertGreaterEqual(receipt["conservative_seconds"], 720)


if __name__ == "__main__":
    unittest.main()
