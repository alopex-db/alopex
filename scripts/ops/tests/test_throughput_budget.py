"""Focused checks for private complete-workload checkpoint decisions."""

import unittest

from scripts.ops.throughput_budget import Budget, Stage, evaluate


def plan(**overrides):
    values = dict(
        stages=(Stage("paired-sql", 12, 4, 4, 40.0, 2),
                Stage("audit-cases", 2, 0, 2, 10.0)),
        elapsed_seconds=50.0,
        deadline_seconds=300.0,
        setup_remaining_seconds=10.0,
        audit_seconds=10.0,
        cleanup_seconds=10.0,
        margin_seconds=20.0,
        conservative_fraction=0.5,
        storage_used_bytes=100,
        storage_remaining_peak_bytes=50,
        storage_cap_bytes=1000,
    )
    values.update(overrides)
    return Budget(**values)


class ThroughputBudgetTests(unittest.TestCase):
    def test_complete_remaining_work_fits_with_margin(self):
        result = evaluate(plan())
        self.assertTrue(result.proceed)
        self.assertEqual(result.code, "READY")
        self.assertEqual(result.private_diagnostics["projected_finish_seconds"], 280.0)

    def test_slower_observed_rate_stops_before_next_group(self):
        stages = (Stage("paired-sql", 12, 4, 2, 40.0, 2),
                  Stage("audit-cases", 2, 0, 2, 10.0))
        result = evaluate(plan(stages=stages))
        self.assertFalse(result.proceed)
        self.assertEqual(result.code, "TIME_BUDGET")

    def test_incomplete_pair_fails_closed_even_when_time_fits(self):
        stages = (Stage("paired-sql", 12, 3, 4, 40.0, 2),)
        result = evaluate(plan(stages=stages))
        self.assertEqual(result.code, "INCOMPLETE_GROUP")
        self.assertFalse(result.proceed)

    def test_perf_two_tests_are_one_acceptance_group(self):
        group = Stage("perf-baseline-pair", 2, 1, 2, 20.0, 2)
        self.assertEqual(evaluate(plan(stages=(group,))).code, "INCOMPLETE_GROUP")
        checkpoint = Stage("perf-baseline-pair", 2, 1, 2, 20.0, 2, 1)
        self.assertEqual(evaluate(plan(stages=(checkpoint,))).code, "READY")
        slow = Stage("perf-baseline-pair", 2, 1, 2, 1000.0, 2, 1)
        self.assertEqual(evaluate(plan(stages=(slow,))).code, "TIME_BUDGET")
        completed = Stage("perf-baseline-pair", 2, 2, 2, 20.0, 2)
        self.assertTrue(evaluate(plan(stages=(completed,))).proceed)

    def test_missing_or_zero_rate_fails_closed(self):
        for units, seconds in ((0, 10.0), (2, 0.0), (2, float("nan"))):
            with self.subTest(units=units, seconds=seconds):
                stages = (Stage("paired-sql", 12, 0, units, seconds, 2),)
                self.assertEqual(evaluate(plan(stages=stages)).code, "RATE_MISSING")

    def test_storage_and_deadline_edges_stop(self):
        self.assertEqual(evaluate(plan(storage_remaining_peak_bytes=900)).code,
                         "STORAGE_BUDGET")
        self.assertEqual(evaluate(plan(storage_used_bytes=1100)).code,
                         "STORAGE_BUDGET")
        self.assertEqual(evaluate(plan(deadline_seconds=280.0)).code,
                         "TIME_BUDGET")

    def test_invalid_manifest_or_margin_is_rejected(self):
        self.assertEqual(evaluate(plan(margin_seconds=0.0)).code, "INVALID_PLAN")
        self.assertEqual(evaluate(plan(stages=(Stage("x", 3, 0, 2, 1.0, 2),))).code,
                         "INVALID_PLAN")
        self.assertEqual(evaluate(plan(stages=(Stage("x", 2, 0, 2, 1.0),
                                               Stage("x", 2, 0, 2, 1.0)))).code,
                         "INVALID_PLAN")
        self.assertEqual(evaluate(plan(stages=42)).code, "INVALID_PLAN")
        self.assertEqual(evaluate(plan(deadline_seconds=10**1000)).code,
                         "INVALID_PLAN")
        for checkpoint_size in (0, -1, 3, 4, True):
            with self.subTest(checkpoint_size=checkpoint_size):
                stage = Stage("pair", 2, 0, 2, 10.0, 2, checkpoint_size)
                self.assertEqual(evaluate(plan(stages=(stage,))).code,
                                 "INVALID_PLAN")

    def test_observation_overflow_fails_closed(self):
        huge = Stage("overflow", 2, 0, 10**1000, 1.0)
        self.assertEqual(evaluate(plan(stages=(huge,))).code, "RATE_MISSING")
        tiny = Stage("overflow", 2, 0, 1, 1e-320)
        self.assertEqual(evaluate(plan(stages=(tiny,))).code, "RATE_MISSING")


if __name__ == "__main__":
    unittest.main()
