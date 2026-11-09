"""Check the fixed ten-trial paired assessment without a product build."""

import unittest

from hnsw_search_contract import EF_VALUES, SUBJECTS
from hnsw_search_stability import assess


def matrix(candidate_scale=0.95):
    return [dict(subject=subject, trial=trial, ef_search=ef,
            query_latency_p50_us=(100 + trial * 10) *
                (candidate_scale if subject == 'candidate' else 1),
            recall_at_10=0.997)
        for trial in range(10) for subject in SUBJECTS for ef in EF_VALUES]


class StabilityTests(unittest.TestCase):
    def test_balanced_drift_does_not_hide_paired_improvement(self):
        result = assess(matrix())
        self.assertTrue(result['all_paired_bounds_within_limit'])
        self.assertEqual(len(result['comparisons']), 10)
        self.assertEqual(result['nominal_upper_bound_coverage'], 1013 / 1024)

    def test_one_outlier_is_reported_but_two_reject_support(self):
        rows = matrix()
        for trial in (0, 1):
            row = next(r for r in rows if r['subject'] == 'candidate'
                and r['trial'] == trial and r['ef_search'] == 16)
            row['query_latency_p50_us'] *= 1.2
            result = assess(rows)
            self.assertEqual(result['comparisons'][0]['paired_bound_within_limit'],
                             trial == 0)
        self.assertFalse(result['all_paired_bounds_within_limit'])

    def test_recall_and_missing_point_reject(self):
        rows = matrix()
        next(r for r in rows if r['subject'] == 'candidate')['recall_at_10'] = 0.98
        self.assertFalse(assess(rows)['all_paired_bounds_within_limit'])
        with self.assertRaises(RuntimeError):
            assess(rows[:-1])


if __name__ == '__main__':
    unittest.main()
