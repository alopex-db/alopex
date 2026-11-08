import copy
import json
import unittest

from hnsw_search_phase_audit import audit_pairs, compare_subjects
from test_hnsw_search_phase_capture import record


def fixture():
    rows = []
    for ef in (16, 32, 64, 128, 256):
        for query in range(4):
            layer = record()
            layer['ef'] = min(ef, 32)
            observation = dict(results=[dict(key_hex=str(i).encode().hex(),
                score_f32_hex='00000000', metadata_hex=None) for i in range(10)],
                stats=dict(nodes_visited=7, distance_computations=8, search_time_us=1),
                duration=dict(wall_ns=20, cpu_ns=15), layers=[])
            on = copy.deepcopy(observation)
            on['layers'] = [layer]
            rows.append(dict(ef_search=ef, query_index=query,
                query_sha256=f'{query:064x}', observations=dict(off=observation, on=on)))
    return rows


def audit(rows):
    return audit_pairs([json.dumps(row) for row in rows], mode='preflight')


class PhaseAuditTests(unittest.TestCase):
    def test_complete_fixed_matrix_reconstructs_phase_and_unclassified_counts(self):
        pairs, summary = audit(fixture())
        self.assertEqual(len(pairs), 20)
        self.assertEqual(len(summary), 5)
        self.assertEqual(summary[0]['layer_total_ns'], 40)
        self.assertEqual(summary[0]['layer_residual_ns'], 12)
        self.assertEqual(summary[0]['unclassified_traversal'],
            dict(nodes_visited=8, distance_computations=16))
        self.assertTrue(compare_subjects(pairs, pairs)['results_and_traversal_identical'])

    def test_missing_duplicate_wrong_query_and_changed_results_are_rejected(self):
        for change in ('missing', 'duplicate', 'query', 'score', 'phase', 'nan', 'traversal'):
            rows = fixture()
            if change == 'missing': rows.pop()
            elif change == 'duplicate': rows[-1] = rows[0]
            elif change == 'query': rows[-1]['query_sha256'] = 'a' * 64
            elif change == 'score': rows[0]['observations']['on']['results'][0]['score_f32_hex'] = '80000000'
            elif change == 'phase': rows[0]['observations']['on']['layers'][0]['total_ns'] += 1
            elif change == 'nan':
                for obs in rows[0]['observations'].values(): obs['results'][0]['score_f32_hex'] = '7fc00000'
            else: rows[0]['observations']['on']['stats']['nodes_visited'] += 1
            with self.subTest(change=change), self.assertRaises(ValueError): audit(rows)

    def test_between_subject_changes_are_not_hidden_by_per_subject_agreement(self):
        before, _ = audit(fixture())
        for change in ('result', 'traversal', 'query'):
            candidate = copy.deepcopy(before)
            row = candidate[16, 0]
            if change == 'query': row['query_sha256'] = 'a' * 64
            elif change == 'result': row['observations']['off']['results'].reverse()
            else: row['observations']['off']['stats']['distance_computations'] += 1
            with self.subTest(change=change), self.assertRaises(ValueError):
                compare_subjects(before, candidate)


if __name__ == '__main__': unittest.main()
