import copy
import unittest

from csv_phase_contract import EVENTS, SUBJECTS, plan, verify_cell
from test_dataframe_phase_contract import observation


def fixture():
    cell = plan('preflight')[0]
    raw = []
    for index, event in enumerate(EVENTS):
        row = dict(event=event, iteration=0, phase='measure', total_ns=2000, resources=observation(index))
        if event == 'sample':
            row.update(first_ns=1000, remaining_ns=1000, rows=3, first_chunk_rows=2,
                outside_original_total_ns=100, cpu_including_close_seconds=.001,
                input_blocks_including_close=0, output_blocks_including_close=0)
        raw.append(row)
    result = dict(rows=3, engine='alopex', warmups=0, samples=1, input_sha256='a'*64,
        result_sha256='b'*64, validation_before='b'*64, validation_after='b'*64, resource_observations=3)
    observations = dict(pending=None, completed=[r['resources'] for r in raw])
    return cell, result, raw, observations


class ContractTests(unittest.TestCase):
    def test_matrix_retains_both_sizes_all_subjects_and_three_trials(self):
        cells = plan('full')
        self.assertEqual(len(cells), 24)
        self.assertEqual(len({c['id'] for c in cells}), 24)
        self.assertEqual({(c['trial'], c['rows'], c['subject']) for c in cells},
            {(t, n, s) for t in range(3) for n in (3, 34250) for s in SUBJECTS})
        self.assertEqual({(c['warmups'], c['samples']) for c in cells}, {(3, 21)})
        self.assertEqual(sum(3*(c['warmups']+c['samples']) for c in cells), 1728)
        self.assertEqual(len(plan('preflight')), 4)
        with self.assertRaises(RuntimeError): plan('other')

    def test_complete_cell(self):
        self.assertEqual(verify_cell(*fixture(), 100)['observations'], 3)

    def test_missing_coverage_bad_order_values_and_resource_mutations_fail(self):
        for fault in ('missing', 'pending', 'order', 'rows', 'first', 'remaining', 'clock', 'oracle', 'usage', 'resource'):
            cell, result, raw, obs = copy.deepcopy(fixture())
            if fault == 'missing': raw.pop()
            if fault == 'pending': obs['pending'] = {'index': 3}
            if fault == 'order': raw[0]['event'] = 'sample'
            if fault == 'rows': raw[2]['rows'] = 2
            if fault == 'first': raw[2]['first_chunk_rows'] = 4
            if fault == 'remaining': raw[2]['remaining_ns'] = 999
            if fault == 'clock': raw[2]['total_ns'] = -1
            if fault == 'oracle': result['validation_after'] = 'c'*64
            if fault == 'usage': raw[2]['cpu_including_close_seconds'] = float('nan')
            if fault == 'resource': raw[1]['resources']['delta']['write_bytes'] = 1
            with self.subTest(fault=fault), self.assertRaises(RuntimeError):
                verify_cell(cell, result, raw, obs, 100)


if __name__ == '__main__': unittest.main()
