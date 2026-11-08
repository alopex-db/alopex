import copy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import csv_sequence_contract as contract
import csv_sequence_measure as measure
from test_csv_phase_contract import fixture as original_fixture
from test_dataframe_phase_contract import observation


def fixture():
    cell, result, raw, observed = original_fixture()
    row = dict(event='stages', iteration=0, phase='measure', points_ns=list(range(7)),
        stages_ns=dict.fromkeys(contract.STAGES, 1), total_ns=6, rows=3,
        first_chunk_rows=3, resources=observation(3))
    raw.append(row)
    observed['completed'].append(row['resources'])
    result['resource_observations'] = 4
    return cell, result, raw, observed


class Sequence(unittest.TestCase):
    def test_original_phases_precede_stages_with_contiguous_observation_indices(self):
        calls, records = [], []
        observer = SimpleNamespace(completed=[])
        def phase(operation, event, **kwargs):
            calls.append(event)
            observer.completed.extend([None] * 2)
        def stream(consume, expected, **kwargs):
            self.assertEqual(expected, [0, 1, 2])
            calls.append('sample')
            observer.completed.extend([None] * 2)
        observer.begin = lambda index: calls.append(('begin', index))
        def finish(index):
            self.assertEqual(index, len(observer.completed))
            observer.completed.append(index)
            return index
        observer.finish = finish
        with patch.object(measure, 'measure_plan_or_collect', side_effect=phase), \
             patch.object(measure, 'measure_stream', side_effect=stream), \
             patch.object(measure, 'consume_stages', return_value=dict(rows=3, first_chunk_rows=3)):
            measure.measure(None, None, [0,1,2], warmups=1, samples=1,
                            observer=observer, record=records.append)
        self.assertEqual(calls, ['plan_sample', 'collect_sample', 'sample', ('begin',6), ('begin',7)])
        self.assertEqual([(r['iteration'], r['phase'], r['resources']) for r in records],
                         [(0, 'warmup', 6), (1, 'measure', 7)])

    def test_combined_evidence_is_complete(self):
        self.assertEqual(contract.verify_cell(*fixture(), 100)['observations'], 4)
        self.assertEqual(len(contract.plan('diagnostic')), 6)

    def test_missing_prelude_and_corrupt_stage_evidence_are_rejected(self):
        for fault in ('prelude', 'stage', 'order', 'index', 'rss_index', 'clock', 'sum', 'rows', 'oracle', 'pending'):
            cell, result, raw, obs = copy.deepcopy(fixture())
            if fault == 'prelude': raw.pop(0)
            if fault == 'stage': raw.pop()
            if fault == 'order': raw[0], raw[3] = raw[3], raw[0]
            if fault == 'index': raw[3]['resources']['index'] = 0
            if fault == 'rss_index': raw[3]['resources']['rss']['phase'] = 'sample-0'
            if fault == 'clock': raw[3]['points_ns'][1] = 0
            if fault == 'sum': raw[3]['total_ns'] = 7
            if fault == 'rows': raw[3]['rows'] = 2
            if fault == 'oracle': result['validation_after'] = 'c'*64
            if fault == 'pending': obs['pending'] = dict(index=4)
            with self.subTest(fault=fault), self.assertRaises(RuntimeError):
                contract.verify_cell(cell, result, raw, obs, 100)


if __name__ == '__main__': unittest.main()
