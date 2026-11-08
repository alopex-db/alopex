import unittest

import hnsw_writer_diagnostic_contract as contract
from test_hnsw_completion_smoke import fixture


def diagnostic(mode):
    args = fixture(mode)
    for event in args[2]:
        if event['phase'] in ('first_insert', 'second_insert') and event['data'].get('event') == 'end':
            first = event['phase'] == 'first_insert'
            event['data'].update(elapsed_ns=1000, writer_internal=dict(
                schema=contract.SCHEMA, fork_calls=int(first), fork_ns=13 if first else 0,
                stage_calls=1, stage_ns=29, scope='synchronous INSERT on the calling thread',
                performance_qualification=False))
    return args


class DiagnosticTests(unittest.TestCase):
    def test_both_modes_preserve_completion_and_internal_results(self):
        for mode in ('commit', 'rollback'):
            result = contract.verify(*diagnostic(mode), mode, 'a'*64, 'b'*64)
            self.assertTrue(result['result_validated'])
            self.assertFalse(result['performance_qualification'])
            self.assertEqual(result['writer_internal']['first_insert']['fork_ns'], 13)
            self.assertEqual(result['writer_internal']['second_insert']['fork_calls'], 0)

    def test_missing_duplicated_or_impossible_counters_fail(self):
        faults = ('missing', 'no_first_fork', 'second_fork', 'second_time', 'extra_stage',
                  'bool_count', 'negative', 'too_large', 'bool_total', 'missing_result',
                  'wrong_schema', 'qualification', 'extra_field')
        for fault in faults:
            args = diagnostic('commit')
            first = args[2][13]['data']
            second = args[2][15]['data']
            value = first['writer_internal']
            if fault == 'missing': first.pop('writer_internal')
            if fault == 'no_first_fork': value['fork_calls'] = 0
            if fault == 'second_fork': second['writer_internal']['fork_calls'] = 1
            if fault == 'second_time': second['writer_internal']['fork_ns'] = 1
            if fault == 'extra_stage': value['stage_calls'] = 2
            if fault == 'bool_count': value['fork_calls'] = True
            if fault == 'negative': value['fork_ns'] = -1
            if fault == 'too_large': value['stage_ns'] = 1000
            if fault == 'bool_total': first['elapsed_ns'] = True
            if fault == 'missing_result': args[2][-1]['data']['result_validated'] = False
            if fault == 'wrong_schema': value['schema'] = 'other'
            if fault == 'qualification': value['performance_qualification'] = True
            if fault == 'extra_field': value['host'] = 'synthetic-private'
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify(*args, 'commit', 'a'*64, 'b'*64)


if __name__ == '__main__': unittest.main()
