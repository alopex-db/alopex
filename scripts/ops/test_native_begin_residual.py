import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import native_begin_residual_probe as probe
import native_begin_residual_matrix as matrix
from native_begin_residual_supervisor import verify_diagnostics, verify_events


class DiagnosticTests(unittest.TestCase):
    def test_timer_boundaries_and_process_counters(self):
        trace = []
        def clock(name, values):
            iterator = iter(values)
            def read():
                trace.append(name)
                return next(iterator)
            return read
        before = SimpleNamespace(**{key: 4 for key in probe.COUNTERS})
        after = SimpleNamespace(**{key: 7 for key in probe.COUNTERS})
        io = {key: 4096 for key in ('read_bytes', 'write_bytes', 'cancelled_write_bytes')}
        def action():
            trace.append('operation')
            return 'result'
        with patch.object(probe, 'io_counters', side_effect=[io, io]), \
             patch.object(probe.resource, 'getrusage', side_effect=[before, after]), \
             patch.object(probe, 'perf_counter_ns', clock('wall', [100, 600])), \
             patch.object(probe, 'thread_time_ns', clock('thread', [30, 380])), \
             patch.object(probe, 'process_time_ns', clock('process', [20, 500])):
            result, value = probe.measured(action)
        self.assertEqual(result, 'result')
        self.assertEqual(trace, ['process', 'thread', 'wall', 'operation', 'wall', 'thread', 'process'])
        self.assertEqual((value['wall_ns'], value['caller_cpu_ns'], value['process_cpu_ns']), (500, 350, 480))
        self.assertEqual(set(value['counters'].values()), {3})
        self.assertEqual(set(value['io_bytes'].values()), {0})

    def test_counter_unavailable_is_not_zero(self):
        with patch.object(probe, 'io_counters', side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                probe.measured(lambda: None)

    def test_failed_operation_is_not_reported_as_success(self):
        with patch.object(probe, 'io_counters', return_value={}):
            with self.assertRaisesRegex(RuntimeError, 'operation failed'):
                probe.measured(lambda: (_ for _ in ()).throw(RuntimeError('operation failed')))

    def test_reversed_counter_is_rejected(self):
        with patch.object(probe, 'io_counters', return_value={}), \
             patch.object(probe, 'perf_counter_ns', side_effect=[2, 1]):
            with self.assertRaisesRegex(RuntimeError, 'non-monotonic'):
                probe.measured(lambda: None)

    def observation(self, wall=10):
        return dict(wall_ns=wall, process_cpu_ns=12, caller_cpu_ns=11,
                    counters={key: 0 for key in probe.COUNTERS},
                    io_bytes={key: 0 for key in ('read_bytes', 'write_bytes', 'cancelled_write_bytes')})

    def run_probe(self, mode, operation, result=None, rows=100, indexes=0):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        (root / '_alopex.so').write_bytes(b'synthetic extension')
        database = root / 'db'
        database.touch()
        outputs, calls = [], []
        class DB:
            warmed = 0
            hits = 0
            def warm_hnsw_index(self, name): self.warmed += 1
            @staticmethod
            def open(*args, **kwargs): return DB()
            def hnsw_cache_stats(self): return dict(entries=self.warmed, hits=self.hits)
            def begin(self, mode): calls.append(('begin', mode)); return self
            def execute_sql(self, sql):
                calls.append(('sql', sql))
                if sql.startswith('EXPLAIN '): return [{'plan': 'HnswSearch' if indexes else 'ExactKnnScan'}]
                if self.warmed: self.hits += 1
                return [{'id': 0}] if result is None else result
            def rollback(self): calls.append(('rollback',))
            def close(self): calls.append(('close',))
        fake = SimpleNamespace(__file__=str(root / '__init__.py'), Database=DB,
                               TxnMode=SimpleNamespace(READ_ONLY='RO', READ_WRITE='RW'))
        argv = ['probe', '--manual', '--action', 'probe', '--path', str(database),
                '--rows', str(rows), '--indexes', str(indexes), '--mode', mode, '--operation', operation]
        with patch.dict(sys.modules, alopex=fake), patch.object(sys, 'argv', argv), \
             patch.object(sys, 'platform', 'linux'), \
             patch.object(probe, 'measured', side_effect=lambda action: (action(), self.observation())), \
             patch.object(probe, 'snapshot', return_value=dict(rss_bytes=1024, allocated_bytes=4096, logical_bytes=1)), \
             patch.object(probe, 'peak_rss_bytes', return_value=1024), \
             patch.object(probe, 'validate_rows', side_effect=lambda db, count: calls.append(('validate', count))), \
             patch.object(probe, 'emit', side_effect=lambda **row: outputs.append(row)):
            probe.main()
        return outputs, calls

    def test_all_phase_shapes_and_final_plan_are_verified(self):
        for rows, indexes, mode, operation in [(100, 0, 'auto', 'query'), (100, 1, 'ro', 'query'), (100, 1, 'ro', 'noop'), (1000, 1, 'ro', 'noop')]:
            with self.subTest(mode=mode, operation=operation):
                events, calls = self.run_probe(mode, operation, rows=rows, indexes=indexes)
                spec = dict(mode=mode, operation=operation, samples=6, repeat=0)
                verify_events(events, dict(rows=rows, indexes=indexes), events[0]['extension_sha256'], spec)
                self.assertEqual(calls[-3:], [('validate', rows), ('sql', 'EXPLAIN ' + events[0]['query']), ('close',)])
                self.assertEqual(len(events[0]['timer_controls']), 21)
                if mode == 'auto': self.assertFalse(any(call[0] == 'begin' for call in calls))
                for mutate in [lambda e: e[0].pop('timer_controls'),
                               lambda e: e[1]['diagnostic_phases'].clear(),
                               lambda e: e[-1].pop('query_plan'),
                               lambda e: next(iter(e[1]['diagnostic_phases'].values()))['io_bytes'].pop('read_bytes'),
                               lambda e: next(iter(e[1]['diagnostic_phases'].values())).__setitem__('wall_ns', 999)]:
                    changed = copy.deepcopy(events)
                    mutate(changed)
                    with self.assertRaises(ValueError): verify_diagnostics(changed, spec)

    def test_wrong_query_result_stops_diagnostic(self):
        with self.assertRaisesRegex(RuntimeError, 'nearest-neighbor result differs'):
            self.run_probe('auto', 'query', [{'id': 1}])

    def test_only_remaining_operations_are_scheduled(self):
        from native_begin_residual_supervisor import probes
        expected = {(100, 0): [("auto", "query")],
                    (100, 1): [("ro", "noop"), ("ro", "query")],
                    (1000, 1): [("ro", "noop")]}
        for pair, operations in expected.items():
            self.assertEqual([(s["mode"], s["operation"]) for s in probes(*pair)], operations)
            self.assertTrue(all(s["samples"] == 6 and s["repeat"] == 0 for s in probes(*pair)))
        for pair in [(0, 0), (1000, 0), (100, 2), (True, 0)]:
            with self.assertRaises(ValueError): probes(*pair)

    def test_exact_bounded_matrix(self):
        cells = matrix.plan()
        self.assertEqual(len(cells), 18)
        self.assertEqual(len({cell['id'] for cell in cells}), 18)
        self.assertEqual({(c['rows'], c['indexes']) for c in cells}, {(100, 0), (100, 1), (1000, 1)})
        self.assertEqual([c['subject'] for c in cells], ['before', 'candidate', 'candidate', 'before', 'before', 'candidate'] * 3)
        self.assertEqual({(c['trial'], c['subject']) for c in cells}, {(trial, subject) for trial in range(3) for subject in ('before', 'candidate')})


if __name__ == '__main__':
    unittest.main()
