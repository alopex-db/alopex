import ast
import copy
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import native_empty_query_pages_probe as probe
from native_empty_query_pages_supervisor import probes, verify_events
from empty_query_pages_observation_checks import expected_specs, extract


class ObserverComparisonTests(unittest.TestCase):
    def test_original_direct_timed_statements_are_preserved(self):
        def blocks(path):
            result = []
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                for _, value in ast.iter_fields(node):
                    if not isinstance(value, list):
                        continue
                    for i, item in enumerate(value):
                        if (isinstance(item, ast.Assign) and isinstance(item.targets[0], ast.Name)
                                and item.targets[0].id == 'started' and isinstance(item.value, ast.Call)
                                and isinstance(item.value.func, ast.Name) and item.value.func.id == 'perf_counter_ns'):
                            result.append([ast.dump(x, include_attributes=False) for x in value[i:i+3]])
            return sorted(result)
        root = Path(__file__).parent
        original = blocks(root / 'native_begin_probe.py')
        self.assertEqual(len(original), 4)
        self.assertEqual(blocks(root / 'native_empty_query_pages_probe.py'), original)

    def run_probe(self, rows, indexes, mode, operation, observer):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '_alopex.so').write_bytes(b'synthetic')
            (root / 'db').touch()
            outputs, trace = [], []
            active = False
            ticks = 0
            def clock():
                nonlocal active, ticks
                active = not active
                ticks += 1
                trace.append(('clock', active))
                return ticks
            class DB:
                warmed = 0
                hits = 0
                @staticmethod
                def open(*args, **kwargs): return DB()
                def __getattribute__(self, name):
                    nonlocal ticks
                    if name in ('begin', 'rollback', 'execute_sql'):
                        ticks += 50
                        trace.append(('lookup', name, active))
                    return object.__getattribute__(self, name)
                def begin(self, mode):
                    nonlocal ticks
                    ticks += 25
                    trace.append(('call', 'begin', active))
                    return self
                def rollback(self):
                    nonlocal ticks
                    ticks += 25
                    trace.append(('call', 'rollback', active))
                def execute_sql(self, sql):
                    nonlocal ticks
                    ticks += 25
                    trace.append(('call', 'execute_sql', active))
                    if sql.startswith('EXPLAIN '):
                        return [{'plan': 'HnswSearch' if indexes else 'ExactKnnScan'}]
                    if self.warmed: self.hits += 1
                    return [{'id': 0}] if rows else []
                def warm_hnsw_index(self, name): self.warmed += 1
                def hnsw_cache_stats(self): return dict(entries=self.warmed, hits=self.hits)
                def close(self): pass
            fake = SimpleNamespace(__file__=str(root/'__init__.py'), Database=DB,
                                   TxnMode=SimpleNamespace(READ_ONLY='RO', READ_WRITE='RW'))
            def counter():
                self.assertFalse(active, 'resource collection entered the wall timer')
                return 100
            def usage(*args):
                counter()
                return SimpleNamespace(**{key: 0 for key in probe.COUNTERS})
            def io():
                counter()
                return dict(read_bytes=0, write_bytes=0, cancelled_write_bytes=0)
            class FakePages:
                def __init__(self, extension): counter()
                def begin(self): counter(); return {}
                def finish(self, before):
                    counter()
                    groups={g:{k:0 for k in probe.FIELDS} for g in probe.GROUPS}
                    return dict(page_size=4096,before=groups,after=groups,extension_pages=[dict(
                        file_offset=0,permissions='r-xp',mapped_pages=1,present_before=1,present_after=1,
                        newly_present=[],no_longer_present=[])])
                def close(self): counter()
            argv = ['probe', '--manual', '--action', 'probe', '--path', str(root/'db'),
                    '--rows', str(rows), '--indexes', str(indexes), '--mode', mode,
                    '--operation', operation, '--observer-mode', observer]
            with patch.object(probe, 'QueryPageObserver', FakePages), patch.dict(sys.modules, alopex=fake), patch.object(sys, 'argv', argv), \
                 patch.object(sys, 'platform', 'linux'), \
                 patch.object(probe, 'perf_counter_ns', side_effect=clock), \
                 patch.object(probe, 'process_time_ns', side_effect=counter), \
                 patch.object(probe, 'thread_time_ns', side_effect=counter), \
                 patch.object(probe.resource, 'getrusage', side_effect=usage), \
                 patch.object(probe, 'io_counters', side_effect=io) as io_mock, \
                 patch.object(probe, 'snapshot', return_value=dict(rss_bytes=1024, allocated_bytes=4096, logical_bytes=1)), \
                 patch.object(probe, 'peak_rss_bytes', return_value=1024), \
                 patch.object(probe, 'validate_rows'), \
                 patch.object(probe, 'emit', side_effect=lambda **row: outputs.append(row)):
                probe.main()
            self.assertFalse(active)
            self.assertEqual(io_mock.call_count == 0, observer == 'plain')
            for row in outputs[1:-1]:
                # Attribute lookup cost (50) must remain within every measured call.
                self.assertTrue(all(value == 76 for key, value in row.items()
                                    if key in ('begin_ns', 'query_ns', 'rollback_ns', 'auto_total_ns') and value is not None))
            return outputs

    def test_modes_preserve_timed_lookup_and_api_results(self):
        for rows, indexes, mode, operation in ((0,2,'rw','query'),):
            for observer in ('plain', 'counters'):
                with self.subTest(rows=rows, indexes=indexes, mode=mode, operation=operation, observer=observer):
                    events = self.run_probe(rows, indexes, mode, operation, observer)
                    spec = dict(mode=mode, operation=operation, samples=6, repeat=0, observer_mode=observer)
                    config = dict(rows=rows, indexes=indexes)
                    digest = events[0]['extension_sha256']
                    verify_events(events, config, digest, spec)
                    self.assertTrue(extract(events, spec, config, digest))
                    for change in ('mode', 'priming', 'counter'):
                        mutated = copy.deepcopy(events)
                        if change == 'mode': mutated[0]['observer_mode'] = 'wrong'
                        elif change == 'priming': mutated[0]['timer_controls'] = []
                        else: next(iter(mutated[1]['diagnostic_phases'].values()))['wall_ns'] = 999
                        with self.assertRaises(ValueError): verify_events(mutated, config, digest, spec)
                        with self.assertRaises(ValueError): extract(mutated, spec, config, digest)

    def test_paired_order_is_independently_checked(self):
        first_modes = []
        for pair in ((0,2),):
            for trial in range(3):
                for subject in ('before', 'candidate'):
                    actual = probes(*pair, trial, subject)
                    self.assertEqual(actual, expected_specs(*pair, trial, subject))
                    for offset in range(0, len(actual), 2):
                        self.assertEqual({x['observer_mode'] for x in actual[offset:offset+2]}, {'plain','counters'})
                        first_modes.append(actual[offset]['observer_mode'])
        self.assertEqual(first_modes.count('plain'), first_modes.count('counters'))
        with self.assertRaises(ValueError): probes(100,0,True,'before')
        with self.assertRaises(ValueError): expected_specs(100,0,0,'unknown')

    def test_counter_failure_is_not_a_plain_result(self):
        with patch.object(probe, 'io_counters', side_effect=PermissionError):
            with self.assertRaises(PermissionError): probe.observe_before('counters')
            self.assertIsNone(probe.observe_before('plain'))

    def test_counter_decrease_fails(self):
        usage = SimpleNamespace(**{key: 0 for key in probe.COUNTERS})
        io = dict(read_bytes=0, write_bytes=0, cancelled_write_bytes=0)
        with patch.object(probe, 'thread_time_ns', return_value=9), \
             patch.object(probe, 'process_time_ns', return_value=11), \
             patch.object(probe.resource, 'getrusage', return_value=usage), \
             patch.object(probe, 'io_counters', return_value=io):
            with self.assertRaisesRegex(RuntimeError, 'non-monotonic'):
                probe.observe_after((io,usage,10,10), 1)


if __name__ == '__main__':
    unittest.main()
