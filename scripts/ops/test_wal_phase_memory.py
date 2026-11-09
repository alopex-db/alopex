import ast
import copy
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from wal_phase_memory import CHILD, MAX_RAW_BYTES, Observer, PHASES, PREFIX, instrument, validate_report

SOURCE = Path(__file__).resolve().parents[2] / 'scripts/compatibility/public_api_cost_original.py'


def raw(anon=20, file=12):
    return dict(status=f'Pid: 7\nVmRSS: {anon+file} kB\nRssAnon: {anon} kB\nRssFile: {file} kB\nRssShmem: 0 kB\n',
                smaps_rollup=f'Rss: {anon+file} kB\nAnonymous: {anon} kB\nPss_Anon: {anon} kB\nPss_File: {file//2} kB\nPss_Shmem: 0 kB\n')


class Boundaries(unittest.TestCase):
    def observer(self, read=raw):
        ticks = iter(range(100))
        return Observer(read=read, clock=lambda: next(ticks))

    def test_complete_sequence_and_signed_memory_changes(self):
        values = iter([raw(20-i) for i in range(len(PHASES))])
        observer = self.observer(lambda: next(values))
        for phase in PHASES:
            observer.capture(phase)
        result = observer.report()
        self.assertEqual(len(result['transitions']), len(PHASES)-1)
        self.assertTrue(all(row['delta_bytes']['smaps_rollup_bytes']['Anonymous'] == -1024 for row in result['transitions']))
        self.assertFalse(result['performance_acceptance'])
        self.assertTrue(result['no_high_water_reset'])
        self.assertEqual(result['rows'][0]['memory']['smaps_rollup_bytes']['Pss_File'], 6144)
        self.assertEqual(validate_report(result), result)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'memory.json'
            observer.save(path)
            self.assertEqual(json.loads(path.read_text()), result)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                observer.save(path)

    def test_missing_duplicate_reordered_or_extra_boundary_rejected(self):
        observer = self.observer()
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            observer.report()
        with self.assertRaisesRegex(ValueError, 'order'):
            observer.capture(PHASES[1])
        observer.capture(PHASES[0])
        with self.assertRaisesRegex(ValueError, 'order'):
            observer.capture(PHASES[0])
        for phase in PHASES[1:]:
            observer.capture(phase)
        with self.assertRaisesRegex(ValueError, 'order'):
            observer.capture(PHASES[-1])

    def test_missing_kernel_counter_is_not_zero_filled(self):
        value = raw(); value['status'] = value['status'].replace('RssFile: 12 kB\n', '')
        observer = self.observer(lambda: value)
        for phase in PHASES:
            observer.capture(phase)
        with self.assertRaisesRegex(ValueError, 'missing memory field'):
            observer.report()

    def test_read_failure_and_oversized_record_never_complete(self):
        def failure():
            raise PermissionError('statistics denied')
        observer = self.observer(failure)
        with self.assertRaises(PermissionError):
            observer.capture(PHASES[0])
        self.assertEqual(observer.rows, [])
        observer = self.observer(lambda: dict(status='x'*(MAX_RAW_BYTES+1), smaps_rollup=''))
        with self.assertRaisesRegex(ValueError, 'bound'):
            observer.capture(PHASES[0])
        self.assertEqual(observer.rows, [])

    def test_reversed_clock_rejected(self):
        ticks = iter([2, 1])
        observer = Observer(read=raw, clock=lambda: next(ticks))
        with self.assertRaisesRegex(ValueError, 'clock'):
            observer.capture(PHASES[0])
        self.assertEqual(observer.rows, [])

    def test_report_recomputes_raw_values_and_rejects_acceptance_or_delta_tampering(self):
        observer = self.observer()
        for phase in PHASES:
            observer.capture(phase)
        value = observer.report()
        bad = copy.deepcopy(value); bad['transitions'][0]['delta_bytes']['status_bytes']['RssAnon'] += 1
        with self.assertRaisesRegex(ValueError, 'differs'):
            validate_report(bad)
        bad = copy.deepcopy(value); bad['performance_acceptance'] = True
        with self.assertRaisesRegex(ValueError, 'differs'):
            validate_report(bad)
        bad = copy.deepcopy(value); bad['rows'][0]['memory']['status_bytes']['VmRSS'] += 1
        with self.assertRaisesRegex(ValueError, 'differs'):
            validate_report(bad)

    def test_mixed_or_missing_process_identity_rejected(self):
        for replacement in ('Pid: 8', '', 'Pid: 7\nPid: 7', 'Pid: 0'):
            observer = self.observer()
            for phase in PHASES:
                observer.capture(phase)
            observer.rows[-1]['raw'] = dict(observer.rows[-1]['raw'])
            observer.rows[-1]['raw']['status'] = observer.rows[-1]['raw']['status'].replace('Pid: 7', replacement)
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, 'identity'):
                observer.report()


class WriterExit(Exception):
    pass


def run_writer(source, surface, size, *, fail_commit=False):
    calls, phases, printed = [], [], []
    timing = False
    tick = 0

    def clock():
        nonlocal timing, tick
        timing = not timing
        tick += 1
        return tick

    def measure(action):
        start = clock()
        result = action()
        return result, clock()-start

    def capture(phase):
        if timing:
            raise AssertionError('observation entered original timed interval')
        phases.append(phase)

    class Tx:
        def put(self, key, value): calls.append(('put', key, value))
        def execute_sql(self, sql): calls.append(('tx_sql', sql))
        def commit(self):
            calls.append(('commit',))
            if fail_commit and sum(call == ('commit',) for call in calls) == 2:
                raise OSError('pending commit EIO')

    class DB:
        @staticmethod
        def open(path, **options):
            calls.append(('open', path, options))
            return DB()
        def begin(self, mode):
            calls.append(('begin', mode))
            return Tx()
        def execute_sql(self, sql): calls.append(('db_sql', sql))
        def wal_overflow_stats(self): return {'gc_failures': 0}

    def require(value, message):
        if not value: raise ValueError(message)
    def exit_writer(code):
        if code != 0: raise AssertionError(code)
        raise WriterExit()
    fake = types.ModuleType('alopex')
    fake.Database = DB
    fake.TxnMode = types.SimpleNamespace(READ_WRITE='write')
    fake.WalOverflowOptions = lambda **options: options
    fake._alopex = types.SimpleNamespace(__file__='native-test')
    globals_ = dict(require=require, digest=lambda _: 'expected', OPTIONS={'fixture': True},
                    time=types.SimpleNamespace(perf_counter_ns=clock), measure=measure,
                    resource=types.SimpleNamespace(RUSAGE_SELF=0, getrusage=lambda _: types.SimpleNamespace(ru_maxrss=1)),
                    sys=types.SimpleNamespace(platform='linux'), sizes=lambda _: {}, json=json,
                    os=types.SimpleNamespace(_exit=exit_writer), print=lambda value, **_: printed.append(value),
                    _wal_phase_capture=capture)
    child, = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == 'child']
    module = ast.Module(body=[child], type_ignores=[])
    with patch.dict(sys.modules, alopex=fake):
        exec(compile(module, '<original-workload>', 'exec'), globals_)
        try:
            globals_['child']('write', Path('fixture'), surface, size, 'expected')
        except WriterExit:
            pass
    return calls, phases, printed


class Instrumentation(unittest.TestCase):
    def test_all_four_tiny_shapes_preserve_operations_results_and_original_timers(self):
        original = SOURCE.read_text()
        observed, proof = instrument(original)
        self.assertTrue(proof['original_ast_preserved'])
        for surface in ('kv', 'sql'):
            for size in (16, 2048):
                with self.subTest(surface=surface, size=size):
                    plain = run_writer(original, surface, size)
                    measured = run_writer(observed, surface, size)
                    self.assertEqual(plain[0], measured[0])
                    self.assertEqual(plain[2], measured[2])
                    self.assertEqual(measured[1], list(CHILD))

    def test_changed_missing_or_duplicate_workload_anchor_rejected(self):
        original = SOURCE.read_text()
        for changed in (original.replace('_, record["commit_ns"] = measure(tx.commit)', 'tx.commit()'),
                        original.replace('options = WalOverflowOptions(**OPTIONS)', 'options = None'),
                        original + '\n_wal_phase_capture = None\n'):
            with self.assertRaises(ValueError): instrument(changed)

    def test_failed_commit_is_not_converted_to_completed_measurement(self):
        observed, _ = instrument(SOURCE.read_text())
        with self.assertRaisesRegex(OSError, 'pending commit EIO'):
            run_writer(observed, 'kv', 16, fail_commit=True)


if __name__ == '__main__':
    unittest.main()
