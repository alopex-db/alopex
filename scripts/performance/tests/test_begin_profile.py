"""No native DB or performance measurement: verify worker handle lifetime."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('begin_profile', Path(__file__).parents[1] / 'begin_profile.py')
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)


class HandleLifetimeTests(unittest.TestCase):
    def run_worker(self, rows=0, backend='memory', fail_seed=False):
        class Transaction:
            def __init__(self, db):
                self.db = db
                db.live += 1
            def rollback(self):
                pass
            def execute_sql(self, sql):
                pass
            @staticmethod
            def commit():
                if fail_seed:
                    raise ValueError('seed commit failed')
            def __del__(self):
                self.db.live -= 1

        class Database:
            live = 0
            closed = False
            calls = 0
            def execute_sql(self, sql):
                return [dict(id=row, value=row * 3) for row in range(rows)]
            def begin(self, mode):
                if self.live:
                    raise RuntimeError('previous handle still alive at next begin')
                self.calls += 1
                return Transaction(self)
            def close(self):
                if self.live:
                    raise RuntimeError('unfinished handle at close')
                self.closed = True

        db = Database()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / '_alopex.fake.so').write_bytes(b'fixture; never loaded')
            module = SimpleNamespace(__file__=str(root / '__init__.py'),
                Database=SimpleNamespace(new=lambda: db, open=lambda path: db),
                TxnMode=SimpleNamespace(READ_ONLY=0, READ_WRITE=1))
            output = io.StringIO()
            with patch.dict(sys.modules, {'alopex': module}), patch.object(WORKER.importlib.metadata, 'version', return_value='fixture'), \
                    patch.object(sys, 'argv', ['begin_profile.py', '--manual', '--backend', backend, '--rows', str(rows), '--mode', 'rw', '--operation', 'noop']), \
                    contextlib.redirect_stdout(output):
                if fail_seed:
                    with self.assertRaisesRegex(ValueError, '^seed commit failed$'):
                        WORKER.main()
                else:
                    WORKER.main()
            self.assertTrue(db.closed)
            self.assertEqual(db.live, 0)
            if fail_seed:
                self.assertEqual(db.calls, 1)
                return
            events = [json.loads(line) for line in output.getvalue().splitlines()]
            samples = [event for event in events if event['event'] == 'sample']
            self.assertEqual(db.calls, 6 + int(rows > 0))
            self.assertEqual(len(samples), 6)
            self.assertTrue(all(s['query_ns'] is None for s in samples))
            self.assertTrue(events[-1]['all_rows_unchanged'])

    def test_previous_handle_is_destroyed_outside_next_begin(self):
        self.run_worker()

    def test_seed_handle_is_destroyed_before_reopen_and_first_begin(self):
        for backend in ['memory', 'disk']:
            with self.subTest(backend=backend):
                self.run_worker(rows=100, backend=backend)

    def test_failed_seed_is_destroyed_before_close_preserving_original_error(self):
        self.run_worker(rows=100, fail_seed=True)


if __name__ == '__main__':
    unittest.main()
