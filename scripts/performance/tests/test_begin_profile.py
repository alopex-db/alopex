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
    def test_previous_handle_is_destroyed_outside_next_begin(self):
        class Transaction:
            def __init__(self, db):
                self.db = db
                db.live += 1
            def rollback(self):
                pass
            def __del__(self):
                self.db.live -= 1

        class Database:
            live = 0
            closed = False
            calls = 0
            def execute_sql(self, sql):
                return []
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
                Database=SimpleNamespace(new=lambda: db), TxnMode=SimpleNamespace(READ_ONLY=0, READ_WRITE=1))
            output = io.StringIO()
            with patch.dict(sys.modules, {'alopex': module}), patch.object(WORKER.importlib.metadata, 'version', return_value='fixture'), \
                    patch.object(sys, 'argv', ['begin_profile.py', '--manual', '--backend', 'memory', '--rows', '0', '--mode', 'rw', '--operation', 'noop']), \
                    contextlib.redirect_stdout(output):
                WORKER.main()
            events = [json.loads(line) for line in output.getvalue().splitlines()]
            samples = [event for event in events if event['event'] == 'sample']
            self.assertEqual(db.calls, 6)
            self.assertTrue(db.closed)
            self.assertEqual(len(samples), 6)
            self.assertTrue(all(s['query_ns'] is None for s in samples))
            self.assertTrue(events[-1]['all_rows_unchanged'])


if __name__ == '__main__':
    unittest.main()
