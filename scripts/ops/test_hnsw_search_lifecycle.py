"""Tiny scratch lifetime regressions; no product build or search workload."""
import gc
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from hnsw_search_lifecycle import prepare_cell_temp, cleanup_cell_temp


PASSED = dict(reaped=True, returncode=0, failure=None)


class LifecycleTests(unittest.TestCase):
    def test_cleanup_after_real_worker_exit_preserves_other_cells(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary); (work/'tmp').mkdir()
            scratch = prepare_cell_temp(work, 'trial0-published')
            other = prepare_cell_temp(work, 'trial0-candidate')
            (other/'keep').write_bytes(b'x')
            child = subprocess.Popen([sys.executable, '-B', '-c',
                'from pathlib import Path; import sys; '
                '(Path(sys.argv[1])/"lock").write_bytes(b"x")', str(scratch)])
            code = child.wait(timeout=10)
            self.assertTrue((scratch/'lock').is_file())
            receipt = cleanup_cell_temp(work, 'trial0-published',
                dict(reaped=True, returncode=code, failure=None))
            self.assertEqual(receipt, dict(cell='trial0-published', removed_after_wait=True))
            self.assertFalse(scratch.exists())
            self.assertEqual((other/'keep').read_bytes(), b'x')

    def test_no_deletion_without_successful_wait_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary); (work/'tmp').mkdir()
            scratch = prepare_cell_temp(work, 'trial0-published')
            for state in ({}, dict(PASSED, reaped=False), dict(PASSED, returncode=None),
                          dict(PASSED, returncode=-15), dict(PASSED, returncode=False),
                          dict(PASSED, failure={'error': 'injected'})):
                with self.subTest(state=state), self.assertRaises(RuntimeError):
                    cleanup_cell_temp(work, 'trial0-published', state)
                self.assertTrue(scratch.is_dir())

    def test_reject_reused_path_traversal_and_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary); (work/'tmp').mkdir()
            prepare_cell_temp(work, 'trial0-published')
            with self.assertRaises(FileExistsError):
                prepare_cell_temp(work, 'trial0-published')
            with self.assertRaises(RuntimeError):
                prepare_cell_temp(work, '../outside')
            outside = work/'outside'; outside.mkdir(); (outside/'keep').write_bytes(b'x')
            (work/'tmp/trial0-candidate').symlink_to(outside, target_is_directory=True)
            with self.assertRaises(RuntimeError):
                cleanup_cell_temp(work, 'trial0-candidate', PASSED)
            self.assertEqual((outside/'keep').read_bytes(), b'x')

    def test_builder_close_and_gc_preserve_files_until_parent_cleanup(self):
        # Stub only the two external modules; execute the actual builder/close.
        contract = ModuleType('hnsw_v0811_contract')
        contract.SearchEngine = lambda *args: SimpleNamespace(close=args[3])
        contract.peak_rss_bytes = lambda: 1
        contract.recall_at_k = contract.tie_aware_recall_at_k = lambda *args: 1.
        events = []
        class Transaction:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def upsert_to_hnsw_batch(self, *args): pass
            def upsert_to_hnsw(self, *args): pass
            def delete_from_hnsw(self, *args): pass
            def commit(self): pass
        class Database:
            @staticmethod
            def open(path):
                directory = Path(path); directory.mkdir(exist_ok=True)
                (directory/'.alopex.lock').write_bytes(b'x')
                return Database()
            def create_hnsw_index(self, *args): pass
            def begin(self, *args): return Transaction()
            def flush(self): pass
            def close(self): events.append('close')
            def drop_hnsw_index(self, *args): events.append('drop')
            def get_hnsw_stats(self, *args): return SimpleNamespace(node_count=2)
        product = ModuleType('alopex'); product.Database = Database
        product.HnswConfig = lambda *args, **kwargs: None
        product.Metric = SimpleNamespace(COSINE=0)
        product.TxnMode = SimpleNamespace(READ_WRITE=0)
        class Vectors(list): shape = (2, 1)
        spec = importlib.util.spec_from_file_location('builder_lifetime_probe',
            Path(__file__).with_name('hnsw_search_build.py'))
        builder = importlib.util.module_from_spec(spec)
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary); (work/'tmp').mkdir()
            scratch = prepare_cell_temp(work, 'trial0-published')
            with patch.dict(sys.modules, {'hnsw_v0811_contract': contract, 'alopex': product}), \
                    patch.object(tempfile, 'tempdir', str(scratch)):
                spec.loader.exec_module(builder)
                engine = builder.build_alopex(Vectors([[1.], [2.]]))
                locks = list(scratch.rglob('.alopex.lock'))
                self.assertEqual(len(locks), 1)
                engine.close(); del engine; gc.collect()
                self.assertEqual(events, ['close', 'drop', 'close'])
                self.assertTrue(locks[0].exists(), 'worker removed files during live census')
            cleanup_cell_temp(work, 'trial0-published', PASSED)
            self.assertFalse(scratch.exists())


if __name__ == '__main__': unittest.main()
