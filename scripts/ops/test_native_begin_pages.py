import copy
import json
from pathlib import Path
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import native_page_observer as observer
import native_begin_pages_probe as probe
import native_begin_pages_supervisor as supervisor
import native_begin_pages_matrix as matrix
import test_native_begin_diagnostic as fixtures


def page_row(new=False):
    return dict(file_offset=0, permissions='r-xp', mapped_pages=2,
                present_before=1 if new else 2, present_after=2,
                newly_present=[4096] if new else [], no_longer_present=[])


class PagesTests(unittest.TestCase):
    def test_present_flag_ignores_hidden_or_nonzero_pfn(self):
        data = struct.pack('=QQQ', 1 << 63, (1 << 63) | 12345, 12345)
        self.assertEqual(observer.present_offsets(data, 8192, 4096), [8192, 12288])
        with self.assertRaises(ValueError): observer.present_offsets(data[:-1], 0, 4096)

    def test_only_exact_readable_extension_mappings_are_selected(self):
        maps = ('1000-3000 r-xp 00001000 00:01 1 /fixture/extension.so\n'
                '3000-4000 ---p 00003000 00:01 1 /fixture/extension.so\n'
                '4000-5000 r--p 00004000 00:01 1 /fixture/extension.so.other\n')
        self.assertEqual(observer.regions(maps, Path('/fixture/extension.so'), 4096),
                         [(4096, 12288, 4096, 'r-xp')])
        with self.assertRaises(ValueError): observer.regions(maps.replace('1000-3000','1001-3000'), Path('/fixture/extension.so'), 4096)
        with self.assertRaises(ValueError): observer.regions('', Path('/fixture/extension.so'), 4096)

    def test_short_read_stops_observation(self):
        instance = observer.PageObserver.__new__(observer.PageObserver)
        instance.regions = [(4096, 8192, 0, 'r-xp')]
        instance.page_size, instance.fd, instance.views = 4096, 99, [[memoryview(bytearray(8))]]
        with patch.object(observer.os, 'preadv', return_value=0):
            with self.assertRaisesRegex(RuntimeError, 'incomplete'): instance.capture(0)

    def test_difference_exports_relative_offsets_not_virtual_addresses(self):
        instance = observer.PageObserver.__new__(observer.PageObserver)
        instance.regions = [(0x70000000, 0x70002000, 0x2000, 'r-xp')]
        instance.page_size = 4096
        instance.buffers = [[struct.pack('=QQ', 1<<63, 0)], [struct.pack('=QQ', 1<<63, 1<<63)]]
        result = instance.difference(0, 1)
        self.assertEqual(result[0]['newly_present'], [0x3000])
        self.assertNotIn(str(0x70000000), json.dumps(result))

    def execute(self, prefault):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root/'_alopex.so').write_bytes(b'fixture')
        (root/'db').touch()
        trace, events = [], []
        class FakeObserver:
            page_size = 4096
            def __init__(self, native): pass
            def capture(self, slot): trace.append(('capture', slot))
            def prefault(self): trace.append(('prefault',))
            def difference(self, before, after): return [page_row(new=(before == 2 and not prefault))]
            def close(self): trace.append(('observer_close',))
        class DB:
            @staticmethod
            def open(*args, **kw): return DB()
            def begin(self, mode): trace.append(('begin',)); return self
            def rollback(self): trace.append(('rollback',))
            def hnsw_cache_stats(self): return dict(entries=0, hits=0)
            def execute_sql(self, sql): return [{'plan': 'ExactKnnScan'}]
            def close(self): trace.append(('db_close',))
        fake = SimpleNamespace(__file__=str(root/'__init__.py'), Database=DB,
                               TxnMode=SimpleNamespace(READ_ONLY='ro', READ_WRITE='rw'))
        args = ['probe','--manual','--action','probe','--path',str(root/'db'),
                '--rows','1000','--indexes','0','--mode','ro','--operation','noop','--samples','1']
        if prefault: args += ['--prefault-extension']
        with patch.dict(sys.modules, alopex=fake), patch.object(sys, 'argv', args), \
             patch.object(sys, 'platform', 'linux'), patch.object(probe, 'PageObserver', FakeObserver), \
             patch.object(probe, 'measured', side_effect=lambda action:(action(),fixtures.DiagnosticTests().observation())), \
             patch.object(probe, 'snapshot', return_value=dict(rss_bytes=1024,allocated_bytes=4096,logical_bytes=1)), \
             patch.object(probe, 'peak_rss_bytes', return_value=1024), \
             patch.object(probe, 'validate_rows', side_effect=lambda db,count:trace.append(('validate',count))), \
             patch.object(probe, 'emit', side_effect=lambda **row: events.append(row)):
            probe.main()
        return events, trace

    def test_both_conditions_bracket_one_begin_and_verify(self):
        for spec in supervisor.probes(1000):
            events, trace = self.execute(spec['prefault_extension'])
            supervisor.verify_events(events,dict(rows=1000,indexes=0),events[0]['extension_sha256'],spec)
            index=trace.index(('begin',))
            self.assertEqual(trace[index-1:index+2],[('capture',2),('begin',),('capture',3)])
            self.assertEqual(trace.count(('begin',)),1)
            self.assertEqual(('prefault',) in trace,spec['prefault_extension'])
            self.assertEqual(trace[-2:],[('observer_close',),('db_close',)])
            for mutate in [lambda e:e[0].__setitem__('prefault_extension',not spec['prefault_extension']),
                           lambda e:e[1].pop('extension_pages'),
                           lambda e:e[0]['page_observer_control'][0].__setitem__('newly_present',[4096]),
                           lambda e:e[1]['extension_pages'][0].__setitem__('newly_present',[123])]:
                altered=copy.deepcopy(events);mutate(altered)
                with self.assertRaises(ValueError): supervisor.verify_diagnostics(altered,spec)

    def test_exact_six_cells_two_fresh_readers(self):
        self.assertEqual(len(matrix.plan()),6)
        self.assertEqual(len({c['id'] for c in matrix.plan()}),6)
        self.assertTrue(all(c['rows']==1000 and c['indexes']==0 for c in matrix.plan()))
        self.assertEqual([s['prefault_extension'] for s in supervisor.probes(1000)],[False,True])
        self.assertTrue(all(s['mode']=='ro' and s['operation']=='noop' and s['samples']==1 for s in supervisor.probes(1000)))


if __name__ == '__main__': unittest.main()
