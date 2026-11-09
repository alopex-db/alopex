import copy
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

import query_prefault_observer as observer


class PageCompositionTests(unittest.TestCase):
    def example(self):
        groups = {g: {k: 0 for k in observer.FIELDS} for g in observer.GROUPS}
        return dict(page_size=4096, before=copy.deepcopy(groups), after=copy.deepcopy(groups),
                    extension_pages=[dict(file_offset=8192, permissions='r-xp', mapped_pages=2,
                                          present_before=1, present_after=2,
                                          newly_present=[12288], no_longer_present=[])])

    def test_file_mapping_keeps_anonymous_cow_pages_separate(self):
        value = ('1000-3000 r-xp 00000000 00:01 1 /fixture/extension.so\n'
                 'Rss: 8 kB\nAnonymous: 0 kB\n'
                 '3000-5000 rw-p 00002000 00:01 1 /fixture/extension.so\n'
                 'Rss: 8 kB\nAnonymous: 4 kB\n'
                 '5000-6000 rw-p 00000000 00:00 0 [heap]\n'
                 'Rss: 4 kB\nAnonymous: 4 kB\n'
                 '6000-7000 r--p 00000000 00:01 2 /fixture/other.so\n'
                 'Rss: 4 kB\nAnonymous: 0 kB\n')
        result = observer.parse_smaps(value.splitlines(True), Path('/fixture/extension.so'))
        self.assertEqual(result['extension_writable'], dict(rss_bytes=8192, anonymous_bytes=4096))
        self.assertEqual(result['extension_exec'], dict(rss_bytes=8192, anonymous_bytes=0))
        self.assertEqual(result['nonfile'], dict(rss_bytes=4096, anonymous_bytes=4096))
        self.assertEqual(result['other_file'], dict(rss_bytes=4096, anonymous_bytes=0))
        self.assertNotIn('/fixture', str(result))

    def test_missing_duplicate_invalid_and_oversized_records_fail(self):
        header = '1000-2000 r-xp 00000000 00:01 1 /fixture/extension.so\n'
        for suffix in ('Rss: 4 kB\n', 'Rss: 4 kB\nRss: 4 kB\nAnonymous: 0 kB\n',
                       'Rss: 4 kB\nAnonymous: 8 kB\n', 'Rss: nan kB\nAnonymous: 0 kB\n',
                       'Rss: 4 B\nAnonymous: 0 kB\n', 'X' * 8193):
            with self.subTest(suffix=suffix[:30]), self.assertRaises(ValueError):
                observer.parse_smaps((header + suffix).splitlines(True), Path('/fixture/extension.so'))

    def test_page_presence_does_not_export_pfns(self):
        values = struct.pack('=QQQ', 1 << 63, (1 << 63) | 987654, 987654)
        self.assertEqual(observer.present_offsets(values, 8192, 4096), [8192, 12288])
        with self.assertRaises(ValueError): observer.present_offsets(values[:-1], 0, 4096)

    def test_short_read_is_not_empty_observation(self):
        value = observer.QueryPageObserver.__new__(observer.QueryPageObserver)
        value.regions = [(4096, 8192, 0, 'r-xp')]
        value.page_size, value.fd, value.views = 4096, 99, [[memoryview(bytearray(8))]]
        with patch.object(observer.os, 'preadv', return_value=0):
            with self.assertRaises(RuntimeError): value.capture(0)

    def test_incomplete_composition_and_page_accounting_are_rejected(self):
        observer.validate_page_observation(self.example())
        for mutate in (lambda v: v['after'].pop('nonfile'),
                       lambda v: v['after']['nonfile'].__setitem__('anonymous_bytes', 1),
                       lambda v: v['extension_pages'][0].__setitem__('newly_present', [12289]),
                       lambda v: v['extension_pages'][0].__setitem__('present_after', 1),
                       lambda v: v['extension_pages'][0].__setitem__('permissions', '---p')):
            value = self.example()
            mutate(value)
            with self.assertRaises(ValueError): observer.validate_page_observation(value)

    def test_control_and_plain_readers_cannot_hide_page_observations(self):
        value = self.example()
        control = copy.deepcopy(value)
        control['extension_pages'][0].update(present_before=2, newly_present=[])
        events = [dict(page_observer_control=control), dict(query_pages=value), {}]
        observer.validate_page_events(events, dict(observer_mode='counters'))
        with self.assertRaises(ValueError): observer.validate_page_events(events, dict(observer_mode='plain'))
        with self.assertRaises(ValueError): observer.validate_page_events([{}, dict(query_pages=value), {}], dict(observer_mode='counters'))
        control['extension_pages'][0].update(present_before=1, newly_present=[12288])
        with self.assertRaises(ValueError): observer.validate_page_events(events, dict(observer_mode='counters'))

    def test_prefault_reads_exactly_one_byte_per_readable_extension_page(self):
        maps = ('1000-3000 r-xp 00000000 00:01 1 /fixture/extension.so\n'
                '3000-4000 ---p 00002000 00:01 1 /fixture/extension.so\n'
                '4000-5000 rw-p 00003000 00:01 1 /fixture/extension.so\n'
                '5000-6000 r--p 00000000 00:01 2 /fixture/other.so\n')
        value = observer.QueryPageObserver.__new__(observer.QueryPageObserver)
        value.regions = observer.regions(maps, Path('/fixture/extension.so'), 4096)
        value.page_size = 4096
        from types import SimpleNamespace
        fake = SimpleNamespace(from_address=lambda address: SimpleNamespace(value=address // 4096))
        with patch.object(fake, 'from_address', wraps=fake.from_address) as access, patch.object(observer.ctypes, 'c_ubyte', fake):
            self.assertEqual(value.prefault(), 1 ^ 2 ^ 4)
            self.assertEqual([call.args[0] for call in access.call_args_list], [4096,8192,16384])

    def test_prefault_requires_complete_first_sample_and_separate_duration(self):
        page = self.example()
        control = copy.deepcopy(page)
        control['extension_pages'][0].update(present_before=2, newly_present=[])
        events = [dict(page_observer_control=control),
                  dict(query_pages=copy.deepcopy(control), prefault_pages=page, prefault_ns=10),
                  dict(query_pages=copy.deepcopy(control), prefault_pages=None, prefault_ns=None), {}]
        observer.validate_page_events(events, dict(observer_mode='prefault'))
        mutations = [lambda e: e[1].__setitem__('prefault_pages', None),
                     lambda e: e[1].__setitem__('prefault_ns', 0),
                     lambda e: e[1]['prefault_pages']['extension_pages'][0].update(present_after=1,newly_present=[]),
                     lambda e: e[2].update(prefault_pages=copy.deepcopy(page),prefault_ns=10)]
        for mutate in mutations:
            changed = copy.deepcopy(events)
            mutate(changed)
            with self.assertRaises(ValueError): observer.validate_page_events(changed, dict(observer_mode='prefault'))
        for mode in ('plain','counters'):
            with self.assertRaises(ValueError): observer.validate_page_events(events, dict(observer_mode=mode))


if __name__ == '__main__':
    unittest.main()
