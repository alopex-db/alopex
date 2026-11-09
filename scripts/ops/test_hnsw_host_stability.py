"""The diagnostic reports missing host counters without inventing values."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import hnsw_host_stability as stability


class StabilityTests(unittest.TestCase):
    def test_missing_optional_counters_are_explicit(self):
        with patch.object(stability, '_text', return_value=None), \
             patch.object(stability.resource, 'getrusage',
                          return_value=SimpleNamespace(ru_utime=1.5, ru_stime=0.25)), \
             patch.object(stability.time, 'monotonic_ns', return_value=123):
            result = stability.snapshot(0)
        self.assertEqual(result['monotonic_ns'], 123)
        self.assertEqual(result['pinned_cpu'], 0)
        self.assertIsNone(result['cpu_ticks'])
        self.assertIsNone(result['cpu_stat'])
        self.assertIsNone(result['frequency_khz'])
        self.assertIsNone(result['frequency_residency'])
        self.assertIsNone(result['context_switches'])
        self.assertEqual(result['self_user_cpu_seconds'], 1.5)

    def test_parses_narrow_cpu_and_context_counters(self):
        data = {
            '/proc/stat': 'cpu 10 20 30 40 50 60 70 80\ncpu7 1 2 3 4 5 6 7 8\n',
            '/proc/self/status': 'voluntary_ctxt_switches:\t12\nnonvoluntary_ctxt_switches:\t3\n',
            '/sys/fs/cgroup/cpu.stat': 'usage_usec 90\nnr_throttled 2\n',
            '/sys/devices/system/cpu/cpu7/cpufreq/scaling_cur_freq': '3000000\n',
            '/sys/devices/system/cpu/cpu7/cpufreq/stats/time_in_state': '3000000 4\n2000000 6\n',
        }
        with patch.object(stability, '_text', side_effect=lambda path: data.get(path)), \
             patch.object(stability.resource, 'getrusage',
                          return_value=SimpleNamespace(ru_utime=1, ru_stime=2)), \
             patch.object(stability.time, 'monotonic_ns', return_value=9):
            result = stability.snapshot(7)
        self.assertEqual(result['cpu_ticks'], [1, 2, 3, 4, 5, 6, 7, 8])
        self.assertEqual(result['context_switches']['nonvoluntary_ctxt_switches'], 3)
        self.assertEqual(result['cpu_stat']['nr_throttled'], 2)
        self.assertEqual(result['frequency_khz'], 3000000)
        self.assertEqual(result['frequency_residency'], {'3000000': 4, '2000000': 6})


if __name__ == '__main__':
    unittest.main()
