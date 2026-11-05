"""Tiny regression fixtures for live permission and kernel-limit verification."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import Failure
from verify_environment import cpu_set, mount_for, ownership, resource_limits


class Mounts(unittest.TestCase):
    def test_readonly_bind_does_not_inherit_superblock_writability(self):
        table = ('1 0 1:1 / / rw - overlay overlay rw\n'
                 '2 1 1:2 /volume /input ro,relatime - ext4 disk rw\n')
        row = mount_for('/input/server', table)
        self.assertIn('ro', row['options'])
        self.assertNotIn('rw', row['options'])
        self.assertIn('rw', row['super_options'])

    def test_mount_matching_uses_path_components_and_decodes_escapes(self):
        table = ('1 0 1:1 / / rw - overlay overlay rw\n'
                 '2 1 1:2 /volume /work noexec,rw - ext4 disk rw\n'
                 '3 1 1:3 /volume /work\\040area ro - ext4 disk rw\n')
        self.assertEqual(mount_for('/workspace/test', table)['mount'], '/')
        self.assertEqual(mount_for('/work area/test', table)['options'], ['ro'])

    def test_ambiguous_or_absent_mount_fails_closed(self):
        row = '1 0 1:1 / / rw - overlay overlay rw\n'
        for table in ('', row + row):
            with self.assertRaises(Failure):
                mount_for('/input', table)


class Ownership(unittest.TestCase):
    def test_readable_by_other_users_or_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix='alopex-test-env-') as temporary:
            path = Path(temporary) / 'data'
            path.write_bytes(b'x')
            path.chmod(0o600)
            ownership(path, os.getuid(), os.getgid(), 0o600)
            path.chmod(0o644)
            with self.assertRaises(Failure):
                ownership(path, os.getuid(), os.getgid(), 0o600)
            path.chmod(0o600)
            link = Path(temporary) / 'link'
            link.symlink_to(path)
            with self.assertRaises(Failure):
                ownership(link, os.getuid(), os.getgid(), 0o600)
            with self.assertRaises(Failure):
                ownership(path, os.getuid() + 1, os.getgid(), 0o600)


class KernelLimits(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='alopex-test-env-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        # Synthetic kernel-file contents; no processes consume these resources.
        self.request = {'limits': {'memory_bytes': 4096, 'swap_bytes': 0, 'pids': 8},
                        'cpu': {'server': [2], 'client': [3]}}
        for key, value in {'cgroup.controllers': 'memory pids cpuset', 'memory.max': '4096',
                           'memory.swap.max': '0', 'pids.max': '8',
                           'cpuset.cpus.effective': '2-3'}.items():
            (self.root / key).write_text(value)

    def verify(self, request=None, affinity=None, observed=None):
        request = request or self.request
        with patch('verify_environment.os.sched_getaffinity', return_value=affinity or {2, 3}, create=True), \
             patch('verify_environment.subprocess.check_output',
                   side_effect=observed or [json.dumps([2]), json.dumps([3])]):
            return resource_limits(request, self.root)

    def test_exact_kernel_and_child_affinity_values_are_accepted(self):
        self.assertEqual(self.verify()['limits'], self.request['limits'])

    def test_unbounded_and_mismatched_kernel_limits_are_rejected(self):
        for name in ('memory.max', 'memory.swap.max', 'pids.max'):
            original = (self.root / name).read_text()
            for wrong in ('max', '99'):
                with self.subTest(name=name, wrong=wrong):
                    (self.root / name).write_text(wrong)
                    with self.assertRaises(Failure):
                        self.verify()
            (self.root / name).write_text(original)

    def test_effective_and_observed_affinity_must_both_match(self):
        with self.assertRaises(Failure):
            self.verify(affinity={2})
        with self.assertRaises(Failure):
            self.verify(observed=['[2]', '[2]'])
        (self.root / 'cpuset.cpus.effective').write_text('2')
        with self.assertRaises(Failure):
            self.verify()

    def test_absent_cgroup_v2_is_a_prerequisite_failure(self):
        (self.root / 'cgroup.controllers').unlink()
        with self.assertRaises(Failure) as caught:
            self.verify()
        self.assertEqual(caught.exception.code, 'PREREQUISITE')

    def test_cpu_ranges_reject_malformed_or_unbounded_inputs(self):
        self.assertEqual(cpu_set('2-4,7'), {2, 3, 4, 7})
        for value in ('', '-1', '3-2', '2-', '1,,2', '0-9999999999'):
            with self.assertRaises(Failure):
                cpu_set(value)


if __name__ == '__main__':
    unittest.main()
