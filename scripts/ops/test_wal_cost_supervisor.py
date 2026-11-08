from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

BASE=Path(__file__).resolve().parent
for root in (BASE.parent.parent/'alopex',BASE.parent.parent):
    for directory in ('scripts/test-env','scripts/performance'):
        if (root/directory).is_dir():sys.path.insert(0,str(root/directory))
from wal_cost_supervisor import sample
from phase_rss import PhaseRssError


class Child:
    pid=123
    returncode=None
    def __init__(self,exit_code=0):self.exit_code=exit_code
    def wait(self,timeout):
        if self.exit_code is None:raise subprocess.TimeoutExpired('fixture',timeout)
        self.returncode=self.exit_code;return self.returncode


class Supervision(unittest.TestCase):
    def read(self,text,child=None):
        with patch.object(Path,'read_text',return_value=text):return sample(child or Child())

    def test_live_valid_sample_is_retained(self):
        self.assertEqual(self.read('VmRSS: 4 kB\nVmHWM: 8 kB\n'),(4096,None))

    def test_terminal_without_mm_requires_reaped_exit(self):
        value,record=self.read('Pid: 123\nState: Z (zombie)\n')
        self.assertIsNone(value);self.assertTrue(record['reaped']);self.assertEqual(record['returncode'],0)

    def test_unavailable_live_sample_still_fails(self):
        with self.assertRaisesRegex(RuntimeError,'live process'):
            self.read('Pid: 123\nState: R (running)\n',Child(None))

    def test_partial_malformed_and_inconsistent_never_become_terminal(self):
        for text in ('VmRSS: 4 kB\n','VmHWM: 4 kB\n','VmRSS: no kB\nVmHWM: 4 kB\n','VmRSS: 8 kB\nVmHWM: 4 kB\n'):
            with self.subTest(text=text),self.assertRaises(PhaseRssError):self.read(text)

    def test_wrong_identity_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'identity'):self.read('Pid: 124\nState: Z\n')

    def test_gone_record_retains_nonzero_exit_for_parent_rejection(self):
        with patch.object(Path,'read_text',side_effect=FileNotFoundError):
            value,record=sample(Child(7))
        self.assertIsNone(value);self.assertEqual(record['returncode'],7)


if __name__=='__main__':unittest.main()
