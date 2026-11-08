"""Bounded fake-process tests; no product workload or network is executed."""
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE=Path(__file__).resolve().parent
for root in (BASE.parent.parent/'alopex',BASE.parent.parent):
    for directory in ('scripts/test-env','scripts/performance'):
        if (root/directory).is_dir():sys.path.insert(0,str(root/directory))
import hosted_dataframe_comparison as runner


class Child:
    pid=123
    def __init__(self,code=0):self.returncode=None;self.code=code
    def poll(self):return self.returncode
    def wait(self,timeout):self.returncode=self.code;return self.code


class Budget:
    def __init__(self,fail=False):self.calls=0;self.fail=fail
    def check(self):
        self.calls+=1
        if self.fail and self.calls>1:raise RuntimeError('capacity failure')


class SupervisionTests(unittest.TestCase):
    def execute(self,*,code=0,rss=8192,limit=16384,timeout=30,capacity_failure=False,counter_failure=False):
        child=Child(code);signals=[]
        def terminate(pid,signum):
            self.assertEqual(pid,child.pid);signals.append(signum);child.returncode=-signum
        def read(path,*args,**kwargs):
            if str(path).startswith('/proc/'):
                if counter_failure:raise FileNotFoundError('counter unavailable')
                return f'VmRSS: {rss//1024} kB\nVmHWM: {rss//1024} kB\n'
            return original(path,*args,**kwargs)
        original=Path.read_text
        with tempfile.TemporaryDirectory() as temp:
            directory=Path(temp)
            with patch.object(runner.subprocess,'Popen',return_value=child),patch.object(runner.os,'killpg',side_effect=terminate),patch.object(Path,'read_text',read):
                try:
                    runner.run_cell(['fixture'],{},directory,Budget(capacity_failure),timeout,limit)
                    error=None
                except RuntimeError as exc:error=str(exc)
            import json
            evidence=json.loads((directory/'supervision.json').read_text())
        self.assertTrue(evidence['reaped']);self.assertIsNotNone(child.returncode)
        return error,signals,evidence

    def test_complete_worker_is_reaped_without_kill(self):
        error,signals,report=self.execute()
        self.assertIsNone(error);self.assertEqual(signals,[]);self.assertEqual(report['returncode'],0)

    def test_nonzero_exit_is_failure(self):
        error,signals,_=self.execute(code=7)
        self.assertEqual(error,'worker failed');self.assertEqual(signals,[])

    def test_timeout_capacity_rss_and_counter_failure_stop_owned_process(self):
        for kwargs,expected in [({'timeout':0},'cell deadline exceeded'),({'capacity_failure':True},'capacity failure'),
            ({'rss':32768},'live process RSS bound exceeded'),({'counter_failure':True},'live process counters disappeared')]:
            with self.subTest(kwargs=kwargs):
                error,signals,_=self.execute(**kwargs)
                self.assertEqual(error,expected);self.assertEqual(signals,[signal.SIGTERM])


if __name__=='__main__':unittest.main()
