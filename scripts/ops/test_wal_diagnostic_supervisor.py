import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import wal_diagnostic_supervisor as subject
from wal_commit_diagnostic import trace_command


def stat(parent=100, ticks=42):
    return '200 (python worker) S '+str(parent)+' '+' '.join(['0']*17+[str(ticks)])


class Contract(unittest.TestCase):
    def test_trace_command_follows_threads_and_keeps_exit_evidence(self):
        command=trace_command(['python3','-B','worker.py'],'commit.trace')
        self.assertIn('-f',command);self.assertIn('-T',command);self.assertIn('-ttt',command)
        self.assertIn('-q',command);self.assertNotIn('-qq',command)
        self.assertEqual(command[-4:],['--','python3','-B','worker.py'])
        self.assertTrue(any('pwritev2' in part for part in command))

    def test_parent_identity_is_required(self):
        subject.verify_parent(stat(),100)
        for text in (stat(999), 'garbage'):
            with self.assertRaises(ValueError):subject.verify_parent(text,100)

    def test_all_traced_threads_must_exit_successfully(self):
        text='200 10.000000 write(2, "x", 1) = 1 <0.000001>\n201 10.000001 +++ exited with 0 +++\n200 10.000002 +++ exited with 0 +++\n'
        self.assertEqual(subject.verify_lifecycle(text,{'pid':200})['processes'],2)
        for changed in (text.replace('201 10.000001 +++ exited with 0 +++\n','201 10.000001 --- SIGCHLD {} ---\n'),
                        text.replace('exited with 0','exited with 1'),
                        text+'200 10.000003 +++ exited with 0 +++\n'):
            with self.assertRaises(ValueError):subject.verify_lifecycle(changed,{'pid':200})
        with self.assertRaises(ValueError):subject.verify_lifecycle(text,{'pid':999})

    def exercise(self, *, wrong_ticks=False, high_rss=False, returncode=0, lingering=False):
        events=[]
        class Tracer:
            pid=100
            returncode=None
            def poll(self):return self.returncode
            def wait(self,timeout):self.returncode=returncode;events.append('wait');return self.returncode
        tracer=Tracer()
        class Monitor:
            identity=43 if wrong_ticks else 42
            def __init__(self,pid):events.append(('monitor',pid))
            def snapshot(self):events.append('snapshot');return {'rss_bytes':1000 if high_rss else 100}
            def close(self):events.append('close')
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)
            (directory/'tracee-ready.json').write_text(json.dumps(dict(pid=200,start_ticks=42)))
            original=Path.read_text;reads=0
            def read(path,*args,**kwargs):
                nonlocal reads
                if str(path)=='/proc/200/stat':
                    reads+=1
                    if reads==1 and not wrong_ticks or lingering:return stat()
                    raise FileNotFoundError()
                return original(path,*args,**kwargs)
            def launch(command,**kwargs):
                self.assertTrue(kwargs['start_new_session'])
                (directory/'commit.trace').write_text('200 10.000000 +++ exited with 0 +++\n')
                return tracer
            budget=SimpleNamespace(check=lambda:events.append('budget'))
            with patch.object(subject,'PhaseRss',Monitor),patch.object(Path,'read_text',read),\
                 patch.object(subject.subprocess,'Popen',launch),\
                 patch.object(subject.os,'killpg',lambda *args:events.append('kill')):
                try:
                    subject.run(['python3','worker'],{},directory,budget,30,500)
                    error=None
                except ValueError as caught:error=str(caught)
            report=json.loads((directory/'supervision.json').read_bytes())
            return error,events,report,(directory/'tracee-go').exists()

    def test_monitors_tracee_before_releasing_barrier_and_reaps_tracer(self):
        error,events,report,released=self.exercise()
        self.assertIsNone(error);self.assertTrue(released)
        self.assertIn(('monitor',200),events)
        self.assertLess(events.index('snapshot'),events.index('wait'))
        self.assertEqual(report['sampled_peak_rss_bytes'],100)
        self.assertEqual(report['live_samples'],1)
        self.assertTrue(report['tracer_reaped']);self.assertTrue(report['tracee_disappeared'])

    def test_identity_or_rss_failure_never_releases_child(self):
        for kwargs in ({'wrong_ticks':True},{'high_rss':True}):
            error,events,report,released=self.exercise(**kwargs)
            self.assertIsNotNone(error);self.assertFalse(released);self.assertIn('kill',events)
            self.assertIsNotNone(report['failure'])

    def test_child_failure_or_lingering_process_is_not_success(self):
        for kwargs in ({'returncode':1},{'lingering':True}):
            error,events,report,released=self.exercise(**kwargs)
            self.assertIsNotNone(error);self.assertTrue(report['tracer_reaped'])

    def test_cleanup_failure_does_not_replace_original_worker_error(self):
        error,events,report,released=self.exercise(returncode=1,lingering=True)
        self.assertEqual(error,'traced worker failed')
        self.assertFalse(report['cleanup_complete'])
        self.assertIn('traced worker failed',report['failure'])


if __name__=='__main__':unittest.main()
