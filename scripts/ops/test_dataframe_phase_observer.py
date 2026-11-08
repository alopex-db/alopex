import copy
from types import SimpleNamespace
import unittest
from dataframe_phase_observer import DataFrameObserver, parse_io


class Rss:
    def __init__(self): self.closed=False; self.peak=40
    def begin(self,phase): self.phase=phase
    def finish(self,phase):
        assert phase==self.phase
        return dict(phase=phase,kernel_reported_peak_rss_bytes=self.peak)
    def close(self): self.closed=True


class ObserverTest(unittest.TestCase):
    def fixture(self):
        start=dict(io=dict(read_bytes=10,write_bytes=20,cancelled_write_bytes=3),waited_children=[0]*16)
        end=copy.deepcopy(start);end['io'].update(read_bytes=15,write_bytes=26,cancelled_write_bytes=4)
        return start,end

    def observer(self,start,end,rss=None):
        snapshots=iter((start,end)); cpu=iter((SimpleNamespace(ru_utime=1,ru_stime=2),SimpleNamespace(ru_utime=1.5,ru_stime=2.25)))
        return DataFrameObserver(rss_limit=100,rss=rss or Rss(),snapshot=lambda:next(snapshots),usage=lambda _:next(cpu))

    def test_deltas_and_raw_counters_are_separate(self):
        start,end=self.fixture();observer=self.observer(start,end)
        observer.begin(0);result=observer.finish(0)
        self.assertEqual(result['delta'],dict(user_cpu_seconds=.5,system_cpu_seconds=.25,read_bytes=5,write_bytes=6,cancelled_write_bytes=1))
        self.assertEqual(result['before']['io'],start['io']);self.assertEqual(result['after']['io'],end['io'])
        self.assertEqual(len(observer.completed),1);self.assertIsNone(observer.pending)
        observer.close();self.assertTrue(observer.rss.closed)

    def test_invalid_io_never_becomes_zero(self):
        for text in ('read_bytes: 0','read_bytes: -1\nwrite_bytes: 0\ncancelled_write_bytes: 0',
            'read_bytes: 0\nread_bytes: 1\nwrite_bytes: 0\ncancelled_write_bytes: 0'):
            with self.subTest(text=text),self.assertRaises(RuntimeError):parse_io(text)

    def test_decrease_child_activity_and_rss_bound_fail_with_pending_evidence(self):
        for fault in ('decrease','children','rss'):
            start,end=self.fixture();rss=Rss()
            if fault=='decrease':end['io']['read_bytes']=9
            if fault=='children':end['waited_children'][0]=1
            if fault=='rss':rss.peak=101
            observer=self.observer(start,end,rss);observer.begin(0)
            with self.subTest(fault=fault),self.assertRaises(RuntimeError):observer.finish(0)
            self.assertEqual(observer.completed,[]);self.assertIn('after',observer.pending)

    def test_duplicate_start_and_wrong_end_are_rejected(self):
        observer=self.observer(*self.fixture());observer.begin(0)
        with self.assertRaises(RuntimeError):observer.begin(0)
        with self.assertRaises(RuntimeError):observer.finish(1)


if __name__=='__main__':unittest.main()
