"""Point-to-point lifetimes are checked with weak references, not a load test."""
import gzip
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
import weakref

from hnsw_search_contract import plan
from hnsw_search_point import run_point,sha


class Row(dict):pass


class PointTests(unittest.TestCase):
    def setUp(self):
        self.cell=next(c for c in plan('preflight') if c['subject']=='hnswlib')
        self.truth=dict(truth=[list(range(10)) for _ in range(4)],acceptable=[list(range(10)) for _ in range(4)])
        self.references=[];self.events=[]
        test=self
        class Observer:
            def begin(self,index):
                test.assertTrue(all(ref() is None for ref in test.references),'previous raw retained into next point')
                test.events.append(('begin',index))
            def finish(self,index):
                test.events.append(('finish',index));return dict(index=index)
        self.observer=Observer();self.engine=SimpleNamespace(search=lambda *a:list(range(10)))

    def prepare(self,*args,**kwargs):return (1.,1.,None),self.truth['truth']

    def measure(self,*args,**kwargs):
        row=Row(engine='hnswlib',ef_search=kwargs['ef_search'],run=1,dataset_size=32,dimension=128,
            query_count=4,duration_seconds=1.,queries_per_second=4.,latency_us=250000.,
            query_latencies_us=[1.,2.,3.,4.],query_latency_p50_us=2.5,query_latency_p95_us=4.,
            query_latency_p99_us=4.,recall_at_10=1.,tie_aware_recall_at_10=1.,
            native_search_latency_us=None,python_binding_residual_us=None)
        self.references.append(weakref.ref(row));return row

    def test_saved_raw_is_released_before_the_next_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)
            for index in range(2):
                receipt=run_point(self.engine,list(range(4)),self.truth,self.cell,index,self.observer,output,
                    prepare=self.prepare,measure=self.measure)
                self.assertEqual(set(receipt),{'ef_search','file','sha256','query_count'})
                self.assertTrue(all(ref() is None for ref in self.references))
                self.assertEqual(receipt['sha256'],sha(output/receipt['file']))
                with gzip.open(output/receipt['file'],'rt') as stream:saved=json.load(stream)
                self.assertEqual(saved['measurement']['query_latencies_us'],[1.,2.,3.,4.])
        self.assertEqual(self.events,[('begin',0),('finish',0),('begin',1),('finish',1)])

    def test_measurement_failure_does_not_produce_a_success_receipt(self):
        def fail(*a,**k):raise RuntimeError('injected measurement failure')
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'injected'):
                run_point(self.engine,list(range(4)),self.truth,self.cell,0,self.observer,Path(tmp),
                    prepare=self.prepare,measure=fail)
            self.assertEqual(list(Path(tmp).iterdir()),[])
        self.assertEqual(self.events,[('begin',0)])

    def test_stability_snapshot_brackets_observation_outside_timer(self):
        self.events=[]
        def snapshot():
            self.events.append(('snapshot',0))
            return {'monotonic_ns':len(self.events)}
        def measure(*args,**kwargs):
            self.events.append(('measure',0))
            return self.measure_original(*args,**kwargs)
        self.measure_original=self.measure
        with tempfile.TemporaryDirectory() as tmp:
            receipt=run_point(self.engine,list(range(4)),self.truth,self.cell,0,self.observer,Path(tmp),
                prepare=self.prepare,measure=measure,stability_snapshot=snapshot)
            with gzip.open(Path(tmp)/receipt['file'],'rt') as stream:saved=json.load(stream)
            self.assertEqual(saved['host_stability'],{'before':{'monotonic_ns':1},
                                                        'after':{'monotonic_ns':5}})
        self.assertEqual(self.events,[('snapshot',0),('begin',0),('measure',0),
                                      ('finish',0),('snapshot',0)])


if __name__=='__main__':unittest.main()
