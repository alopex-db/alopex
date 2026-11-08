"""Small fake-stream contracts for stage boundaries and failure handling."""
import copy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import csv_stage_measure as m
import csv_stage_contract as contract
from test_csv_phase_measure import Observer


class Stages(unittest.TestCase):
    def build(self, events, failure=None, heights=(2,1)):
        class Stream:
            def __init__(self): self.rows=iter(heights)
            def __enter__(self): events.append('enter'); return self
            def __exit__(self,*args): events.append('close')
            def __iter__(self): return self
            def __next__(self):
                events.append('next')
                if failure: raise RuntimeError('read failure')
                return SimpleNamespace(height=next(self.rows))
            def __del__(self): events.append('stream-drop')
        class Plan:
            def collect_batches(self, **kwargs):
                assert kwargs == {'chunk_size':8192}
                events.append('open'); return Stream()
            def __del__(self): events.append('plan-drop')
        def build(): events.append('plan'); return Plan()
        return build

    def test_every_batch_close_and_disposal_are_between_the_expected_clocks(self):
        events=[];tick=iter(range(7))
        def clock(): events.append('clock'); return next(tick)*10
        row=m.consume_stages(self.build(events),clock)
        self.assertEqual(events,['clock','plan','clock','open','plan-drop','enter','clock','next','clock',
            'next','next','clock','close','clock','stream-drop','clock'])
        self.assertEqual(row['rows'],3)
        self.assertEqual(row['stages_ns'],dict.fromkeys(m.STAGES,10))
        self.assertEqual(row['total_ns'],60)

    def test_read_failure_closes_stream_without_success_sample(self):
        events=[];rows=[];observer=Observer(events)
        with self.assertRaisesRegex(RuntimeError,'read failure'):
            m.measure(self.build(events,True),3,warmups=0,samples=1,observer=observer,record=rows.append)
        self.assertIn('close',events);self.assertEqual(rows,[]);self.assertEqual(observer.pending,0)

    def test_incomplete_stream_and_counter_failure_never_emit_success(self):
        for counter_failure in (False,True):
            rows=[];observer=Observer([])
            if counter_failure: observer.finish=lambda _: (_ for _ in ()).throw(RuntimeError('counter missing'))
            with self.assertRaises(RuntimeError):
                m.measure(self.build([],heights=(1,)),3,warmups=0,samples=1,observer=observer,record=rows.append)
            self.assertEqual(rows,[])

    def test_invalid_clock_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'invalid stage clock'):
            m.consume_stages(self.build([]),lambda:1)

    def test_inventory_and_raw_stage_corruption_rejected(self):
        cells=contract.plan('diagnostic');self.assertEqual(len(cells),6)
        self.assertEqual([x['subject'] for x in cells],['before','candidate','candidate','before','before','candidate'])
        cell=dict(cells[0],warmups=0,samples=1)
        row=dict(m.consume_stages(self.build([],heights=(3,)),iter(range(7)).__next__),event='stages',iteration=0,phase='measure',resources={})
        result=dict(rows=3,engine='alopex',warmups=0,samples=1,resource_observations=1,
            validation_before='a'*64,validation_after='a'*64,result_sha256='a'*64,input_sha256='b'*64)
        observed=dict(completed=[{}],pending=None)
        with patch.object(contract,'verify_resources'):
            contract.verify_cell(cell,result,[row],observed,100)
            for field,value in [('rows',2),('total_ns',7),('points_ns',[0]*7),('iteration',1)]:
                bad=copy.deepcopy(row);bad[field]=value
                with self.assertRaises(RuntimeError):contract.verify_cell(cell,result,[bad],observed,100)
            with self.assertRaises(RuntimeError):contract.verify_cell(cell,result,[],observed,100)


if __name__=='__main__':unittest.main()
