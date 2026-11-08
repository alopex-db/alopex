"""Tiny fake objects verify timing, disposal, close and failure boundaries."""
import ast
import copy
import inspect
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import csv_phase_measure as m
import csv_stream_profile as original
import sql_stream_profile


class Observer:
    def __init__(self, events):
        self.events, self.completed, self.pending = events, [], None

    def begin(self, index):
        assert self.pending is None and index == len(self.completed)
        self.pending = index
        self.events.append('begin')

    def finish(self, index):
        assert self.pending == index
        self.events.append('finish')
        value = dict(index=index)
        self.completed.append(value)
        self.pending = None
        return value


class Contracts(unittest.TestCase):
    def test_plan_disposal_stays_inside_timer_and_output_outside_observation(self):
        events, rows = [], []
        observer = Observer(events)
        class Plan:
            def __del__(self): events.append('dispose')
        def clock():
            events.append('clock')
            return 1.0
        def record(row):
            self.assertIsNone(observer.pending)
            events.append('record');rows.append(row)
        with patch.object(original.time, 'perf_counter', side_effect=clock):
            m.measure_plan_or_collect(Plan,'plan_sample',warmups=1,samples=1,observer=observer,record=record)
        self.assertEqual(events,['begin','clock','dispose','clock','finish','record']*2)
        self.assertEqual([r['phase'] for r in rows],['warmup','measure'])
        self.assertEqual([r['resources']['index'] for r in rows],[0,1])

    def test_stream_counts_all_batches_and_observes_close_after_original_end(self):
        events, rows = [], []
        observer = Observer(events)
        class Stream:
            def __init__(self): self.batch = iter([SimpleNamespace(height=2),SimpleNamespace(height=1)])
            def __iter__(self): return self
            def __next__(self): events.append('next');return next(self.batch)
            def __enter__(self): events.append('enter');return self
            def __exit__(self,*args): events.append('close')
        build = lambda: SimpleNamespace(collect_batches=lambda **kwargs: Stream())
        clocks=iter([1.0,1.001,1.003])
        def clock(): events.append('inner-clock');return next(clocks)
        outer=iter([1_000_000_000,1_004_000_000])
        def outer_clock(): events.append('outer-clock');return next(outer)
        usage=SimpleNamespace(ru_utime=1.,ru_stime=.5,ru_inblock=0,ru_oublock=0)
        def get_usage(*args): events.append('usage');return usage
        def validate(*args): events.append('validate');return sql_stream_profile.validate_result(*args)
        def record(row): events.append('record');rows.append(row)
        with patch.object(original.time,'perf_counter',side_effect=clock), \
             patch.object(m.time,'perf_counter_ns',side_effect=outer_clock), \
             patch.object(m.resource,'getrusage',side_effect=get_usage), \
             patch.object(m,'validate_result',side_effect=validate):
            m.measure_stream(lambda: (*original.consume_alopex(build),None),[0,1,2],
                             warmups=0,samples=1,observer=observer,record=record)
        self.assertEqual(events,['begin','usage','outer-clock','inner-clock','enter','next',
            'inner-clock','next','next','inner-clock','close','outer-clock','usage','finish','validate','record'])
        self.assertEqual((rows[0]['rows'],rows[0]['first_chunk_rows']), (3,2))
        self.assertEqual((rows[0]['first_ns'],rows[0]['total_ns'],rows[0]['outside_original_total_ns']),
                         (1_000_000,3_000_000,1_000_000))

    def test_failure_never_emits_a_successful_sample(self):
        for phase in ('plan','stream'):
            events,rows=[],[];observer=Observer(events)
            def fail(): raise RuntimeError('operation failed')
            with self.subTest(phase=phase),self.assertRaisesRegex(RuntimeError,'operation failed'):
                if phase=='plan':
                    m.measure_plan_or_collect(fail,'plan_sample',warmups=0,samples=1,observer=observer,record=rows.append)
                else:m.measure_stream(fail,[0],warmups=0,samples=1,observer=observer,record=rows.append)
            self.assertEqual(rows,[]);self.assertEqual(observer.completed,[]);self.assertEqual(observer.pending,0)

    def test_incomplete_stream_is_rejected_after_observation_before_output(self):
        observer=Observer([]);rows=[]
        with self.assertRaisesRegex(RuntimeError,'incomplete stream'):
            m.measure_stream(lambda:(.001,.002,1,1,None),[0,1],warmups=0,samples=1,observer=observer,record=rows.append)
        self.assertEqual(len(observer.completed),1);self.assertEqual(rows,[])

    def test_observer_failure_prevents_output(self):
        rows=[];observer=Observer([])
        with patch.object(observer,'finish',side_effect=RuntimeError('counter missing')):
            with self.assertRaisesRegex(RuntimeError,'counter missing'):
                m.measure_stream(lambda:(.001,.002,1,1,None),[0],warmups=0,samples=1,observer=observer,record=rows.append)
        self.assertEqual(rows,[])

    def test_stream_timer_statements_match_existing_driver(self):
        old=ast.parse(inspect.getsource(sql_stream_profile.measure)).body[0].body[0].body
        new=ast.parse(inspect.getsource(m.measure_stream)).body[0].body[1].body
        # All statements between the usage snapshots are retained verbatim.
        self.assertEqual([ast.dump(x) for x in old[:5]], [ast.dump(x) for x in new[2:7]])


if __name__=='__main__':unittest.main()
