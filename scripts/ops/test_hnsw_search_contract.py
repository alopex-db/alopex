"""Tiny clocks and fake neighbors verify HNSW evidence without a load test."""
import ast
import copy
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import hnsw_search_contract as c
import hnsw_search_measure as m


class Queries:
    shape = (2, 128)
    def __len__(self): return 2
    def __getitem__(self, index): return index


class ContractTests(unittest.TestCase):
    def test_fixed_matrix_preserves_every_engine_ef_trial_and_work_floor(self):
        full = c.plan('full')
        self.assertEqual(len(full), 15)
        self.assertEqual(len({r['id'] for r in full}), 15)
        for row in full:
            self.assertEqual((row['rows'], row['dimension'], row['query_vectors']), (9171,128,200))
            self.assertEqual((row['m'], row['ef_construction'], row['seed'], row['k']), (16,200,42,10))
            self.assertEqual((row['min_queries'], row['min_seconds']), (10000,2.0))
            self.assertEqual(row['ef_values'], [16,32,64,128,256])
        self.assertEqual(len(c.plan('preflight')), 5)
        with self.assertRaises(RuntimeError): c.plan('fast')

    def test_only_raw_return_field_differs_from_original_timer(self):
        here = Path(__file__).resolve().parent
        candidates = [here/'hnsw_harness/hnsw_v0811_contract.py',
            here.parent.parent/'alopex/scripts/performance/hnsw_v0811_contract.py']
        original = next(p for p in candidates if p.is_file())
        def fn(path):
            tree = ast.parse(path.read_text())
            return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_measure_prepared_run')
        old, new = fn(original), fn(Path(m.__file__))
        result = new.body[-1].value
        self.assertEqual(result.keys[0].value, 'query_latencies_us')
        self.assertEqual(ast.dump(result.values[0]), ast.dump(ast.Name(id='latencies', ctx=ast.Load())))
        result.keys.pop(0);result.values.pop(0)
        self.assertEqual(ast.dump(old), ast.dump(new))

    def test_both_query_and_duration_minima_are_required(self):
        for minimum, seconds, clocks, count in [
            (3, 0, [0, 1, 1], 3),
            (1, 2, [0, 1, 2, 2], 2),
        ]:
            calls = []
            engine = SimpleNamespace(name='hnswlib', search=lambda q,k,e:calls.append((q,k,e)))
            nanos = iter(range(100, 10000, 100))
            with patch.object(m.time,'perf_counter',side_effect=clocks), \
                 patch.object(m.time,'perf_counter_ns',side_effect=lambda:next(nanos)):
                row = m._measure_prepared_run(engine,Queries(),(1.0,1.0,None),ef_search=16,
                    min_queries=minimum,run=1,k=10,min_duration_seconds=seconds,dataset_size=32)
            self.assertEqual(len(calls), count)
            self.assertEqual(row['query_count'], count)
            self.assertEqual(row['query_latencies_us'], [0.1]*count)
            self.assertEqual([v[0] for v in calls], [i%2 for i in range(count)])

    def test_original_build_and_preparation_unchanged_except_diagnostic_retention(self):
        here=Path(__file__).resolve().parent
        original=ast.parse((here/'hnsw_harness/hnsw_v0811_contract.py').read_text())
        observed=ast.parse((here/'hnsw_search_build.py').read_text())
        def fn(tree,name):return copy.deepcopy(next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name))
        old,new=fn(original,'build_alopex'),fn(observed,'build_alopex')
        self.assertEqual(len(new.body),len(old.body)+3)
        added=next(n for n in new.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)
            and n.targets[0].id=='native_observations')
        self.assertEqual(ast.dump(added.value),ast.dump(ast.List(elts=[],ctx=ast.Load())))
        new.body.remove(added)
        native=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name=='native_search_time_us')
        retained=native.body.pop(1)
        self.assertEqual(retained.value.func.value.id,'native_observations')
        self.assertEqual(retained.value.func.attr,'append')
        self.assertEqual({k.arg for k in retained.value.args[0].keywords},
            {'nodes_visited','distance_computations','search_time_us'})
        self.assertEqual(new.body[-1].value.id,'engine')
        self.assertEqual(new.body[-2].targets[0].attr,'native_observations')
        new.body=new.body[:-2]
        new.body[-1]=ast.Return(value=new.body[-1].value)
        self.assertEqual(ast.dump(old),ast.dump(new))
        old,new=fn(original,'_prepare_setting'),fn(observed,'_prepare_setting')
        self.assertEqual(new.body[-1].value.elts[1].id,'predicted')
        new.body[-1].value=new.body[-1].value.elts[0]
        self.assertEqual(ast.dump(old),ast.dump(new))

    def test_predictions_reject_duplicate_missing_out_of_range_and_wrong_types(self):
        valid = [list(range(10))]
        self.assertEqual(c.validate_predictions(valid,valid,valid,rows=32)['recall_at_10'],1)
        for bad in [[], [list(range(9))], [[0]*10], [[True,*range(1,10)]],
                    [[32,*range(1,10)]], [[-1,*range(1,10)]]]:
            with self.assertRaises(RuntimeError): c.validate_predictions(bad,valid,valid,rows=32)

    def test_ties_and_low_recall_remain_visible(self):
        truth = [list(range(10))]
        acceptable = [list(range(11))]
        prediction = [[10,*range(1,10)]]
        self.assertEqual(c.validate_predictions(prediction,truth,acceptable,rows=32),
            dict(recall_at_10=.9,tie_aware_recall_at_10=1.0))

    def test_summary_and_recall_recomputed_from_raw_before_and_after(self):
        cell=c.plan('preflight')[0];cell['engine']='hnswlib'
        truth=[list(range(10)) for _ in range(4)]
        predictions=dict(before=truth,after=truth)
        row=dict(engine='hnswlib',ef_search=16,run=1,dataset_size=32,dimension=128,
            query_count=4,duration_seconds=1.0,query_latencies_us=[1.,2.,3.,4.],
            queries_per_second=4.,latency_us=250000.,query_latency_p50_us=2.5,
            query_latency_p95_us=4.,query_latency_p99_us=4.,recall_at_10=1.,
            tie_aware_recall_at_10=1.,native_search_latency_us=None,python_binding_residual_us=None)
        c.verify_point(cell,row,predictions,truth,truth)
        for key,value in [('query_count',3),('duration_seconds',float('nan')),
            ('query_latencies_us',[1.,2.,3.]),('latency_us',3.),('query_latency_p95_us',2.),
            ('recall_at_10',.9),('ef_search',8),('run',2),('python_binding_residual_us',0.)]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                c.verify_point(cell,dict(row,**{key:value}),predictions,truth,truth)
        changed=copy.deepcopy(predictions);changed['after'][0][0]=20
        with self.assertRaises(RuntimeError):c.verify_point(cell,row,changed,truth,truth)

    def test_fastest_at_recall_requires_all_trials_not_a_pooled_average(self):
        rows=[dict(trial=t,ef_search=e,latency_us=float(e),query_latency_p50_us=float(e)/2,
            queries_per_second=1e6/e,recall_at_10=1.)
            for e in c.EF_VALUES for t in range(3)]
        rows[0]['recall_at_10']=.98
        self.assertEqual(c.same_recall_point(rows,.99)['ef_search'],32)
        self.assertEqual(c.same_recall_point(rows,.99)['latency_us'],16.)
        self.assertEqual(c.same_recall_point(rows,.99)['mean_latency_us'],32.)
        self.assertEqual(c.same_recall_point(rows,.95)['ef_search'],16)
        for row in rows:row['recall_at_10']=.9
        self.assertIsNone(c.same_recall_point(rows,.99))
        with self.assertRaises(RuntimeError):c.same_recall_point(rows[:-1],.99)
        rows[0]['trial']=1
        with self.assertRaises(RuntimeError):c.same_recall_point(rows,.99)

    def test_worker_rejects_normal_ci_before_reading_configuration_or_loading_product(self):
        env=dict(os.environ,CI='true',GITHUB_EVENT_NAME='push')
        env.pop('ALOPEX_MANUAL_HNSW_SEARCH',None)
        result=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('hnsw_search_worker.py')),
            '--configuration','not-created.json'],env=env,capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('manual HNSW boundary required',result.stderr)
        self.assertNotIn('FileNotFoundError',result.stderr)


if __name__=='__main__':unittest.main()
