import copy
import unittest
from hnsw_search_sampling import measure_wrapper
from hnsw_search_sampling_contract import plan,verify_sampling,collector_command

class SamplingTests(unittest.TestCase):
    def test_original_arguments_and_result_preserved(self):
        calls=[];result=object();args=(object(),object(),object());kw={'ef_search':128,'min_queries':10000,'min_duration_seconds':2.0}
        def original(*a,**k):self.assertEqual(a,args);self.assertEqual(k,kw);calls.append('loop');return result
        records=[];wrapped=measure_wrapper(original,'sampled',lambda:calls.append('pause'),lambda:calls.append('resume'),records)
        self.assertIs(wrapped(*args,**kw),result);self.assertEqual(calls,['resume','loop','pause'])
        self.assertEqual(records,[dict(ef_search=128,sampling=True,completed=True)])

    def test_failure_pauses_and_keeps_original_error(self):
        calls=[];records=[]
        def fail(**kw):raise RuntimeError('query failure')
        with self.assertRaisesRegex(RuntimeError,'query failure'):
            measure_wrapper(fail,'sampled',lambda:calls.append('pause'),lambda:calls.append('resume'),records)(ef_search=256)
        self.assertEqual(calls,['resume','pause']);self.assertFalse(records[0]['completed'])

    def test_no_callbacks_for_controls_or_low_ef(self):
        for mode,ef in [('baseline',128),('paused',256),('sampled',64)]:
            records=[]
            def forbidden():self.fail('unexpected collector call')
            self.assertEqual(measure_wrapper(lambda **kw:7,mode,forbidden,forbidden,records)(ef_search=ef),7)
            self.assertFalse(records[0]['sampling'])

    def test_matrix_preserves_all_original_search_conditions(self):
        from hnsw_search_contract import plan as original
        for mode,count in [('preflight',6),('full',18)]:
            cells=plan(mode);self.assertEqual(len(cells),count);self.assertEqual(len({c['id'] for c in cells}),count)
            for c in cells:self.assertIn(c['normal'],original(mode))
        self.assertEqual({c['normal']['subject'] for c in plan('full')},{'before','candidate'})

    def test_missing_or_outside_window_is_rejected(self):
        cell=next(c for c in plan('full') if c['sampling_mode']=='sampled');digest='a'*64
        record=dict(mode='sampled',status='passed',collector_sha256=digest,
            calls=[dict(ef_search=ef,sampling=ef in (128,256),completed=True) for ef in [16,32,64,128,256]],
            product_worker_modified=False,query_loop_modified=False,performance_acceptance=False)
        verify_sampling(record,cell,digest)
        for key,value in [('calls',record['calls'][:-1]),('collector_sha256','b'*64),('query_loop_modified',True)]:
            bad=copy.deepcopy(record);bad[key]=value
            with self.assertRaises(ValueError):verify_sampling(bad,cell,digest)
        bad=copy.deepcopy(record);bad['calls'][0]['sampling']=True
        with self.assertRaises(ValueError):verify_sampling(bad,cell,digest)

    def test_collector_starts_paused_without_tracing_or_child_follow(self):
        command=collector_command('gprofng','sample.er',['python','worker.py'])
        self.assertIn('-y',command);self.assertNotIn(',r',command[command.index('-y')+1])
        self.assertEqual(command[command.index('-F')+1],'off')
        self.assertEqual(command[command.index('-a')+1],'off')
        self.assertEqual(command[-2:],['python','worker.py'])

if __name__=='__main__':unittest.main()
