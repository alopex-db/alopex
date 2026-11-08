import copy
import unittest
from hnsw_search_sampling import measure_wrapper,verify_start_parameters
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
            initial_paused=True,product_worker_modified=False,query_loop_modified=False,performance_acceptance=False)
        verify_sampling(record,cell,digest)
        for key,value in [('calls',record['calls'][:-1]),('collector_sha256','b'*64),('query_loop_modified',True),('initial_paused',False)]:
            bad=copy.deepcopy(record);bad[key]=value
            with self.assertRaises(ValueError):verify_sampling(bad,cell,digest)
        bad=copy.deepcopy(record);bad['calls'][0]['sampling']=True
        with self.assertRaises(ValueError):verify_sampling(bad,cell,digest)

    def test_collector_starts_paused_without_tracing_or_child_follow(self):
        command=collector_command('gprofng','sample.er',['python','worker.py'])
        self.assertEqual(command[command.index('-y')+1],'SIGUSR2')
        self.assertEqual(command[command.index('-F')+1],'off')
        self.assertEqual(command[command.index('-a')+1],'off')
        self.assertEqual(command[-2:],['python','worker.py'])

    def test_driver_must_send_a_single_paused_start_descriptor(self):
        import signal
        valid='p:10000;d:'+str(int(signal.SIGUSR2))+'p;'
        for mode in ('paused','sampled'):
            self.assertTrue(verify_start_parameters(mode,valid))
            for invalid in (None,'p:10000;',valid.replace('p;',';',1),
                            valid+'d:0;',valid.replace('p;','r;'),valid.replace('d:','x:')):
                with self.assertRaises(ValueError):verify_start_parameters(mode,invalid)

    def test_baseline_rejects_inherited_collector_parameters(self):
        self.assertFalse(verify_start_parameters('baseline',None))
        with self.assertRaises(ValueError):verify_start_parameters('baseline','p:10000;')

    def test_missing_full_profile_is_not_zero_or_success(self):
        from hnsw_search_sampling_contract import verify_profile
        log='<event kind="resume"/><event kind="resume"/>'
        with self.assertRaises(ValueError):verify_profile('sampled','full',log,0,None)
        self.assertIsNone(verify_profile('sampled','preflight',log,0,None)['observed_cpu_seconds'])
        with self.assertRaises(ValueError):verify_profile('sampled','full',log,12,'no data')
        self.assertEqual(verify_profile('sampled','full',log,12,'<Total> 1.23')['observed_cpu_seconds'],1.23)

    def test_paused_experiment_rejects_unexpected_samples(self):
        from hnsw_search_sampling_contract import verify_profile
        self.assertEqual(verify_profile('paused','full','',0,None)['sample_status'],'disabled')
        with self.assertRaises(ValueError):verify_profile('paused','full','',12,'<Total> 1.23')
        with self.assertRaises(ValueError):verify_profile('paused','full','<event kind="resume"/>',0,None)

    def test_builder_and_sampler_have_no_unbound_globals(self):
        import builtins,symtable
        from pathlib import Path
        import gprofng_collector_build,hnsw_search_sampling,hosted_hnsw_search_sampling
        for module in (gprofng_collector_build,hnsw_search_sampling,hosted_hnsw_search_sampling):
            source=Path(module.__file__).read_text()
            table=symtable.symtable(source,module.__name__,'exec')
            bound={s.get_name() for s in table.get_symbols() if s.is_assigned() or s.is_imported()}
            bound.update(dir(builtins));bound.update(['__name__','__file__'])
            pending=list(table.get_children());missing=set()
            while pending:
                child=pending.pop();pending.extend(child.get_children())
                missing.update(s.get_name() for s in child.get_symbols()
                    if s.is_global() and s.is_referenced() and s.get_name() not in bound)
            self.assertEqual(missing,set(),module.__name__)
        # The original extraction defect must be caught without a native build.
        source=Path(gprofng_collector_build.__file__).read_text().replace('import os,shutil','import shutil')
        table=symtable.symtable(source,'missing-import','exec')
        bound={s.get_name() for s in table.get_symbols() if s.is_assigned() or s.is_imported()}
        child=next(c for c in table.get_children() if c.get_name()=='build_collector')
        self.assertIn('os',{s.get_name() for s in child.get_symbols() if s.is_global() and s.is_referenced() and s.get_name() not in bound})

if __name__=='__main__':unittest.main()
