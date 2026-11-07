import copy
import unittest

import hosted_hnsw_stage as stage


def config(rows=1000):
    product=dict(platform='fixture',example_source_sha256='example',rust_version='fixture',
                 parser_contract='fixture',parser_sha256='parser')
    seconds={1000:120,10000:300,100000:900}[rows]
    return dict(schema='alopex.hnsw-stage-input/v1',subjects={'before':dict(product),'candidate':dict(product)},
                rows=rows,cpu_count=2,limits=dict(case_seconds=seconds,rss_bytes=4*1024**3,runtime_bytes=5_000_000_000),
                stage_seconds=10*(seconds+30)+120,instrument=dict(example_source_sha256='example'),
                previous_stage=None if rows==1000 else dict(run=1))


class StageControlTests(unittest.TestCase):
    def test_no_automatic_scale_expansion(self):
        for size in (1000,10000,100000):
            value=config(size)
            self.assertEqual(stage.validate_config(value),value)
            called=[]
            rows=stage.run_samples(value,lambda cell:called.append(cell) or {'verified':True},lambda rows:None,clock=lambda:0)
            self.assertEqual(len(rows),10)
            self.assertTrue(all(row['status']=='passed' for row in rows))
            self.assertEqual({row['rows'] for row in called},{size})
            self.assertEqual([r['sample'] for r in rows if r['subject']=='candidate'],list(range(5)))

    def test_mismatched_conditions_or_missing_previous_stage_rejected(self):
        for change in ('profile','dimension_source','memory','duration','cpus','previous'):
            value=config(10000)
            if change=='profile':value['subjects']['candidate']['rust_version']='other'
            if change=='dimension_source':value['instrument']['example_source_sha256']='other'
            if change=='memory':value['limits']['rss_bytes']*=2
            if change=='duration':value['stage_seconds']+=1
            if change=='cpus':value['cpu_count']=True
            if change=='previous':value['previous_stage']=None
            with self.assertRaises(ValueError):stage.validate_config(value)

    def test_first_failure_stops_new_samples_and_preserves_original_failure(self):
        called,saved=[],[]
        def execute(cell):
            called.append(cell)
            if len(called)==3:raise ValueError('bad sample')
            return {'verified':True}
        rows=stage.run_samples(config(),execute,lambda value:saved.append(copy.deepcopy(value)),clock=lambda:0)
        self.assertEqual(len(called),3)
        self.assertEqual([r['status'] for r in rows],['passed','passed','failed']+['not_run']*7)
        self.assertIn('bad sample',rows[2]['error'])
        self.assertEqual(saved[-1],rows)

    def test_cancel_preserves_attempt_and_is_not_swallowed(self):
        saved=[]
        def execute(cell):raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            stage.run_samples(config(),execute,lambda value:saved.append(copy.deepcopy(value)),clock=lambda:0)
        self.assertEqual(saved[-1][0]['status'],'failed')
        self.assertEqual(len(saved[-1]),1)

    def test_insufficient_remaining_time_never_starts_a_sample(self):
        ticks=iter([0,10000])
        rows=stage.run_samples(config(),lambda cell:self.fail('unexpected sample'),lambda rows:None,clock=lambda:next(ticks))
        self.assertEqual(len(rows),10)
        self.assertTrue(all(row['status']=='not_run' for row in rows))


if __name__=='__main__':unittest.main()
