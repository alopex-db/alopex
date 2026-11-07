import copy
import json
from pathlib import Path
import tempfile
import unittest

import hnsw_stage_contract as contract


class StageTests(unittest.TestCase):
    def test_five_samples_with_alternating_subject_order(self):
        for rows in (1000, 10000, 100000):
            cells = contract.plan(rows)
            self.assertEqual(len(cells), 10)
            self.assertEqual(len({r['id'] for r in cells}), 10)
            for i in range(5):
                self.assertEqual([r['subject'] for r in cells[2*i:2*i+2]],
                                 ['before','candidate'] if i%2==0 else ['candidate','before'])
            for subject in ('before','candidate'):
                self.assertEqual([r['sample'] for r in cells if r['subject']==subject], list(range(5)))
        for rows in (True, 1001, 0, 1000000):
            with self.assertRaises(ValueError): contract.plan(rows)

    def fixture(self, subject='candidate'):
        expected = dict(binary_sha256='binary', example_source_sha256='example')
        instrument = dict(runner_sha256='runner', phase_rss_source_sha256='observer')
        cell = next(r for r in contract.plan(1000) if r['subject']==subject)
        limits = dict(rss_bytes=4*1024**3, case_seconds=120)
        inv = dict(allocated_bytes=100, roots=['synthetic'], method='native allocated bytes fixture')
        identity = dict(source='controller', **expected, **instrument, rows=1000, dimensions=384,
                        phase_kernel_rss=True, rss_limit=limits['rss_bytes'], timeout_seconds=120,
                        initial_inventory=copy.deepcopy(inv))
        events = []
        for phase in contract.PHASES:
            start, end = dict(event='start'), dict(event='end')
            if phase == 'seed': start.update(rows=1000, dimensions=384)
            if phase in contract.TIMED_PHASES: end['elapsed_ns']=100
            if phase == 'warm': end['graph_bytes']=1024
            if phase == 'cold_transaction_1':
                start['stats']=dict(entries=0, misses=0, hits=0)
                end['stats']=dict(entries=int(subject=='candidate'), misses=1, hits=0)
            if phase == 'cold_transaction_2':
                end['stats']=dict(entries=int(subject=='candidate'), misses=1 if subject=='candidate' else 2,
                                  hits=1 if subject=='candidate' else 0)
            events.extend([dict(phase=phase,data=start),dict(phase=phase,data=end)])
        events.append(dict(phase='complete',data={}))
        observation = dict(rss_bytes=1000,high_water_bytes=1024)
        phases = [dict(phase=p,before_reset=copy.deepcopy(observation),after_reset=copy.deepcopy(observation),
                       after=copy.deepcopy(observation),kernel_reported_peak_rss_bytes=1024) for p in contract.PHASES]
        summary = dict(outcome='passed',failure=None,kernel_rss_reset=True,children_lifetime_peak_rss_bytes=None,
                       phase_kernel_rss=phases,final_inventory=copy.deepcopy(inv))
        return identity, summary, events, cell, expected, instrument, 'controller', limits

    def test_both_cache_contracts_and_all_phases(self):
        for subject in ('before','candidate'):
            result = contract.verify_sample(*self.fixture(subject))
            self.assertEqual(set(result['elapsed_ns']), set(contract.TIMED_PHASES))
            self.assertEqual(set(result['kernel_peak_rss_bytes']), set(contract.PHASES))

    def test_cache_reload_or_missing_first_load_rejected(self):
        for phase,field,value in (('cold_transaction_1','misses',0),('cold_transaction_1','entries',0),
                                  ('cold_transaction_2','misses',2),('cold_transaction_2','hits',0)):
            args=self.fixture()
            end=next(e for e in args[2] if e['phase']==phase and e['data'].get('event')=='end')
            end['data']['stats'][field]=value
            with self.assertRaises(ValueError): contract.verify_sample(*args)

    def test_source_binding_and_phase_coverage_rejected(self):
        for field in ('source','binary_sha256','runner_sha256','phase_rss_source_sha256','example_source_sha256'):
            args=self.fixture();args[0][field]='wrong'
            with self.assertRaises(ValueError):contract.verify_sample(*args)
        for removal in (0, 5, 18):
            args=self.fixture();args[2].pop(removal)
            with self.assertRaises(ValueError):contract.verify_sample(*args)

    def test_failure_peak_limit_and_measurement_failure_rejected(self):
        for change in ('outcome','peak','lifetime','inventory','kernel_coverage'):
            args=self.fixture();summary=args[1]
            if change=='outcome':summary['outcome']='failed'
            if change=='peak':summary['phase_kernel_rss'][0]['kernel_reported_peak_rss_bytes']=args[-1]['rss_bytes']
            if change=='lifetime':summary['children_lifetime_peak_rss_bytes']=123
            if change=='inventory':summary['final_inventory']['allocated_bytes']=0
            if change=='kernel_coverage':summary['phase_kernel_rss'].pop()
            with self.assertRaises(ValueError):contract.verify_sample(*args)

    def test_no_favorable_sample_selection(self):
        before=[100,102,98,101,99];after=[70,72,68,71,69]
        result=contract.compare(before,after)
        self.assertEqual((result['baseline_median'],result['candidate_median'],result['noise']), (100,70,3))
        self.assertEqual(result['before'],before)
        self.assertTrue(result['improvement_at_least_ten_percent'])
        self.assertFalse(contract.compare([100]*5,[90]*5)['increase_exceeds_noise'])
        self.assertFalse(contract.compare([50,100,150,200,250],[40,80,120,160,200])['improvement_at_least_ten_percent'])
        for values in ([1,2,3], [1,2,3,4,0], [1,2,3,4,True]):
            with self.assertRaises(ValueError):contract.compare(values,[1]*5)


class AssetTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve();self.assets=self.root/'assets';self.assets.mkdir()
        self.expected=dict(source_commit='a'*40,source_tree='b'*40,platform='synthetic-platform',
                           binary_sha256=contract.sha(b'binary'),parser_sha256=contract.sha(b'parser'),
                           example_source_sha256=contract.sha(b'example'),rust_version='fixture',
                           parser_contract='fixture',cargo_lock_sha256='lock')
        source=dict(commit='a'*40,tree='b'*40,profile='release')
        receipt=dict(schema='alopex.hnsw-example-build/v1',source=source,
                     **{k:self.expected[k] for k in ('binary_sha256','parser_sha256','example_source_sha256',
                                                    'rust_version','parser_contract','cargo_lock_sha256')},
                     build=dict(command=['cargo','build','--locked','--release','-p','alopex-embedded',
                                         '--example','issue681_hnsw_first_write'],incremental=False,release_debug=False),
                     smoke=dict(status='passed',rows=2,dimensions=4,phases=9))
        content={'binary':b'binary','library':b'parser','example':b'example','receipt':json.dumps(receipt).encode()}
        self.manifest=dict(schema='alopex.hnsw-example-assets/v1',source=source,platform='synthetic-platform',
                           expanded_limit_bytes=268435456,files=[])
        for role,data in content.items():
            (self.assets/role).write_bytes(data)
            self.manifest['files'].append(dict(role=role,path=role,size=len(data),sha256=contract.sha(data)))
        self.public=dict(status='passed',source=source,performance_qualification=False)
        self.save()

    def save(self):
        data=json.dumps(self.manifest).encode();(self.assets/'assets.json').write_bytes(data)
        self.expected['assets_digest']=contract.sha(data);self.public['assets_digest']=contract.sha(data)
        (self.root/'result.json').write_text(json.dumps(self.public))

    def test_verified_four_assets(self):
        self.assertEqual(set(contract.verify_assets(self.root,self.expected)),{'binary','library','example','receipt'})

    def test_binary_tamper_and_unlisted_files_rejected(self):
        (self.assets/'binary').write_bytes(b'tamper')
        with self.assertRaises(ValueError):contract.verify_assets(self.root,self.expected)
        (self.assets/'binary').write_bytes(b'binary');(self.assets/'extra').write_bytes(b'extra')
        with self.assertRaises(ValueError):contract.verify_assets(self.root,self.expected)

    def test_duplicate_traversal_and_symlink_rejected(self):
        original=copy.deepcopy(self.manifest)
        for change in ('duplicate','traversal','symlink'):
            self.manifest=copy.deepcopy(original)
            if change=='duplicate':self.manifest['files'][1]=self.manifest['files'][0]
            if change=='traversal':self.manifest['files'][0]['path']='../binary'
            if change=='symlink':
                (self.assets/'binary').unlink();(self.assets/'binary').symlink_to(self.assets/'library')
            self.save()
            with self.assertRaises(ValueError):contract.verify_assets(self.root,self.expected)


if __name__=='__main__':unittest.main()
