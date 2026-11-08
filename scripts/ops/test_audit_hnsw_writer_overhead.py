import json
import unittest

from audit_hnsw_writer_overhead import audit
from hnsw_writer_overhead_contract import plan, sha, verify_sample
import test_hnsw_writer_overhead as fixture_source


class MemoryEvidence:
    def __init__(self): self.data = {}; self.members = {}
    def put(self, name, value):
        self.data[name] = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.members[name] = type('Regular', (), {'isfile': lambda self: True})()
    def read(self, name): return self.data[name]
    def json(self, name): return json.loads(self.read(name))


class AuditTests(unittest.TestCase):
    def fixture(self, finish="commit", sample_fixture=None, stage_rows=1000):
        sample_fixture = sample_fixture or fixture_source.OverheadTests().fixture
        ev = MemoryEvidence(); rows = []
        original = sample_fixture(finish)
        config = dict(finish=finish, rows=stage_rows, cpu_count=2, limits=original[-1], instrument=original[-3],
                      subjects={s:dict(sample_fixture(finish, s)[4], assets_digest=s) for s in ('normal','diagnostic')})
        config['limits']['case_seconds'] = {1000:120,10000:300,100000:900}[stage_rows]
        if stage_rows != 1000:
            ev.put('previous-result.json', fixture_source.previous_result(config))
        for cell in plan(stage_rows, finish):
            args = list(sample_fixture(finish, cell['subject'])); args[3] = cell
            identity, summary, events = args[:3]
            args[-1] = config['limits']
            identity.update(rows=stage_rows, timeout_seconds=config['limits']['case_seconds'])
            events[0]['data']['rows'] = stage_rows
            contents = {'output/identity.json':identity, 'output/summary.json':summary,
                        'output/events.jsonl':b'\n'.join(json.dumps(e).encode() for e in events),
                        'output/rss.jsonl':b'', 'output/stderr.log':b'', 'runner.log':b''}
            hashes = {}
            for name, value in contents.items():
                key=cell['id']+'/'+name; ev.put(key,value); hashes[name]=sha(ev.read(key))
            rows.append(dict(cell,status='passed',files=hashes,values=verify_sample(*args)))
        ev.put('plan.json',dict(configuration=config,controller='controller',cells=plan(stage_rows, finish),affinity=[0,1]))
        ev.put('results.json',rows);ev.put('progress.json',rows);ev.put('storage-before.json',dict(allocated_bytes=100))
        public=dict(schema='alopex.hnsw-writer-overhead/v1',finish=finish,status='passed',stage_rows=stage_rows,controller='controller',
                    completed_samples=10,owned_resources_cleaned=True,assets={'normal':'normal','diagnostic':'diagnostic'},
                    performance_qualification=False)
        cleanup=dict(imports_removed=True,raw_work_removed=True,restored_affinity=[0,1],allocated_bytes=100)
        return ev, public, cleanup, config, 'controller'

    def test_reconstructs_all_five_samples_and_seventeen_phase_metrics(self):
        result=audit(*self.fixture())
        self.assertEqual(result['completed_samples'],10)
        self.assertEqual(sum(map(len,result['comparisons'].values())),17)
        self.assertEqual(result['comparisons']['elapsed_ns']['first_insert']['normal'],[100]*5)

    def test_candidate_both_modes_reconstruct_all_samples_and_internal_observations(self):
        for finish in ('commit', 'rollback'):
            result = audit(*self.fixture(finish, fixture_source.candidate_fixture))
            self.assertEqual(result['completed_samples'], 10)
            self.assertTrue(result['cold_cache_contracts_verified'])
            self.assertEqual(result['comparisons']['elapsed_ns'][finish]['normal'], [321] * 5)
            self.assertEqual(result['comparisons']['elapsed_ns'][finish]['diagnostic'], [321] * 5)
            self.assertTrue(result['writer_internal'])

    def test_scaled_raw_evidence_reconstructs_both_engines_and_finishes(self):
        for size in (10000, 100000):
            for finish in ('commit', 'rollback'):
                for source in (None, fixture_source.candidate_fixture):
                    with self.subTest(size=size, finish=finish, candidate=source is not None):
                        result = audit(*self.fixture(finish, source, size))
                        self.assertEqual(result['stage_rows'], size)
                        self.assertEqual(result['completed_samples'], 10)
                        self.assertEqual(len(result['writer_internal']), 5)

    def test_missing_tampered_or_wrong_preceding_stage_rejected_independently(self):
        for fault in ('missing', 'hash', 'stage', 'finish', 'assets'):
            args = self.fixture(stage_rows=10000); evidence, _, _, config, _ = args
            if fault == 'missing':
                del evidence.members['previous-result.json']
            elif fault == 'hash':
                evidence.put('previous-result.json', b'{}')
            else:
                raw = evidence.json('previous-result.json')
                if fault == 'stage': raw['stage_rows'] = 100000
                if fault == 'finish': raw['finish'] = 'rollback'
                if fault == 'assets': raw['assets']['normal'] = 'other'
                evidence.put('previous-result.json', raw)
                config['previous_stage']['result_sha256'] = sha(evidence.read('previous-result.json'))
                frozen = evidence.json('plan.json'); frozen['configuration'] = config
                evidence.put('plan.json', frozen)
            with self.subTest(fault=fault), self.assertRaises(ValueError): audit(*args)

    def test_first_stage_cannot_claim_a_predecessor(self):
        args = self.fixture(); args[0].put('previous-result.json', {})
        with self.assertRaises(ValueError): audit(*args)

    def test_candidate_cache_fault_rejected_even_with_recomputed_raw_hash(self):
        args = self.fixture(sample_fixture=fixture_source.candidate_fixture)
        evidence = args[0]
        rows = evidence.json('results.json')
        key = rows[0]['id'] + '/output/events.jsonl'
        events = [json.loads(line) for line in evidence.read(key).splitlines()]
        next(e['data']['stats'] for e in events if e['phase'] == 'cold_transaction_2'
             and e['data'].get('event') == 'end')['misses'] += 1
        evidence.put(key, b'\n'.join(json.dumps(e).encode() for e in events))
        rows[0]['files']['output/events.jsonl'] = sha(evidence.read(key))
        evidence.put('results.json', rows)
        evidence.put('progress.json', rows)
        with self.assertRaises(ValueError):
            audit(*args)

    def test_tampered_raw_file_rejected(self):
        args=self.fixture();args[0].put('hnsw-overhead-commit-1000-0-normal/output/events.jsonl',b'{}')
        with self.assertRaises(ValueError):audit(*args)

    def test_reconstructs_declining_readings_from_reset_and_end(self):
        args=self.fixture();ev=args[0];rows=ev.json('results.json')
        for record in rows:
            key=record['id']+'/output/summary.json';summary=ev.json(key)
            phase=summary['phase_kernel_rss'][-1]
            phase['after']=dict(rss_bytes=800,high_water_bytes=900)
            phase['kernel_high_water_decreased']=True
            ev.put(key,summary);record['files']['output/summary.json']=sha(ev.read(key))
        ev.put('results.json',rows);ev.put('progress.json',rows)
        result=audit(*args)['comparisons']['kernel_peak_rss_bytes']['commit']
        self.assertEqual(result['normal'],[1024]*5)
        self.assertEqual(result['diagnostic'],[1024]*5)

    def test_removed_or_reordered_sample_rejected(self):
        for remove in (True, False):
            args=self.fixture();ev=args[0];rows=ev.json('results.json')
            if remove:rows.pop()
            else:rows[0],rows[1]=rows[1],rows[0]
            ev.put('results.json',rows);ev.put('progress.json',rows)
            with self.assertRaises(ValueError):audit(*args)

    def test_forged_derived_value_rejected(self):
        args=self.fixture();ev=args[0];rows=ev.json('results.json')
        rows[0]['values']['elapsed_ns']['first_insert']=1
        ev.put('results.json',rows);ev.put('progress.json',rows)
        with self.assertRaises(ValueError):audit(*args)

    def test_cleanup_and_public_private_field_leak_rejected(self):
        args=self.fixture();args[2]['raw_work_removed']=False
        with self.assertRaises(ValueError):audit(*args)
        args=self.fixture();args[1]['machine']='synthetic-confidential'
        with self.assertRaises(ValueError):audit(*args)

    def periodic(self, value):
        args=self.fixture();ev=args[0];rows=ev.json('results.json')
        name='output/storage.jsonl';key=rows[0]['id']+'/'+name
        ev.put(key,dict(allocated_bytes=value,roots=['synthetic'],method='native allocated bytes fixture'))
        rows[0]['files'][name]=sha(ev.read(key))
        ev.put('results.json',rows);ev.put('progress.json',rows)
        return args

    def test_periodic_storage_is_hashed_and_validated(self):
        self.assertEqual(audit(*self.periodic(100))['status'],'verified')
        args=self.periodic(100);args[0].put('hnsw-overhead-commit-1000-0-normal/output/storage.jsonl',b'{}')
        with self.assertRaises(ValueError):audit(*args)

    def test_invalid_periodic_storage_refuses_acceptance(self):
        for value in (0,40_000_000_000,True):
            with self.assertRaises(ValueError):audit(*self.periodic(value))

    def test_unlisted_sample_file_refuses_acceptance(self):
        args=self.fixture();args[0].put('hnsw-overhead-commit-1000-0-normal/output/unexpected.json',{})
        with self.assertRaises(ValueError):audit(*args)


    def test_both_modes_reconstruct_completion_time_independently(self):
        for finish in ('commit', 'rollback'):
            result = audit(*self.fixture(finish))
            self.assertEqual(result['finish'], finish)
            metric = result['comparisons']['elapsed_ns'][finish]
            self.assertEqual(metric['normal'], [321] * 5)
            self.assertEqual(metric['diagnostic'], [321] * 5)
            self.assertFalse(metric['increase_exceeds_noise'])

    def test_old_or_cross_mode_public_result_rejected(self):
        for field, value in (('schema', 'alopex.hnsw-stage/v1'), ('finish', 'rollback')):
            args = self.fixture()
            args[1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): audit(*args)

    def test_rehashed_missing_result_validation_cannot_pass(self):
        for finish in ('commit', 'rollback'):
            args = self.fixture(finish)
            ev = args[0]
            rows = ev.json('results.json')
            key = rows[0]['id'] + '/output/events.jsonl'
            events = [json.loads(line) for line in ev.read(key).splitlines()]
            events[-1]['data']['result_validated'] = False
            ev.put(key, b'\n'.join(json.dumps(e).encode() for e in events))
            rows[0]['files']['output/events.jsonl'] = sha(ev.read(key))
            ev.put('results.json', rows)
            ev.put('progress.json', rows)
            with self.subTest(finish=finish), self.assertRaises(ValueError): audit(*args)


if __name__=='__main__':unittest.main()

