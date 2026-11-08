import json
import unittest

from audit_hnsw_stage import audit
from hnsw_stage_contract import plan, sha, verify_sample
import test_hnsw_stage_contract as fixture_source


class MemoryEvidence:
    def __init__(self): self.data = {}; self.members = {}
    def put(self, name, value):
        self.data[name] = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.members[name] = type('Regular', (), {'isfile': lambda self: True})()
    def read(self, name): return self.data[name]
    def json(self, name): return json.loads(self.read(name))


class AuditTests(unittest.TestCase):
    def fixture(self):
        ev = MemoryEvidence(); rows = []
        original = fixture_source.StageTests().fixture()
        config = dict(rows=1000, cpu_count=2, limits=original[-1], instrument=original[-3],
                      subjects={s:dict(original[4], assets_digest=s) for s in ('before','candidate')})
        for cell in plan(1000):
            args = list(fixture_source.StageTests().fixture(cell['subject'])); args[3] = cell
            identity, summary, events = args[:3]
            contents = {'output/identity.json':identity, 'output/summary.json':summary,
                        'output/events.jsonl':b'\n'.join(json.dumps(e).encode() for e in events),
                        'output/rss.jsonl':b'', 'output/stderr.log':b'', 'runner.log':b''}
            hashes = {}
            for name, value in contents.items():
                key=cell['id']+'/'+name; ev.put(key,value); hashes[name]=sha(ev.read(key))
            rows.append(dict(cell,status='passed',files=hashes,values=verify_sample(*args)))
        ev.put('plan.json',dict(configuration=config,controller='controller',cells=plan(1000),affinity=[0,1]))
        ev.put('results.json',rows);ev.put('progress.json',rows);ev.put('storage-before.json',dict(allocated_bytes=100))
        public=dict(schema='alopex.hnsw-stage/v1',status='passed',stage_rows=1000,controller='controller',
                    completed_samples=10,owned_resources_cleaned=True,assets={'before':'before','candidate':'candidate'},
                    performance_qualification=False)
        cleanup=dict(imports_removed=True,raw_work_removed=True,restored_affinity=[0,1],allocated_bytes=100)
        return ev, public, cleanup, config, 'controller'

    def test_reconstructs_all_five_samples_and_sixteen_phase_metrics(self):
        result=audit(*self.fixture())
        self.assertEqual(result['completed_samples'],10)
        self.assertEqual(sum(map(len,result['comparisons'].values())),16)
        self.assertEqual(result['comparisons']['elapsed_ns']['first_insert']['before'],[100]*5)

    def test_tampered_raw_file_rejected(self):
        args=self.fixture();args[0].put('hnsw-1000-0-before/output/events.jsonl',b'{}')
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
        result=audit(*args)['comparisons']['kernel_peak_rss_bytes']['rollback']
        self.assertEqual(result['before'],[1024]*5)
        self.assertEqual(result['candidate'],[1024]*5)

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
        args=self.periodic(100);args[0].put('hnsw-1000-0-before/output/storage.jsonl',b'{}')
        with self.assertRaises(ValueError):audit(*args)

    def test_invalid_periodic_storage_refuses_acceptance(self):
        for value in (0,40_000_000_000,True):
            with self.assertRaises(ValueError):audit(*self.periodic(value))

    def test_unlisted_sample_file_refuses_acceptance(self):
        args=self.fixture();args[0].put('hnsw-1000-0-before/output/unexpected.json',{})
        with self.assertRaises(ValueError):audit(*args)


if __name__=='__main__':unittest.main()
