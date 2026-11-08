import copy
import unittest

from wal_cost_contract import METRICS, plan, summarize, verify_record


def records(mode='full'):
    rows=[]
    for cell in plan(mode):
        writer=dict.fromkeys(METRICS,100)
        writer.update(status='passed',phase='write',surface=cell['surface'],value_bytes=cell['value_bytes'],
            native_sha256=cell['subject'],overflow_stats={'gc_failures':0})
        reader={key:writer[key] for key in ('status','surface','value_bytes','native_sha256','open_ns',
            'wal_bytes','spool_bytes','fixture_bytes','allocated_bytes')}
        reader.update(phase='verify',verified=True)
        rows.append(dict(cell=cell,writer=writer,readers=[dict(reader),dict(reader)]))
    return rows


class Contract(unittest.TestCase):
    def test_matrix_is_alternating_and_complete(self):
        result=summarize(records(),'full')
        self.assertEqual((result['writers'],result['readers']),(48,96))
        self.assertFalse(result['performance_acceptance'])
        self.assertEqual(plan('full')[8]['subject'],'candidate')
        self.assertEqual(len(plan('preflight')),8)

    def test_partial_duplicate_wrong_order_and_missing_reopen_rejected(self):
        valid=records()
        broken=[valid[:-1], valid[:-1]+[valid[0]], list(reversed(valid))]
        missing=copy.deepcopy(valid);missing[0]['readers'].pop();broken.append(missing)
        for rows in broken:
            with self.subTest(rows=len(rows)),self.assertRaises(ValueError):summarize(rows,'full')

    def test_identity_and_recovery_required(self):
        valid=records('preflight')[0]
        for key,value in [('native_sha256','other'),('verified',False),('value_bytes',2048)]:
            row=dict(valid['readers'][0]);row[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):
                verify_record(row,valid['cell'],'verify','before')

    def test_invalid_measurements_and_bounds_rejected(self):
        valid=records('preflight')[0]
        for key,value in [('commit_ns',True),('stage_ns',-1),('open_ns',float('nan')),
                          ('fixture_bytes',1_048_577),('peak_rss_bytes',1_073_741_825)]:
            row=dict(valid['writer']);row[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):verify_record(row,valid['cell'],'write','before')

    def test_noise_holds_even_with_faster_median(self):
        rows=records()
        for row in rows:
            if row['cell']['subject']=='candidate':row['writer']['commit_ns']=[50,60,70,80,90,100][row['cell']['round']]
        result=summarize(rows,'full')
        self.assertTrue(all(r['decision']=='pending_noise' for r in result['comparison'].values()))

    def test_increases_retained_and_warmup_excluded(self):
        rows=records()
        for row in rows:
            if row['cell']['round']==0:row['writer']['commit_ns']=999999
            elif row['cell']['subject']=='candidate':
                row['writer']['commit_ns']=110;row['writer']['peak_rss_bytes']=120
        result=summarize(rows,'full')
        for value in result['comparison'].values():
            self.assertEqual(value['decision'],'investigate_increase')
            self.assertEqual(value['commit_ratio'],1.1)
            self.assertEqual(value['resource_increases'],{'peak_rss_bytes':20})


if __name__=='__main__':unittest.main()
