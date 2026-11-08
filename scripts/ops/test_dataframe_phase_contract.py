import copy
import unittest
from dataframe_phase_contract import CASES, SUBJECTS, plan, verify_cell, verify_resources


def observation(index=0):
    io=dict(read_bytes=0,write_bytes=0,cancelled_write_bytes=0)
    point=dict(user_cpu_seconds=1.0,system_cpu_seconds=.5,io=io,waited_children=[0]*16)
    return dict(index=index,before=copy.deepcopy(point),after=copy.deepcopy(point),
        delta=dict(user_cpu_seconds=0.0,system_cpu_seconds=0.0,**io),rss=dict(
        phase='sample-'+str(index),before_reset=dict(rss_bytes=40,high_water_bytes=1000),
        after_reset=dict(rss_bytes=40,high_water_bytes=40),after=dict(rss_bytes=30,high_water_bytes=35),
        kernel_reported_peak_rss_bytes=40,kernel_high_water_decreased=True,
        aggregation='maximum of reset and end kernel high-water readings'))


class ContractTest(unittest.TestCase):
    def test_full_matrix_retains_all_subjects_phases_trials_and_original_scale(self):
        cells=plan('full')
        self.assertEqual(len(cells),48)
        self.assertEqual(len({c['id'] for c in cells}),48)
        self.assertEqual({(c['trial'],c['case'],c['subject']) for c in cells},
            {(t,c,s) for t in range(3) for c in CASES for s in SUBJECTS})
        self.assertEqual({(c['rows'],c['warmups'],c['samples']) for c in cells},{(34250,3,21)})
        self.assertEqual(len(plan('preflight')),16)
        with self.assertRaises(RuntimeError):plan('other')

    def test_reset_and_end_maximum_excludes_old_phase_peak(self):
        verify_resources(observation(),0,40)
        value=observation();value['rss']['kernel_reported_peak_rss_bytes']=1000
        with self.assertRaises(RuntimeError):verify_resources(value,0,2000)

    def test_counter_mutation_missing_metric_and_child_changes_fail(self):
        for fault in ('delta','missing','children','nonfinite','rss_flag','limit'):
            value=observation();limit=100
            if fault=='delta':value['delta']['read_bytes']=1
            if fault=='missing':del value['after']['io']['write_bytes']
            if fault=='children':value['after']['waited_children'][0]=1
            if fault=='nonfinite':value['before']['user_cpu_seconds']=float('nan')
            if fault=='rss_flag':value['rss']['kernel_high_water_decreased']=False
            if fault=='limit':limit=39
            with self.subTest(fault=fault),self.assertRaises(RuntimeError):verify_resources(value,0,limit)

    def test_complete_cell_requires_matching_raw_and_all_observations(self):
        cell=plan('preflight')[0]
        result=dict(case=cell['case'],engine=cell['engine'],fixture_rows=4,warmups=0,samples=1,
            resource_observations=1,input_sha256='a'*64,result_sha256='b'*64)
        raw=[dict(phase='measure',index=0,seconds=.1,resources=observation())]
        observations=dict(pending=None,completed=[observation()])
        self.assertEqual(verify_cell(cell,result,raw,observations,100)['seconds'],[.1])
        for fault in ('raw','pending','missing','result'):
            selected=copy.deepcopy(raw);obs=copy.deepcopy(observations);res=copy.deepcopy(result)
            if fault=='raw':selected[0]['resources']['delta']['read_bytes']=1
            if fault=='pending':obs['pending']={'index':0}
            if fault=='missing':obs['completed']=[]
            if fault=='result':res['fixture_rows']=3
            with self.subTest(fault=fault),self.assertRaises(RuntimeError):verify_cell(cell,res,selected,obs,100)


if __name__=='__main__':unittest.main()
