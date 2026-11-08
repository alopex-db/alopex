import copy
import unittest
from hnsw_completion_smoke import PHASES, verify


def fixture(mode):
    identity = dict(finish=mode, rows=2, dimensions=4, binary_sha256='a'*64,
        runner_sha256='b'*64, phase_kernel_rss=True, timeout_seconds=30, rss_limit=268435456)
    phases = (*PHASES, mode)
    events = [dict(phase=p, data=dict(event=e)) for p in phases for e in ('start','end')]
    events[-1]['data']['elapsed_ns'] = 100
    events.append(dict(phase='complete', data=dict(finish=mode, result_validated=True)))
    report = dict(outcome='passed', failure=None, kernel_rss_reset=True,
        incomplete_kernel_phase=None, children_lifetime_peak_rss_bytes=None,
        phase_kernel_rss=[dict(phase=p, kernel_reported_peak_rss_bytes=10,
            after_reset=dict(high_water_bytes=10), after=dict(high_water_bytes=9)) for p in phases])
    return identity, report, events


class Smoke(unittest.TestCase):
    def test_both_modes_and_valid_kernel_counter_decrease(self):
        for mode in ('commit','rollback'):
            self.assertEqual(verify(*fixture(mode),mode,'a'*64,'b'*64)['elapsed_ns'],100)

    def test_partial_wrong_or_unverified_completion_is_rejected(self):
        for fault in ('identity','mode','rows','time','verified','failure','phase','peak'):
            i,r,e=copy.deepcopy(fixture('commit'))
            if fault=='identity':i['binary_sha256']='c'*64
            if fault=='mode':e[-1]['data']['finish']='rollback'
            if fault=='rows':i['rows']=3
            if fault=='time':e[-2]['data']['elapsed_ns']=True
            if fault=='verified':e[-1]['data']['result_validated']=False
            if fault=='failure':r['failure']='failure'
            if fault=='phase':e.pop(0)
            if fault=='peak':r['phase_kernel_rss'][-1]['kernel_reported_peak_rss_bytes']=9
            with self.subTest(fault=fault),self.assertRaises(ValueError):
                verify(i,r,e,'commit','a'*64,'b'*64)


if __name__=='__main__':unittest.main()
