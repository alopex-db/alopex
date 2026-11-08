from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

BASE=Path(__file__).resolve().parent
for root in (BASE.parent.parent/'alopex',BASE.parent.parent):
    if (root/'scripts/performance').is_dir():sys.path.insert(0,str(root/'scripts/performance'))
from wal_cost_commit_observer import observe


class CommitObserver(unittest.TestCase):
    def exercise(self,negative=False,child_changed=False,fail=False):
        events=[];i=iter([0,1]);j=iter([0,1])
        def snapshot():
            index=next(i);events.append('snapshot'+str(index))
            return dict(io=dict(read_bytes=0,write_bytes=index*4096,cancelled_write_bytes=0),
                        waited_children=[index if child_changed else 0])
        def usage(_):
            index=next(j);events.append('cpu'+str(index))
            return SimpleNamespace(ru_utime=.1+(-1 if negative else .02)*index,ru_stime=.01+.01*index,
                                   ru_nvcsw=index*2,ru_nivcsw=0)
        def measure(action):
            events.append('timer-start');value=action();events.append('timer-end');return value,123
        def action():
            events.append('commit')
            if fail:raise ValueError('commit failed')
            return 'committed'
        return observe(action,measure,snapshot=snapshot,usage=usage),events

    def test_original_timer_and_resource_boundaries(self):
        (result,report),events=self.exercise()
        self.assertEqual(result,('committed',123));self.assertEqual(report['delta']['write_bytes'],4096)
        self.assertEqual(report['delta']['ru_nvcsw'],2)
        self.assertEqual(events,['snapshot0','cpu0','timer-start','commit','timer-end','cpu1','snapshot1'])

    def test_decreased_counter_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'counter'):self.exercise(negative=True)

    def test_child_usage_change_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'child'):self.exercise(child_changed=True)

    def test_original_commit_failure_propagates(self):
        with self.assertRaisesRegex(ValueError,'commit failed'):self.exercise(fail=True)


if __name__=='__main__':unittest.main()
