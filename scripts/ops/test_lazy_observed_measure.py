"""Tiny fake-frame contracts for exact original clocks and complete validation."""
import ast
import inspect
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars_entrypoints_v0816 as original
import lazy_observed_measure as measured
from test_lazy_file_profile import FakeOperations
from test_csv_phase_measure import Observer
from lazy_phase_contract import CASES, plan


class Observation(unittest.TestCase):
    def test_all_engine_case_operations_match_original(self):
        for engine in ('alopex','polars'):
            for case in CASES:
                with self.subTest(engine=engine,case=case),tempfile.TemporaryDirectory() as tmp:
                    root=Path(tmp);(root/('rows.'+case.split('-')[0])).write_bytes(b'fake')
                    sequences=[]
                    for module,name in ((original,'old'),(measured,'new')):
                        events=[];ops=FakeOperations(3,events)
                        ops.api.scan_csv=ops.scan;ops.api.scan_parquet=ops.scan
                        options={} if module is original else dict(observer=Observer([]))
                        with patch.object(module,'Operations',return_value=ops):
                            result=module.measure(engine,case,root,root/name,3,1,1,**options)
                        sequences.append(events)
                        self.assertEqual(result['output_rows'],3)
                    self.assertEqual(*sequences)

    def test_observation_surrounds_timer_but_excludes_conversion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'rows.csv').write_bytes(b'fake')
            events=[];ops=FakeOperations(3,events);observer=Observer(events)
            ticks=iter([1.,2.])
            def clock(): events.append('clock');return next(ticks)
            with patch.object(measured,'Operations',return_value=ops),patch.object(measured.time,'perf_counter',side_effect=clock):
                measured.measure('alopex','csv-batches',root,root/'raw',3,0,1,observer=observer)
            self.assertEqual(events[0:2],['begin','clock'])
            self.assertLess(events.index('close'),events.index('finish'))
            self.assertLess(events.index('finish'),events.index('convert'))
            row=json.loads((root/'raw').read_text());self.assertEqual(row['seconds'],1.)
            self.assertEqual(row['resources']['index'],0)

    def test_counter_failure_has_no_success_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'rows.csv').write_bytes(b'fake');observer=Observer([])
            with patch.object(measured,'Operations',return_value=FakeOperations(3,[])),patch.object(observer,'finish',side_effect=RuntimeError('missing counter')):
                with self.assertRaisesRegex(RuntimeError,'missing counter'):
                    measured.measure('alopex','csv-collect',root,root/'raw',3,0,1,observer=observer)
            self.assertEqual((root/'raw').read_text(),'')

    def test_measurement_body_diff_is_only_observation(self):
        old=ast.parse(inspect.getsource(original.measure)).body[0]
        new=ast.parse(inspect.getsource(measured.measure)).body[0]
        class Strip(ast.NodeTransformer):
            def visit_Expr(self,node):
                if isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute) and isinstance(node.value.func.value,ast.Name) and node.value.func.value.id=='observer':return None
                return self.generic_visit(node)
            def visit_Assign(self,node):
                if isinstance(node.targets[0],ast.Name) and node.targets[0].id=='observation':return None
                return self.generic_visit(node)
            def visit_Dict(self,node):
                pairs=[(k,v) for k,v in zip(node.keys,node.values) if not isinstance(k,ast.Constant) or k.value!='resources']
                node.keys=[k for k,v in pairs];node.values=[v for k,v in pairs]
                return self.generic_visit(node)
        new=Strip().visit(new);new.args=old.args
        self.assertEqual(ast.dump(old),ast.dump(new))

    def test_inventory_preserves_all_cases_and_full_dimensions(self):
        for mode,n in [('preflight',16),('full',48)]:
            cells=plan(mode);self.assertEqual(len(cells),n)
            self.assertEqual(len({c['id'] for c in cells}),n)
            self.assertEqual({c['case'] for c in cells},set(CASES))
        self.assertTrue(all((c['rows'],c['warmups'],c['samples'])==(34250,3,21) for c in plan('full')))


if __name__=='__main__':unittest.main()
