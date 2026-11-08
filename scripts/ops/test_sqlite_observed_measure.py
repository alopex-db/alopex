import ast,copy,inspect,sqlite3,unittest
from contextlib import closing
import sqlite_composite_profile as original
import sqlite_observed_measure as observed
from sqlite_phase_contract import plan,verify_cell
from test_dataframe_phase_contract import observation

class Observer:
    def __init__(self,events):self.events=events
    def begin(self,index):self.events.append('begin')
    def finish(self,index):self.events.append('finish');return observation(index)

class Contracts(unittest.TestCase):
    def test_observation_is_outside_clock_and_checks(self):
        events=[];rows=[]
        def clock():events.append('clock');return len(events)
        def operation():events.append('operation');return 1
        observed.measure(operation,lambda actual:events.append('validate'),lambda:events.append('state'),
            warmups=0,samples=1,clock=clock,record=rows.append,observer=Observer(events))
        self.assertEqual(events,['begin','clock','operation','clock','finish','validate','state'])
        self.assertEqual(rows[0]['resources']['index'],0)

    def test_counter_failure_cannot_emit_success(self):
        class Broken(Observer):
            def finish(self,index):raise RuntimeError('counter missing')
        rows=[]
        with self.assertRaisesRegex(RuntimeError,'counter missing'):
            observed.measure(lambda:None,lambda actual:None,lambda:None,warmups=0,samples=1,record=rows.append,observer=Broken([]))
        self.assertEqual(rows,[])

    def test_original_function_is_unchanged_except_observer(self):
        fn=ast.parse(inspect.getsource(observed.measure)).body[0]
        fn.args.kwonlyargs.pop();fn.args.kw_defaults.pop()
        loop=fn.body[0]
        loop.body=[n for n in loop.body if not (isinstance(n,ast.Expr) and isinstance(n.value,ast.Call)
            and isinstance(n.value.func,ast.Attribute) and isinstance(n.value.func.value,ast.Name)
            and n.value.func.value.id=='observer') and not (isinstance(n,ast.Assign) and n.targets[0].id=='observation')]
        for n in ast.walk(fn):
            if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='dict':
                n.keywords=[k for k in n.keywords if k.arg!='resources']
        self.assertEqual(ast.dump(fn,include_attributes=False),ast.dump(ast.parse(inspect.getsource(original.measure)).body[0],include_attributes=False))

    def test_three_row_original_and_checked_preserve_state(self):
        for checked in (False,True):
            with self.subTest(checked=checked),closing(sqlite3.connect(':memory:',isolation_level=None)) as connection:
                if checked:connection.execute('PRAGMA foreign_keys=ON')
                original.original._populate_sql(connection,'sqlite',3)
                original.original._prepare_sql_workload(connection,'sqlite','sql-sqlite-curated')
                initial=original.validate_state(connection,'sqlite',3)
                if checked:
                    original.validate_constraints(connection,'sqlite');original.validate_savepoint(connection,'sqlite',3)
                def operation():
                    if checked:return original.checked_composite(connection,'sqlite',3)[0]
                    return original.original._sql_workload(connection,'sqlite','sql-sqlite-curated',3)
                def validate(actual):
                    if checked:original.validate_result(actual,3)
                    else:self.assertIsNone(actual)
                rows=[]
                observed.measure(operation,validate,lambda:self.assertEqual(original.validate_state(connection,'sqlite',3),initial),
                    warmups=0,samples=1,record=rows.append,observer=Observer([]))
                self.assertEqual(len(rows),1)

    def test_inventory_and_missing_validation_rejected(self):
        self.assertEqual(len(plan('preflight')),8);self.assertEqual(len(plan('full')),24)
        self.assertEqual(sum(c['samples'] for c in plan('full')),504)
        cell=plan('preflight')[0]
        result=dict(connection_closed=True,state_checks=1,checked_contracts=False)
        row=dict(state_verified=False,result_verified=False)
        with self.assertRaisesRegex(RuntimeError,'SQL validation missing'):verify_cell(cell,result,[row],{},1024)
        result['connection_closed']=False
        with self.assertRaisesRegex(RuntimeError,'cleanup'):verify_cell(cell,result,[],{},1024)

if __name__=='__main__':unittest.main()
