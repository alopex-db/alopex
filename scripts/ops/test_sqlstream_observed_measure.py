"""Tiny stream/wheel fixtures protect observed SQL streaming contracts."""
import ast,copy,hashlib,inspect,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import sql_stream_profile as original
import sqlstream_observed_measure as observed
import sqlstream_reference as reference
from sqlstream_phase_contract import plan,verify_cell,validate_fixture
from test_dataframe_phase_contract import observation

ROWS=[{'id':0,'value':0.0},{'id':2,'value':2.0}]
class Observer:
    def __init__(self,events):self.events=events
    def begin(self,index):self.events.append('begin')
    def finish(self,index):self.events.append('finish');return observation(index)

class Contracts(unittest.TestCase):
    def test_original_measure_body_preserved_except_resource_observation(self):
        fn=ast.parse(inspect.getsource(observed.measure)).body[0]
        loop=fn.body[0]
        loop.body=[n for n in loop.body if not (isinstance(n,ast.Expr) and isinstance(n.value,ast.Call)
            and isinstance(n.value.func,ast.Attribute) and isinstance(n.value.func.value,ast.Name)
            and n.value.func.value.id=='observer') and not (isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='observation')]
        for n in ast.walk(fn):
            if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='dict':
                n.keywords=[k for k in n.keywords if k.arg!='resources']
        other=ast.parse(inspect.getsource(original.measure)).body[0]
        self.assertEqual([ast.dump(n) for n in fn.body],[ast.dump(n) for n in other.body])

    def test_resource_observation_is_outside_clock_and_row_validation(self):
        events=[];records=[]
        def clock():events.append('clock');return len(events)*1000
        def consume():events.append('consume');return (0.000001,0.000002,2,1,ROWS)
        def validate(*args):events.append('validate')
        with patch.object(observed.time,'perf_counter_ns',side_effect=clock),patch.object(observed,'validate_result',side_effect=validate):
            observed.measure(consume,ROWS,True,Observer(events),warmups=0,samples=1,record=records.append)
        self.assertEqual(events,['begin','clock','consume','clock','finish','validate'])
        self.assertEqual(records[0]['total_ns'],2000)
        self.assertEqual(records[0]['first_ns'],1000)

    def test_missing_counter_and_incomplete_rows_cannot_emit_success(self):
        class Broken(Observer):
            def finish(self,index):raise RuntimeError('counter missing')
        for observer, result in [(Broken([]),(1,2,2,1,ROWS)),(Observer([]),(1,2,1,1,ROWS[:1]))]:
            records=[]
            with self.assertRaises(RuntimeError):observed.measure(lambda:result,ROWS,True,observer,warmups=0,samples=1,record=records.append)
            self.assertEqual(records,[])

    def test_inventory_and_evidence_reject_changed_rows_lane_intervals_and_resources(self):
        self.assertEqual(len(plan('preflight')),8);self.assertEqual(len(plan('full')),24)
        cell=plan('preflight')[0]
        result=dict(case=cell['case'],engine=cell['engine'],fixture_rows=3,warmups=0,samples=1,
            input_sha256='a'*64,result_sha256='b'*64,resource_observations=1,
            connection_closed=True,fixture_checks=2,full_result_checks=2,returned_rows=2,rows_lane=False)
        obs=observation(0)
        raw=dict(phase='measure',index=0,seconds=2.0,first_seconds=1.0,remaining_seconds=1.0,
            rows=2,first_chunk_rows=1,rows_verified=False,resources=obs,original_usage={})
        self.assertEqual(verify_cell(cell,result,[raw],dict(completed=[obs],pending=None),1000)['first_seconds'],[1.0])
        for field,value in [('rows',1),('first_seconds',3.0),('remaining_seconds',0.0),('rows_verified',True),('first_chunk_rows',2)]:
            bad=dict(raw,**{field:value})
            with self.subTest(field=field),self.assertRaises(RuntimeError):verify_cell(cell,result,[bad],dict(completed=[obs],pending=None),1000)
        with self.assertRaises(RuntimeError):verify_cell(cell,result,[raw],dict(completed=[],pending=None),1000)
        with self.assertRaises(RuntimeError):verify_cell(cell,dict(result,full_result_checks=0),[raw],dict(completed=[obs],pending=None),1000)

    def test_fixture_oracle_rejects_type_value_column_order_and_missing_rows(self):
        rows=[dict(id=i,value=float(i),enabled=i%2==0) for i in range(3)]
        self.assertEqual(len(validate_fixture(rows,3)),64)
        for bad in [rows[:-1],list(reversed(rows)),[dict(rows[0],enabled=1),*rows[1:]],
            [dict(rows[0],value=0),*rows[1:]],[{'value':0.0,'id':0,'enabled':True},*rows[1:]]]:
            with self.assertRaises(RuntimeError):validate_fixture(bad,3)

    def reference_install(self,corrupt=False,missing=False):
        data=b'tiny wheel bytes';commands=[]
        records=[dict(name=name,version=version,filename=name+'.whl',url='https://files.pythonhosted.org/'+name+'.whl',
            size=len(data),sha256=hashlib.sha256(data).hexdigest()) for name,version in reference.VERSIONS.items()]
        if corrupt:records[0]['sha256']='0'*64
        if missing:records.pop()
        class Budget:
            def run(self,command,**kwargs):
                commands.append(command)
                if len(commands)==2:Path(command[-1]).write_text(json.dumps(records))
            def check(self):pass
        def response(url,**kwargs):
            result=io.BytesIO(data);result.url=url;return result
        with tempfile.TemporaryDirectory() as d,patch.object(reference.urllib.request,'urlopen',side_effect=response):
            _,identity=reference.install_reference({},Path(d),Budget(),{})
            self.assertEqual(len(json.loads(identity.read_bytes())['wheels']),3)
            self.assertEqual(commands[-1][-2:],['pip','check'])
            self.assertIn('--no-index',commands[-2]);self.assertIn('--no-deps',commands[-2])

    def test_reference_install_offline_and_dependency_check(self):self.reference_install()
    def test_changed_wheel_stops_before_install(self):
        with self.assertRaisesRegex(RuntimeError,'reference wheel differs'):self.reference_install(corrupt=True)
    def test_missing_dependency_stops_before_download(self):
        with self.assertRaisesRegex(RuntimeError,'reference selection differs'):self.reference_install(missing=True)

if __name__=='__main__':unittest.main()
