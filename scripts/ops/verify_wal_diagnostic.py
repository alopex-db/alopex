"""Independently audit cost evidence, including failures; no product execution."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import tempfile

from audit_native_sql_matrix import Evidence
import audit_native_sql_matrix
import wal_cost_contract
import wal_commit_diagnostic
import wal_diagnostic_supervisor
from wal_commit_diagnostic import BEGIN, END, memory, summarize_trace
from wal_diagnostic_supervisor import verify_lifecycle
from wal_cost_contract import plan, summarize, verify_record

BASE=Path(__file__).resolve().parent


def digest(data):return hashlib.sha256(data).hexdigest()


def verify_observation(observation,timer):
    assert observation['schema']=='alopex.wal-commit-diagnostic/v1'
    assert observation['performance_acceptance'] is False
    assert type(timer) is int and timer>0 and observation['timer_nanoseconds']==timer
    before=memory(observation['raw_before']);after=memory(observation['raw_after'])
    assert before==observation['before'] and after==observation['after']
    delta={group:{key:after[group][key]-value for key,value in values.items()}
           for group,values in before.items()}
    assert observation['delta_bytes']==delta
    return delta


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run',type=int,required=True)
    parser.add_argument('--controller',required=True)
    parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args()
    config=json.loads(args.configuration.read_bytes());cells=plan(config['mode'])
    root=BASE/f'wal-diagnostic-run-{args.run}'
    public=json.loads((root/'result.json').read_bytes())
    assert public['schema']=='alopex.wal-commit-diagnostic-run/v1' and public['controller']==args.controller
    assert public['mode']==config['mode'] and public['sources']==config['sources']
    assert public['work_removed'] is True and public['performance_acceptance'] is False
    assert digest((root/'private-evidence.cms').read_bytes())==public['encrypted_sha256']
    proof=dict(run=args.run,controller=args.controller,mode=config['mode'],
        evidence_sha256=public['evidence_sha256'],encrypted_sha256=public['encrypted_sha256'],
        auditor_sha256=digest(Path(__file__).read_bytes()),configuration_sha256=digest(args.configuration.read_bytes()),
        performance_acceptance=False)
    with tempfile.NamedTemporaryFile(dir=BASE,suffix='.private.tar.gz') as temporary:
        result=subprocess.run(['openssl','cms','-decrypt','-binary','-inform','DER','-in',str(root/'private-evidence.cms'),
            '-inkey',str(BASE/'native-evidence.key'),'-out',temporary.name],capture_output=True,timeout=30)
        assert result.returncode==0
        assert digest(Path(temporary.name).read_bytes())==public['evidence_sha256']
        evidence=Evidence(temporary.name)
        try:
            cleanup=evidence.json('cleanup.json')
            assert cleanup['imports_venvs_fixtures_removed'] is True
            assert all(type(cleanup[k]) is int and 0<=cleanup[k]<40_000_000_000 for k in ('initial_allocated_bytes','final_allocated_bytes'))
            assert not any(n.startswith(('imports/','venvs/','fixtures/','tmp/')) for n in evidence.members)
            proof.update(owned_resources_removed=True,storage_within_budget=True)
            if public['status']!='passed':
                proof.update(status='failed',failure=evidence.json('failure.json'),complete_cells=public['complete_cells'],
                    supervised=[dict(case=n,**evidence.json(n)) for n in evidence.members if n.endswith('supervision.json')])
                assert all(r.get('reaped',r.get('tracer_reaped')) is True for r in proof['supervised'])
            else:
                assert public['complete_cells']==public['planned_cells']==len(cells)
                identity=evidence.json('identity.json')
                assert identity['controller']==args.controller and identity['configuration']==config and identity['cells']==cells
                assert re.fullmatch('[0-9a-f]{64}',identity['tracer']['sha256']) and identity['tracer']['version'].startswith('strace')
                assert re.search(r'Ran '+str(config.get('contract_tests',6))+r' tests in [\d.]+s\s+OK\s*$',evidence.read('contracts.log').decode())
                receipts={}
                for name,producer in config['products'].items():
                    prefix='identities/'+name+'/'
                    assert digest(evidence.read(prefix+'assets.json'))==producer['manifest_sha256']
                    manifest=evidence.json(prefix+'assets.json')
                    receipt=evidence.json(prefix+'build-receipt.json');subject=evidence.json(prefix+'subject.json')
                    receipt_entry=next(x for x in manifest['files'] if x['role']=='receipt')
                    assert digest(evidence.read(prefix+'build-receipt.json'))==receipt_entry['sha256']
                    assert receipt['source']==subject['source']==dict(commit=producer['source_commit'],tree=producer['source_tree'],profile='release')
                    assert receipt['python_wheel_sha256']==subject['python_wheel']['sha256']==producer['wheel_sha256']
                    receipts[name]=receipt
                for key in ('tools','parser_record_sha256','parser_contract'):
                    assert receipts['before'][key]==receipts['candidate'][key]
                recorded=evidence.json('rows.json');assert len(recorded)==len(cells);rows=[];supervision_records=[];memory_boundaries=[];commit_observations=[]
                for cell,row in zip(cells,recorded):
                    assert row['cell']==cell
                    results=[]
                    subject_prefix='identities/'+cell['subject']+'/'
                    producer=config['products'][cell['subject']]
                    for index,phase in enumerate(('write','verify','verify')):
                        prefix='cells/'+cell['id']+'/'+str(index)+'/'
                        parameters=evidence.json(prefix+'configuration.json')
                        assert all(parameters[k]==v for k,v in cell.items()) and parameters['phase']==phase
                        assert parameters['sources']==config['sources'] and parameters['options']==config['options']
                        assert parameters['diagnostic']==config['diagnostic']=='wal-commit-trace/v1'
                        assert parameters['affinity']==identity['allocation']['client']
                        actual=evidence.json(prefix+'identity.json')
                        assert actual['manifest_sha256']==parameters['subject_manifest_sha256']==digest(evidence.read(subject_prefix+'subject.json'))
                        assert actual['python_wheel_sha256']==producer['wheel_sha256']
                        assert actual['source']==receipts[cell['subject']]['source']
                        assert actual['identity_checker_sha256']==config['sources']['scripts/ops/dataframe_harness/subject_identity.py']
                        assert actual['installed']['verified_files']>0
                        raw_log=evidence.read(prefix+'worker.log')
                        if phase=='write':
                            assert raw_log.count(BEGIN)==raw_log.count(END)==1
                            raw_log=raw_log.replace(BEGIN,b'').replace(END,b'')
                        result=json.loads(raw_log)
                        verify_record(result,cell,phase,producer['native_sha256']);results.append(result)
                        supervised=evidence.json(prefix+'supervision.json')
                        supervision_records.append(supervised)
                        assert supervised['returncode']==0 and supervised['failure'] is None
                        assert supervised['elapsed_seconds']<=config['cell_seconds']+1
                        assert 0<supervised['sampled_peak_rss_bytes']<=config['rss_bytes'] and supervised['live_samples']>0
                        if phase=='write':
                            assert supervised['tracer_reaped'] is True and supervised['tracee_disappeared'] is True
                            trace=evidence.read(prefix+'commit.trace').decode()
                            trace_identity=supervised['tracee_identity']
                            lifecycle=verify_lifecycle(trace,trace_identity)
                            rebuilt_trace=dict(**summarize_trace(trace,Path(parameters['fixture'])/'db.alopex'),lifecycle=lifecycle)
                            assert rebuilt_trace['marker_pid']==trace_identity['pid']
                            assert rebuilt_trace==evidence.json(prefix+'trace-summary.json')
                        else:
                            assert supervised['reaped'] is True
                            for excluded in supervised['terminal_samples_excluded']:
                                assert excluded['reaped'] is True and excluded['returncode']==0
                                raw=excluded['status_record']
                                assert raw is None or ('VmRSS:' not in raw and 'VmHWM:' not in raw)
                        if config.get('contract_tests',0)>=14:
                            memory=evidence.json(prefix+'memory-before-workload.json')
                            for key in ('before_identity','after_identity_and_native_digest'):
                                assert 0<memory[key]['rss_bytes']<=memory[key]['high_water_bytes']<=config['rss_bytes']
                            assert 0<memory['lifetime_high_water_bytes']<=config['rss_bytes']
                            if phase=='write':memory_boundaries.append(dict(cell=cell,**memory,writer_lifetime_high_water_bytes=result['peak_rss_bytes']))
                        if phase=='write':
                            observation=evidence.json(prefix+'commit-diagnostic.json')
                            verify_observation(observation,result['commit_ns'])
                            commit_observations.append(dict(cell=cell,observation=observation,trace=rebuilt_trace))
                    rebuilt=dict(cell=cell,writer=results[0],readers=results[1:]);assert rebuilt==row;rows.append(rebuilt)
                summary=summarize(rows,config['mode']);assert summary==evidence.json('summary.json')
                proof.update(status='passed',summary=summary,supervised_processes=len(supervision_records),
                    terminal_samples_excluded=sum(len(r.get('terminal_samples_excluded',[])) for r in supervision_records),
                    maximum_fixture_bytes=max(r['writer']['fixture_bytes'] for r in rows),
                    maximum_writer_rss_bytes=max(r['writer']['peak_rss_bytes'] for r in rows),
                    writer_memory_boundaries=memory_boundaries,commit_observations=commit_observations)
        finally:evidence.close()
    proof['plaintext_removed']=True
    sources=BASE/'wal-diagnostic-auditor-sources';sources.mkdir(exist_ok=True,mode=0o700)
    proof['audit_source_sha256']={}
    for path in (Path(__file__),Path(audit_native_sql_matrix.__file__),Path(wal_cost_contract.__file__),Path(wal_commit_diagnostic.__file__),Path(wal_diagnostic_supervisor.__file__)):
        data=path.read_bytes();sha=digest(data);saved=sources/(sha+'.py')
        if saved.exists():assert saved.read_bytes()==data
        else:saved.write_bytes(data);saved.chmod(0o600)
        proof['audit_source_sha256'][path.name]=sha
    destination=BASE/f'wal-diagnostic-{args.run}-proof.private.json'
    destination.write_text(json.dumps(proof,indent=2)+'\n');destination.chmod(0o600)
    print(json.dumps({k:v for k,v in proof.items() if k not in ('failure','supervised','commit_observations','writer_memory_boundaries')}))
    return 0 if proof['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
