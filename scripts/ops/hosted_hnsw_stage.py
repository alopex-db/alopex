"""One explicitly selected #681 stage on the existing normal-user managed VM."""
import base64
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import time

from hnsw_stage_contract import PHASES, STAGES, plan, require, sha, verify_assets, verify_sample

ROOT = Path(__file__).resolve().parents[2]


def validate_config(config):
    require(config['schema'] == 'alopex.hnsw-stage-input/v1' and set(config['subjects']) == {'before','candidate'},
            'stage configuration differs')
    plan(config['rows'])
    time_budget = config.get('time_budget', 'original')
    require(time_budget in ('original', 'fullscale-1800')
            and (time_budget != 'fullscale-1800' or config['rows'] == 100000),
            'time budget does not match selected stage')
    case_seconds = 1800 if time_budget == 'fullscale-1800' else {1000:120,10000:300,100000:900}[config['rows']]
    require(config['limits'] == dict(case_seconds=case_seconds,
                                     rss_bytes=4*1024**3, runtime_bytes=5_000_000_000)
            and type(config['cpu_count']) is int and 1 <= config['cpu_count'] <= 64,
            'stage execution limits differ')
    require(config['stage_seconds'] == 10 * (config['limits']['case_seconds'] + 30) + 120,
            'stage deadline differs')
    before, candidate = (config['subjects'][role] for role in ('before','candidate'))
    for name in ('platform','example_source_sha256','rust_version','parser_contract','parser_sha256'):
        require(before[name] == candidate[name], 'comparison build conditions differ')
    require(config['instrument']['example_source_sha256'] == before['example_source_sha256'],
            'workload source differs')
    require((config.get('previous_stage') is None) == (config['rows'] == STAGES[0]),
            'previous stage evidence required')
    return config


def run_samples(config, execute, save, clock=time.monotonic):
    rows, failed = [], False
    deadline = clock() + config['stage_seconds']
    for cell in plan(config['rows']):
        row = dict(cell, status='not_run')
        rows.append(row)
        if failed or clock() + config['limits']['case_seconds'] + 30 > deadline:
            failed = True
            save(rows)
            continue
        try:
            row.update(execute(cell), status='passed')
        except BaseException as error:
            row.update(status='failed', error=repr(error))
            failed = True
            save(rows)
            if not isinstance(error, Exception):
                raise
        save(rows)
    return rows


def main():
    os.umask(0o077)
    require(sys.platform == 'linux' and os.geteuid() != 0
            and os.environ.get('RUNNER_KIND') == 'github-hosted', 'normal managed Linux VM required')
    sys.path.insert(0, str(ROOT / 'scripts/test-env'))
    from common import atomic_json, private_dir
    from capacity import Budget
    config = validate_config(json.loads(base64.b64decode(os.environ['HNSW_STAGE_CONFIG'],validate=True)))
    signal.alarm(config['stage_seconds'] + 1200)
    work, export = private_dir(ROOT/'hnsw-stage-private'), private_dir(ROOT/'hnsw-stage-export')
    roots = [ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget = Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=2_000_000_000,
                         poll_seconds=2,planned_bytes=dict(fetch=1_000_000_000,run=5_000_000_000,collect=128_000_000)))
    imports = {role:ROOT/f'hnsw-{role}-import' for role in ('before','candidate')}
    status, failure, rows, paths = 'failed', None, [], {}
    old_affinity = os.sched_getaffinity(0)
    try:
        initial = budget.before('fetch')
        atomic_json(work/'storage-before.json',dict(allocated_bytes=initial,roots=list(map(str,roots))))
        def metadata(endpoint):
            result=subprocess.run(['gh','api',endpoint],capture_output=True,text=True,check=True,timeout=30)
            return json.loads(result.stdout)
        def download(expected,name,directory):
            require(not directory.exists(), 'download destination already exists')
            endpoint='repos/alopex-db/alopex/actions/runs/'+str(expected['run'])
            run=metadata(endpoint)
            require(run['head_sha']==expected['controller'] and run['status']=='completed'
                    and run['conclusion']=='success' and run['event']=='workflow_dispatch'
                    and run['head_repository']['full_name']=='alopex-db/alopex','producer identity differs')
            records=[r for r in metadata(endpoint+'/artifacts')['artifacts'] if r['name']==name]
            require(len(records)==1 and not records[0]['expired'] and 0<records[0]['size_in_bytes']<268435456,
                    'artifact identity or bound differs')
            budget.run(['gh','run','download',str(expected['run']),'--repo','alopex-db/alopex',
                        '--name',name,'--dir',str(directory)],timeout=300,log=work/(directory.name+'.log'))
        for role,expected in config['subjects'].items():
            download(expected,'native-hnsw-example-assets',imports[role])
            paths[role]=verify_assets(imports[role],expected)
            paths[role]['binary'].chmod(0o700)
        previous=config.get('previous_stage')
        previous_import=ROOT/'hnsw-previous-import'
        if previous is not None:
            download(previous,'native-hnsw-stage',previous_import)
            raw=(previous_import/'result.json').read_bytes()
            require(sha(raw)==previous['result_sha256'],'previous stage digest differs')
            result=json.loads(raw)
            require(result['status']=='passed' and result['stage_rows']==STAGES[STAGES.index(config['rows'])-1]
                    and result['completed_samples']==10 and result['owned_resources_cleaned'] is True
                    and result['assets']=={role:s['assets_digest'] for role,s in config['subjects'].items()},
                    'previous stage is not the same comparison')
        expected_instrument=config['instrument']
        for name,field in (('scripts/performance/issue681_manual.py','runner_sha256'),
                           ('scripts/performance/phase_rss.py','phase_rss_source_sha256'),
                           ('crates/alopex-embedded/examples/issue681_hnsw_first_write.rs','example_source_sha256')):
            require(sha((ROOT/name).read_bytes())==expected_instrument[field],'instrument hash differs')
        allowed=sorted(old_affinity)
        require(len(allowed)>=config['cpu_count'],'CPU allocation unavailable')
        chosen=allowed[:config['cpu_count']]
        os.sched_setaffinity(0,chosen)
        require(os.sched_getaffinity(0)==set(chosen),'CPU affinity differs')
        controller=os.environ['GITHUB_SHA']
        atomic_json(work/'plan.json',dict(configuration=config,controller=controller,cells=plan(config['rows']),affinity=chosen))
        budget.before('run')
        def execute(cell):
            budget.check()
            case=private_dir(work/cell['id']);temporary=private_dir(case/'tmp')
            output=case/'output'
            selected=paths[cell['subject']]
            env=dict(os.environ,LD_LIBRARY_PATH=str(selected['library'].parent),TMPDIR=str(temporary),
                     PYTHONDONTWRITEBYTECODE='1')
            for name in ('CI','GH_TOKEN','GITHUB_TOKEN','HNSW_STAGE_CONFIG','NATIVE_IMAGE_INPUTS'):
                env.pop(name,None)
            budget.run([sys.executable,'-B',str(ROOT/'scripts/performance/issue681_manual.py'),str(selected['binary']),
                        '--output',str(output),'--rows',str(cell['rows']),'--dimensions','384',
                        '--timeout',str(config['limits']['case_seconds']),'--rss-limit',str(config['limits']['rss_bytes']),
                        '--phase-kernel-rss'],timeout=config['limits']['case_seconds']+30,log=case/'runner.log',env=env)
            raw=(output/'events.jsonl').read_bytes()
            values=verify_sample(json.loads((output/'identity.json').read_bytes()),
                                 json.loads((output/'summary.json').read_bytes()),
                                 [json.loads(line) for line in raw.splitlines()],cell,
                                 config['subjects'][cell['subject']],expected_instrument,controller,config['limits'])
            budget.check()
            shutil.rmtree(temporary)
            return dict(values=values,files={p.relative_to(case).as_posix():sha(p.read_bytes())
                                            for p in case.rglob('*') if p.is_file()})
        rows=run_samples(config,execute,lambda values:atomic_json(work/'progress.json',values))
        require(len(rows)==10 and all(r['status']=='passed' for r in rows),'stage sample incomplete')
        for role in imports:verify_assets(imports[role],config['subjects'][role])
        status='passed'
    except BaseException as error:
        failure=repr(error)
        atomic_json(work/'failure.json',dict(error=failure))
    finally:
        os.sched_setaffinity(0,old_affinity)
        atomic_json(work/'results.json',rows)
        # Keep failed process logs as well; only encrypted data is exported.
        budget.before('collect')
        members=[p for p in work.rglob('*') if p.is_file()]
        require(not any(p.is_symlink() for p in work.rglob('*'))
                and sum(p.stat().st_size for p in members)<64_000_000,'evidence size or type bound')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as tar:
            for p in members:budget.check();tar.add(p,arcname=p.relative_to(work).as_posix())
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
                        '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
                       capture_output=True,check=True,timeout=30)
        for path in [*imports.values(),ROOT/'hnsw-previous-import']:
            if path.exists():shutil.rmtree(path)
        shutil.rmtree(work)
        final=budget.check()
        cleanup_file=ROOT/'hnsw-cleanup-private.json'
        atomic_json(cleanup_file,dict(allocated_bytes=final,imports_removed=True,raw_work_removed=True,
                                     restored_affinity=sorted(os.sched_getaffinity(0))))
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(cleanup_file),
                        '-out',str(export/'cleanup.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
                       capture_output=True,check=True,timeout=30)
        cleanup_file.unlink()
        budget.check()
        public=dict(schema='alopex.hnsw-stage/v1',status=status,stage_rows=config['rows'],
                    controller=os.environ['GITHUB_SHA'],completed_samples=sum(r['status']=='passed' for r in rows),
                    assets={role:s['assets_digest'] for role,s in config['subjects'].items()},
                    owned_resources_cleaned=True,performance_qualification=False)
        atomic_json(export/'result.json',public)
        signal.alarm(0)
    return 0 if status=='passed' else 1


if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError('stage interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGALRM,interrupted)
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed',error_code=None if code==0 else 'PERFORMANCE')))
    raise SystemExit(code)
