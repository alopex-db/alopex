"""Manual-only release-wheel comparison; tiny cases, no native builds."""
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
import traceback

from hosted_dataframe_comparison import ROOT, Budget, atomic_json, install_product, require, sha
from wal_cost_contract import plan, summarize, verify_record
from wal_cost_supervisor import run_cell
from wal_phase_memory import validate_report


def main():
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('RUNNER_KIND')=='github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch','manual execution boundary required')
    os.umask(0o077)
    config=json.loads(base64.b64decode(os.environ['WAL_COST_CONFIG'],validate=True))
    require(config['diagnostic']=='wal-phase-memory/v1','diagnostic configuration required')
    cells=plan(config['mode'])
    require(config['cpu_selection']=='first_allowed' and config['cell_seconds']==30
        and config['rss_bytes']==1_073_741_824,'execution limits differ')
    allowed=sorted(os.sched_getaffinity(0));require(len(allowed)>=2,'CPU allocation unavailable')
    allocation=dict(client=allowed[:1],server=allowed[1:2])
    work=ROOT/'wal-cost-private';export=ROOT/'wal-cost-export'
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=256_000_000,
        poll_seconds=2,planned_bytes=dict(prepare=1_000_000_000,measure=128_000_000,collect=32_000_000)))
    initial=budget.before('prepare');work.mkdir();export.mkdir()
    for name in ('imports','venvs','identities','cells','tmp','fixtures'):(work/name).mkdir()
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PIP_DISABLE_PIP_VERSION_CHECK='1',PIP_NO_CACHE_DIR='1',TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN','GITHUB_TOKEN','WAL_COST_CONFIG','PYTHONPATH','PYTHONHOME','LD_LIBRARY_PATH',
        'ALOPEX_PERF_SUBJECT_MANIFEST','ALOPEX_PERF_SUBJECT_SHA256','ALOPEX_PERF_SERVER_URL',
        'NIM_SQL_PARSER_LIB_DIR','ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD'):env.pop(key,None)
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','POLARS_MAX_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):env[key]='1'
    deadline=time.monotonic()+config['stage_seconds'];signal.alarm(config['stage_seconds'])
    rows=[];status='failed';failure=None
    try:
        for name,expected in config['sources'].items():require(sha(ROOT/name)==expected,'controller source differs')
        test_env=dict(env,PYTHONPATH=os.pathsep.join((str(ROOT/'scripts/ops'),str(ROOT/'scripts/test-env/tests'))))
        budget.run([sys.executable,'-B','-m','unittest','test_wal_cost_contract','test_wal_cost_supervisor',
            'test_wal_cost_worker','test_wal_cost_commit_observer','test_capacity_snapshot',
            'test_wal_phase_memory','test_wal_commit_diagnostic','-v'],timeout=30,log=work/'contracts.log',env=test_env)
        products={name:install_product(name,config['products'][name],work,budget,env) for name in ('before','candidate')}
        before=products['before']['receipt'];after=products['candidate']['receipt']
        for key in ('tools','parser_record_sha256','parser_contract'):
            require(before[key]==after[key],'comparison toolchain/parser differs')
        require(before['source']['profile']==after['source']['profile']=='release','comparison profile differs')
        for name,product in products.items():
            subject=dict(schema='alopex.performance-subject/v1',version=product['receipt']['version'],
                source=product['receipt']['source'],cpu=allocation,
                python_wheel=dict(path=str(product['files']['wheel']),sha256=sha(product['files']['wheel'])),
                build_receipt=dict(path=str(product['files']['receipt']),sha256=sha(product['files']['receipt'])))
            path=work/'identities'/name/'subject.json';atomic_json(path,subject);product['subject']=path
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,
            allocation=allocation,python_version=sys.version,cells=cells))
        budget.before('measure')
        for cell in cells:
            require(deadline-time.monotonic()>3*(config['cell_seconds']+5)+60,'insufficient stage time')
            directory=work/'cells'/cell['id'];directory.mkdir()
            fixture=work/'fixtures'/cell['id'];fixture.mkdir()
            product=products[cell['subject']];manifest=product['subject']
            child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'),ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                LD_LIBRARY_PATH=str(product['files']['library'].parent))
            results=[]
            try:
                for index,phase in enumerate(('write','verify','verify')):
                    output=directory/str(index);output.mkdir()
                    parameters=dict(cell,phase=phase,fixture=str(fixture),output=str(output),affinity=allocation['client'],
                        options=config['options'],sources=config['sources'],subject_manifest_sha256=sha(manifest),diagnostic=config['diagnostic'])
                    path=output/'configuration.json';atomic_json(path,parameters)
                    run_cell(['taskset','--cpu-list',','.join(map(str,allocation['client'])),str(product['python']),'-B',
                        str(ROOT/'scripts/ops/wal_cost_worker.py'),'--configuration',str(path)],child_env,output,budget,
                        config['cell_seconds'],config['rss_bytes'])
                    record=json.loads((output/'worker.log').read_bytes())
                    identity=json.loads((output/'identity.json').read_bytes())
                    require(identity['manifest_sha256']==sha(manifest),'worker subject differs')
                    verify_record(record,cell,phase,config['products'][cell['subject']]['native_sha256'])
                    if phase=='write':
                        validate_report(json.loads((output/'phase-memory.json').read_bytes()))
                    results.append(record)
                rows.append(dict(cell=cell,writer=results[0],readers=results[1:]))
                atomic_json(work/'rows.json',rows)
            finally:
                shutil.rmtree(fixture);budget.check()
        atomic_json(work/'summary.json',summarize(rows,config['mode']))
        for product in products.values():
            require(sha(product['manifest'])==product['producer']['manifest_sha256'],'manifest changed')
            for item in product['files'].values():
                require(item.is_file(),'input disappeared')
            require(sha(product['files']['wheel'])==product['producer']['wheel_sha256'],'wheel changed')
        status='passed'
    except BaseException as error:
        failure=type(error).__name__;atomic_json(work/'failure.json',dict(error=repr(error),
            detail=getattr(error,'detail',''),traceback=traceback.format_exc()))
    finally:
        signal.alarm(0)
        for name in ('imports','venvs','fixtures','tmp'):
            path=work/name
            if path.exists():shutil.rmtree(path)
        atomic_json(work/'cleanup.json',dict(imports_venvs_fixtures_removed=True,initial_allocated_bytes=initial,
            final_allocated_bytes=budget.check()))
        budget.before('collect')
        members=[p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members)<16_000_000 and all(not p.is_symlink() for p in members),'evidence bound exceeded')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as output:
            for path in members:output.add(path,arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
            '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
            capture_output=True,check=True,timeout=30)
        evidence_hash=sha(archive);shutil.rmtree(work);budget.check()
        atomic_json(export/'result.json',dict(schema='alopex.wal-phase-memory-result/v1',status=status,error_type=failure,
            controller=os.environ['GITHUB_SHA'],sources=config['sources'],mode=config['mode'],
            complete_cells=len(rows),planned_cells=len(cells),evidence_sha256=evidence_hash,
            encrypted_sha256=sha(export/'private-evidence.cms'),work_removed=not work.exists(),performance_acceptance=False))
    return 0 if status=='passed' else 1


if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError('comparison interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGALRM,interrupted)
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed')))
    raise SystemExit(code)
