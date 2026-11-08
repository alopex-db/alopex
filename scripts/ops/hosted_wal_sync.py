"""Manual ordinary-extension EIO acceptance using an already built release wheel."""
import base64
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import traceback

from hosted_dataframe_comparison import ROOT, Budget, atomic_json, install_product, require, sha
from wal_cost_supervisor import run_cell
from wal_sync_contract import CASES, verify


def main():
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('RUNNER_KIND')=='github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch','manual boundary required')
    os.umask(0o077)
    config=json.loads(base64.b64decode(os.environ['WAL_SYNC_CONFIG'],validate=True))
    require(config['cases']==list(CASES) and config['cell_seconds']==90
        and config['rss_bytes']==1073741824 and config['stage_seconds']==1200,'limits differ')
    allowed=sorted(os.sched_getaffinity(0));require(len(allowed)>=2,'CPU allocation unavailable')
    allocation=dict(client=allowed[:1],server=allowed[1:2])
    work=ROOT/'wal-sync-private';export=ROOT/'wal-sync-export'
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40000000000,reserve_bytes=256000000,
        poll_seconds=2,planned_bytes=dict(prepare=1000000000,measure=128000000,collect=32000000)))
    initial=budget.before('prepare');work.mkdir();export.mkdir()
    for name in ('imports','venvs','identities','cases','tmp'):(work/name).mkdir()
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PIP_DISABLE_PIP_VERSION_CHECK='1',PIP_NO_CACHE_DIR='1',TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN','GITHUB_TOKEN','WAL_SYNC_CONFIG','PYTHONPATH','PYTHONHOME','LD_LIBRARY_PATH',
        'ALOPEX_PERF_SUBJECT_MANIFEST','ALOPEX_PERF_SUBJECT_SHA256','ALOPEX_PERF_SERVER_URL',
        'NIM_SQL_PARSER_LIB_DIR','ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD'):env.pop(key,None)
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):env[key]='1'
    status='failed';failure=None;rows=[];signal.alarm(config['stage_seconds'])
    try:
        require(shutil.which('strace') is not None,'strace unavailable')
        for name,expected in config['sources'].items():require(sha(ROOT/name)==expected,'controller source differs')
        test_env=dict(env,PYTHONPATH=str(ROOT/'scripts/ops'))
        budget.run([sys.executable,'-B','-m','unittest','test_wal_sync_contract',
            'scripts.compatibility.test_sync_syscall','-v'],timeout=30,log=work/'contracts.log',env=test_env)
        product=install_product('candidate',config['product'],work,budget,env)
        subject=dict(schema='alopex.performance-subject/v1',version=product['receipt']['version'],
            source=product['receipt']['source'],cpu=allocation,
            python_wheel=dict(path=str(product['files']['wheel']),sha256=sha(product['files']['wheel'])),
            build_receipt=dict(path=str(product['files']['receipt']),sha256=sha(product['files']['receipt'])))
        manifest=work/'identities/candidate/subject.json';atomic_json(manifest,subject)
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,allocation=allocation))
        budget.before('measure')
        for case in CASES:
            output=work/'cases'/case;output.mkdir()
            params=dict(case=case,output=str(output),affinity=allocation['client'],sources=config['sources'],
                subject_manifest_sha256=sha(manifest),native_sha256=config['product']['native_sha256'])
            path=output/'configuration.json';atomic_json(path,params)
            child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'),
                ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                LD_LIBRARY_PATH=str(product['files']['library'].parent))
            run_cell(['taskset','--cpu-list',','.join(map(str,allocation['client'])),str(product['python']),'-B',
                str(ROOT/'scripts/ops/wal_sync_worker.py'),'--configuration',str(path)],child_env,output,budget,
                config['cell_seconds'],config['rss_bytes'])
            result=output/'result';report=json.loads((result/'report.json').read_bytes())
            row=verify(report,case,config['product'],config['sources'],
                (result/(case+'.baseline.trace')).read_text(),(result/(case+'.fault.trace')).read_text())
            require(not any(p.is_dir() for p in result.iterdir()),'fixture not removed')
            rows.append(row);atomic_json(work/'rows.json',rows)
        require(sha(product['manifest'])==config['product']['manifest_sha256']
            and sha(product['files']['wheel'])==config['product']['wheel_sha256'],'input changed')
        status='passed'
    except BaseException as error:
        failure=type(error).__name__;atomic_json(work/'failure.json',dict(error=repr(error),
            detail=getattr(error,'detail',''),traceback=traceback.format_exc()))
    finally:
        signal.alarm(0)
        for name in ('imports','venvs','tmp'):
            path=work/name
            if path.exists():shutil.rmtree(path)
        # Interrupted cases may retain a fixture directory, but traces stay as files.
        for result in (work/'cases').glob('*/result'):
            for path in result.iterdir():
                if path.is_dir():shutil.rmtree(path)
        atomic_json(work/'cleanup.json',dict(imports_venvs_fixtures_removed=True,initial_allocated_bytes=initial,
            final_allocated_bytes=budget.check()))
        budget.before('collect')
        members=[p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members)<16000000 and all(not p.is_symlink() for p in members),'evidence bound exceeded')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as output:
            for path in members:output.add(path,arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
            '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],capture_output=True,check=True,timeout=30)
        evidence_hash=sha(archive);shutil.rmtree(work);budget.check()
        atomic_json(export/'result.json',dict(schema='alopex.wal-sync-acceptance/v1',status=status,error_type=failure,
            controller=os.environ['GITHUB_SHA'],sources=config['sources'],complete_cases=len(rows),planned_cases=len(CASES),
            evidence_sha256=evidence_hash,encrypted_sha256=sha(export/'private-evidence.cms'),work_removed=not work.exists(),
            scope='ordinary extension Linux syscall EIO; not real device failure or power loss'))
    return 0 if status=='passed' else 1


if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError('acceptance interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGALRM,interrupted)
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed')))
    raise SystemExit(code)
