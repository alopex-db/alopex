"""Bounded, manual, source-pinned DataFrame comparison on a managed Linux VM."""
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'scripts/test-env'),str(ROOT/'scripts/performance')]
from capacity import Budget
from common import atomic_json
from phase_rss import PhaseRssError, parse_status
from dataframe_phase_contract import plan, verify_cell


def require(value, message):
    if not value:raise RuntimeError(message)


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def artifact_file(root, entry):
    relative=PurePosixPath(entry['path'])
    require(not relative.is_absolute() and '..' not in relative.parts,'unsafe asset path')
    path=root/relative
    require(path.resolve().is_relative_to(root.resolve()) and path.is_file() and not path.is_symlink()
        and path.stat().st_size==entry['size'] and sha(path)==entry['sha256'],'asset identity differs')
    return path


def metadata(endpoint):
    result=subprocess.run(['gh','api',endpoint],capture_output=True,text=True,check=True,timeout=30)
    return json.loads(result.stdout)


def install_product(subject, producer, work, budget, env):
    directory=work/'imports'/subject
    endpoint='repos/alopex-db/alopex/actions/runs/'+str(producer['run'])
    run=metadata(endpoint)
    require(run['head_sha']==producer['controller'] and run['status']=='completed' and run['conclusion']=='success'
        and run['event']=='workflow_dispatch' and run['head_repository']['full_name']=='alopex-db/alopex','producer differs')
    records=[r for r in metadata(endpoint+'/artifacts')['artifacts'] if r['name']==producer['artifact_name']]
    require(len(records)==1 and records[0]['id']==producer['artifact_id'] and not records[0]['expired']
        and records[0]['digest']==producer['artifact_digest'] and 0<records[0]['size_in_bytes']<64_000_000,'artifact differs')
    budget.run(['gh','run','download',str(producer['run']),'--repo','alopex-db/alopex','--name',producer['artifact_name'],
        '--dir',str(directory)],timeout=120,log=work/(subject+'-download.log'))
    path=directory/'assets/assets.json'
    require(sha(path)==producer['manifest_sha256'],'asset manifest differs')
    manifest=json.loads(path.read_bytes())
    require(manifest['source_commit']==producer['source_commit'],'product source differs')
    files={entry['role']:artifact_file(path.parent,entry) for entry in manifest['files']}
    require(len(files)==len(manifest['files']),'duplicate asset role')
    receipt=json.loads(files['receipt'].read_bytes())
    require(receipt['source']['commit']==producer['source_commit'] and receipt['source']['tree']==producer['source_tree']
        and receipt['source']['profile']=='release' and receipt['python_wheel_sha256']==sha(files['wheel'])==producer['wheel_sha256'],
        'original build receipt differs')
    target=work/'venvs'/subject
    budget.run([sys.executable,'-B','-m','venv',str(target)],timeout=90,log=work/(subject+'-install.log'),env=env)
    python=target/'bin/python'
    budget.run([str(python),'-B','-m','pip','install','--no-index','--no-deps','--no-cache-dir',str(files['wheel'])],
        timeout=90,log=work/(subject+'-install.log'),env=env)
    saved=work/'identities'/subject;saved.mkdir()
    shutil.copyfile(path,saved/'assets.json');shutil.copyfile(files['receipt'],saved/'build-receipt.json')
    return dict(python=python,files=files,receipt=receipt,manifest=path,producer=producer)


def install_reference(lock, work, budget, env):
    target=work/'venvs/polars'
    budget.run([sys.executable,'-B','-m','venv',str(target)],timeout=90,log=work/'polars-install.log',env=env)
    python=target/'bin/python';lock_path=work/'reference-lock.json';atomic_json(lock_path,lock)
    selection=work/'selected-wheels.json'
    code="""import json,sys
from pip._vendor.packaging.tags import sys_tags
from pip._vendor.packaging.utils import parse_wheel_filename
tags=list(sys_tags()); records=json.load(open(sys.argv[1])); selected=[]
for name in ('polars','polars-runtime-32'):
 candidates=[]
 for item in records[name]:
  parsed=parse_wheel_filename(item['filename']); matches=[tags.index(t) for t in parsed[3] if t in tags]
  if matches:candidates.append((min(matches),item['filename'],item))
 assert candidates
 selected.append(sorted(candidates,key=lambda x:x[:2])[0][2])
with open(sys.argv[2],'w') as stream:json.dump(selected,stream)
"""
    budget.run([str(python),'-B','-c',code,str(lock_path),str(selection)],timeout=30,log=work/'polars-select.log',env=env)
    selected=json.loads(selection.read_bytes());wheels=work/'reference-wheels';wheels.mkdir()
    identity=dict(schema='alopex.reference-wheels/v1',engine='polars',wheels=[])
    for record in selected:
        require(record['name'] in ('polars','polars-runtime-32') and record['version']=='1.43.2'
            and record['url'].startswith('https://files.pythonhosted.org/')
            and Path(record['filename']).name==record['filename'] and 0<record['size']<64_000_000,'reference lock invalid')
        destination=wheels/record['filename'];size=0
        with urllib.request.urlopen(record['url'],timeout=30) as response,destination.open('xb') as output:
            require(response.url==record['url'],'unexpected reference redirect')
            while block:=response.read(1024*1024):
                size+=len(block);require(size<=record['size'],'reference download oversized');output.write(block);budget.check()
        require(size==record['size'] and sha(destination)==record['sha256'],'reference wheel differs')
        identity['wheels'].append(dict(name=record['name'],version=record['version'],path=str(destination),sha256=record['sha256']))
    budget.run([str(python),'-B','-m','pip','install','--no-index','--no-deps','--no-cache-dir',
        *(entry['path'] for entry in identity['wheels'])],timeout=90,log=work/'polars-install.log',env=env)
    path=work/'reference-identity.json';atomic_json(path,identity)
    return python,path


def run_cell(command, env, directory, budget, timeout, rss_limit):
    """Stop/reap the owned group on error; preserve the original exit outcome."""
    budget.check();started=time.monotonic();peak=0
    with (directory/'worker.log').open('xb') as log:
        child=subprocess.Popen(command,stdout=log,stderr=log,env=env,start_new_session=True)
        try:
            while child.poll() is None:
                require(time.monotonic()-started<timeout,'cell deadline exceeded')
                budget.check()
                try:
                    observed=parse_status(Path(f'/proc/{child.pid}/status').read_text())['rss_bytes']
                    peak=max(peak,observed);require(observed<=rss_limit,'live process RSS bound exceeded')
                except (FileNotFoundError,PhaseRssError):
                    # /proc can lose RSS fields as the worker exits before poll()
                    # observes it. Allow one bounded exit transition, but fail if
                    # counters remain unavailable while the worker is live.
                    try:
                        child.wait(timeout=.25)
                    except subprocess.TimeoutExpired:
                        raise RuntimeError('live process counters disappeared') from None
                try:child.wait(timeout=.25)
                except subprocess.TimeoutExpired:pass
            require(child.returncode==0,'worker failed')
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
            atomic_json(directory/'supervision.json',dict(returncode=child.returncode,reaped=child.poll() is not None,
                elapsed_seconds=time.monotonic()-started,sampled_peak_rss_bytes=peak,
                sampled_peak_scope='supervisor samples across process startup, identity checks, workload and result saving'))


def main():
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ['RUNNER_KIND']=='github-hosted','unexpected execution boundary')
    os.umask(0o077)
    config=json.loads(base64.b64decode(os.environ['DATAFRAME_CONFIG'],validate=True))
    cells=plan(config['mode']);require(config['cpu_selection']=='first_allowed' and config['rss_bytes']>0,'invalid execution profile')
    allowed=sorted(os.sched_getaffinity(0));require(len(allowed)>=2,'CPU assignment unavailable')
    allocation=dict(client=allowed[:1],server=allowed[1:2])
    work=ROOT/'dataframe-comparison-private';export=ROOT/'dataframe-comparison-export'
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=256_000_000,poll_seconds=2,
        planned_bytes=dict(prepare=2_000_000_000,measure=256_000_000,collect=64_000_000)))
    initial=budget.before('prepare');work.mkdir();export.mkdir()
    for name in ('imports','venvs','identities','cells','tmp'):(work/name).mkdir()
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PIP_DISABLE_PIP_VERSION_CHECK='1',PIP_NO_CACHE_DIR='1',TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN','GITHUB_TOKEN','DATAFRAME_CONFIG','PYTHONPATH','PYTHONHOME','LD_LIBRARY_PATH','ALOPEX_PERF_SUBJECT_MANIFEST',
        'ALOPEX_PERF_SUBJECT_SHA256','ALOPEX_PERF_SERVER_URL','NIM_SQL_PARSER_LIB_DIR','ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD'):env.pop(key,None)
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','POLARS_MAX_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):env[key]='1'
    deadline=time.monotonic()+config['stage_seconds'];signal.alarm(config['stage_seconds'])
    rows=[];status='failed';failure=None
    try:
        for name,expected in config['sources'].items():require(sha(ROOT/name)==expected,'controller source differs')
        test_env=dict(env,PYTHONPATH=os.pathsep.join((str(ROOT/'scripts/ops'),str(ROOT/'scripts/performance'),str(ROOT/'scripts/test-env'))))
        budget.run([sys.executable,'-B','-m','unittest','test_dataframe_phase_observer','test_dataframe_phase_contract',
            'test_dataframe_reference_identity','test_hosted_dataframe_comparison','-v'],timeout=60,log=work/'contracts.log',env=test_env)
        products={name:install_product(name,config['products'][name],work,budget,env) for name in ('published','before','candidate')}
        reference_python,reference_identity=install_reference(config['reference_lock'],work,budget,env)
        for name,product in products.items():
            subject=dict(schema='alopex.performance-subject/v1',version=product['receipt']['version'],source=product['receipt']['source'],cpu=allocation,
                python_wheel=dict(path=str(product['files']['wheel']),sha256=sha(product['files']['wheel'])),
                build_receipt=dict(path=str(product['files']['receipt']),sha256=sha(product['files']['receipt'])))
            path=work/'identities'/name/'subject.json';atomic_json(path,subject);product['subject']=path
        harness=ROOT/'scripts/ops/dataframe_harness'
        hashes={name:sha(harness/name) for name in ('dataframe_phases_v0816.py','subject_identity.py')}
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,allocation=allocation,
            python_version=sys.version,harness_files=hashes,cells=cells))
        budget.before('measure')
        for cell in cells:
            require(deadline-time.monotonic()>config['cell_seconds']+60,'insufficient stage time for another cell')
            directory=work/'cells'/cell['id'];directory.mkdir()
            parameters=dict(cell,harness=str(harness),harness_files=hashes,output=str(directory),affinity=allocation['client'],
                rss_bytes=config['rss_bytes'],observer_sha256=sha(ROOT/'scripts/ops/dataframe_phase_observer.py'),
                phase_rss_sha256=sha(ROOT/'scripts/performance/phase_rss.py'))
            child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'))
            if cell['subject']=='polars':
                python=reference_python;parameters.update(reference_identity=str(reference_identity),reference_identity_sha256=sha(reference_identity))
            else:
                product=products[cell['subject']];python=product['python'];manifest=product['subject']
                parameters['subject_manifest_sha256']=sha(manifest)
                child_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                    LD_LIBRARY_PATH=str(product['files']['library'].parent))
            path=directory/'configuration.json';atomic_json(path,parameters)
            run_cell(['taskset','--cpu-list',','.join(map(str,allocation['client'])),str(python),'-B',
                str(ROOT/'scripts/ops/dataframe_phase_worker.py'),'--configuration',str(path)],child_env,directory,budget,
                config['cell_seconds'],config['rss_bytes'])
            result=json.loads((directory/'measurement.json').read_bytes())
            require(result['worker_sha256']==sha(ROOT/'scripts/ops/dataframe_phase_worker.py')
                and result['raw_sha256']==sha(directory/'raw.ndjson'),'worker or raw identity differs')
            raw=[json.loads(line) for line in (directory/'raw.ndjson').read_text().splitlines()]
            observations=json.loads((directory/'observations.json').read_bytes())
            checked=verify_cell(cell,result,raw,observations,config['rss_bytes'])
            if rows:require(checked['input_sha256']==rows[0]['checked']['input_sha256'] and checked['result_sha256']==rows[0]['checked']['result_sha256'],'cross-subject oracle differs')
            rows.append(dict(cell=cell,checked=checked,status='passed'));atomic_json(work/'cells.json',rows)
        status='passed'
    except BaseException as error:
        failure=type(error).__name__;atomic_json(work/'failure.json',dict(error=repr(error)))
    finally:
        signal.alarm(0)
        for name in ('imports','venvs','reference-wheels','tmp'):
            path=work/name
            if path.exists():shutil.rmtree(path)
        atomic_json(work/'cleanup.json',dict(imports_venvs_fixtures_removed=True,initial_allocated_bytes=initial,final_allocated_bytes=budget.check()))
        budget.before('collect')
        members=[p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members)<32_000_000 and all(not p.is_symlink() for p in members),'evidence bound exceeded')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as output:
            for path in members:output.add(path,arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
            '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],capture_output=True,check=True,timeout=30)
        evidence_hash=sha(archive);shutil.rmtree(work);budget.check()
        atomic_json(export/'result.json',dict(schema='alopex.dataframe-comparison/v1',status=status,error_type=failure,
            controller=os.environ['GITHUB_SHA'],sources=config['sources'],mode=config['mode'],complete_cells=len(rows),planned_cells=len(cells),
            evidence_sha256=evidence_hash,encrypted_sha256=sha(export/'private-evidence.cms'),work_removed=not work.exists(),
            performance_acceptance=False))
    return 0 if status=='passed' else 1


if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError('comparison interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGALRM,interrupted)
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed')))
    raise SystemExit(code)
