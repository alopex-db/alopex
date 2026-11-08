"""Manual fixed HNSW comparison within the existing managed VM boundary."""
import base64
import gzip
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request
import zipfile

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/test-env'),str(ROOT/'scripts/performance')]
from capacity import Budget
from common import atomic_json
from hosted_dataframe_comparison import install_product,require,sha
from private_failure import error_record
from wal_cost_supervisor import run_cell
from hnsw_search_contract import plan,verify_point,RESOURCE_SCOPE
from dataframe_phase_contract import verify_resources
from hnsw_search_dependencies import install,install_set,PREPARE,REFERENCE


def fetch_dataset(directory,budget):
    path=directory/'amazon.zip';size=0
    with urllib.request.urlopen('https://github.com/user-attachments/files/31764184/archive.zip',timeout=30) as response,path.open('xb') as output:
        require(response.url.startswith(('https://github.com/','https://objects.githubusercontent.com/',
            'https://github-production-user-asset-6210df.s3.amazonaws.com/')),'dataset redirect differs')
        while block:=response.read(1024*1024):
            size+=len(block);require(size<=32_000_000,'dataset download exceeds bound');output.write(block);budget.check()
    require(sha(path)=='fb3c0bc51dbfaae83c8b72cf472a69fe4a894d4d9e8fa639ff22f4100da123ea','dataset archive differs')
    with zipfile.ZipFile(path) as archive:
        members=archive.infolist();require(sum(m.file_size for m in members)<64_000_000,'dataset archive exceeds bound')
        candidates=[m for m in members if m.filename.endswith('.csv')]
        require(len(candidates)==1,'dataset member ambiguous')
        (directory/'amazon.csv').write_bytes(archive.read(candidates[0]))
    require(sha(directory/'amazon.csv')=='ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291',
        'dataset CSV differs')
    path.unlink()


def verify_cell(cell,directory,truth,config):
    result=json.loads((directory/'measurement.json').read_bytes())
    require(result['cell']==cell and result['index_closed'] is True and result['sources']==config['sources']
        and result['worker_sha256']==config['sources']['scripts/ops/hnsw_search_worker.py'],'worker result differs')
    points=json.loads((directory/'points.json').read_bytes())
    require(result['points']==points and [p['ef_search'] for p in points]==cell['ef_values'],'search coverage differs')
    observed=json.loads((directory/'observations.json').read_bytes())
    require(observed['pending'] is None and len(observed['completed'])==len(points),'search observations incomplete')
    checked=[]
    for index,point in enumerate(points):
        filename=f"ef-{point['ef_search']}.json.gz"
        require(point['file']==filename and sha(directory/filename)==point['sha256'],'search raw differs')
        with gzip.open(directory/filename,'rt') as stream:record=json.load(stream)
        row=record['measurement'];verify_point(cell,row,record['predictions'],truth['truth'],truth['acceptable'])
        require(row['query_count']==point['query_count'] and record['trial']==cell['trial'],'search sequence differs')
        resources=record['resources'];verify_resources(resources,index,config['rss_bytes'])
        require(resources==observed['completed'][index] and resources['scope']==RESOURCE_SCOPE,'search resource scope differs')
        if cell['engine']=='alopex-hnsw':
            require(len(record['native_diagnostics'])==cell['query_vectors'] and
                all(r['nodes_visited']>0 and r['distance_computations']>0 for r in record['native_diagnostics']),
                'native search path evidence missing')
        else:require(record['native_diagnostics']==[],'unexpected native diagnostics')
        checked.append(dict(ef_search=row['ef_search'],query_count=row['query_count'],recall=row['recall_at_10']))
    return checked


def main():
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('RUNNER_KIND')=='github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_HNSW_SEARCH')=='1','unexpected HNSW boundary')
    os.umask(0o077);config=json.loads(base64.b64decode(os.environ['HNSW_SEARCH_CONFIG'],validate=True))
    cells=plan(config['mode']);allowed=sorted(os.sched_getaffinity(0))
    require(config['cpu_selection']=='first_allowed' and len(allowed)>=2,'CPU assignment unavailable')
    allocation=dict(client=allowed[:1],server=allowed[1:2])
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=512_000_000,
        poll_seconds=2,planned_bytes=dict(prepare=4_000_000_000,measure=512_000_000,collect=512_000_000)))
    initial=budget.before('prepare')
    work,export=ROOT/'hnsw-search-private',ROOT/'hnsw-search-export';work.mkdir();export.mkdir()
    for name in ('imports','venvs','identities','cells','fixtures','tmp'):(work/name).mkdir()
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PIP_DISABLE_PIP_VERSION_CHECK='1',PIP_NO_CACHE_DIR='1',TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN','GITHUB_TOKEN','HNSW_SEARCH_CONFIG','PYTHONPATH','PYTHONHOME','LD_LIBRARY_PATH',
        'ALOPEX_PERF_SUBJECT_MANIFEST','ALOPEX_PERF_SUBJECT_SHA256','ALOPEX_PERF_SERVER_URL',
        'ALOPEX_PERF_RAW_PATH','ALOPEX_PROFILE_HNSW','NIM_SQL_PARSER_LIB_DIR','ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD',
        'HNSWLIB_NO_NATIVE','CFLAGS','CXXFLAGS','LDFLAGS'):
        env.pop(key,None)
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        env[key]='1'
    deadline=time.monotonic()+config['stage_seconds'];signal.alarm(config['stage_seconds'])
    completed=[];status='failed';failure=None
    try:
        for name,expected in config['sources'].items():require(sha(ROOT/name)==expected,'HNSW source differs')
        test_env=dict(env,PYTHONPATH=os.pathsep.join(str(ROOT/p) for p in ('scripts/ops','scripts/performance','scripts/test-env')))
        budget.run([sys.executable,'-B','-m','unittest','test_hnsw_search_contract','test_hnsw_search_identity','test_hnsw_search_point',
            'test_private_failure','-v'],timeout=60,log=work/'contracts.log',env=test_env)
        products={name:install_product(name,config['products'][name],work,budget,env) for name in ('published','before','candidate')}
        lock=json.loads((ROOT/'scripts/ops/hnsw_search.lock.json').read_bytes())
        paths,records=install(lock,work,budget,env,ROOT)
        interpreters={};identities={}
        for name,names in (('prepare',PREPARE),('reference',REFERENCE)):
            target=work/'venvs'/name
            budget.run([sys.executable,'-B','-m','venv',str(target)],timeout=90,log=work/'dependencies.log',env=env)
            interpreters[name]=target/'bin/python'
            identities[name]=install_set(name,interpreters[name],names,paths,records,work,budget,env,ROOT)
        for name,product in products.items():
            identities[name]=install_set(name,product['python'],('numpy',),paths,records,work,budget,env,ROOT)
            subject=dict(schema='alopex.performance-subject/v1',version=product['receipt']['version'],source=product['receipt']['source'],
                cpu=allocation,python_wheel=dict(path=str(product['files']['wheel']),sha256=sha(product['files']['wheel'])),
                build_receipt=dict(path=str(product['files']['receipt']),sha256=sha(product['files']['receipt'])))
            path=work/'identities'/name/'subject.json';atomic_json(path,subject);product['subject']=path
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,allocation=allocation,
            python_version=sys.version,cells=cells))
        if config['mode']=='full':fetch_dataset(work/'fixtures',budget)
        fixture=work/'fixture-process';fixture.mkdir()
        parameters=dict(mode=config['mode'],root=str(ROOT),sources=config['sources'],data=str(work/'fixtures'),
            affinity=allocation['client'],dependency_identity=str(identities['prepare']),
            dependency_identity_sha256=sha(identities['prepare']))
        atomic_json(fixture/'configuration.json',parameters)
        child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'))
        run_cell(['taskset','--cpu-list',','.join(map(str,allocation['client'])),str(interpreters['prepare']),'-B',
            str(ROOT/'scripts/ops/hnsw_search_fixture.py'),'--configuration',str(fixture/'configuration.json')],
            child_env,fixture,budget,config['fixture_seconds'],config['rss_bytes'])
        evidence=work/'input';evidence.mkdir()
        for name in ('input.npz','input-manifest.json','truth.json'):shutil.copyfile(work/'fixtures'/name,evidence/name)
        truth=json.loads((evidence/'truth.json').read_bytes());budget.before('measure')
        for cell in cells:
            require(deadline-time.monotonic()>config['cell_seconds']+60,'insufficient HNSW stage time')
            directory=work/'cells'/cell['id'];directory.mkdir()
            key=cell['subject'] if cell['engine']=='alopex-hnsw' else 'reference'
            parameters=dict(cell,mode=config['mode'],root=str(ROOT),sources=config['sources'],output=str(directory),
                data=str(work/'fixtures'),input_sha256=sha(evidence/'input.npz'),truth_sha256=sha(evidence/'truth.json'),
                affinity=allocation['client'],rss_bytes=config['rss_bytes'],dependency_identity=str(identities[key]),
                dependency_identity_sha256=sha(identities[key]))
            child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'))
            if cell['engine']=='alopex-hnsw':
                product=products[key];python=product['python'];manifest=product['subject']
                parameters['subject_manifest_sha256']=sha(manifest)
                child_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                    LD_LIBRARY_PATH=str(product['files']['library'].parent))
            else:python=interpreters['reference']
            path=directory/'configuration.json';atomic_json(path,parameters)
            run_cell(['taskset','--cpu-list',','.join(map(str,allocation['client'])),str(python),'-B',
                str(ROOT/'scripts/ops/hnsw_search_worker.py'),'--configuration',str(path)],
                child_env,directory,budget,config['cell_seconds'],config['rss_bytes'])
            checked=verify_cell(cell,directory,truth,config)
            completed.append(dict(cell=cell,points=checked,status='passed'));atomic_json(work/'cells.json',completed)
        status='passed'
    except BaseException as error:
        failure=type(error).__name__;atomic_json(work/'failure.json',error_record(error))
    finally:
        signal.alarm(0)
        for name in ('imports','venvs','reference-wheels','reference-built','fixtures','tmp'):
            path=work/name
            if path.exists():shutil.rmtree(path)
        atomic_json(work/'cleanup.json',dict(imports_venvs_fixtures_removed=True,
            initial_allocated_bytes=initial,final_allocated_bytes=budget.check()))
        budget.before('collect')
        members=[p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members)<256_000_000 and all(not p.is_symlink() for p in members),
            'HNSW evidence exceeds bound')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as output:
            for path in members:output.add(path,arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
            '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
            capture_output=True,check=True,timeout=30)
        evidence_hash=sha(archive);shutil.rmtree(work);budget.check()
        atomic_json(export/'result.json',dict(schema='alopex.hnsw-search/v1',status=status,error_type=failure,
            controller=os.environ['GITHUB_SHA'],sources=config['sources'],mode=config['mode'],
            complete_cells=len(completed),planned_cells=len(cells),evidence_sha256=evidence_hash,
            encrypted_sha256=sha(export/'private-evidence.cms'),work_removed=not work.exists(),performance_acceptance=False))
    return 0 if status=='passed' else 1


if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError('HNSW comparison interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGALRM,interrupted)
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed')))
    raise SystemExit(code)
