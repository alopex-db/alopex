"""Manual sampling of the unchanged continuous HNSW search loop."""
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
from hnsw_search_sampling_contract import plan,verify_sampling,collector_command,verify_profile
from hnsw_search_phase_contract import verify_input
from hosted_hnsw_search import verify_cell
from gprofng_collector_build import build_collector
from hnsw_timer_profiler_probe import safe_executable
import re
from dataframe_phase_contract import verify_resources
from hnsw_search_dependencies import install_set,PREPARE
from hnsw_search_phase_dependencies import install
from hnsw_search_phase_venv import create_venv
from hnsw_search_lifecycle import prepare_cell_temp,cleanup_cell_temp


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


def main():
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('RUNNER_KIND')=='github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_HNSW_SEARCH_SAMPLING')=='1','unexpected HNSW boundary')
    os.umask(0o077);config=json.loads(base64.b64decode(os.environ['HNSW_SEARCH_SAMPLING_CONFIG'],validate=True))
    require(set(config['products'])=={'before','candidate'},'diagnostic products differ')
    require(set(config['input_identity'])=={'queries_sha256','csv_sha256'},
        'fixed input identity is incomplete')
    cells=plan(config['mode']);allowed=sorted(os.sched_getaffinity(0))
    require(config['cpu_selection']=='first_allowed' and len(allowed)>=2,'CPU assignment unavailable')
    allocation=dict(client=allowed[:1],server=allowed[1:2])
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=512_000_000,
        poll_seconds=2,planned_bytes=dict(prepare=5_000_000_000,measure=512_000_000,collect=512_000_000)))
    initial=budget.before('prepare')
    work,export=ROOT/'hnsw-search-sampling-private',ROOT/'hnsw-search-sampling-export';work.mkdir();export.mkdir()
    for name in ('imports','venvs','identities','cells','fixtures','tmp'):(work/name).mkdir()
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PIP_DISABLE_PIP_VERSION_CHECK='1',PIP_NO_CACHE_DIR='1',TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN','GITHUB_TOKEN','HNSW_SEARCH_SAMPLING_CONFIG','PYTHONPATH','PYTHONHOME','LD_LIBRARY_PATH',
        'ALOPEX_PERF_SUBJECT_MANIFEST','ALOPEX_PERF_SUBJECT_SHA256','ALOPEX_PERF_SERVER_URL',
        'ALOPEX_PERF_RAW_PATH','ALOPEX_PROFILE_HNSW','ALOPEX_HNSW_PHASE_PROBE','NIM_SQL_PARSER_LIB_DIR','ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD',
        'HNSWLIB_NO_NATIVE','CFLAGS','CXXFLAGS','LDFLAGS','LD_PRELOAD','GPROFNG_PRELOAD_LIBDIRS','EXPECTED_COLLECTOR'):
        env.pop(key,None)
    for key in list(env):
        if key.startswith('SP_COLLECTOR_'):env.pop(key)
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        env[key]='1'
    deadline=time.monotonic()+config['stage_seconds'];signal.alarm(config['stage_seconds'])
    completed=[];status='failed';failure=None
    try:
        for name,expected in config['sources'].items():require(sha(ROOT/name)==expected,'HNSW source differs')
        test_env=dict(env,PYTHONPATH=os.pathsep.join(str(ROOT/p) for p in ('scripts/ops','scripts/performance','scripts/test-env')))
        budget.run([sys.executable,'-B','-m','unittest','test_hnsw_search_sampling','test_hnsw_search_point',
            'test_hnsw_search_contract','test_hnsw_search_phase_venv','test_hnsw_search_lifecycle','-v'],
            timeout=60,log=work/'contracts.log',env=test_env)
        proc=Path('/proc/self/status').read_text()
        require(int(re.search(r'^CapEff:\s*([0-9a-f]+)$',proc,re.M)[1],16)==0,'effective capabilities')
        atomic_json(work/'process-boundary.json',dict(effective_capabilities_empty=True,process_status=proc))
        collector_work=work/'collector';collector_work.mkdir()
        for name in ('home','tmp','source','build','assets'):(collector_work/name).mkdir(mode=0o700)
        collector_env=dict(PATH=os.defpath,HOME=str(collector_work/'home'),TMPDIR=str(collector_work/'tmp'),
            LC_ALL='C',XDG_CONFIG_HOME=str(collector_work/'home'),XDG_CACHE_HOME=str(collector_work/'home'),
            DEBUGINFOD_URLS='',PYTHONDONTWRITEBYTECODE='1',CFLAGS='-O2 -g0',CXXFLAGS='-O2 -g0')
        collector_assets=build_collector(collector_work,collector_env,budget,[])
        collector_library=collector_assets/'libgp-collector.so'
        collector=dict(path=str(collector_library),sha256=sha(collector_library))
        for name in ('source','build'):shutil.rmtree(collector_work/name)
        (collector_work/'timer-test').unlink()
        tool=safe_executable(shutil.which('gprofng',path=os.defpath))
        atomic_json(collector_work/'tool.json',dict(path=str(tool),sha256=sha(tool)))
        env.update(HOME=str(collector_work/'home'),XDG_CONFIG_HOME=str(collector_work/'home'),
            XDG_CACHE_HOME=str(collector_work/'home'),DEBUGINFOD_URLS='')
        products={name:install_product(name,config['products'][name],work,budget,env,create_environment=create_venv) for name in ('before','candidate')}
        lock=json.loads((ROOT/'scripts/ops/hnsw_search.lock.json').read_bytes())
        paths,records=install(lock,work,budget,env,ROOT)
        interpreters={};identities={}
        for name,names in (('prepare',PREPARE),):
            target=work/'venvs'/name
            create_venv(target,work,budget,env,work/'dependencies.log')
            interpreters[name]=target/'bin/python'
            identities[name]=install_set(name,interpreters[name],names,paths,records,work,budget,env,ROOT)
        for name,product in products.items():
            identities[name]=install_set(name,product['python'],('numpy',),paths,records,work,budget,env,ROOT)
            subject=dict(schema='alopex.performance-subject/v1',version=product['receipt']['version'],source=product['receipt']['source'],
                cpu=allocation,python_wheel=dict(path=str(product['files']['wheel']),sha256=sha(product['files']['wheel'])),
                build_receipt=dict(path=str(product['files']['receipt']),sha256=sha(product['files']['receipt'])))
            path=work/'identities'/name/'subject.json';atomic_json(path,subject);product['subject']=path
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,allocation=allocation,
            python_version=sys.version,cells=cells,collector=collector))
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
        fixture_identity=json.loads((evidence/'input-manifest.json').read_bytes())
        verify_input(fixture_identity,config['input_identity'],config['mode'])
        truth=json.loads((evidence/'truth.json').read_bytes());budget.before('measure')
        for probe in cells:
            cell=probe['normal'];mode=probe['sampling_mode']
            require(deadline-time.monotonic()>config['cell_seconds']+60,'insufficient HNSW stage time')
            directory=work/'cells'/probe['id'];directory.mkdir()
            scratch=prepare_cell_temp(work,cell['id'])
            key=cell['subject']
            parameters=dict(cell,mode=config['mode'],root=str(ROOT),sources=config['sources'],output=str(directory),
                data=str(work/'fixtures'),input_sha256=sha(evidence/'input.npz'),truth_sha256=sha(evidence/'truth.json'),
                affinity=allocation['client'],rss_bytes=config['rss_bytes'],dependency_identity=str(identities[key]),
                dependency_identity_sha256=sha(identities[key]),sampling_mode=mode,collector=collector)
            child_env=dict(env,PYTHONPATH=str(ROOT/'scripts/performance'),TMPDIR=str(scratch))
            product=products[key];python=product['python'];manifest=product['subject']
            parameters['subject_manifest_sha256']=sha(manifest)
            child_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                LD_LIBRARY_PATH=str(product['files']['library'].parent))
            path=directory/'configuration.json';atomic_json(path,parameters)
            command=[str(python),'-B',str(ROOT/'scripts/ops/hnsw_search_sampling.py'),'--configuration',str(path)]
            if mode!='baseline':
                child_env.update(GPROFNG_PRELOAD_LIBDIRS=str(collector_assets),
                    LD_LIBRARY_PATH=str(collector_assets)+os.pathsep+child_env['LD_LIBRARY_PATH'])
                command=collector_command(tool,directory/'search.er',command)
            command=['taskset','--cpu-list',','.join(map(str,allocation['client'])),*command]
            run_cell(command,child_env,directory,budget,config['cell_seconds'],config['rss_bytes'])
            checked=verify_cell(cell,directory,truth,config)
            verify_sampling(json.loads((directory/'sampling.json').read_bytes()),probe,collector['sha256'])
            if mode!='baseline':
                log=(directory/'search.er/log.xml').read_text()
                profile=directory/'search.er/profile'
                profile_bytes=profile.stat().st_size if profile.exists() else 0
                report=directory/'profile-report.log';raw=None
                if profile_bytes:
                    budget.run([str(tool),'display','text','-header','-metrics','name:e.totalcpu','-limit','100',
                        '-functions',str(directory/'search.er')],timeout=60,log=report,env=env)
                    raw=report.read_text()
                receipt=verify_profile(mode,config['mode'],log,profile_bytes,raw)
                atomic_json(directory/'profile-receipt.json',dict(receipt,mode=mode,profile_bytes=profile_bytes,
                    report_sha256=sha(report) if raw is not None else None,collector_sha256=collector['sha256'],performance_acceptance=False))
            cleanup=cleanup_cell_temp(work,cell['id'],json.loads((directory/'supervision.json').read_bytes()))
            atomic_json(directory/'temporary-cleanup.json',cleanup);budget.check()
            completed.append(dict(probe=probe,points=checked,status='passed'));atomic_json(work/'cells.json',completed)
        status='passed'
    except BaseException as error:
        failure=type(error).__name__;atomic_json(work/'failure.json',error_record(error))
    finally:
        signal.alarm(0)
        for name in ('imports','venvs','bootstrap-wheels','reference-wheels','reference-built','fixtures','tmp','collector/assets','collector/home','collector/tmp','collector/source','collector/build'):
            path=work/name
            if path.exists():shutil.rmtree(path)
        for name in ('timer-test','source.tar.xz'):(work/'collector'/name).unlink(missing_ok=True)
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
        atomic_json(export/'result.json',dict(schema='alopex.hnsw-search-sampling/v1',status=status,error_type=failure,
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
