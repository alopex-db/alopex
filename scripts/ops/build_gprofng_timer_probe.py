"""Build only a pinned private collector and test its clock sampler."""
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request

from hnsw_timer_profiler_probe import ROOT, Budget, atomic_json, require, sha, safe_executable, run, sampled_cpu
from patch_gprofng_timer import patch

ARCHIVE_URL = 'https://ftp.gnu.org/gnu/binutils/binutils-2.42.tar.xz'
ARCHIVE_SHA = 'f6e4d41fd5fc778b06b7891457b3620da5ecea1006c6a4a41ae998109f85a800'
SOURCE_DIRS = ('gprofng/', 'include/', 'config/', 'opcodes/', 'bfd/', 'libiberty/')


def source_members(archive):
    selected, total, names = [], 0, set()
    for member in archive:
        parts = PurePosixPath(member.name).parts
        require(parts and parts[0] == 'binutils-2.42' and '..' not in parts, 'archive_path')
        relative = '/'.join(parts[1:])
        if not relative or not ('/' not in relative or relative.startswith(SOURCE_DIRS)):
            continue
        require(member.isdir() or member.isfile(), 'archive_type')
        if member.isfile():
            require(relative not in names, 'archive_duplicate')
            names.add(relative)
            total += member.size
            require(len(names) <= 2500 and total <= 110_000_000, 'source_bound')
            selected.append((member, relative))
    require(len(selected) == 1876 and total == 98_250_115, 'source_inventory')
    return selected


def freeze_measure(process, budget):
    """Stop only the owned build group while enumerating its transient files."""
    try:
        os.killpg(process.pid, signal.SIGSTOP)
    except ProcessLookupError:
        return budget.check()
    try:
        deadline = time.monotonic() + 2
        while True:
            states = []
            for entry in Path('/proc').iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
                except (FileNotFoundError, ProcessLookupError):
                    continue  # A process exited; storage enumeration remains strict.
                if int(fields[2]) == process.pid:
                    states.append(fields[0])
            if all(state in ('T', 't', 'Z', 'X') for state in states):
                return budget.check()
            require(time.monotonic() < deadline, 'build_freeze_timeout')
            time.sleep(0.01)
    finally:
        try:
            os.killpg(process.pid, signal.SIGCONT)
        except ProcessLookupError:
            pass


def build_command(command, name, cwd, work, env, budget, records):
    budget.check()
    launcher = ('import os,resource,sys; '
                'resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432)); '
                'resource.setrlimit(resource.RLIMIT_CPU,(180,180)); '
                'resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
                'os.execv(sys.argv[1],sys.argv[1:])')
    with (work / (name + '.log')).open('xb') as log:
        process = subprocess.Popen([sys.executable, '-I', '-B', '-c', launcher, *command],
            cwd=cwd, env=env, stdout=log, stderr=log, start_new_session=True)
        deadline = time.monotonic() + 180
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline, 'build_timeout')
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    freeze_measure(process, budget)
            require(process.returncode == 0, 'build_command_failed')
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
            records.append(dict(name=name, command=command, returncode=process.returncode, waited=True))
            atomic_json(work / 'processes.json', records)
    budget.check()


def workload():
    return '''import ctypes,hashlib,json,os,time,threading
from pathlib import Path
library=Path(os.environ['EXPECTED_COLLECTOR']).resolve()
loaded={Path(line.split()[-1]).resolve() for line in Path('/proc/self/maps').read_text().splitlines() if 'libgp-collector.so' in line}
assert loaded=={library}
api=ctypes.CDLL(str(library))
for name in ('collector_pause','collector_resume'):
    getattr(api,name).argtypes=[];getattr(api,name).restype=None
def spin(seconds):
    end=time.thread_time()+seconds;value=1
    while time.thread_time()<end:value=(value*1664525+1013904223)&0xffffffff
spin(.15);api.collector_pause();spin(.05);api.collector_resume();spin(.15)
thread=threading.Thread(target=spin,args=(.10,));thread.start();thread.join()
Path('loaded-collector.json').write_text(json.dumps({'sha256':hashlib.sha256(library.read_bytes()).hexdigest(),'pause_resume':True,'thread_joined':True}))
'''


def main():
    os.umask(0o077)
    require(sys.platform == 'linux' and os.getuid() != 0 and os.geteuid() != 0
            and os.environ.get('RUNNER_KIND') == 'github-hosted'
            and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
            and os.environ.get('GITHUB_REPOSITORY') == 'alopex-db/alopex', 'boundary')
    config = json.loads(base64.b64decode(os.environ['PROFILER_PROBE_CONFIG'], validate=True))
    for name, digest in config['sources'].items():
        require((ROOT/name).resolve().is_relative_to(ROOT) and sha(ROOT/name)==digest, 'source_identity')
    roots = [ROOT.parent, Path(os.environ['RUNNER_TEMP'])]
    roots += [p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists()]
    budget = Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=256_000_000,
        poll_seconds=2,planned_bytes={'build':1_000_000_000,'collect':64_000_000}))
    initial = budget.before('build')
    work, export = ROOT/'collector-repair-private', ROOT/'collector-repair-export'
    work.mkdir(mode=0o700);export.mkdir(mode=0o700)
    for name in ('home','tmp','source','build','assets'): (work/name).mkdir(mode=0o700)
    env = dict(PATH=os.defpath,HOME=str(work/'home'),TMPDIR=str(work/'tmp'),LC_ALL='C',
        XDG_CONFIG_HOME=str(work/'home'),XDG_CACHE_HOME=str(work/'home'),DEBUGINFOD_URLS='',
        PYTHONDONTWRITEBYTECODE='1',CFLAGS='-O2 -g0',CXXFLAGS='-O2 -g0')
    records, status, cpu = [], 'failed', 0
    try:
        proc = Path('/proc/self/status').read_text()
        require(int(re.search(r'^CapEff:\s*([0-9a-f]+)$',proc,re.M)[1],16)==0,'effective_capabilities')
        atomic_json(work/'identity.json',dict(controller=os.environ['GITHUB_SHA'],configuration=config,
            effective_capabilities_empty=True,process_status=proc))
        archive=work/'source.tar.xz'
        with urllib.request.urlopen(ARCHIVE_URL,timeout=30) as response, archive.open('xb') as out:
            total=0
            while block:=response.read(1024*1024):
                total+=len(block);require(total<=32_000_000,'download_bound');out.write(block);budget.check()
        require(sha(archive)==ARCHIVE_SHA,'archive_identity')
        source=work/'source'
        with tarfile.open(archive) as tar:
            for member,relative in source_members(tar):
                path=source/relative;path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                with tar.extractfile(member) as inp,path.open('xb') as out:shutil.copyfileobj(inp,out)
                path.chmod(0o700 if member.mode&0o111 else 0o600)
        budget.check();archive.unlink()
        dispatcher=source/'gprofng/libcollector/dispatcher.c'
        dispatcher.write_bytes(patch(dispatcher.read_bytes()))
        atomic_json(work/'source.json',dict(archive_sha256=ARCHIVE_SHA,patched_dispatcher=sha(dispatcher),
            extracted_files=1876,extracted_bytes=98250115))
        shell=safe_executable(shutil.which('sh',path=os.defpath))
        make=safe_executable(shutil.which('make',path=os.defpath))
        cc=safe_executable(shutil.which('cc',path=os.defpath))
        tool=safe_executable(shutil.which('gprofng',path=os.defpath))
        tracer=safe_executable(shutil.which('strace',path=os.defpath))
        atomic_json(work/'tools.json',{name:dict(path=str(path),sha256=sha(path)) for name,path in
            [('sh',shell),('make',make),('cc',cc),('gprofng',tool),('strace',tracer)]})
        for name in ('bfd','gprofng'): (work/'build'/name).mkdir(mode=0o700)
        bfd=work/'build/bfd';gp=work/'build/gprofng'
        build_command([str(shell),str(source/'bfd/configure'),'--disable-nls','--disable-werror'],
            'bfd-configure',bfd,work,env,budget,records)
        build_command([str(make),'-j2','stmp-bfd-h'],'bfd-headers',bfd,work,env,budget,records)
        build_command([str(shell),str(source/'gprofng/configure'),'--disable-gprofng-tools','--disable-werror','--disable-nls'],
            'collector-configure',gp,work,env,budget,records)
        build_command([str(make),'-C',str(gp/'libcollector'),'-j2','libgp-collector.la','libgp-collectorAPI.la'],
            'collector-build',gp,work,env,budget,records)
        build_command([str(cc),'-std=c11','-O1','-g0','-Wall','-Wextra','-Werror',
            str(ROOT/'scripts/ops/gprofng_timer_validity_test.c'),'-o',str(work/'timer-test')],
            'timer-test-build',work,work,env,budget,records)
        require(run([str(work/'timer-test')],'timer-unit',work,env,budget,records)==0,'timer_unit')
        assets=work/'assets'
        for name in ('libgp-collector.so','libgp-collectorAPI.so'):
            file=gp/'libcollector/.libs'/name
            require(file.is_file() and file.stat().st_size<32_000_000,'library_bound')
            shutil.copyfile(file,assets/name);(assets/name).chmod(0o500)
        library=assets/'libgp-collector.so'
        atomic_json(work/'asset-manifest.json',{p.name:sha(p) for p in assets.iterdir()})
        # Use both documented loader inputs. The driver prepends its search list;
        # LD_LIBRARY_PATH also preserves a valid private entry on older drivers.
        env.update(GPROFNG_PRELOAD_LIBDIRS=str(assets),LD_LIBRARY_PATH=str(assets),EXPECTED_COLLECTOR=str(library))
        command=[str(tracer),'-f','-qq','-e','trace=timer_create,timer_settime,timer_gettime,timer_delete',
            '-o',str(work/'timer-syscalls.log'),str(tool),'collect','app','-p','10','-F','off','-a','off',
            '-S','off','-j','off','-s','off','-H','off','-i','off','-o',str(work/'clock.er'),
            sys.executable,'-I','-B','-c',workload()]
        require(run(command,'record',work,env,budget,records)==0,'clock_collection')
        require(json.loads((work/'loaded-collector.json').read_text())['sha256']==sha(library),'loaded_identity')
        require((work/'clock.er/profile').stat().st_size>0,'clock_profile')
        log=(work/'clock.er/log.xml').read_text()
        require('itimer could not be set' not in log and 'kind="cerror"' not in log,'collector_error')
        require(run([str(tool),'display','text','-header','-metrics','name:e.totalcpu','-limit','20','-functions',
            str(work/'clock.er')],'report',work,env,budget,records)==0,'decode')
        report=(work/'report.stdout').read_text();require('Clock-profiling, interval =' in report,'clock_header')
        cpu=sampled_cpu(report);status='passed'
    except BaseException as error:
        atomic_json(work/'failure.json',dict(type=type(error).__name__,message=str(error)))
    # Retain compact provenance, probe traces and private libraries; source and
    # intermediate objects are regenerable and never uploaded.
    for name in ('source','build','tmp','home'):shutil.rmtree(work/name)
    for name in ('timer-test','source.tar.xz'):(work/name).unlink(missing_ok=True)
    atomic_json(work/'cleanup.json',dict(all_commands_waited=all(r['waited'] for r in records),
        source_and_build_removed=True,initial_allocated_bytes=initial,final_allocated_bytes=budget.check()))
    atomic_json(work/'outcome.json',dict(status=status,observed_cpu_seconds=cpu))
    budget.before('collect')
    members=[p for p in work.rglob('*') if p.is_file()]
    require(sum(p.stat().st_size for p in members)<64_000_000,'evidence_bound')
    archive=work/'evidence.tar.gz'
    with tarfile.open(archive,'w:gz') as tar:
        for path in members:tar.add(path,arcname=path.relative_to(work).as_posix())
    subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
        '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
        capture_output=True,check=True,timeout=20)
    digest=sha(archive);shutil.rmtree(work);budget.check()
    atomic_json(export/'result.json',dict(schema='alopex.collector-timer-repair/v1',status=status,
        controller=os.environ['GITHUB_SHA'],sources=config['sources'],observed_cpu_seconds=cpu,
        evidence_sha256=digest,encrypted_sha256=sha(export/'private-evidence.cms'),
        work_removed=True,performance_acceptance=False))
    return 0 if status=='passed' else 1

if __name__=='__main__':
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='completed' if code==0 else 'failed')))
    raise SystemExit(code)
