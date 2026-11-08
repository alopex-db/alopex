"""Bounded, manual capability check; no product benchmark or privilege changes."""
import base64
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/test-env'))
from capacity import Budget
from common import atomic_json

LIMIT = 8 * 1024 * 1024

def require(ok, reason):
    if not ok: raise ValueError(reason)

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def safe_executable(path):
    path = Path(path).resolve(strict=True)
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
            not info.st_mode & (stat.S_ISUID | stat.S_ISGID | 0o022), 'unsafe_executable')
    try: caps = os.getxattr(path, 'security.capability')
    except OSError as error:
        if error.errno not in (errno.ENODATA, errno.ENOTSUP): raise
        caps = b''
    require(not caps, 'file_capabilities_present')
    return path

def record_command(perf, output, python):
    child = ('import time; end=time.monotonic()+0.3; value=1\n'
             'while time.monotonic()<end: value=(value*1664525+1013904223)&0xffffffff\n')
    return [str(perf), 'record', '--no-buildid', '--no-buildid-cache', '--no-inherit',
            '--per-thread', '-e', 'cpu-clock:u', '-F', '99', '-m', '8', '-o', str(output),
            '--', str(python), '-I', '-B', '-c', child]

def sample_count(raw):
    rows = [line.strip() for line in raw.splitlines() if line.strip()]
    require(bool(rows) and all(re.fullmatch(r'[0-9a-fA-F]+', line) and int(line, 16) > 0
                              for line in rows), 'invalid_samples')
    return len(rows)

def public_result(status, reason, count, controller, sources, evidence, encrypted):
    require(status in ('supported', 'unavailable', 'failed'), 'status')
    require(reason in ('samples_observed', 'tool_missing', 'tool_unusable', 'permission_denied',
                       'event_unavailable', 'probe_failed'), 'reason')
    return dict(schema='alopex.profiler-capability/v1', status=status, reason=reason,
                samples=count, controller=controller, sources=sources,
                evidence_sha256=evidence, encrypted_sha256=encrypted,
                work_removed=True, performance_acceptance=False)

def run(command, name, work, env, budget, records):
    budget.check()
    # Limits apply only to the newly owned command, including its single child.
    launcher = ('import os,resource,sys; '
                'resource.setrlimit(resource.RLIMIT_FSIZE,(8388608,8388608)); '
                'resource.setrlimit(resource.RLIMIT_CPU,(3,3)); '
                'resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
                'os.execv(sys.argv[1],sys.argv[1:])')
    with (work / (name + '.stdout')).open('xb') as out, (work / (name + '.stderr')).open('xb') as err:
        process = subprocess.Popen([sys.executable, '-I', '-B', '-c', launcher, *command],
            cwd=work, env=env, stdout=out, stderr=err, start_new_session=True)
        start = time.monotonic()
        try:
            while process.poll() is None:
                require(time.monotonic() - start < 10, 'command_timeout')
                try: process.wait(timeout=0.1)
                except subprocess.TimeoutExpired: budget.check()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=2)
            records.append(dict(name=name, command=command, returncode=process.returncode, waited=True))
            atomic_json(work / 'processes.json', records)
    budget.check()
    return process.returncode

def main():
    os.umask(0o077)
    require(sys.platform == 'linux' and os.getuid() != 0 and os.geteuid() != 0
            and os.environ.get('RUNNER_KIND') == 'github-hosted'
            and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
            and os.environ.get('GITHUB_REPOSITORY') == 'alopex-db/alopex', 'boundary')
    config = json.loads(base64.b64decode(os.environ['PROFILER_PROBE_CONFIG'], validate=True))
    for name, digest in config['sources'].items():
        require((ROOT / name).resolve().is_relative_to(ROOT), 'source_path')
        require(sha(ROOT / name) == digest, 'source_identity')
    roots = [ROOT.parent, Path(os.environ['RUNNER_TEMP'])]
    roots += [p for p in (ROOT.parent.parent / '_actions', Path.home() / '.cargo', Path.home() / '.cache') if p.exists()]
    budget = Budget(dict(roots=list(map(str, roots)), stop_bytes=40_000_000_000,
        reserve_bytes=256_000_000, poll_seconds=0.1, planned_bytes={'probe':64_000_000,'collect':64_000_000}))
    initial = budget.before('probe')
    work, export = ROOT / 'profiler-probe-private', ROOT / 'profiler-probe-export'
    work.mkdir(mode=0o700); export.mkdir(mode=0o700)
    for name in ('home','tmp'): (work / name).mkdir(mode=0o700)
    env = dict(PATH=os.defpath, HOME=str(work/'home'), XDG_CONFIG_HOME=str(work/'home'),
        XDG_CACHE_HOME=str(work/'home'), TMPDIR=str(work/'tmp'), PERF_CONFIG='/dev/null',
        DEBUGINFOD_URLS='', LC_ALL='C', PYTHONDONTWRITEBYTECODE='1')
    status, reason, count = 'failed', 'probe_failed', 0
    records = []
    try:
        proc = Path('/proc/self/status').read_text()
        require(int(re.search(r'^CapEff:\s*([0-9a-f]+)$',proc,re.M)[1],16)==0, 'effective_capabilities')
        paranoid = Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip()
        atomic_json(work/'identity.json', dict(controller=os.environ['GITHUB_SHA'], configuration=config,
            effective_capabilities_empty=True, perf_event_paranoid=paranoid, process_status=proc))
        selected = shutil.which('perf', path=os.defpath)
        if selected is None:
            status, reason = 'unavailable', 'tool_missing'
        else:
            # Ubuntu's wrapper uses the running kernel's installed tool. Never try unrelated versions.
            matching = Path('/usr/lib/linux-tools') / os.uname().release / 'perf'
            perf = safe_executable(matching if matching.is_file() else selected)
            atomic_json(work/'tool.json', dict(path=str(perf), sha256=sha(perf)))
            if run([str(perf),'--version'],'version',work,env,budget,records) != 0:
                status, reason = 'unavailable', 'tool_unusable'
            else:
                code = run(record_command(perf,work/'perf.data',sys.executable), 'record',work,env,budget,records)
                if code:
                    raw = (work/'record.stderr').read_text(errors='replace')
                    if re.search(r'Permission denied|Operation not permitted|perf_event_paranoid',raw,re.I):
                        status, reason = 'unavailable', 'permission_denied'
                    elif re.search(r'not supported|No such device',raw,re.I):
                        status, reason = 'unavailable', 'event_unavailable'
                else:
                    require((work/'perf.data').stat().st_size <= LIMIT, 'record_bound')
                    require(run([str(perf),'script','-i',str(work/'perf.data'),'-F','ip'],
                        'samples',work,env,budget,records)==0,'decode_failed')
                    count = sample_count((work/'samples.stdout').read_text())
                    status, reason = 'supported', 'samples_observed'
    except BaseException as error:
        atomic_json(work/'failure.json',dict(type=type(error).__name__,message=str(error)))
    atomic_json(work/'outcome.json', dict(status=status,reason=reason,samples=count))
    atomic_json(work/'cleanup.json',dict(all_commands_waited=all(p['waited'] for p in records),
        initial_allocated_bytes=initial,final_allocated_bytes=budget.check()))
    budget.before('collect')
    members = [p for p in work.rglob('*') if p.is_file()]
    require(all(not p.is_symlink() for p in members) and sum(p.stat().st_size for p in members)<32_000_000,'evidence_bound')
    archive = work/'evidence.tar.gz'
    with tarfile.open(archive,'w:gz') as output:
        for path in members: output.add(path,arcname=path.relative_to(work).as_posix())
    subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
        '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],
        capture_output=True,check=True,timeout=20)
    evidence = sha(archive)
    shutil.rmtree(work); budget.check()
    atomic_json(export/'result.json',public_result(status,reason,count,os.environ['GITHUB_SHA'],config['sources'],
        evidence,sha(export/'private-evidence.cms')))
    return 0 if status != 'failed' else 1

if __name__ == '__main__':
    try: code=main()
    except BaseException: code=1
    print(json.dumps(dict(status='completed' if code==0 else 'failed')))
    raise SystemExit(code)
