"""Actual Linux self-observation using bounded memory and one filesystem page."""
import hashlib
import json
import mmap
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/performance'))
sys.path.insert(0,str(ROOT/'scripts/test-env'))
from capacity import Budget
from common import atomic_json
from dataframe_phase_observer import DataFrameObserver
from dataframe_phase_contract import verify_resources


def main():
    assert sys.platform=='linux' and os.getuid()!=0 and os.environ['RUNNER_KIND']=='github-hosted'
    os.umask(0o077)
    work=ROOT/'dataframe-observer-private';export=ROOT/'dataframe-observer-export'
    roots=[ROOT.parent,Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache') if p.exists())
    budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,reserve_bytes=128_000_000,
        poll_seconds=2,planned_bytes=dict(accept=64_000_000,collect=64_000_000)))
    before=budget.before('accept');work.mkdir();export.mkdir()
    observer=None;status='failed';rows=[]
    try:
        env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=os.pathsep.join((str(ROOT/'scripts/ops'),str(ROOT/'scripts/performance'))))
        budget.run([sys.executable,'-B','-m','unittest','test_dataframe_phase_observer','test_dataframe_phase_contract','-v'],
            timeout=60,log=work/'contracts.log',env=env)
        observer=DataFrameObserver(rss_limit=1024**3)
        for index,case in enumerate(('empty','write_fsync','transient_memory','empty_after_raw')):
            budget.check()
            observer.begin(index)
            if case=='write_fsync':
                with (work/'one-page').open('xb') as stream:
                    stream.write(b'x'*4096);stream.flush();os.fsync(stream.fileno())
            if case=='transient_memory':
                with mmap.mmap(-1,8*1024*1024,flags=mmap.MAP_PRIVATE|mmap.MAP_ANONYMOUS) as memory:
                    for offset in range(0,len(memory),mmap.PAGESIZE):memory[offset]=1
            result=observer.finish(index)
            verify_resources(result,index,1024**3)
            if case=='write_fsync':assert result['delta']['write_bytes']>=4096
            if case.startswith('empty'):assert result['delta']['write_bytes']==0
            if case=='transient_memory':
                rss=result['rss'];peak=rss['kernel_reported_peak_rss_bytes']
                assert peak>rss['after_reset']['high_water_bytes']+4*1024*1024
                assert peak>rss['after']['rss_bytes']+4*1024*1024
            rows.append(dict(case=case,status='passed',resources=result))
            # Write after finish; the next empty window must exclude this I/O.
            atomic_json(work/'observations.json',rows)
        status='passed'
    except BaseException as error:
        atomic_json(work/'failure.json',dict(error=repr(error),pending=None if observer is None else observer.pending))
    finally:
        if observer is not None:observer.close()
        (work/'one-page').unlink(missing_ok=True)
        atomic_json(work/'storage.json',dict(before=before,after=budget.check()))
        budget.before('collect')
        archive=work/'evidence.tar.gz'
        with tarfile.open(archive,'w:gz') as output:
            for path in work.iterdir():
                if path.suffix in ('.json','.log'):
                    assert path.stat().st_size<1_000_000
                    output.add(path,arcname=path.name)
        subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER','-in',str(archive),
            '-out',str(export/'private-evidence.cms'),str(ROOT/'.github/ops/native-evidence-cert.pem')],capture_output=True,check=True,timeout=30)
        evidence_hash=hashlib.sha256(archive.read_bytes()).hexdigest()
        shutil.rmtree(work);budget.check()
        sources={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
            'scripts/ops/dataframe_phase_observer.py','scripts/ops/dataframe_phase_contract.py',
            'scripts/ops/hosted_dataframe_observer_acceptance.py','scripts/performance/phase_rss.py')}
        atomic_json(export/'result.json',dict(schema='alopex.dataframe-observer-acceptance/v1',status=status,
            controller=os.environ['GITHUB_SHA'],sources=sources,cases=[dict(case=r['case'],status=r['status']) for r in rows],
            work_removed=not work.exists(),evidence_sha256=evidence_hash,
            encrypted_sha256=hashlib.sha256((export/'private-evidence.cms').read_bytes()).hexdigest(),
            performance_acceptance=False))
    return 0 if status=='passed' else 1


if __name__=='__main__':
    try:code=main()
    except BaseException:code=1
    print(json.dumps(dict(status='passed' if code==0 else 'failed')))
    raise SystemExit(code)
