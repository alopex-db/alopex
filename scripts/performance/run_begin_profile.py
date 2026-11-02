#!/usr/bin/env python3
"""Serial macOS #684 diagnostic runner. Explicit binaries, bounded disk/RSS/time."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/compatibility'))
from storage import check


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--native', type=Path)
    p.add_argument('--api', choices=['borrowed', 'owned', 'Python'], nargs='+', default=['borrowed', 'owned', 'Python'])
    p.add_argument('--python', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--rows', type=int, nargs='+', choices=[0, 100, 1000, 10000], default=[0, 100, 1000])
    p.add_argument('--profile', action='store_true', help='separate stack sampling; not timing evidence')
    a = p.parse_args()
    if sys.platform != 'darwin':
        p.error('this bounded runner uses native macOS ps/sample; choose a platform-specific monitor elsewhere')
    if any(api != 'Python' for api in a.api) and a.native is None:
        p.error('--native is required for Rust APIs')
    a.output.mkdir(parents=True, exist_ok=False)
    baseline = check(a.output / 'storage.jsonl')['allocated_bytes']
    if baseline + 2_000_000_000 >= 40_000_000_000:
        raise RuntimeError('insufficient host storage headroom')
    used = shutil.disk_usage(ROOT).used
    records = []
    binaries = {'native':hashlib.sha256(a.native.read_bytes()).hexdigest() if a.native else None,
                'python':str(a.python.resolve()), 'product_source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
    (a.output / 'identity.json').write_text(json.dumps(binaries,indent=2)+'\n')
    cases = [(api, backend, rows, mode, op) for rows in a.rows for api in a.api
             for backend in ('memory','disk') for mode in ('ro','rw') for op in ('noop','query')]
    if a.profile:
        cases = [(api, 'disk', max(a.rows), 'rw', 'profile') for api in a.api]
    for api, backend, rows, mode, op in cases:
        name = f'{api}-{backend}-{rows}-{mode}-{op}'
        command = ([str(a.python), str(ROOT/'scripts/performance/begin_profile.py'), '--manual', '--backend',backend,
                    '--rows',str(rows),'--mode',mode,'--operation',op] if api == 'Python' else
                   [str(a.native),'--manual',backend,str(rows),mode,api,op])
        with tempfile.TemporaryDirectory(prefix='alopex-684-case-') as temporary, (a.output/(name+'.jsonl')).open('w') as out, (a.output/(name+'.stderr')).open('w') as err:
            env = dict(os.environ, TMPDIR=temporary, RAYON_NUM_THREADS='2', OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='1', PYTHONDONTWRITEBYTECODE='1')
            start = time.monotonic(); peak_rss = 0; sampler = None; failure = None
            child = subprocess.Popen(command, stdout=out, stderr=err, env=env)
            try:
                while child.poll() is None:
                    usage = shutil.disk_usage(ROOT)
                    if baseline + max(0, usage.used-used) >= 40_000_000_000 or usage.used-used > 2_000_000_000 or usage.free < 16*1024**3:
                        raise RuntimeError('host storage stop')
                    if time.monotonic()-start > 90:
                        raise RuntimeError('90-second timeout')
                    rss = subprocess.run(['ps','-o','rss=','-p',str(child.pid)], capture_output=True, text=True)
                    if rss.returncode and child.poll() is None:
                        raise RuntimeError('cannot measure live child RSS: '+rss.stderr.strip())
                    if rss.stdout.strip():
                        peak_rss = max(peak_rss, int(rss.stdout.strip())*1024)
                    if peak_rss >= 2*1024**3:
                        raise RuntimeError('2 GiB RSS stop')
                    if op == 'profile' and sampler is None and (a.output/(name+'.jsonl')).stat().st_size:
                        sampler = subprocess.Popen(['sample',str(child.pid),'3','1','-file',str(a.output/(name+'.sample.txt'))],stdout=err,stderr=err)
                    time.sleep(.1)
                if child.returncode:
                    raise RuntimeError(f'child exit {child.returncode}')
                out.flush()
                events = [json.loads(line) for line in (a.output/(name+'.jsonl')).read_text().splitlines()]
                samples = [event for event in events if event['event'] == 'sample']
                if not events or events[-1].get('all_rows_unchanged') is not True or (op != 'profile' and len(samples) != 6):
                    raise RuntimeError('missing completed result or samples')
                if any((event['query_ns'] is not None) != (op == 'query') for event in samples):
                    raise RuntimeError('query and noop timing intervals mixed')
            except BaseException as error:
                failure = str(error)
                child.kill(); child.wait()
            finally:
                if sampler:
                    try: sampler.wait(timeout=10)
                    except subprocess.TimeoutExpired: sampler.kill(); sampler.wait()
            if sampler and sampler.returncode != 0 and failure is None:
                failure = 'stack sampler failed: ' + str(sampler.returncode)
            record = dict(case=name, exit_code=child.returncode, failure=failure, peak_sampled_rss_bytes=peak_rss,
                          elapsed_seconds=time.monotonic()-start, sampler_exit=sampler.returncode if sampler else None)
            records.append(record)
        record['fixture_removed'] = not Path(temporary).exists()
        (a.output/'results.json').write_text(json.dumps(records,indent=2)+'\n')
        print(json.dumps(record),flush=True)
        if failure:
            raise RuntimeError(failure)
    check(a.output / 'storage.jsonl')


if __name__ == '__main__':
    main()
