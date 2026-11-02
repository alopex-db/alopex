#!/usr/bin/env python3
"""Serial macOS #684 source-bound wheel acceptance; no build or CI load lane."""
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


def guarded(command, output, name, baseline, initial_used):
    started = time.monotonic()
    sampled_rss = 0
    failure = None
    env = dict(os.environ, RAYON_NUM_THREADS='2', OMP_NUM_THREADS='2',
               OPENBLAS_NUM_THREADS='1', PYTHONDONTWRITEBYTECODE='1')
    with (output / (name + '.jsonl')).open('w') as out, (output / (name + '.stderr')).open('w') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env=env)
        try:
            while child.poll() is None:
                usage = shutil.disk_usage(ROOT)
                growth = max(0, usage.used - initial_used)
                if baseline + growth >= 40_000_000_000 or growth > 2_000_000_000 or usage.free < 16 * 1024**3:
                    raise RuntimeError('host storage limit reached')
                if time.monotonic() - started > 90:
                    raise RuntimeError('90-second process limit reached')
                rss = subprocess.run(['ps', '-o', 'rss=', '-p', str(child.pid)],
                                     capture_output=True, text=True)
                current_rss = int(rss.stdout.strip()) * 1024 if rss.stdout.strip() else 0
                if rss.returncode or current_rss <= 0:
                    # ps can observe the exit transition before poll reaps it.
                    # Wait briefly to distinguish exit from an unmeasurable live process.
                    try:
                        child.wait(timeout=.05)
                    except subprocess.TimeoutExpired:
                        raise RuntimeError('live RSS measurement failed')
                    break
                if current_rss:
                    sampled_rss = max(sampled_rss, current_rss)
                if sampled_rss >= 2 * 1024**3:
                    raise RuntimeError('2 GiB RSS limit reached')
                time.sleep(.1)
            if child.returncode:
                raise RuntimeError(f'child exit {child.returncode}')
        except BaseException as error:
            failure = str(error)
            if child.poll() is None:
                child.kill()
            child.wait()
    return dict(case=name, exit_code=child.returncode, failure=failure,
                elapsed_seconds=time.monotonic() - started, sampled_peak_rss_bytes=sampled_rss)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python', type=Path, required=True)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, nargs='+', choices=[0, 100, 1000], default=[0, 100, 1000])
    parser.add_argument('--indexes', type=int, nargs='+', choices=[0, 1, 2], default=[0, 1, 2])
    args = parser.parse_args()
    if sys.platform != 'darwin':
        parser.error('native macOS resource monitor required')
    args.output.mkdir(parents=True, exist_ok=False)
    baseline = check(args.output / 'storage.jsonl')['allocated_bytes']
    if baseline + 2_000_000_000 >= 40_000_000_000:
        raise RuntimeError('insufficient host storage headroom')
    initial_used = shutil.disk_usage(ROOT).used
    identity = json.loads(args.identity.read_text())
    worker = Path(__file__).with_name('begin_acceptance.py')
    record = dict(product=identity, harness_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in [worker, Path(__file__)]})
    (args.output / 'identity.json').write_text(json.dumps(record, indent=2) + '\n')
    records = []
    for rows in args.rows:
        for indexes in args.indexes:
            fixture_records = []
            temporary = None
            try:
                with tempfile.TemporaryDirectory(prefix='alopex-684-grid-') as temporary:
                    path = Path(temporary) / 'db.alopex'
                    common = [str(args.python), str(worker), '--manual', '--path', str(path),
                              '--rows', str(rows), '--indexes', str(indexes)]
                    cases = [('seed', 'rw', 'noop', 1, 0)]
                    cases += [('probe', mode, operation, 6, 0) for mode in ['ro', 'rw'] for operation in ['noop', 'query']]
                    cases += [('probe', 'auto', 'query', 6, 0)]
                    if rows == 1000:
                        cases += [('probe', 'rw', 'noop', 1, repeat) for repeat in [1, 2]]
                    for action, mode, operation, samples, repeat in cases:
                        name = f'{rows}-i{indexes}-{action}-{mode}-{operation}-r{repeat}'
                        command = common + ['--action', action, '--mode', mode, '--operation', operation, '--samples', str(samples)]
                        result = guarded(command, args.output, name, baseline, initial_used)
                        fixture_records.append(result)
                        records.append(result)
                        if not result['failure']:
                            try:
                                events = [json.loads(line) for line in (args.output / (name + '.jsonl')).read_text().splitlines()]
                                if not events or events[0]['extension_sha256'] != identity['extension_sha256']:
                                    raise RuntimeError('loaded extension identity differs')
                                if events[-1].get('all_rows_unchanged') is not True:
                                    raise RuntimeError('missing final row validation')
                                if action == 'probe' and len([e for e in events if e['event'] == 'sample']) != samples:
                                    raise RuntimeError('missing measurement samples')
                            except Exception as error:
                                result['failure'] = str(error)
                        print(json.dumps(result), flush=True)
                        if result['failure']:
                            raise RuntimeError(result['failure'])
            finally:
                for result in fixture_records:
                    result['fixture_removed'] = temporary is not None and not Path(temporary).exists()
                (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                check(args.output / 'storage.jsonl')


if __name__ == '__main__':
    main()
