#!/usr/bin/env python3
"""Serial source-matched first-write comparison; shares the tested resource guard."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_begin_acceptance import ROOT, check, guarded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python', type=Path, required=True)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, nargs='+', choices=[100, 1000], default=[100, 1000])
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
    directory = Path(__file__).parent
    workers = [directory / name for name in [
        'first_write_profile.py', 'run_first_write_profile.py',
        'begin_acceptance.py', 'run_begin_acceptance.py']]
    record = dict(product=identity, harness_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in workers})
    (args.output / 'identity.json').write_text(json.dumps(record, indent=2) + '\n')
    records = []
    for rows in args.rows:
        for indexes in args.indexes:
            fixture_records = []
            temporary = None
            try:
                with tempfile.TemporaryDirectory(prefix='alopex-684-write-') as temporary:
                    path = Path(temporary) / 'db.alopex'
                    common = ['--manual', '--path', str(path), '--rows', str(rows), '--indexes', str(indexes)]
                    cases = [('seed', [str(args.python), str(directory / 'begin_acceptance.py'), *common, '--action', 'seed'])]
                    cases += [(f'keys{keys}', [str(args.python), str(workers[0]), *common, '--keys', str(keys)]) for keys in [1, 8]]
                    for case, command in cases:
                        name = f'{rows}-i{indexes}-{case}'
                        result = guarded(command, args.output, name, baseline, initial_used)
                        records.append(result)
                        fixture_records.append(result)
                        if not result['failure']:
                            try:
                                events = [json.loads(line) for line in (args.output / (name + '.jsonl')).read_text().splitlines()]
                                if not events or events[0]['extension_sha256'] != identity['extension_sha256']:
                                    raise RuntimeError('loaded extension identity differs')
                                if events[-1].get('all_rows_unchanged') is not True:
                                    raise RuntimeError('missing final row validation')
                                if case != 'seed':
                                    samples = [e for e in events if e['event'] == 'sample']
                                    if len(samples) != 6 or not all(s['updated_values_valid'] for s in samples):
                                        raise RuntimeError('missing samples or transaction-local validation')
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
