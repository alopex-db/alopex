#!/usr/bin/env python3
"""Manual #684 native resource matrix using the shared serial process guard."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

from run_begin_acceptance import ROOT, check, guarded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--product-source', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if actual != args.product_source:
        raise RuntimeError('checkout source differs from requested measurement source')
    args.output.mkdir(parents=True, exist_ok=False)
    baseline = check(args.output / 'storage.jsonl')['allocated_bytes']
    if baseline + 2_000_000_000 >= 40_000_000_000:
        raise RuntimeError('insufficient storage headroom')
    initial_used = shutil.disk_usage(ROOT).used
    digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    paths = [ROOT / p for p in ['crates/alopex-embedded/examples/begin_resources.rs',
             'scripts/performance/run_native_begin_resources.py', 'scripts/performance/run_begin_acceptance.py']]
    identity = dict(product_source=actual, binary_sha256=digest,
                    harness_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})
    (args.output / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    records = []
    for indexes in [0, 1, 2]:
        temporary = None
        fixture_records = []
        try:
            with tempfile.TemporaryDirectory(prefix='alopex-684-native-') as temporary:
                path = Path(temporary) / 'db.alopex'
                cases = [('seed', 'borrowed', 'noop', 1, 0)]
                cases += [('probe', api, operation, 6, 0) for api in ['borrowed', 'owned'] for operation in ['noop', 'query']]
                cases += [('probe', 'auto', 'query', 6, 0)]
                cases += [('probe', api, 'noop', 1, repeat) for api in ['borrowed', 'owned'] for repeat in [1, 2]]
                for action, api, operation, samples, repeat in cases:
                    name = f'i{indexes}-{action}-{api}-{operation}-r{repeat}'
                    command = [str(args.binary), '--manual', action, str(path), str(indexes), api, operation, str(samples)]
                    result = guarded(command, args.output, name, baseline, initial_used)
                    records.append(result)
                    fixture_records.append(result)
                    if not result['failure']:
                        try:
                            events = [json.loads(line) for line in (args.output / (name + '.jsonl')).read_text().splitlines()]
                            if not events or events[0]['binary_sha256'] != digest:
                                raise RuntimeError('executed binary identity differs')
                            if events[-1].get('all_rows_unchanged') is not True:
                                raise RuntimeError('missing final complete row validation')
                            if action == 'probe' and len([e for e in events if e['event'] == 'sample']) != samples:
                                raise RuntimeError('missing phase samples')
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
