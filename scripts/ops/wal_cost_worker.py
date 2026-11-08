"""Run one original tiny public API cost case under a manual-only controller."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    assert not sys.flags.optimize and sys.platform == 'linux'
    assert os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    assert os.environ.get('RUNNER_KIND') == 'github-hosted' and os.getuid() != 0
    config = json.loads(args.configuration.read_bytes())
    assert sorted(os.sched_getaffinity(0)) == config['affinity']
    root = Path(__file__).resolve().parents[2]
    for name, expected in config['sources'].items():
        assert sha(root/name) == expected
    sys.path[:0] = [str(root/'scripts/compatibility'), str(root/'scripts/ops/dataframe_harness')]
    import public_api_cost as workload
    from subject_identity import verify_subject
    from alopex import _alopex
    identity = verify_subject()
    assert identity['manifest_sha256'] == config['subject_manifest_sha256']
    assert workload.OPTIONS == config['options']
    assert (config['surface'], config['value_bytes']) in workload.SHAPES
    assert config['phase'] in ('write', 'verify')
    output = Path(config['output'])
    (output/'identity.json').write_text(json.dumps(identity, allow_nan=False)+'\n')
    # Calling the workload function does not disable its CLI's ordinary-CI guard.
    # The controller and this worker independently require workflow_dispatch.
    # The writer deliberately os._exit()s to preserve live WAL before two readers.
    workload.child(config['phase'], Path(config['fixture']), config['surface'],
                   config['value_bytes'], sha(Path(_alopex.__file__)))


if __name__ == '__main__':
    main()
