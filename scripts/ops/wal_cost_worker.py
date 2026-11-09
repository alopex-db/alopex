"""Run one original tiny public API cost case under a manual-only controller."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import sys


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    assert not sys.flags.optimize and sys.platform == 'linux'
    assert os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    assert os.environ.get('RUNNER_KIND') == 'github-hosted' and os.getuid() != 0
    config = json.loads(args.configuration.read_bytes())
    assert config['diagnostic'] == 'wal-mapping/v1'
    assert sorted(os.sched_getaffinity(0)) == config['affinity']
    root = Path(__file__).resolve().parents[2]
    for name, expected in config['sources'].items():
        assert sha(root/name) == expected
    sys.path[:0] = [str(root/'scripts/compatibility'), str(root/'scripts/ops/dataframe_harness')]
    import public_api_cost as workload
    from subject_identity import verify_subject
    from wal_phase_memory import Observer
    from wal_mapping_diagnostic import BoundaryObserver
    mapping = BoundaryObserver() if config['phase'] == 'write' else None
    def mapping_capture(phase):
        assert mapping is not None
        mapping.capture(phase)
        if phase == 'after_begin':
            mapping.save(Path(config['output']) / 'mapping-boundaries.json')
    workload._wal_mapping_capture = mapping_capture
    observer = Observer() if config['phase'] == 'write' else None
    def capture(phase):
        assert observer is not None
        observer.capture(phase)
        if phase == 'after_pending_commit':
            observer.save(Path(config['output']) / 'phase-memory.json')
    workload._wal_phase_capture = capture
    from alopex import _alopex
    if observer is not None: capture('after_wrapper_import')
    from phase_rss import parse_status
    boundary_before_identity=parse_status(Path('/proc/self/status').read_text())
    identity = verify_subject()
    assert identity['manifest_sha256'] == config['subject_manifest_sha256']
    if observer is not None: capture('after_identity')
    assert workload.OPTIONS == config['options']
    assert (config['surface'], config['value_bytes']) in workload.SHAPES
    assert config['phase'] in ('write', 'verify')
    output = Path(config['output'])
    (output/'identity.json').write_text(json.dumps(identity, allow_nan=False)+'\n')
    native_digest=sha(Path(_alopex.__file__))
    if observer is not None: capture('after_wrapper_native_digest')
    (output/'memory-before-workload.json').write_text(json.dumps(dict(
        before_identity=boundary_before_identity,
        after_identity_and_native_digest=parse_status(Path('/proc/self/status').read_text()),
        lifetime_high_water_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        scope='after import, original-wheel identity verification and streaming native digest; before workload'))+'\n')
    from wal_cost_commit_observer import observe
    original_measure=workload.measure
    def measure(action):
        if getattr(action,'__name__',None)!='commit':return original_measure(action)
        result,observation=observe(action,original_measure)
        (output/'commit-observation.json').write_text(json.dumps(observation,allow_nan=False)+'\n')
        return result
    workload.measure=measure
    # Calling the workload function does not disable its CLI's ordinary-CI guard.
    # The controller and this worker independently require workflow_dispatch.
    # The writer deliberately os._exit()s to preserve live WAL before two readers.
    workload.child(config['phase'], Path(config['fixture']), config['surface'],
                   config['value_bytes'], native_digest)


if __name__ == '__main__':
    main()
