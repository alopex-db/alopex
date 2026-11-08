"""Source-bound worker for the original four Lazy file comparisons."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import sys

from dataframe_phase_worker import reference_identity, save, sha


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077);config=json.loads(args.configuration.read_bytes())
    assert sys.platform=='linux' and not sys.flags.optimize
    assert os.environ.get('ALOPEX_MANUAL_LAZY')=='1' and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
    assert set(os.sched_getaffinity(0))==set(config['affinity'])
    assert (config['rows'],config['warmups'],config['samples']) in ((3,0,1),(34250,3,21))
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS','POLARS_MAX_THREADS'):
        assert os.environ[key]=='1'
    root=Path(config['root'])
    for name,expected in config['sources'].items():assert sha(root/name)==expected
    sys.path[:0]=[str(root/'scripts/ops/dataframe_harness'),str(root/'scripts/performance/workloads')]
    import lazy_observed_measure as workload
    from lazy_phase_contract import CASES
    from dataframe_profile import CurrentOperations
    from dataframe_phase_observer import DataFrameObserver
    from subject_identity import verify_subject,_verify
    assert config['case'] in CASES
    fixture=Path(config['fixture']);output=Path(config['output'])
    assert sha(fixture)==config['input_sha256'] and output.is_dir() and not (output/'raw.ndjson').exists()
    if config['engine']=='alopex':
        identity=verify_subject();assert identity['manifest_sha256']==config['subject_manifest_sha256']
        import alopex
        # The asset receipt and full installed-file hashes replace the archived version-only gate.
        workload.Operations=lambda engine,rows:CurrentOperations(alopex,rows)
    else:
        assert config['engine']=='polars'
        identity=reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])
        import polars
        assert polars.__version__==importlib.metadata.version('polars-runtime-32')=='1.43.2'
        assert polars.thread_pool_size()==1
    observer=DataFrameObserver(rss_limit=config['rss_bytes']);status='failed'
    try:
        result=workload.measure(config['engine'],config['case'],fixture.parent,output/'raw.ndjson',
            config['rows'],config['warmups'],config['samples'],observer=observer)
        assert sha(fixture)==config['input_sha256']
        if config['engine']=='alopex':
            _verify.cache_clear();assert verify_subject()==identity
        else:assert reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])==identity
        result.update(subject_identity=identity,resource_observations=len(observer.completed),
            worker_sha256=sha(Path(__file__)),sources=config['sources'],raw_sha256=sha(output/'raw.ndjson'))
        save(output/'measurement.json',result);status='passed'
    finally:
        save(output/'observations.json',dict(completed=observer.completed,pending=observer.pending))
        observer.close();save(output/'worker-status.json',dict(status=status))
    print(json.dumps(dict(status=status)))


if __name__=='__main__':main()
