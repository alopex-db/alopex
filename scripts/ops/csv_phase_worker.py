"""One fresh-process plain-CSV cell; only a manual controller invokes this."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import sys

from dataframe_phase_worker import reference_identity, save, sha
from csv_phase_measure import measure_plan_or_collect, measure_stream


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    config = json.loads(args.configuration.read_bytes())
    assert sys.platform == 'linux' and not sys.flags.optimize
    assert os.environ.get('ALOPEX_MANUAL_CSV') == '1'
    assert os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    assert set(os.sched_getaffinity(0)) == set(config['affinity'])
    assert (config['rows'], config['warmups'], config['samples']) in ((3, 0, 1), (3, 3, 21), (34250, 3, 21))
    for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                'POLARS_MAX_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
        assert os.environ[key] == '1'
    root = Path(config['root'])
    for name, expected in config['sources'].items():
        assert sha(root / name) == expected
    sys.path.insert(0, str(root / 'scripts/ops/dataframe_harness'))
    from subject_identity import verify_subject, _verify
    from dataframe_phase_observer import DataFrameObserver
    import csv_stream_profile as workload
    fixture, output = Path(config['fixture']), Path(config['output'])
    assert fixture.is_file() and sha(fixture) == config['input_sha256']
    assert output.is_dir() and not (output / 'raw.ndjson').exists()
    expected = workload.oracle(config['rows'])
    if config['engine'] == 'alopex':
        identity = verify_subject()
        assert identity['manifest_sha256'] == config['subject_manifest_sha256']
        import alopex
        build = lambda: alopex.LazyFrame.scan_csv(str(fixture))
        consume = lambda: (*workload.consume_alopex(build), None)
    else:
        assert config['engine'] == 'polars'
        identity = reference_identity(Path(config['reference_identity']), config['reference_identity_sha256'])
        import polars
        assert polars.__version__ == importlib.metadata.version('polars-runtime-32') == '1.43.2'
        assert polars.thread_pool_size() == 1
        build = lambda: polars.scan_csv(fixture)
        consume = lambda: (*workload.consume_polars(build), None)

    def validate_all():
        collected = workload.validate_values(build().collect().to_dict(as_series=False), expected)
        streamed = workload.validate_values(workload.read_stream(build), expected)
        assert collected == streamed == workload.digest(expected)
        return collected

    observer = DataFrameObserver(rss_limit=config['rss_bytes'])
    status = 'failed'
    try:
        before = validate_all()
        with (output / 'raw.ndjson').open('x') as raw:
            def record(row):
                row['resources']['scope'] = ('CSV operation plus observation-boundary overhead; stream includes close '
                    'after the original total clock; excludes validation and raw output')
                raw.write(json.dumps(row, allow_nan=False) + '\n')
                raw.flush()
            options = dict(warmups=config['warmups'], samples=config['samples'], observer=observer, record=record)
            measure_plan_or_collect(build, 'plan_sample', **options)
            measure_plan_or_collect(lambda: build().collect().height, 'collect_sample', **options)
            measure_stream(consume, expected['id'], **options)
        after = validate_all()
        assert sha(fixture) == config['input_sha256']
        if config['engine'] == 'alopex':
            _verify.cache_clear()
            assert verify_subject() == identity
        else:
            assert reference_identity(Path(config['reference_identity']), config['reference_identity_sha256']) == identity
        save(output / 'measurement.json', dict(rows=config['rows'], engine=config['engine'], warmups=config['warmups'],
            samples=config['samples'], input_sha256=config['input_sha256'], result_sha256=workload.digest(expected),
            validation_before=before, validation_after=after, subject_identity=identity,
            resource_observations=len(observer.completed), worker_sha256=sha(Path(__file__)),
            sources=config['sources'], raw_sha256=sha(output / 'raw.ndjson')))
        status = 'passed'
    finally:
        save(output / 'observations.json', dict(completed=observer.completed, pending=observer.pending))
        observer.close()
        save(output / 'worker-status.json', dict(status=status))
    print(json.dumps(dict(status=status)))


if __name__ == '__main__': main()
