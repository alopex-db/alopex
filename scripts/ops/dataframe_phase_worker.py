"""One fresh-process cell of a bounded manual DataFrame comparison."""
import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import sys
import zipfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False)+'\n')


def reference_identity(path, expected):
    assert sha(path) == expected
    record = json.loads(path.read_bytes())
    assert record['schema'] == 'alopex.reference-wheels/v1' and record['engine'] == 'polars'
    assert {entry['name'] for entry in record['wheels']} == {'polars','polars-runtime-32'}
    assert len(record['wheels']) == 2
    prefix = Path(sys.prefix).resolve()
    packages = {}
    for entry in record['wheels']:
        assert entry['version'] == '1.43.2'
        wheel_path = Path(entry['path'])
        assert sha(wheel_path) == entry['sha256']
        distribution = importlib.metadata.distribution(entry['name'])
        assert distribution.version == entry['version']
        with zipfile.ZipFile(wheel_path) as archive:
            infos = archive.infolist()
            assert len(infos) == len({i.filename for i in infos})
            assert sum(i.file_size for i in infos) <= 256_000_000
            for info in infos:
                name = PurePosixPath(info.filename)
                assert not name.is_absolute() and '..' not in name.parts
                if info.is_dir() or name.parts[0].endswith('.dist-info'):
                    continue
                assert not name.parts[0].endswith('.data')
                installed = Path(distribution.locate_file(info.filename)).resolve()
                assert installed.is_relative_to(prefix) and installed.is_file()
                assert sha(installed) == hashlib.sha256(archive.read(info)).hexdigest()
                package_root = Path(distribution.locate_file(name.parts[0])).resolve()
                packages.setdefault(package_root,set()).add(installed.relative_to(package_root).as_posix())
    for root, names in packages.items():
        actual = {p.relative_to(root).as_posix() for p in root.rglob('*')
            if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc'}
        assert actual == names
    module = importlib.util.find_spec('polars')
    assert module is not None and module.origin
    assert Path(module.origin).resolve() == Path(importlib.metadata.distribution('polars').locate_file('polars/__init__.py')).resolve()
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    config = json.loads(args.configuration.read_bytes())
    assert sys.platform == 'linux' and not sys.flags.optimize
    assert set(os.sched_getaffinity(0)) == set(config['affinity'])
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','POLARS_MAX_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        assert os.environ[key] == '1'
    assert (config['rows'],config['warmups'],config['samples']) in ((4,0,1),(34250,3,21))
    output = Path(config['output'])
    assert output.is_dir() and not (output/'raw.ndjson').exists()
    harness = Path(config['harness'])
    for name, expected in config['harness_files'].items():
        assert sha(harness/name) == expected
    sys.path.insert(0,str(harness))
    import dataframe_phases_v0816 as workload
    from subject_identity import verify_subject, _verify
    from dataframe_phase_observer import DataFrameObserver
    import dataframe_phase_observer
    import phase_rss
    assert sha(Path(dataframe_phase_observer.__file__)) == config['observer_sha256']
    assert sha(Path(phase_rss.__file__)) == config['phase_rss_sha256']
    assert config['case'] in workload.CASES and config['engine'] in ('alopex','polars')
    if config['engine'] == 'alopex':
        identity = verify_subject()
        assert identity['manifest_sha256'] == config['subject_manifest_sha256']
    else:
        assert importlib.metadata.version('polars') == '1.43.2'
        identity = reference_identity(Path(config['reference_identity']), config['reference_identity_sha256'])
    observer = DataFrameObserver(rss_limit=config['rss_bytes'])
    result, status = {}, 'failed'
    try:
        result = workload.run(config['engine'],config['case'],output/'raw.ndjson',
            rows=config['rows'],warmups=config['warmups'],runs=config['samples'],observer=observer)
        assert len(observer.completed) == config['warmups'] + config['samples']
        if config['engine'] == 'alopex':
            _verify.cache_clear()
            assert verify_subject() == identity
        else:
            assert reference_identity(Path(config['reference_identity']), config['reference_identity_sha256']) == identity
        result.update(subject_identity=identity, resource_observations=len(observer.completed),
            worker_sha256=sha(Path(__file__)),harness_files=config['harness_files'],
            observer_sha256=config['observer_sha256'],phase_rss_sha256=config['phase_rss_sha256'],
            raw_sha256=sha(output/'raw.ndjson'))
        save(output/'measurement.json',result)
        status = 'passed'
    finally:
        save(output/'observations.json',dict(completed=observer.completed,pending=observer.pending))
        observer.close()
        save(output/'worker-status.json',dict(status=status))
    print(json.dumps(dict(status=status)))


if __name__ == '__main__':
    main()
