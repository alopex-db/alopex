"""Manual public-API phase diagnosis, separate from the qualification worker."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sys

from hnsw_search_phase_capture import require, run_pair


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    require(sys.platform == 'linux' and os.getuid() != 0 and not sys.flags.optimize
        and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_HNSW_SEARCH_PHASE') == '1',
        'manual diagnostic boundary required')
    config = json.loads(args.configuration.read_bytes())
    require(config['mode'] in ('preflight', 'full'), 'unknown diagnostic mode')
    require(config['subject'] in ('before', 'candidate'), 'unknown diagnostic subject')
    require(config['trial'] in (0, 1, 2), 'unknown diagnostic trial')
    require(set(os.sched_getaffinity(0)) == set(config['affinity']), 'affinity differs')
    for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
        require(os.environ.get(key) == '1', 'thread setting differs')
    require(not any(os.environ.get(key) for key in
        ('ALOPEX_PROFILE_HNSW', 'ALOPEX_PERF_RAW_PATH', 'ALOPEX_HNSW_PHASE_PROBE')),
        'unexpected prior instrumentation')
    root, data, output = (Path(config[key]) for key in ('root', 'data', 'output'))
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not any(output.iterdir()), 'diagnostic output already exists')
    for name, digest in config['sources'].items():
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and '..' not in relative.parts,
                'source path escapes')
        require(sha(root / name) == digest, 'diagnostic source differs')
    sys.path.insert(0, str(root / 'scripts/ops/hnsw_harness'))
    sys.path.insert(0, str(root / 'scripts/ops/dataframe_harness'))
    from subject_identity import verify_subject, _verify
    from hnsw_search_identity import verify_identity
    from hnsw_search_phase_build import build_alopex
    import numpy as np
    require(np.__version__ == '2.4.6', 'NumPy version differs')
    dependency = verify_identity(Path(config['dependency_identity']),
                                 config['dependency_identity_sha256'])
    identity = verify_subject()
    require(identity['manifest_sha256'] == config['subject_manifest_sha256'],
            'diagnostic product differs')
    require(sha(data / 'input.npz') == config['input_sha256'], 'input differs')
    with np.load(data / 'input.npz', allow_pickle=False) as source:
        vectors, queries = source['vectors'], source['queries']
    rows, count = (32, 4) if config['mode'] == 'preflight' else (9171, 200)
    require(vectors.shape == (rows, 128) and queries.shape == (count, 128)
        and vectors.dtype == queries.dtype == np.float32
        and vectors.flags.c_contiguous and queries.flags.c_contiguous
        and np.isfinite(vectors).all() and np.isfinite(queries).all(),
        'fixed diagnostic input differs')
    require(np.allclose(np.linalg.norm(vectors, axis=1), 1.0, rtol=1e-5, atol=1e-6)
        and np.allclose(np.linalg.norm(queries, axis=1), 1.0, rtol=1e-5, atol=1e-6),
        'vectors are not normalized')
    engine = None
    status = 'failed'
    completed = 0
    try:
        # Never instrument construction: it invokes search for every insertion.
        os.environ['ALOPEX_HNSW_PHASE_PROBE'] = '0'
        engine = build_alopex(vectors, m=16, ef_construction=200)
        require(engine.node_count == rows, 'index node count differs')
        with (output / 'query-pairs.jsonl').open('x') as stream:
            for ef in (16, 32, 64, 128, 256):
                # Same warmup inputs, outside recorded query intervals.
                for query in queries:
                    engine.diagnostic_search(query, 10, ef)
                for index, query in enumerate(queries):
                    pair = run_pair(engine.diagnostic_search, query, k=10, ef=ef, active_rows=rows,
                        reverse=(index + config['trial']) % 2 == 1)
                    row = dict(query_index=index, query_sha256=hashlib.sha256(query.tobytes()).hexdigest(),
                        ef_search=ef, observations=pair)
                    stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + '\n')
                    completed += 1
                    require(stream.tell() <= 32_000_000, 'diagnostic result exceeds bound')
        engine.close()
        engine = None
        _verify.cache_clear()
        require(verify_subject() == identity, 'product changed during diagnosis')
        require(verify_identity(Path(config['dependency_identity']),
            config['dependency_identity_sha256']) == dependency, 'dependencies changed')
        require(sha(data / 'input.npz') == config['input_sha256'], 'input changed')
        for name, digest in config['sources'].items():
            require(sha(root / name) == digest, 'diagnostic source changed')
        require(completed == count * 5, 'query pair coverage incomplete')
        save(output / 'diagnostic.json', dict(schema='alopex.hnsw-search-diagnostic/v1',
            mode=config['mode'], subject=config['subject'], trial=config['trial'],
            pairs=completed, input_sha256=config['input_sha256'],
            query_pairs_sha256=sha(output / 'query-pairs.jsonl'), subject_identity=identity,
            dependency_identity=dependency, sources=config['sources'],
            worker_sha256=sha(Path(__file__)), index_closed=True,
            performance_qualification=False,
            timing_scope='Public API call including probe writes and capture-thread CPU; '
            'layer timers include upper beam layers but exclude route probing and binding/output work; off mode still '
            'contains instrumentation branches. Durations quantify diagnostic overhead only.'))
        status = 'passed'
    finally:
        os.environ['ALOPEX_HNSW_PHASE_PROBE'] = '0'
        try:
            if engine is not None:
                engine.close()
        finally:
            os.environ.pop('ALOPEX_HNSW_PHASE_PROBE', None)
            save(output / 'worker-status.json', dict(status=status, completed_pairs=completed))
    print(json.dumps(dict(status=status)))


if __name__ == '__main__':
    main()
