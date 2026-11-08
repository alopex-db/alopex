"""One manually dispatched source-bound HNSW index and all five search widths."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys

from hnsw_search_contract import RESOURCE_SCOPE, plan, require, verify_point


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False)+'\n')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration', type=Path, required=True)
    args=parser.parse_args();os.umask(0o077)
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_HNSW_SEARCH')=='1', 'manual HNSW boundary required')
    config=json.loads(args.configuration.read_bytes())
    cell=next(c for c in plan(config['mode']) if c['id']==config['id'])
    require(all(config[k]==v for k,v in cell.items()), 'fixed search cell differs')
    require(set(os.sched_getaffinity(0))==set(config['affinity']), 'search affinity differs')
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS',
                'RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        require(os.environ.get(key)=='1', 'thread setting differs')
    require(not any(os.environ.get(k) for k in ('ALOPEX_PROFILE_HNSW', 'ALOPEX_PERF_RAW_PATH')),
        'unexpected instrumentation inside search timer')
    root=Path(config['root']);output=Path(config['output']);data=Path(config['data'])
    for name,expected in config['sources'].items():
        require(sha(root/name)==expected, 'HNSW source differs')
    sys.path.insert(0,str(root/'scripts/ops/hnsw_harness'))
    sys.path.insert(0,str(root/'scripts/ops/dataframe_harness'))
    from hnsw_v0811_contract import build_hnswlib, build_faiss_hnsw
    from hnsw_search_build import build_alopex, _prepare_setting
    from hnsw_search_measure import _measure_prepared_run
    from dataframe_phase_observer import DataFrameObserver
    from subject_identity import verify_subject, _verify
    from hnsw_search_identity import verify_identity
    dependency_identity=verify_identity(Path(config['dependency_identity']),config['dependency_identity_sha256'])
    import numpy as np
    require(np.__version__=='2.4.6', 'NumPy version differs')
    require(sha(data/'input.npz')==config['input_sha256']
        and sha(data/'truth.json')==config['truth_sha256'], 'HNSW input differs')
    with np.load(data/'input.npz',allow_pickle=False) as source:
        vectors,queries=source['vectors'],source['queries']
    truth=json.loads((data/'truth.json').read_bytes())
    require(vectors.shape==(cell['rows'],128) and queries.shape==(cell['query_vectors'],128)
        and vectors.dtype==queries.dtype==np.float32 and vectors.flags.c_contiguous
        and queries.flags.c_contiguous and np.isfinite(vectors).all() and np.isfinite(queries).all(),
        'HNSW shape/dtype differs')
    require(np.allclose(np.linalg.norm(vectors,axis=1),1.0,rtol=1e-5,atol=1e-6)
        and np.allclose(np.linalg.norm(queries,axis=1),1.0,rtol=1e-5,atol=1e-6), 'vectors are not normalized')
    identity=None;engine=None;observer=None;status='failed';points=[]
    try:
        if cell['engine']=='alopex-hnsw':
            identity=verify_subject()
            require(identity['manifest_sha256']==config['subject_manifest_sha256'], 'subject differs')
        # Native NumPy/reference files are checked before and after this worker.
        engine={'alopex-hnsw':build_alopex,'hnswlib':build_hnswlib,'faiss-hnsw':build_faiss_hnsw}[cell['engine']](vectors)
        require(engine.node_count==cell['rows'], 'index node count differs')
        save(output/'build.json',dict(node_count=engine.node_count,
            build_time_seconds=engine.build_time_seconds,index_size_bytes=engine.index_size_bytes,
            note='Original build/update/delete/reopen boundaries; not cross-product build performance.'))
        observer=DataFrameObserver(rss_limit=config['rss_bytes'])
        for index,ef in enumerate(cell['ef_values']):
            if cell['engine']=='alopex-hnsw':engine.native_observations.clear()
            prepared,before=_prepare_setting(engine,queries,truth['truth'],truth['acceptable'],ef_search=ef,k=10)
            native=[]
            if cell['engine']=='alopex-hnsw':
                native=list(engine.native_observations)
                require(len(native)==cell['query_vectors'] and all(
                    type(r['nodes_visited']) is int and r['nodes_visited']>0
                    and type(r['distance_computations']) is int and r['distance_computations']>0
                    and type(r['search_time_us']) is int and r['search_time_us']>=0 for r in native),
                    'real HNSW traversal diagnostics missing')
            observer.begin(index)
            row=_measure_prepared_run(engine,queries,prepared,ef_search=ef,k=10,run=1,
                min_queries=cell['min_queries'],min_duration_seconds=cell['min_seconds'],dataset_size=cell['rows'])
            resources=observer.finish(index);resources['scope']=RESOURCE_SCOPE
            after=[engine.search(q,10,ef) for q in queries]
            predictions=dict(before=before,after=after)
            verify_point(cell,row,predictions,truth['truth'],truth['acceptable'])
            record=dict(trial=cell['trial'],measurement=row,predictions=predictions,
                native_diagnostics=native,resources=resources,
                residual_note='Legacy difference of separate measurements; not Python-only latency.')
            filename=f'ef-{ef}.json.gz'
            with gzip.open(output/filename,'wt',encoding='utf-8') as stream:
                json.dump(record,stream,allow_nan=False,separators=(',',':'))
            points.append(dict(ef_search=ef,file=filename,sha256=sha(output/filename),query_count=row['query_count']))
            save(output/'points.json',points)
        engine.close();engine=None
        if cell['engine']=='alopex-hnsw':
            _verify.cache_clear();require(verify_subject()==identity, 'product changed during measurement')
        require(sha(data/'input.npz')==config['input_sha256']
            and sha(data/'truth.json')==config['truth_sha256'], 'input changed during measurement')
        require(verify_identity(Path(config['dependency_identity']),config['dependency_identity_sha256'])==dependency_identity,
            'dependencies changed during measurement')
        save(output/'measurement.json',dict(cell=cell,points=points,input_sha256=config['input_sha256'],
            truth_sha256=config['truth_sha256'],subject_identity=identity,dependency_identity=dependency_identity,
            sources=config['sources'],worker_sha256=sha(Path(__file__)),index_closed=True))
        status='passed'
    finally:
        try:
            if engine is not None:engine.close()
        finally:
            if observer is not None:
                save(output/'observations.json',dict(completed=observer.completed,pending=observer.pending))
                observer.close()
            save(output/'worker-status.json',dict(status=status))
    print(json.dumps(dict(status=status)))


if __name__=='__main__':main()
