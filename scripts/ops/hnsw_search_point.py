"""One search point owns raw arrays; return only its compact receipt."""
import gzip
import hashlib
import json
from hnsw_search_contract import require, verify_point, RESOURCE_SCOPE


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def run_point(engine, queries, truth, cell, index, observer, output, *, prepare, measure):
    ef=cell['ef_values'][index]
    if cell['engine']=='alopex-hnsw':engine.native_observations.clear()
    prepared,before=prepare(engine,queries,truth['truth'],truth['acceptable'],ef_search=ef,k=10)
    native=[]
    if cell['engine']=='alopex-hnsw':
        native=list(engine.native_observations)
        require(len(native)==cell['query_vectors'] and all(
            type(r['nodes_visited']) is int and r['nodes_visited']>0
            and type(r['distance_computations']) is int and r['distance_computations']>0
            and type(r['search_time_us']) is int and r['search_time_us']>=0 for r in native),
            'real HNSW traversal diagnostics missing')
    observer.begin(index)
    row=measure(engine,queries,prepared,ef_search=ef,k=10,run=1,
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
    return dict(ef_search=ef,file=filename,sha256=sha(output/filename),query_count=row['query_count'])
