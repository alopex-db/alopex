"""Create one fixed input and verify exact neighbors independently of ANN engines."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

from hnsw_search_contract import require
from hnsw_search_identity import verify_identity


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args();config=json.loads(args.configuration.read_bytes())
    require(sys.platform=='linux' and os.getuid()!=0 and not sys.flags.optimize
        and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_HNSW_SEARCH')=='1','manual fixture boundary required')
    require(config['mode'] in ('preflight','full','stability') and set(os.sched_getaffinity(0))==set(config['affinity']),
        'fixture mode/affinity differs')
    root=Path(config['root']);directory=Path(config['data'])
    for name,expected in config['sources'].items():require(digest(root/name)==expected,'fixture source differs')
    identity=verify_identity(Path(config['dependency_identity']),config['dependency_identity_sha256'])
    sys.path.insert(0,str(root/'scripts/ops/hnsw_harness'))
    from hnsw_v0811_contract import build_embeddings,load_amazon_products,exact_ground_truth
    import numpy as np
    require(np.__version__=='2.4.6','fixture NumPy differs')
    variance=None;csv_hash=None
    if config['mode']!='preflight':
        csv=directory/'amazon.csv';csv_hash=digest(csv)
        require(csv_hash=='ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291',
            'Amazon input differs')
        products=load_amazon_products(csv);require(len(products)==9171,'cleaned dataset size differs')
        vectors,variance=build_embeddings(products)
        queries=np.ascontiguousarray(vectors[np.random.default_rng(42).choice(len(vectors),size=200,replace=False)])
    else:
        vectors=np.random.default_rng(42).normal(size=(32,128)).astype(np.float32)
        vectors=np.ascontiguousarray(vectors/np.linalg.norm(vectors,axis=1,keepdims=True))
        queries=np.ascontiguousarray(vectors[:4])
    truth,acceptable=exact_ground_truth(vectors,queries,10)
    # Independent float64 reduction checks float32 BLAS-produced exact neighbors.
    # Validate every returned ID and cutoff with the original tolerance retained.
    maximum_error=0.0
    for query,expected,allowed in zip(queries,truth,acceptable):
        scores=np.einsum('ij,j->i',vectors.astype(np.float64),query.astype(np.float64),optimize=False)
        float32=vectors@query
        maximum_error=max(maximum_error,float(np.max(np.abs(scores-float32))))
        order=np.argsort(-scores);cutoff=scores[order[9]]
        require(len(expected)==10 and len(set(expected))==10 and set(expected)<=set(allowed),
            'exact neighbor shape differs')
        require(min(scores[expected])>=cutoff-1e-6 and all(scores[i]>=cutoff-1.1e-6 for i in allowed)
            and all(int(i) in allowed for i in np.flatnonzero(scores>cutoff+1e-6)), 'independent exact oracle differs')
    require(maximum_error<=1e-6,'exact oracle precision differs')
    np.savez(directory/'input.npz',vectors=vectors,queries=queries)
    manifest=dict(vectors_shape=list(vectors.shape),queries_shape=list(queries.shape),dtype=str(vectors.dtype),
        vectors_sha256=hashlib.sha256(vectors.tobytes()).hexdigest(),queries_sha256=hashlib.sha256(queries.tobytes()).hexdigest(),
        csv_sha256=csv_hash,variance=variance,oracle='float32 full dot-product sorted top10, checked by float64 einsum',
        oracle_maximum_score_error=maximum_error,source_seed=42)
    (directory/'truth.json').write_text(json.dumps(dict(truth=truth,acceptable=acceptable),allow_nan=False)+'\n')
    (directory/'input-manifest.json').write_text(json.dumps(manifest,allow_nan=False)+'\n')
    require(verify_identity(Path(config['dependency_identity']),config['dependency_identity_sha256'])==identity,
        'fixture dependencies changed')
    print(json.dumps(dict(status='passed',rows=len(vectors),queries=len(queries))))


if __name__=='__main__':main()
