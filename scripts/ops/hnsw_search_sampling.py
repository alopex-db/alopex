"""Continuous-loop sampling controls; no per-query instrumentation."""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import sys

MODES=('baseline','paused','sampled')


def require(condition,message):
    if not condition:raise ValueError(message)


def measure_wrapper(original, mode, pause, resume, records):
    require(mode in MODES,'sampling mode')
    def measure(*args,**kwargs):
        enabled=mode=='sampled' and kwargs['ef_search'] in (128,256)
        if enabled:resume()
        completed=False
        try:
            value=original(*args,**kwargs)
            completed=True
            return value
        finally:
            if enabled:pause()
            records.append(dict(ef_search=kwargs['ef_search'],sampling=enabled,completed=completed))
    return measure


def main():
    require(len(sys.argv)==3 and sys.argv[1]=='--configuration','sampling invocation')
    config=json.loads(Path(sys.argv[2]).read_bytes());mode=config['sampling_mode'];require(mode in MODES,'sampling mode')
    output=Path(config['output']);require(output.is_dir(),'sampling output missing')
    loaded={Path(line.split()[-1]).resolve() for line in Path('/proc/self/maps').read_text().splitlines()
            if 'libgp-collector.so' in line}
    pause=resume=lambda:None
    identity=None
    if mode=='baseline':
        require(not loaded,'baseline collector loaded')
    else:
        library=Path(config['collector']['path']).resolve(strict=True)
        require(loaded=={library},'unexpected collector loaded')
        identity=hashlib.sha256(library.read_bytes()).hexdigest()
        require(identity==config['collector']['sha256'],'collector identity')
        api=ctypes.CDLL(str(library))
        pause,resume=api.collector_pause,api.collector_resume
        for call in (pause,resume):call.argtypes=[];call.restype=None
        pause()
    import hnsw_search_measure
    import hnsw_search_worker
    original=hnsw_search_measure._measure_prepared_run
    records=[]
    hnsw_search_measure._measure_prepared_run=measure_wrapper(original,mode,pause,resume,records)
    status='failed'
    try:
        hnsw_search_worker.main()
        require([r['ef_search'] for r in records]==[16,32,64,128,256]
            and all(r['completed'] for r in records),'continuous-loop coverage')
        status='passed'
    finally:
        hnsw_search_measure._measure_prepared_run=original
        if mode!='baseline':pause()
        (output/'sampling.json').write_text(json.dumps(dict(mode=mode,status=status,collector_sha256=identity,
            calls=records,product_worker_modified=False,query_loop_modified=False,performance_acceptance=False))+'\n')

if __name__=='__main__':main()
