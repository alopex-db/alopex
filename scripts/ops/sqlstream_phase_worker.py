"""One source-bound SQL stream cell; original and matched Python-row lanes."""
import argparse,json,os,sys
from pathlib import Path
from dataframe_phase_worker import save,sha


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077);config=json.loads(args.configuration.read_bytes())
    assert sys.platform=='linux' and not sys.flags.optimize and os.getuid()!=0
    assert os.environ.get('ALOPEX_MANUAL_SQLSTREAM')=='1' and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
    assert not os.environ.get('ALOPEX_PERF_RAW_PATH')
    assert set(os.sched_getaffinity(0))==set(config['affinity'])
    assert (config['rows'],config['warmups'],config['samples']) in ((3,0,1),(34250,3,21))
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        assert os.environ[key]=='1'
    root=Path(config['root']);output=Path(config['output'])
    for name,expected in config['sources'].items():assert sha(root/name)==expected
    sys.path.insert(0,str(root/'scripts/ops/dataframe_harness'))
    import sql_stream_profile as workload
    from sqlstream_observed_measure import measure,measure_plan
    from sqlstream_reference_identity import reference_identity
    from sqlstream_phase_contract import CASES,validate_fixture
    from dataframe_phase_observer import DataFrameObserver
    from subject_identity import verify_subject,_verify
    assert config['case'] in CASES and output.is_dir() and not (output/'raw.ndjson').exists()
    n=config['rows'];rows_lane=config['case']=='rows'
    expected=[{'id':i,'value':float(i)} for i in range(0,n,2)]
    connection=None;context=None;observer=None;status='failed';versions=None
    try:
        if config['engine']=='alopex':
            identity=verify_subject();assert identity['manifest_sha256']==config['subject_manifest_sha256']
            import alopex
            connection=alopex.Database.new()
            workload.original_helpers._populate_sql(connection,'alopex',n)
            open_stream=lambda:connection.execute_sql_stream(workload.QUERY,resource_limit_bytes=1024**3)
            build_plan=lambda:connection.execute_sql(f'EXPLAIN (FORMAT JSON) {workload.QUERY}')
            consume=(lambda:workload.consume_rows_alopex(open_stream)) if rows_lane else (lambda:(*workload.consume_original_alopex(open_stream),None))
            read_all=lambda:workload.consume_rows_alopex(open_stream)[4]
            read_fixture=lambda:connection.execute_sql('SELECT id, value, enabled FROM bench ORDER BY id')
        else:
            assert config['engine']=='datafusion'
            identity=reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])
            import datafusion
            import pyarrow as pa
            versions={'datafusion':datafusion.__version__,'pyarrow':pa.__version__}
            assert versions=={'datafusion':'50.0.0','pyarrow':'26.0.0'}
            context=datafusion.SessionContext(datafusion.SessionConfig().with_target_partitions(1))
            batches=[pa.record_batch({'id':range(start,min(start+8192,n)),
                'value':[float(i) for i in range(start,min(start+8192,n))],
                'enabled':[i%2==0 for i in range(start,min(start+8192,n))]}) for start in range(0,n,8192)]
            context.register_record_batches('bench',[batches])
            build=lambda:context.sql(workload.QUERY)
            build_plan=lambda:build().logical_plan()
            consume=(lambda:workload.consume_rows_datafusion(build)) if rows_lane else (lambda:(*workload.consume_original_datafusion(build),None))
            read_all=lambda:workload.consume_rows_datafusion(build)[4]
            read_fixture=lambda:[row for batch in context.sql('SELECT id, value, enabled FROM bench ORDER BY id').collect() for row in batch.to_pylist()]
        initial=validate_fixture(read_fixture(),n)
        workload.canonical_rows(read_all(),expected)
        observer=DataFrameObserver(rss_limit=config['rss_bytes'])
        plan_raw=measure_plan(build_plan,observer,warmups=config['warmups'],samples=config['samples'])
        (output/'plan.ndjson').write_text(''.join(json.dumps(r,allow_nan=False)+'\n' for r in plan_raw))
        save(output/'plan-observations.json',dict(completed=observer.completed,pending=observer.pending))
        observer.close();observer=None
        observer=DataFrameObserver(rss_limit=config['rss_bytes'])
        with (output/'raw.ndjson').open('x') as raw:
            def record(row):
                if row['phase']=='measure':
                    total,first=row['total_ns']/1e9,row['first_ns']/1e9
                    raw.write(json.dumps(dict(phase='measure',index=row['iteration']-config['warmups'],
                        seconds=total,first_seconds=first,remaining_seconds=total-first,
                        rows=row['rows'],first_chunk_rows=row['first_chunk_rows'],rows_verified=rows_lane,
                        resources=row['resources'],original_usage={k:row[k] for k in ('outside_original_total_ns',
                        'cpu_including_close_seconds','input_blocks_including_close','output_blocks_including_close')}),allow_nan=False)+'\n');raw.flush()
            measure(consume,expected,rows_lane,observer,warmups=config['warmups'],samples=config['samples'],record=record)
        workload.canonical_rows(read_all(),expected)
        assert validate_fixture(read_fixture(),n)==initial
        if connection is not None:connection.close();connection=None
        context=None
        if config['engine']=='alopex':
            _verify.cache_clear();assert verify_subject()==identity
        else:assert reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])==identity
        result=dict(case=config['case'],engine=config['engine'],fixture_rows=n,warmups=config['warmups'],samples=config['samples'],
            input_sha256=initial,result_sha256=workload.digest(expected),connection_closed=True,fixture_checks=2,
            full_result_checks=2+(config['warmups']+config['samples'] if rows_lane else 0),returned_rows=len(expected),
            rows_lane=rows_lane,versions=versions,target_partitions=1 if config['engine']=='datafusion' else None,
            subject_identity=identity,resource_observations=len(observer.completed),worker_sha256=sha(Path(__file__)),
            sources=config['sources'],raw_sha256=sha(output/'raw.ndjson'),plan_raw_sha256=sha(output/'plan.ndjson'))
        save(output/'measurement.json',result);status='passed'
    finally:
        if connection is not None:connection.close()
        if observer is not None:
            save(output/'observations.json',dict(completed=observer.completed,pending=observer.pending));observer.close()
        save(output/'worker-status.json',dict(status=status))
    print(json.dumps(dict(status=status)))

if __name__=='__main__':main()
