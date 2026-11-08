"""One verified, fresh-process SQLite composite cell."""
import argparse,json,os,sys
from pathlib import Path
from dataframe_phase_worker import save,sha

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077);config=json.loads(args.configuration.read_bytes())
    assert sys.platform=='linux' and not sys.flags.optimize and os.getuid()!=0
    assert os.environ.get('ALOPEX_MANUAL_SQLITE')=='1' and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
    assert not os.environ.get('ALOPEX_PERF_RAW_PATH')
    assert set(os.sched_getaffinity(0))==set(config['affinity'])
    assert (config['rows'],config['warmups'],config['samples']) in ((3,0,1),(34250,3,21))
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS','TOKIO_WORKER_THREADS'):
        assert os.environ[key]=='1'
    root=Path(config['root']);output=Path(config['output'])
    for name,expected in config['sources'].items():assert sha(root/name)==expected
    sys.path.insert(0,str(root/'scripts/ops/dataframe_harness'))
    import sqlite_composite_profile as workload
    from sqlite_observed_measure import measure
    from sqlite_reference_identity import reference_identity
    from sqlite_phase_contract import CASES
    from dataframe_phase_observer import DataFrameObserver
    from subject_identity import verify_subject,_verify
    assert config['case'] in CASES and output.is_dir() and not (output/'raw.ndjson').exists()
    if config['engine']=='alopex':
        identity=verify_subject();assert identity['manifest_sha256']==config['subject_manifest_sha256']
        import alopex
        connection=alopex.Database.new();version=None;foreign_keys=None
    else:
        assert config['engine']=='sqlite'
        identity=reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])
        import pysqlite3 as sqlite3
        assert sqlite3.sqlite_version=='3.46.1'
        connection=sqlite3.connect(':memory:',isolation_level=None);version=sqlite3.sqlite_version
        if config['case']=='checked':connection.execute('PRAGMA foreign_keys=ON')
        foreign_keys=connection.execute('PRAGMA foreign_keys').fetchone()[0]
        assert foreign_keys==(1 if config['case']=='checked' else 0)
    observer=None;status='failed';result={};state_checks=0
    try:
        engine,n=config['engine'],config['rows'];checked=config['case']=='checked'
        workload.original._populate_sql(connection,engine,n)
        workload.original._prepare_sql_workload(connection,engine,'sql-sqlite-curated')
        initial=workload.validate_state(connection,engine,n)
        assert initial==workload.original._validate_sql_state(connection,engine,'sql-sqlite-curated',n)
        workload.validate_result(workload.checked_composite(connection,engine,n)[0],n)
        if checked:
            workload.validate_constraints(connection,engine)
            workload.validate_savepoint(connection,engine,n)
        def state():
            nonlocal state_checks
            assert workload.validate_state(connection,engine,n)==initial
            state_checks+=1
        def validate(actual):
            if checked:workload.validate_result(actual,n)
            else:assert actual is None
        operation=(lambda:workload.checked_composite(connection,engine,n)[0]) if checked else (
            lambda:workload.original._sql_workload(connection,engine,'sql-sqlite-curated',n))
        observer=DataFrameObserver(rss_limit=config['rss_bytes'])
        with (output/'raw.ndjson').open('x') as raw:
            def record(row):
                if row['phase']=='measure':
                    raw.write(json.dumps(dict(phase='measure',index=row['iteration']-config['warmups'],
                        seconds=row['total_ns']/1e9,resources=row['resources'],state_verified=True,result_verified=checked,
                        original_usage={k:row[k] for k in ('operation_cpu_seconds','operation_input_blocks','operation_output_blocks')}),allow_nan=False)+'\n');raw.flush()
            measure(operation,validate,state,warmups=config['warmups'],samples=config['samples'],record=record,observer=observer)
        workload.validate_result(workload.checked_composite(connection,engine,n)[0],n)
        assert workload.validate_state(connection,engine,n)==initial
        connection.close();connection=None
        if engine=='alopex':
            _verify.cache_clear();assert verify_subject()==identity
        else:assert reference_identity(Path(config['reference_identity']),config['reference_identity_sha256'])==identity
        result=dict(case=config['case'],engine=engine,fixture_rows=n,warmups=config['warmups'],samples=config['samples'],
            input_sha256=workload.digest([(i,float(i),i%2==0) for i in range(n)]),result_sha256=initial,
            state_checks=state_checks,connection_closed=True,checked_contracts=checked,sqlite_version=version,foreign_keys=foreign_keys,
            subject_identity=identity,resource_observations=len(observer.completed),worker_sha256=sha(Path(__file__)),
            sources=config['sources'],raw_sha256=sha(output/'raw.ndjson'))
        save(output/'measurement.json',result);status='passed'
    finally:
        if connection is not None:connection.close()
        if observer is not None:
            save(output/'observations.json',dict(completed=observer.completed,pending=observer.pending));observer.close()
        save(output/'worker-status.json',dict(status=status))
    print(json.dumps(dict(status=status)))

if __name__=='__main__':main()
