#!/usr/bin/env python3
"""Manual #684 bounded disk/HNSW probe. Preparation runs in a separate process."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
from time import perf_counter_ns

QUERY = "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 1 WITH (enable_hnsw = true)"


def snapshot(path):
    result = subprocess.run(['ps', '-o', 'rss=', '-p', str(os.getpid())],
                            check=True, capture_output=True, text=True)
    rss = int(result.stdout.strip()) * 1024
    if rss <= 0 or rss >= 2 * 1024**3:
        raise RuntimeError('RSS unavailable or at 2 GiB stop')
    files = [p for p in path.parent.rglob('*') if p.is_file()]
    return dict(rss_bytes=rss, allocated_bytes=sum(p.stat().st_blocks * 512 for p in files),
                logical_bytes=sum(p.stat().st_size for p in files))


def validate_rows(db, count):
    actual = db.execute_sql('SELECT id, value, e, f FROM items ORDER BY id')
    expected = [dict(id=i, value=i * 3, e=[float(i), 0.0], f=[0.0, float(i)])
                for i in range(count)]
    if actual != expected:
        raise RuntimeError('complete row/vector state differs')


def emit(**value):
    print(json.dumps(value), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manual', action='store_true', required=True)
    parser.add_argument('--action', choices=['seed', 'probe'], required=True)
    parser.add_argument('--path', type=Path, required=True)
    parser.add_argument('--rows', type=int, choices=[0, 100, 1000], required=True)
    parser.add_argument('--indexes', type=int, choices=[0, 1, 2], required=True)
    parser.add_argument('--mode', choices=['ro', 'rw', 'auto'], default='rw')
    parser.add_argument('--operation', choices=['noop', 'query'], default='noop')
    parser.add_argument('--samples', type=int, choices=[1, 6], default=6)
    args = parser.parse_args()
    if sys.platform != 'darwin':
        parser.error('this resource probe requires native macOS ps/getrusage')
    if args.mode == 'auto' and args.operation != 'query':
        parser.error('autocommit measures execute_sql total, never a synthetic noop begin')
    query = QUERY if args.indexes else QUERY.replace(' WITH (enable_hnsw = true)', '')

    import alopex
    from alopex import Database, TxnMode
    native = list(Path(alopex.__file__).parent.glob('_alopex*.so'))
    if len(native) != 1:
        raise RuntimeError('expected one installed extension')
    extension_sha256 = hashlib.sha256(native[0].read_bytes()).hexdigest()
    if args.action == 'seed' and args.path.exists():
        raise RuntimeError('seed refuses an existing database')
    if args.action == 'probe' and not args.path.is_file():
        raise RuntimeError('probe requires a separately prepared database')

    db = Database.open(str(args.path), hnsw_cache_bytes=8 * 1024**2, hnsw_cache_entries=2)
    try:
        if args.action == 'seed':
            db.execute_sql('CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, e VECTOR(2,L2), f VECTOR(2,L2))')
            tx = db.begin(TxnMode.READ_WRITE)
            try:
                with tx:
                    for i in range(args.rows):
                        tx.execute_sql(f'INSERT INTO items VALUES ({i}, {i * 3}, [{i},0], [0,{i}])')
                    tx.commit()
            finally:
                del tx
            for column in ['e', 'f'][:args.indexes]:
                db.execute_sql(f'CREATE INDEX i_{column} ON items ({column}) USING HNSW')
            validate_rows(db, args.rows)
            emit(event='seeded', extension_sha256=extension_sha256, rows=args.rows,
                 indexes=args.indexes, all_rows_unchanged=True)
            return

        initial_cache = db.hnsw_cache_stats()
        if initial_cache['entries'] != 0:
            raise RuntimeError('fresh reader must start with an empty HNSW cache')
        emit(event='ready', pid=os.getpid(), extension_sha256=extension_sha256,
             rows=args.rows, indexes=args.indexes, mode=args.mode, operation=args.operation,
             query=query,
             initial_cache=initial_cache,
             cache_note='fresh reader; not OS-cold; sample0 shared cache empty, samples1..5 explicitly warmed')
        mode = TxnMode.READ_ONLY if args.mode == 'ro' else TxnMode.READ_WRITE
        for iteration in range(args.samples):
            if iteration == 1:
                for column in ['e', 'f'][:args.indexes]:
                    db.warm_hnsw_index('i_' + column)
                cache = db.hnsw_cache_stats()
                if cache['entries'] != args.indexes:
                    raise RuntimeError('all requested HNSW indexes must fit the shared cache')
            before = snapshot(args.path)
            cache_before = db.hnsw_cache_stats()
            begin_ns = query_ns = rollback_ns = auto_total_ns = None
            after_begin = after_query = None
            if args.mode == 'auto':
                started = perf_counter_ns()
                result = db.execute_sql(query)
                auto_total_ns = perf_counter_ns() - started
                after_query = snapshot(args.path)
            else:
                started = perf_counter_ns()
                tx = db.begin(mode)
                begin_ns = perf_counter_ns() - started
                try:
                    after_begin = snapshot(args.path)
                    if args.operation == 'query':
                        started = perf_counter_ns()
                        result = tx.execute_sql(query)
                        query_ns = perf_counter_ns() - started
                        after_query = snapshot(args.path)
                finally:
                    started = perf_counter_ns()
                    tx.rollback()
                    rollback_ns = perf_counter_ns() - started
                del tx
            if args.operation == 'query' and result != ([{'id': 0}] if args.rows else []):
                raise RuntimeError('nearest-neighbor result differs')
            cache_after = db.hnsw_cache_stats()
            if iteration > 0 and args.rows and args.indexes and args.operation == 'query':
                if cache_after['hits'] <= cache_before['hits']:
                    raise RuntimeError('warm HNSW query did not hit the shared cache')
            emit(event='sample', iteration=iteration, first_after_open=iteration == 0,
                 begin_ns=begin_ns, query_ns=query_ns, rollback_ns=rollback_ns,
                 auto_total_ns=auto_total_ns, before=before, after_begin=after_begin,
                 after_query=after_query, after_finish=snapshot(args.path),
                 cache_before=cache_before, cache_after=cache_after)
        peak_before_validation = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        validate_rows(db, args.rows)
        emit(event='complete', all_rows_unchanged=True,
             peak_before_validation_bytes=peak_before_validation,
             peak_after_validation_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    finally:
        db.close()


if __name__ == '__main__':
    main()
