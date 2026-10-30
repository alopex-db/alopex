#!/usr/bin/env python3
"""Manual #684 first-write follow-up using separately seeded, bounded fixtures."""
import argparse
import hashlib
from pathlib import Path
import resource
import sys
from time import perf_counter_ns

from begin_acceptance import emit, snapshot, validate_rows


def measure(db, path, rows, keys, mode):
    before = snapshot(path)
    started = perf_counter_ns()
    tx = db.begin(mode)
    begin_ns = perf_counter_ns() - started
    try:
        with tx:
            after_begin = snapshot(path)
            started = perf_counter_ns()
            tx.execute_sql('UPDATE items SET value = value + 1 WHERE id = 0')
            first_write_ns = perf_counter_ns() - started
            after_first = snapshot(path)
            started = perf_counter_ns()
            tx.execute_sql('UPDATE items SET value = value + 2 WHERE id = 0')
            repeat_write_ns = perf_counter_ns() - started
            after_repeat = snapshot(path)
            remaining_writes_ns = None
            if keys > 1:
                started = perf_counter_ns()
                for key in range(1, keys):
                    tx.execute_sql(f'UPDATE items SET value = value + 1 WHERE id = {key}')
                remaining_writes_ns = perf_counter_ns() - started
            after_writes = snapshot(path)
            # Validate transaction-local state outside every measured interval.
            actual = tx.execute_sql(f'SELECT id, value FROM items WHERE id < {keys} ORDER BY id')
            expected = [dict(id=i, value=3 if i == 0 else i * 3 + 1) for i in range(keys)]
            if actual != expected:
                raise RuntimeError('transaction-local updated values differ')
            started = perf_counter_ns()
            tx.rollback()
            rollback_ns = perf_counter_ns() - started
    finally:
        del tx
    after_finish = snapshot(path)
    return dict(begin_ns=begin_ns, first_write_ns=first_write_ns,
                repeat_write_ns=repeat_write_ns, remaining_writes_ns=remaining_writes_ns,
                rollback_ns=rollback_ns, begin_first_ns=begin_ns + first_write_ns,
                begin_all_writes_ns=begin_ns + first_write_ns + repeat_write_ns + (remaining_writes_ns or 0),
                before=before, after_begin=after_begin, after_first=after_first,
                after_repeat=after_repeat, after_writes=after_writes, after_finish=after_finish,
                updated_values_valid=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manual', action='store_true', required=True)
    parser.add_argument('--path', type=Path, required=True)
    parser.add_argument('--rows', type=int, choices=[100, 1000], required=True)
    parser.add_argument('--indexes', type=int, choices=[0, 1, 2], required=True)
    parser.add_argument('--keys', type=int, choices=[1, 8], required=True)
    args = parser.parse_args()
    if sys.platform != 'darwin' or not args.path.is_file():
        parser.error('native macOS and a separately seeded fixture required')
    import alopex
    from alopex import Database, TxnMode
    native = list(Path(alopex.__file__).parent.glob('_alopex*.so'))
    if len(native) != 1:
        raise RuntimeError('expected one installed extension')
    extension_sha256 = hashlib.sha256(native[0].read_bytes()).hexdigest()
    db = Database.open(str(args.path), hnsw_cache_bytes=8 * 1024**2, hnsw_cache_entries=2)
    try:
        if db.hnsw_cache_stats()['entries']:
            raise RuntimeError('fresh reader must start with an empty cache')
        emit(event='ready', extension_sha256=extension_sha256, rows=args.rows,
             indexes=args.indexes, keys=args.keys,
             note='fresh reader, not OS cold; scalar UPDATE preserves both vector columns')
        for iteration in range(6):
            if iteration == 1:
                for column in ['e', 'f'][:args.indexes]:
                    db.warm_hnsw_index('i_' + column)
                if db.hnsw_cache_stats()['entries'] != args.indexes:
                    raise RuntimeError('requested indexes must fit the shared cache')
            values = measure(db, args.path, args.rows, args.keys, TxnMode.READ_WRITE)
            emit(event='sample', iteration=iteration, first_after_open=iteration == 0, **values)
        peak_before_validation = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        validate_rows(db, args.rows)
        emit(event='complete', all_rows_unchanged=True,
             peak_before_validation_bytes=peak_before_validation,
             peak_after_validation_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    finally:
        db.close()


if __name__ == '__main__':
    main()
