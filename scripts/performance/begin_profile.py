#!/usr/bin/env python3
"""Manual #684 public Python diagnostic; one bounded fixture per process."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import tempfile
from time import perf_counter_ns, sleep


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manual', action='store_true', required=True)
    parser.add_argument('--backend', choices=['memory', 'disk'], required=True)
    parser.add_argument('--rows', type=int, choices=[0, 100, 1000, 10000], required=True)
    parser.add_argument('--mode', choices=['ro', 'rw'], required=True)
    parser.add_argument('--operation', choices=['noop', 'query', 'profile'], required=True)
    args = parser.parse_args()
    import alopex
    from alopex import Database, TxnMode
    extensions = list(Path(alopex.__file__).parent.glob("_alopex*.so"))
    if len(extensions) != 1:
        raise RuntimeError("expected one native extension for source identity")
    extension_sha256 = hashlib.sha256(extensions[0].read_bytes()).hexdigest()
    mode = TxnMode.READ_ONLY if args.mode == 'ro' else TxnMode.READ_WRITE
    with tempfile.TemporaryDirectory(prefix='alopex-684-db-') as temporary:
        path = str(Path(temporary) / 'db')
        def open_db():
            return Database.open(path) if args.backend == 'disk' else Database.new()
        seed_started = perf_counter_ns()
        db = open_db()
        try:
            db.execute_sql('CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)')
            if args.rows:
                tx = db.begin(TxnMode.READ_WRITE)
                try:
                    for row in range(args.rows):
                        tx.execute_sql(f'INSERT INTO items VALUES ({row}, {row * 3})')
                    tx.commit()
                except BaseException:
                    tx.rollback()
                    raise
                finally:
                    # Seed-handle destruction must precede reopen and the first timed begin.
                    del tx
            seed_ns = perf_counter_ns() - seed_started
            reopened = perf_counter_ns()
            if args.backend == 'disk':
                db.close()
                db = open_db()
            reopen_ns = perf_counter_ns() - reopened
            print(json.dumps(dict(event='ready', pid=os.getpid(), backend=args.backend, rows=args.rows,
                                  indexes=0, api='Python-owned', mode=args.mode, operation=args.operation,
                                  package=importlib.metadata.version('alopex'), extension_sha256=extension_sha256, seed_ns=seed_ns,
                                  reopen_ns=reopen_ns, cache_note='first-after-open is not OS cold cache')), flush=True)
            if args.operation == 'profile':
                sleep(1)
            profile_started = perf_counter_ns()
            iteration = 0
            while True:
                started = perf_counter_ns()
                tx = db.begin(mode)
                begin_ns = perf_counter_ns() - started
                try:
                    query_started = perf_counter_ns()
                    if args.operation == 'query':
                        result = tx.execute_sql('SELECT id FROM items WHERE id = 0')
                        query_ns = perf_counter_ns() - query_started
                        if result != ([{'id': 0}] if args.rows else []):
                            raise ValueError('point query mismatch')
                    else:
                        query_ns = None
                finally:
                    rollback_started = perf_counter_ns()
                    tx.rollback()
                    rollback_ns = perf_counter_ns() - rollback_started
                # Avoid charging destruction of the previous handle to the next begin.
                del tx
                if args.operation != 'profile':
                    print(json.dumps(dict(event='sample', iteration=iteration, first_after_open=iteration == 0,
                                          begin_ns=begin_ns, query_ns=query_ns, rollback_ns=rollback_ns)), flush=True)
                iteration += 1
                if (args.operation == 'profile' and perf_counter_ns() - profile_started >= 4_000_000_000) or (args.operation != 'profile' and iteration == 6):
                    break
            result = db.execute_sql('SELECT id, value FROM items ORDER BY id')
            if result != [dict(id=row, value=row * 3) for row in range(args.rows)]:
                raise ValueError('final complete state mismatch')
            print(json.dumps(dict(event='complete', iterations=iteration, all_rows_unchanged=True)), flush=True)
        finally:
            db.close()


if __name__ == '__main__':
    main()
