#!/usr/bin/env python3
"""Manual native #684 phase probe. Preparation runs in a separate process."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
from time import perf_counter_ns, process_time_ns, thread_time_ns

QUERY = "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 1 WITH (enable_hnsw = true)"


def peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if value < 0:
        raise RuntimeError('invalid lifetime RSS counter')
    if sys.platform == 'linux':
        return value * 1024
    if sys.platform == 'darwin':
        return value
    raise RuntimeError('unsupported RSS counter unit')


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


COUNTERS = ("ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw", "ru_inblock", "ru_oublock")


def io_counters():
    fields = {}
    for line in Path("/proc/self/io").read_text().splitlines():
        key, value = line.split(":", 1)
        fields[key] = int(value)
    return {name: fields[name] for name in ("read_bytes", "write_bytes", "cancelled_write_bytes")}


def measured(operation):
    io_before = io_counters()
    usage_before = resource.getrusage(resource.RUSAGE_SELF)
    process_before = process_time_ns()
    thread_before = thread_time_ns()
    wall_before = perf_counter_ns()
    result = operation()
    wall_ns = perf_counter_ns() - wall_before
    thread_ns = thread_time_ns() - thread_before
    process_ns = process_time_ns() - process_before
    usage_after = resource.getrusage(resource.RUSAGE_SELF)
    io_after = io_counters()
    observation = dict(wall_ns=wall_ns, process_cpu_ns=process_ns, caller_cpu_ns=thread_ns,
                       counters={name: getattr(usage_after, name) - getattr(usage_before, name)
                                 for name in COUNTERS},
                       io_bytes={name: io_after[name] - value for name, value in io_before.items()})
    values = [wall_ns, thread_ns, process_ns, *observation["counters"].values(),
              *observation["io_bytes"].values()]
    if any(type(value) is not int or value < 0 for value in values) or wall_ns == 0:
        raise RuntimeError("missing or non-monotonic diagnostic counter")
    return result, observation


"""Read only our native extension's present PTE flags, retaining file offsets."""
import ctypes
import os
from pathlib import Path
import struct


def regions(text, extension, page_size):
    output = []
    for line in text.splitlines():
        parts = line.split(None, 5)
        if len(parts) != 6 or parts[5] != str(extension):
            continue
        bounds, permissions, offset = parts[:3]
        if not permissions.startswith('r'):
            continue
        start, end = (int(value, 16) for value in bounds.split('-'))
        offset = int(offset, 16)
        if start % page_size or end <= start or end % page_size or offset % page_size:
            raise ValueError('unaligned native mapping')
        output.append((start, end, offset, permissions))
    if not output or sum(end - start for start, end, _, _ in output) > 128_000_000:
        raise ValueError('missing or oversized native mappings')
    return output


def present_offsets(data, file_offset, page_size):
    if len(data) % 8:
        raise ValueError('incomplete pagemap entry')
    # PFNs are intentionally ignored, even if the kernel exposes them.
    return [file_offset + index * page_size
            for index, (flags,) in enumerate(struct.iter_unpack('=Q', data))
            if flags & (1 << 63)]


class PageObserver:
    def __init__(self, extension):
        self.page_size = os.sysconf('SC_PAGE_SIZE')
        if type(self.page_size) is not int or self.page_size <= 0:
            raise ValueError('missing page size')
        self.regions = regions(Path('/proc/self/maps').read_text(), extension, self.page_size)
        self.fd = os.open('/proc/self/pagemap', os.O_RDONLY)
        self.buffers = [[bytearray((end-start)//self.page_size*8) for start,end,_,_ in self.regions]
                        for _ in range(4)]
        self.views = [[memoryview(buf) for buf in slot] for slot in self.buffers]
        # Allocate/touch observer buffers and warm its read path before recording.
        try:
            for slot in range(4): self.capture(slot)
        except BaseException:
            self.close()
            raise

    def capture(self, slot):
        for region, view in zip(self.regions, self.views[slot]):
            offset = region[0] // self.page_size * 8
            if os.preadv(self.fd, [view], offset) != len(view):
                raise RuntimeError('incomplete own-process pagemap read')

    def prefault(self):
        # Readable mappings only. No writable mapping or memory protection changes.
        for start, end, _, _ in self.regions:
            for address in range(start, end, self.page_size):
                ctypes.c_ubyte.from_address(address).value

    def difference(self, before, after):
        rows = []
        for index, (_, end, offset, permissions) in enumerate(self.regions):
            old = set(present_offsets(self.buffers[before][index], offset, self.page_size))
            new = set(present_offsets(self.buffers[after][index], offset, self.page_size))
            rows.append(dict(file_offset=offset, permissions=permissions,
                             mapped_pages=len(self.buffers[before][index])//8,
                             present_before=len(old), present_after=len(new),
                             newly_present=sorted(new-old), no_longer_present=sorted(old-new)))
        return rows

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


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
    parser.add_argument('--prefault-extension', action='store_true')
    args = parser.parse_args()
    if args.action == 'probe' and (args.mode != 'ro' or args.operation != 'noop' or args.samples != 1):
        parser.error('page diagnostic requires one first READ_ONLY noop begin')
    if sys.platform != 'linux':
        parser.error('Linux process diagnostics required')
    if args.mode == 'auto' and args.operation != 'query':
        parser.error('autocommit measures execute_sql total, never a synthetic noop begin')
    if args.rows != 1000 or args.indexes != 0:
        parser.error('diagnostic scope is 1000 rows without indexes')
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
    observer = None
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
        observer = PageObserver(native[0])
        observer.capture(0)
        if args.prefault_extension:
            observer.prefault()
        observer.capture(1)
        prefault_observation = observer.difference(0, 1)
        controls = [measured(lambda: None)[1] for _ in range(21)]
        observer.capture(0)
        measured(lambda: None)
        observer.capture(1)
        observer_control = observer.difference(0, 1)
        emit(event='ready', prefault_extension=args.prefault_extension, page_size=observer.page_size,
             prefault_observation=prefault_observation, page_observer_control=observer_control, diagnostic_schema='alopex.begin-phase-diagnostics/v1',
             timer_controls=controls, pid=os.getpid(), extension_sha256=extension_sha256,
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
            phases = {}
            after_begin = after_query = None
            if args.mode == 'auto':
                result, phases['auto'] = measured(lambda: db.execute_sql(query))
                auto_total_ns = phases['auto']['wall_ns']
                after_query = snapshot(args.path)
            else:
                observer.capture(2)
                tx, phases['begin'] = measured(lambda: db.begin(mode))
                observer.capture(3)
                begin_ns = phases['begin']['wall_ns']
                try:
                    after_begin = snapshot(args.path)
                    if args.operation == 'query':
                        result, phases['query'] = measured(lambda: tx.execute_sql(query))
                        query_ns = phases['query']['wall_ns']
                        after_query = snapshot(args.path)
                finally:
                    _, phases['rollback'] = measured(tx.rollback)
                    rollback_ns = phases['rollback']['wall_ns']
                del tx
            if args.operation == 'query' and result != ([{'id': 0}] if args.rows else []):
                raise RuntimeError('nearest-neighbor result differs')
            cache_after = db.hnsw_cache_stats()
            if iteration > 0 and args.rows and args.indexes and args.operation == 'query':
                if cache_after['hits'] <= cache_before['hits']:
                    raise RuntimeError('warm HNSW query did not hit the shared cache')
            emit(event='sample', extension_pages=observer.difference(2, 3), diagnostic_phases=phases, iteration=iteration, first_after_open=iteration == 0,
                 begin_ns=begin_ns, query_ns=query_ns, rollback_ns=rollback_ns,
                 auto_total_ns=auto_total_ns, before=before, after_begin=after_begin,
                 after_query=after_query, after_finish=snapshot(args.path),
                 cache_before=cache_before, cache_after=cache_after)
        peak_before_validation = peak_rss_bytes()
        validate_rows(db, args.rows)
        query_plan = db.execute_sql('EXPLAIN ' + query)
        if 'ExactKnnScan' not in json.dumps(query_plan):
            raise RuntimeError('unexpected physical query path')
        emit(event='complete', all_rows_unchanged=True, query_plan=query_plan,
             peak_before_validation_bytes=peak_before_validation,
             peak_after_validation_bytes=peak_rss_bytes())
    finally:
        if observer is not None: observer.close()
        db.close()


if __name__ == '__main__':
    main()
