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


def observe_before(mode):
    if mode == 'plain':
        return None
    return (io_counters(), resource.getrusage(resource.RUSAGE_SELF),
            process_time_ns(), thread_time_ns())


def observe_after(before, wall_ns):
    if before is None:
        return {'wall_ns': wall_ns}
    thread_after = thread_time_ns()
    process_after = process_time_ns()
    usage_after = resource.getrusage(resource.RUSAGE_SELF)
    io_after = io_counters()
    io_before, usage_before, process_before, thread_before = before
    result = dict(wall_ns=wall_ns, process_cpu_ns=process_after-process_before,
                  caller_cpu_ns=thread_after-thread_before,
                  counters={key: getattr(usage_after,key)-getattr(usage_before,key) for key in COUNTERS},
                  io_bytes={key: io_after[key]-value for key,value in io_before.items()})
    values = [wall_ns, result['process_cpu_ns'], result['caller_cpu_ns'],
              *result['counters'].values(), *result['io_bytes'].values()]
    if any(type(v) is not int or v < 0 for v in values) or wall_ns == 0:
        raise RuntimeError('missing or non-monotonic diagnostic counter')
    return result


"""Own-process page presence and RSS composition; no prefault or protection changes.

Linux definitions: https://docs.kernel.org/filesystems/proc.html
Anonymous pages can occur inside a file mapping. Preserve both RSS and Anonymous,
including writable extension mappings; do not call their difference heap usage.
"""
import os
from pathlib import Path
import re
import struct

GROUPS = ('extension_exec', 'extension_readonly', 'extension_writable', 'other_file', 'nonfile')
FIELDS = ('rss_bytes', 'anonymous_bytes')
HEADER = re.compile(r'^([0-9a-f]+)-([0-9a-f]+) ([r-][w-][x-][ps]) ([0-9a-f]+) [0-9a-f]+:[0-9a-f]+ ([0-9]+)(?: +(.*))?$')


def parse_smaps(lines, extension):
    groups = {name: {key: 0 for key in FIELDS} for name in GROUPS}
    current = None
    values = {}
    count = total = 0
    def finish():
        if current is not None:
            if set(values) != {'Rss', 'Anonymous'} or not 0 <= values['Anonymous'] <= values['Rss']:
                raise ValueError('missing or inconsistent mapping counters')
            groups[current]['rss_bytes'] += values['Rss']
            groups[current]['anonymous_bytes'] += values['Anonymous']
    for line in lines:
        total += len(line)
        if total > 4_000_000 or len(line) > 8192:
            raise ValueError('mapping inventory exceeds diagnostic bound')
        match = HEADER.fullmatch(line.rstrip('\n'))
        if match:
            finish()
            start, end, permissions, offset, inode, name = match.groups()
            if int(end, 16) <= int(start, 16):
                raise ValueError('invalid mapping bounds')
            count += 1
            if count > 4096:
                raise ValueError('too many mappings')
            if name == str(extension):
                current = ('extension_writable' if permissions[1] == 'w' else
                           'extension_exec' if permissions[2] == 'x' else 'extension_readonly')
            else:
                current = 'other_file' if name and int(inode) and not name.startswith('[') else 'nonfile'
            values = {}
            continue
        key, separator, value = line.partition(':')
        if key in ('Rss', 'Anonymous'):
            fields = value.split()
            if current is None or not separator or key in values or len(fields) != 2 or not fields[0].isdigit() or fields[1] != 'kB':
                raise ValueError('invalid mapping counter')
            values[key] = int(fields[0]) * 1024
        elif current is None or not separator:
            raise ValueError('invalid mapping record')
    finish()
    if not count or not sum(groups[g]['rss_bytes'] for g in GROUPS[:3]):
        raise ValueError('extension RSS unavailable')
    return groups


def regions(text, extension, page_size):
    output = []
    for line in text.splitlines():
        parts = line.split(None, 5)
        if len(parts) != 6 or parts[5] != str(extension) or not parts[1].startswith('r'):
            continue
        start, end = (int(v, 16) for v in parts[0].split('-'))
        offset = int(parts[2], 16)
        if start % page_size or end <= start or end % page_size or offset % page_size:
            raise ValueError('unaligned extension mapping')
        output.append((start, end, offset, parts[1]))
    if not output or sum(end - start for start, end, _, _ in output) > 128_000_000:
        raise ValueError('missing or oversized extension mappings')
    return output


def present_offsets(data, file_offset, page_size):
    if len(data) % 8:
        raise ValueError('incomplete pagemap entry')
    return [file_offset + index * page_size
            for index, (flags,) in enumerate(struct.iter_unpack('=Q', data)) if flags & (1 << 63)]


class QueryPageObserver:
    def __init__(self, extension):
        self.extension = extension
        self.fd = None
        self.page_size = os.sysconf('SC_PAGE_SIZE')
        if type(self.page_size) is not int or self.page_size <= 0:
            raise ValueError('page size unavailable')
        self.regions = regions(Path('/proc/self/maps').read_text(), extension, self.page_size)
        self.buffers = [[bytearray((end-start)//self.page_size*8) for start,end,_,_ in self.regions]
                        for _ in range(2)]
        self.views = [[memoryview(value) for value in slot] for slot in self.buffers]
        try:
            self.fd = os.open('/proc/self/pagemap', os.O_RDONLY)
            self.smaps()  # Warm the observer's parse/allocation path, not product pages.
            self.capture(0)
            self.capture(1)
        except BaseException:
            self.close()
            raise

    def smaps(self):
        with Path('/proc/self/smaps').open() as stream:
            return parse_smaps(stream, self.extension)

    def capture(self, slot):
        for region, view in zip(self.regions, self.views[slot]):
            if os.preadv(self.fd, [view], region[0] // self.page_size * 8) != len(view):
                raise RuntimeError('incomplete own-process pagemap read')

    def begin(self):
        before = self.smaps()
        self.capture(0)
        return before

    def finish(self, before):
        self.capture(1)
        after = self.smaps()
        pages = []
        for i, (_, _, offset, permissions) in enumerate(self.regions):
            old = set(present_offsets(self.buffers[0][i], offset, self.page_size))
            new = set(present_offsets(self.buffers[1][i], offset, self.page_size))
            pages.append(dict(file_offset=offset, permissions=permissions,
                              mapped_pages=len(self.buffers[0][i])//8,
                              present_before=len(old), present_after=len(new),
                              newly_present=sorted(new-old), no_longer_present=sorted(old-new)))
        return dict(page_size=self.page_size, before=before, after=after, extension_pages=pages)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def validate_page_observation(value):
    if not isinstance(value, dict) or set(value) != {'page_size', 'before', 'after', 'extension_pages'}:
        raise ValueError('missing page observation')
    size = value['page_size']
    if type(size) is not int or size <= 0:
        raise ValueError('invalid page size')
    for phase in ('before', 'after'):
        groups = value[phase]
        if not isinstance(groups, dict) or set(groups) != set(GROUPS):
            raise ValueError('missing memory composition')
        for row in groups.values():
            if set(row) != set(FIELDS) or any(type(v) is not int or v < 0 for v in row.values()) or row['anonymous_bytes'] > row['rss_bytes']:
                raise ValueError('invalid memory composition')
    pages = value['extension_pages']
    if not isinstance(pages, list) or not pages:
        raise ValueError('missing extension pages')
    for row in pages:
        if set(row) != {'file_offset', 'permissions', 'mapped_pages', 'present_before', 'present_after', 'newly_present', 'no_longer_present'}:
            raise ValueError('invalid extension page fields')
        if type(row['file_offset']) is not int or row['file_offset'] < 0 or row['file_offset'] % size or row['permissions'] not in ('r--p','r-xp','rw-p','rwxp','r--s','r-xs','rw-s','rwxs'):
            raise ValueError('invalid extension mapping')
        if type(row['mapped_pages']) is not int or row['mapped_pages'] <= 0:
            raise ValueError('invalid mapped page count')
        if any(type(row[k]) is not int or not 0 <= row[k] <= row['mapped_pages'] for k in ('present_before', 'present_after')):
            raise ValueError('invalid resident page count')
        for name in ('newly_present', 'no_longer_present'):
            offsets = row[name]
            if not isinstance(offsets, list) or any(type(v) is not int for v in offsets) or offsets != sorted(set(offsets)):
                raise ValueError('invalid page offsets')
            if any(v % size or not row['file_offset'] <= v < row['file_offset'] + size * row['mapped_pages'] for v in offsets):
                raise ValueError('out-of-range page offset')
        if set(row['newly_present']) & set(row['no_longer_present']) or row['present_after']-row['present_before'] != len(row['newly_present'])-len(row['no_longer_present']):
            raise ValueError('page count mismatch')


def validate_page_events(events, spec):
    if spec is None:
        return
    control = events[0].get('page_observer_control')
    observations = [event.get('query_pages') for event in events[1:-1]]
    if spec['observer_mode'] == 'plain':
        if control is not None or any(value is not None for value in observations):
            raise ValueError('plain reader unexpectedly observed pages')
        return
    validate_page_observation(control)
    if any(row['newly_present'] or row['no_longer_present'] for row in control['extension_pages']):
        raise ValueError('observer control touched extension pages')
    for value in observations:
        validate_page_observation(value)
        if value['page_size'] != control['page_size']:
            raise ValueError('page granularity changed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manual', action='store_true', required=True)
    parser.add_argument('--observer-mode', choices=['plain', 'counters'], required=True)
    parser.add_argument('--action', choices=['seed', 'probe'], required=True)
    parser.add_argument('--path', type=Path, required=True)
    parser.add_argument('--rows', type=int, choices=[0, 100, 1000], required=True)
    parser.add_argument('--indexes', type=int, choices=[0, 1, 2], required=True)
    parser.add_argument('--mode', choices=['ro', 'rw', 'auto'], default='rw')
    parser.add_argument('--operation', choices=['noop', 'query'], default='noop')
    parser.add_argument('--samples', type=int, choices=[1, 6], default=6)
    args = parser.parse_args()
    if (args.rows, args.indexes) != (0, 2) or (args.mode, args.operation) != ('rw', 'query'):
        parser.error('fixture outside observer comparison scope')
    if sys.platform not in ('linux', 'darwin'):
        parser.error('native Linux or macOS resource observation required')
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
    page_observer = None
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
        page_control = None
        if args.observer_mode == 'counters':
            page_observer = QueryPageObserver(native[0])
            page_control = page_observer.finish(page_observer.begin())
            validate_page_observation(page_control)
            if any(r['newly_present'] or r['no_longer_present'] for r in page_control['extension_pages']):
                raise RuntimeError('observer control touched extension pages')
        emit(event='ready', page_observer_control=page_control, diagnostic_schema='alopex.begin-observer-comparison/v1',
             observer_mode=args.observer_mode, pid=os.getpid(), extension_sha256=extension_sha256,
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
            phases = {}
            query_pages = None
            if args.mode == 'auto':
                observation = observe_before(args.observer_mode)
                started = perf_counter_ns()
                result = db.execute_sql(query)
                auto_total_ns = perf_counter_ns() - started
                phases['auto'] = observe_after(observation, auto_total_ns)
                after_query = snapshot(args.path)
            else:
                observation = observe_before(args.observer_mode)
                started = perf_counter_ns()
                tx = db.begin(mode)
                begin_ns = perf_counter_ns() - started
                phases['begin'] = observe_after(observation, begin_ns)
                try:
                    after_begin = snapshot(args.path)
                    if args.operation == 'query':
                        page_before = page_observer.begin() if page_observer else None
                        observation = observe_before(args.observer_mode)
                        started = perf_counter_ns()
                        result = tx.execute_sql(query)
                        query_ns = perf_counter_ns() - started
                        phases['query'] = observe_after(observation, query_ns)
                        query_pages = page_observer.finish(page_before) if page_observer else None
                        after_query = snapshot(args.path)
                finally:
                    observation = observe_before(args.observer_mode)
                    started = perf_counter_ns()
                    tx.rollback()
                    rollback_ns = perf_counter_ns() - started
                    phases['rollback'] = observe_after(observation, rollback_ns)
                del tx
            if args.operation == 'query' and result != ([{'id': 0}] if args.rows else []):
                raise RuntimeError('nearest-neighbor result differs')
            cache_after = db.hnsw_cache_stats()
            if iteration > 0 and args.rows and args.indexes and args.operation == 'query':
                if cache_after['hits'] <= cache_before['hits']:
                    raise RuntimeError('warm HNSW query did not hit the shared cache')
            emit(event='sample', query_pages=query_pages, diagnostic_phases=phases, observer_mode=args.observer_mode, iteration=iteration, first_after_open=iteration == 0,
                 begin_ns=begin_ns, query_ns=query_ns, rollback_ns=rollback_ns,
                 auto_total_ns=auto_total_ns, before=before, after_begin=after_begin,
                 after_query=after_query, after_finish=snapshot(args.path),
                 cache_before=cache_before, cache_after=cache_after)
        peak_before_validation = peak_rss_bytes()
        validate_rows(db, args.rows)
        query_plan = db.execute_sql('EXPLAIN ' + query)
        expected_plan = 'HnswSearch' if args.indexes else 'ExactKnnScan'
        if expected_plan not in json.dumps(query_plan):
            raise RuntimeError('unexpected physical query path')
        emit(event='complete', query_plan=query_plan, all_rows_unchanged=True,
             peak_before_validation_bytes=peak_before_validation,
             peak_after_validation_bytes=peak_rss_bytes())
    finally:
        if page_observer is not None: page_observer.close()
        db.close()


if __name__ == '__main__':
    main()
