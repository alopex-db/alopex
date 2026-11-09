"""Independently audit #684 raw phases and compute predeclared comparisons."""
import json
import math
from pathlib import PurePosixPath
import statistics

from audit_native_sql_matrix import Evidence, require, sha


def middle(values, positive=False):
    require(values and all(type(v) in (int, float) and math.isfinite(v) and
                           (not positive or v > 0) for v in values), "invalid observations")
    center = statistics.median(values)
    return center, statistics.median(abs(value - center) for value in values)


def compare(before, after, metric, rows):
    require(len(before) == len(after) == 3, "three diagnostic trial medians required")
    b, db = middle(before)
    c, dc = middle(after)
    return dict(baseline_median=b, candidate_median=c, baseline_trial_mad=db,
                candidate_trial_mad=dc, difference=c-b, noise=3*max(db,dc),
                ratio=c/b if b > 0 else None, performance_acceptance=False)


def expected_specs(rows, indexes, trial=0, subject="before"):
    require(type(trial) is int and trial in range(3) and subject in ("before", "candidate"), "observer order identity differs")
    require(type(rows) is int and type(indexes) is int, "invalid fixture type")
    require((rows, indexes) == (0, 2), "unexpected empty query fixture")
    operations = [("rw", "query")]
    modes = ['plain', 'counters']
    if (trial % 2 == 1) != (subject == 'candidate'):
        modes.reverse()
    return [dict(mode=mode, operation=operation, samples=6, repeat=0, observer_mode=observer)
            for mode, operation in operations for observer in modes]


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


def extract(events, spec, row, extension):
    require(len(events) == spec["samples"] + 2, "sample count differs")
    first, final = events[0], events[-1]
    require(first["event"] == "ready" and first["extension_sha256"] == extension and
            first["initial_cache"]["entries"] == 0 and
            all(first[key] == row[key] for key in ("rows", "indexes")) and
            all(first[key] == spec[key] for key in ("mode", "operation", "observer_mode")), "fresh-reader identity differs")
    require(final["event"] == "complete" and final["all_rows_unchanged"] is True,
            "full final state validation missing")
    middle([final["peak_before_validation_bytes"], final["peak_after_validation_bytes"]], positive=True)
    output = {}
    explicit = spec["mode"] != "auto"
    querying = spec["operation"] == "query"
    phases = (["begin_ns", "rollback_ns"] + (["query_ns"] if querying else [])) if explicit else ["auto_total_ns"]
    for index, event in enumerate(events[1:-1]):
        require(event["event"] == "sample" and event["iteration"] == index and
                event["first_after_open"] is (index == 0) and event.get("observer_mode") == spec["observer_mode"], "sample sequence differs")
        middle([event[key] for key in phases], positive=True)
        require(all(event[key] is None for key in {"begin_ns", "rollback_ns", "query_ns", "auto_total_ns"} - set(phases)),
                "phase boundaries differ")
        if index:
            require(event["cache_before"]["entries"] == row["indexes"], "warm cache missing")
            if querying and row["rows"] and row["indexes"]:
                require(event["cache_after"]["hits"] > event["cache_before"]["hits"], "warm query missed cache")
        snapshots = ["before", "after_finish"] + (["after_begin"] if explicit else []) + (["after_query"] if querying else [])
        for name in snapshots:
            snapshot = event[name]
            middle([snapshot["rss_bytes"]], positive=True)
            require(snapshot["rss_bytes"] < 2 * 1024**3 and
                    all(type(snapshot[key]) is int and snapshot[key] >= 0 for key in
                        ("allocated_bytes", "logical_bytes")), "invalid memory/storage observation")
        require(all(event[key] is None for key in {"after_begin", "after_query"} - set(snapshots)),
                "unexpected memory observation")
        state = "first_after_open" if index == 0 else "explicitly_warmed"
        for metric in phases:
            output.setdefault((state, metric), []).append(event[metric])
        if explicit:
            output.setdefault((state, "begin_retained_rss_bytes"), []).append(
                event["after_begin"]["rss_bytes"] - event["before"]["rss_bytes"])
        output.setdefault((state, "finish_retained_rss_bytes"), []).append(
            event["after_finish"]["rss_bytes"] - event["before"]["rss_bytes"])
    require(first.get("diagnostic_schema") == "alopex.begin-observer-comparison/v1", "missing diagnostic schema")
    def numeric_observation(value):
        if spec['observer_mode'] == 'plain':
            require(isinstance(value,dict) and set(value)=={'wall_ns'} and type(value['wall_ns']) is int and value['wall_ns']>0,
                    'plain timer observation differs')
            return value
        require(isinstance(value, dict) and set(value) == {"wall_ns", "process_cpu_ns", "caller_cpu_ns", "counters", "io_bytes"},
                "missing diagnostic observation")
        require(set(value["counters"]) == {"ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw", "ru_inblock", "ru_oublock"},
                "incomplete scheduler/fault counters")
        require(set(value["io_bytes"]) == {"read_bytes", "write_bytes", "cancelled_write_bytes"}, "incomplete physical I/O")
        flat = {name: value[name] for name in ("wall_ns", "process_cpu_ns", "caller_cpu_ns")}
        flat.update(value["counters"])
        flat.update(value["io_bytes"])
        require(all(type(v) is int and v >= 0 for v in flat.values()) and flat["wall_ns"] > 0,
                "invalid diagnostic counters")
        return flat
    require('timer_controls' not in first, 'unexpected observer priming')
    names = {"auto": "auto_total_ns"} if not explicit else {"begin": "begin_ns", "rollback": "rollback_ns"}
    if explicit and querying: names["query"] = "query_ns"
    for index, sample in enumerate(events[1:-1]):
        require(set(sample.get("diagnostic_phases", {})) == set(names), "diagnostic phase set differs")
        for phase, timing in names.items():
            flat = numeric_observation(sample["diagnostic_phases"][phase])
            require(flat["wall_ns"] == sample[timing], "wall interval differs")
            for name, value in flat.items():
                output.setdefault(("first_after_open" if index == 0 else "explicitly_warmed", phase + "." + name), []).append(value)
    validate_page_events(events, spec)
    if spec['observer_mode'] == 'counters':
        for index, sample in enumerate(events[1:-1]):
            pages = sample['query_pages']
            state = 'first_after_open' if index == 0 else 'explicitly_warmed'
            for group in GROUPS:
                for field in FIELDS:
                    difference = pages['after'][group][field] - pages['before'][group][field]
                    output.setdefault((state, 'query_smaps.'+group+'.'+field), []).append(difference)
            for field in ('newly_present','no_longer_present'):
                output.setdefault((state, 'query_extension_pages.'+field), []).append(
                    sum(len(row[field]) for row in pages['extension_pages']))
    expected_plan = "HnswSearch" if row["indexes"] else "ExactKnnScan"
    require(expected_plan in json.dumps(final.get("query_plan")), "physical plan evidence missing")
    return output

