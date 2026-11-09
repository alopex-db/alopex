"""Own-process page presence and RSS composition; readable-extension residency control; no protection changes.

Linux definitions: https://docs.kernel.org/filesystems/proc.html
Anonymous pages can occur inside a file mapping. Preserve both RSS and Anonymous,
including writable extension mappings; do not call their difference heap usage.
"""
import ctypes
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

    def prefault(self):
        # The captured regions are readable mappings of this immutable extension.
        # Reading bytes changes only this process's residency; never mprotect/write.
        checksum = 0
        for start, end, _, _ in self.regions:
            for address in range(start, end, self.page_size):
                checksum ^= ctypes.c_ubyte.from_address(address).value
        return checksum

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
    for index, event in enumerate(events[1:-1]):
        prefault = event.get('prefault_pages')
        elapsed = event.get('prefault_ns')
        if spec['observer_mode'] == 'prefault' and index == 0:
            validate_page_observation(prefault)
            if type(elapsed) is not int or elapsed <= 0 or any(r['present_after'] != r['mapped_pages'] for r in prefault['extension_pages']):
                raise ValueError('prefault incomplete or missing duration')
        elif prefault is not None or elapsed is not None:
            raise ValueError('unexpected prefault operation')
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
