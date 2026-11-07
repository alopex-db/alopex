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
