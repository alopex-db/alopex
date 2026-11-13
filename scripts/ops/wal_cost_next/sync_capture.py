"""Bounded, diagnostic-only access to the process-local sync interposer."""

import ctypes
import errno
import os
from pathlib import Path

LIMIT = 128
INVALID_DURATION = 2**64 - 1
EXPECTED_SQL2048 = {'before': (2, 2), 'candidate': (2, 1)}


class Record(ctypes.Structure):
    _fields_ = [('duration_ns', ctypes.c_uint64), ('result', ctypes.c_int32),
                ('error_number', ctypes.c_int32), ('operation', ctypes.c_uint8),
                ('reserved', ctypes.c_uint8 * 7)]


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def open_shim(path, digest, sha):
    require(path.is_file() and not path.is_symlink() and sha(path) == digest and
            os.environ.get('LD_PRELOAD') == str(path.resolve()),
            'sync interposer identity differs')
    shim = ctypes.CDLL(str(path))
    shim.alopex_sync_smoke_ready.restype = ctypes.c_int
    shim.alopex_sync_smoke_snapshot.argtypes = [ctypes.POINTER(Record), ctypes.c_size_t,
                                                ctypes.POINTER(ctypes.c_size_t),
                                                ctypes.POINTER(ctypes.c_size_t)]
    shim.alopex_sync_smoke_snapshot.restype = ctypes.c_int
    require(shim.alopex_sync_smoke_ready() == 1, 'sync interposer unavailable')
    return shim


def snapshot(shim):
    rows = (Record * LIMIT)()
    count, overflow = ctypes.c_size_t(), ctypes.c_size_t()
    require(shim.alopex_sync_smoke_snapshot(rows, LIMIT, ctypes.byref(count),
                                            ctypes.byref(overflow)) == 0 and
            count.value <= LIMIT and overflow.value == 0,
            'sync interposer record overflow')
    return [dict(operation=int(row.operation), result=int(row.result),
                 errno=int(row.error_number), duration_ns=int(row.duration_ns))
            for row in rows[:count.value]]


def controlled_check(shim, output):
    target = output / 'sync-controlled.tmp'
    try:
        with target.open('xb') as stream:
            stream.write(b'x')
            for call, operation in ((os.fsync, 1), (os.fdatasync, 2)):
                before = len(snapshot(shim))
                call(stream.fileno())
                rows = snapshot(shim)
                require(len(rows) == before + 1 and rows[-1]['operation'] == operation and
                        rows[-1]['result'] == 0 and
                        0 <= rows[-1]['duration_ns'] <= 30_000_000_000,
                        'controlled sync success unobserved')
        descriptor = os.open(target, os.O_RDONLY)
        os.close(descriptor)
        for call, operation in ((os.fsync, 1), (os.fdatasync, 2)):
            before = len(snapshot(shim))
            try:
                call(descriptor)
            except OSError as error:
                require(error.errno == errno.EBADF, 'controlled sync error changed')
            else:
                raise RuntimeError('controlled sync error missing')
            rows = snapshot(shim)
            require(len(rows) == before + 1 and rows[-1]['operation'] == operation and
                    rows[-1]['result'] == -1 and rows[-1]['errno'] == errno.EBADF and
                    0 <= rows[-1]['duration_ns'] <= 30_000_000_000,
                    'controlled sync failure unobserved')
    finally:
        target.unlink(missing_ok=True)


def commit_calls(before, after, subject):
    require(subject in EXPECTED_SQL2048 and len(after) >= len(before) and
            after[:len(before)] == before, 'sync record sequence changed')
    calls = after[len(before):]
    expected_fsync, expected_fdatasync = EXPECTED_SQL2048[subject]
    require(len(calls) == expected_fsync + expected_fdatasync and
            sum(row['operation'] == 1 for row in calls) == expected_fsync and
            sum(row['operation'] == 2 for row in calls) == expected_fdatasync and
            all(row['result'] == 0 and 0 <= row['duration_ns'] <= 30_000_000_000
                for row in calls), 'expected branch-local sync observation missing')
    return dict(schema='alopex.682-intercepted-sync/v1', coverage='unproven',
                scope='intercepted fsync/fdatasync calls only', calls=calls,
                acceptance_sample=False)
