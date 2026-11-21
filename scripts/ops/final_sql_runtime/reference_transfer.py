"""Source-pinned, fail-closed transfer of the three locked originals to one runner.

The caller supplies independently reviewed assignment and SSH inputs. No VM or
runner is created here. The remote receiver owns the private workspace files.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import shlex
import stat
import subprocess
import tempfile
import time

from . import reference_handoff, reference_staging


CHUNK = 1024 * 1024
MAX_SECONDS = 900
# The receiver is fixed code. Arguments are shell-quoted because ssh invokes a
# remote shell even when the local SSH process is started with an argv vector.
RECEIVER = r'''
import hashlib, json, os, pathlib, re, shutil, stat, sys

def fail(code):
    raise ValueError(code)

def exact_read(n):
    data = sys.stdin.buffer.read(n)
    if len(data) != n: fail('TRANSFER_SHORT')
    return data

def owned_dir(path):
    s = os.lstat(path)
    if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid(): fail('TRANSFER_REMOTE_PATH')

root = pathlib.Path(sys.argv[1])
repo = sys.argv[2]
if not root.is_absolute() or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', repo) or repo in ('.', '..'):
    fail('TRANSFER_REMOTE_INPUT')
# Check every ancestor without following symlinks into another workspace.
for path in reversed((root, *root.parents)):
    s = os.lstat(path)
    if not stat.S_ISDIR(s.st_mode): fail('TRANSFER_REMOTE_PATH')
for path in (root, root / '_work', root / '_work' / repo, root / '_work' / repo / repo):
    owned_dir(path)
workspace = root / '_work' / repo / repo
stage = workspace / 'reference-originals'
marker = workspace / 'reference-originals.ready'
if os.path.lexists(stage) or os.path.lexists(marker): fail('TRANSFER_REMOTE_EXISTS')
header = sys.stdin.buffer.readline(4096)
if len(header) >= 4096 or not header.endswith(b'\n'): fail('TRANSFER_HEADER')
pins = json.loads(header)
if not isinstance(pins, list) or len(pins) != 3: fail('TRANSFER_HEADER')
import tempfile
names = [p['basename'] for p in pins]
if len(set(names)) != 3 or any(not re.fullmatch(r'[a-z][a-z0-9-]{0,95}\.zip', n) for n in names):
    fail('TRANSFER_HEADER')
tmp = pathlib.Path(tempfile.mkdtemp(prefix='.reference-originals-', dir=workspace))
staged = False
pending = workspace / '.reference-originals.ready.pending'
pending_created = False
try:
    os.chmod(tmp, 0o700)
    for pin in pins:
        size, digest = pin['zip_size_bytes'], pin['zip_sha256']
        if type(size) is not int or not 0 < size <= 1000000000 or not re.fullmatch(r'[0-9a-f]{64}', digest):
            fail('TRANSFER_HEADER')
        fd = os.open(tmp / pin['basename'], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        h = hashlib.sha256()
        try:
            remaining = size
            while remaining:
                block = exact_read(min(1048576, remaining))
                view = memoryview(block)
                while view:
                    view = view[os.write(fd, view):]
                h.update(block)
                remaining -= len(block)
            os.fsync(fd)
        finally:
            os.close(fd)
        if h.hexdigest() != digest: fail('TRANSFER_HASH')
    if exact_read(1) != b'!': fail('TRANSFER_COMMIT')
    if sys.stdin.buffer.read(1): fail('TRANSFER_TRAILING')
    if set(os.listdir(tmp)) != set(names): fail('TRANSFER_INVENTORY')
    for pin in pins:
        s = os.lstat(tmp / pin['basename'])
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o400 or s.st_size != pin['zip_size_bytes']:
            fail('TRANSFER_REMOTE_PRIVACY')
    if os.path.lexists(stage) or os.path.lexists(marker): fail('TRANSFER_REMOTE_EXISTS')
    fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
    pending_created = True
    try: os.fsync(fd)
    finally: os.close(fd)
    os.rename(tmp, stage)
    tmp = None
    staged = True
    os.rename(pending, marker)
    pending_created = False
    staged = False
    print('TRANSFER_COMPLETE')
finally:
    if tmp is not None: shutil.rmtree(tmp)
    if staged: shutil.rmtree(stage)
    if pending_created: os.unlink(pending)
'''


def _require(ok: bool, code: str) -> None:
    if not ok:
        raise ValueError(code)


def _private_file(path: Path, code: str) -> None:
    _require(isinstance(path, Path) and path.is_absolute(), code)
    s = path.lstat()
    _require(stat.S_ISREG(s.st_mode) and s.st_uid == os.getuid() and
             stat.S_IMODE(s.st_mode) in (0o400, 0o600), code)


def _send(fd: int, data: bytes, deadline: float) -> None:
    offset = 0
    while offset < len(data):
        remaining = deadline - time.monotonic()
        _require(remaining > 0, 'TRANSFER_TIMEOUT')
        _require(bool(select.select([], [fd], [], remaining)[1]), 'TRANSFER_TIMEOUT')
        offset += os.write(fd, data[offset:])


def transfer(*, repository: str, expected: dict, token: str,
             source: Path, ssh_host: str, ssh_port: int, ssh_user: str,
             known_hosts: Path, private_key: Path, runner_root: str,
             runner_repo: str, timeout_seconds: int = 900,
             fetch=reference_handoff.fetch_and_verify,
             popen=subprocess.Popen) -> dict:
    """Verify, then stream exact bytes to the assigned ephemeral runner.

    ``known_hosts`` must pin the selected host and port. The caller reviews
    the runner root and repository against the JIT runner configuration.
    Only fixed status codes are returned; credentials and paths are not logged.
    """
    reference_handoff._review_expected(expected)
    _require(type(token) is str and token and '\n' not in token and '\r' not in token,
             'TRANSFER_TOKEN')
    _require(type(timeout_seconds) is int and 0 < timeout_seconds <= MAX_SECONDS,
             'TRANSFER_TIMEOUT_INPUT')
    _require(type(ssh_host) is str, 'TRANSFER_SSH_INPUT')
    try:
        ipaddress.ip_address(ssh_host)
    except ValueError:
        raise ValueError('TRANSFER_SSH_INPUT') from None
    _require(type(ssh_port) is int and 0 < ssh_port <= 65535 and
             type(ssh_user) is str and re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', ssh_user) and
             type(runner_root) is str and runner_root.startswith('/') and
             all(part not in ('.', '..') for part in runner_root.split('/')) and
             '\n' not in runner_root and '\r' not in runner_root and
             type(runner_repo) is str and re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', runner_repo) and
             runner_repo not in ('.', '..'), 'TRANSFER_SSH_INPUT')
    _private_file(known_hosts, 'TRANSFER_HOST_KEY')
    _private_file(private_key, 'TRANSFER_PRIVATE_KEY')
    lock = json.loads(reference_staging.LOCK.read_text(encoding='utf-8'))
    pins = reference_staging._pins(lock)
    _require(isinstance(source, Path) and source.is_absolute(), 'TRANSFER_SOURCE')
    # Local staging validation establishes exact inventory, privacy and hashes.
    reference_staging.verify_staging(source, run={
        'run_id': expected['run_id'], 'run_attempt': 1,
        'event': 'workflow_dispatch', 'head_sha': expected['head_sha'],
        'repository_id': expected['repository_id'], 'workflow_id': expected['workflow_id']},
        expected={key: expected[key] for key in ('head_sha', 'repository_id', 'workflow_id')},
        lock=lock)
    entries = [pins[key] for key in sorted(pins)]
    directory = os.open(source, os.O_RDONLY | os.O_DIRECTORY | getattr(os, 'O_NOFOLLOW', 0))
    opened = []
    try:
        for pin in entries:
            fd = os.open(pin['basename'], os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0), dir_fd=directory)
            s = os.fstat(fd)
            _require(stat.S_ISREG(s.st_mode) and s.st_uid == os.getuid() and
                     stat.S_IMODE(s.st_mode) == 0o400 and s.st_size == pin['zip_size_bytes'],
                     'TRANSFER_SOURCE_CHANGED')
            opened.append((fd, s))
        _require(set(os.listdir(directory)) == {p['basename'] for p in entries},
                 'TRANSFER_SOURCE_CHANGED')
        # Authenticated assignment is fetched immediately before any bytes leave.
        assignment = fetch(repository=repository, expected=expected, token=token,
                           timeout_seconds=min(30, timeout_seconds))
        _require(type(assignment) is dict and assignment.get('run_id') == expected['run_id'] and
                 assignment.get('runner_id') == expected['runner_id'] and
                 assignment.get('head_sha') == expected['head_sha'] and
                 type(assignment.get('job_id')) is int and assignment['job_id'] > 0,
                 'TRANSFER_ASSIGNMENT')
        remote = 'python3 -c ' + shlex.quote(RECEIVER) + ' ' + shlex.quote(runner_root) + ' ' + shlex.quote(runner_repo)
        argv = ['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                '-o', 'UserKnownHostsFile=' + str(known_hosts), '-o', 'GlobalKnownHostsFile=/dev/null',
                '-o', 'IdentitiesOnly=yes', '-o', 'IdentityAgent=none',
                '-o', 'ClearAllForwardings=yes', '-o', 'PermitLocalCommand=no',
                '-o', 'ProxyCommand=none', '-o', 'ProxyJump=none', '-o', 'LogLevel=ERROR',
                '-T', '-i', str(private_key), '-p', str(ssh_port),
                ssh_user + '@' + ssh_host, remote]
        deadline = time.monotonic() + timeout_seconds
        with tempfile.TemporaryFile() as output:
            process = popen(argv, stdin=subprocess.PIPE, stdout=output,
                            stderr=subprocess.DEVNULL)
            try:
                _require(process.stdin is not None, 'TRANSFER_IO')
                pipe = process.stdin.fileno()
                os.set_blocking(pipe, False)
                _send(pipe, json.dumps(entries, separators=(',', ':')).encode() + b'\n', deadline)
                for pin, (fd, before) in zip(entries, opened):
                    digest = hashlib.sha256()
                    remaining = pin['zip_size_bytes']
                    while remaining:
                        block = os.read(fd, min(CHUNK, remaining))
                        _require(bool(block), 'TRANSFER_SOURCE_CHANGED')
                        digest.update(block)
                        _send(pipe, block, deadline)
                        remaining -= len(block)
                    after = os.fstat(fd)
                    entry = os.stat(pin['basename'], dir_fd=directory, follow_symlinks=False)
                    _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                              before.st_ctime_ns) ==
                             (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                              after.st_ctime_ns) and
                             (entry.st_dev, entry.st_ino) == (before.st_dev, before.st_ino) and
                             digest.hexdigest() == pin['zip_sha256'], 'TRANSFER_SOURCE_CHANGED')
                _require(set(os.listdir(directory)) == {p['basename'] for p in entries},
                         'TRANSFER_SOURCE_CHANGED')
                _send(pipe, b'!', deadline)
                process.stdin.close()
                _require(process.wait(timeout=max(0.1, deadline - time.monotonic())) == 0,
                         'TRANSFER_REMOTE')
                output.seek(0)
                _require(output.read(64) == b'TRANSFER_COMPLETE\n' and not output.read(1),
                         'TRANSFER_REMOTE')
            except (OSError, subprocess.TimeoutExpired, ValueError):
                process.kill()
                process.wait()
                raise
        return {'status': 'transferred', 'run_id': assignment['run_id'],
                'job_id': assignment['job_id'], 'runner_id': assignment['runner_id'],
                'head_sha': assignment['head_sha']}
    finally:
        for fd, _ in opened:
            os.close(fd)
        os.close(directory)
