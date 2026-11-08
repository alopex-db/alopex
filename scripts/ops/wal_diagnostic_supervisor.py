"""Bounded own-child tracing with a handshake before the product workload."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from phase_rss import PhaseRss, PhaseRssError, start_ticks
from wal_commit_diagnostic import require, trace_command


def ready(directory):
    pid = os.getpid()
    temporary = directory / 'tracee-ready.tmp'
    temporary.write_text(json.dumps(dict(pid=pid,
                start_ticks=start_ticks(Path('/proc/self/stat').read_text())))+'\n')
    temporary.replace(directory / 'tracee-ready.json')
    deadline = time.monotonic() + 10
    while not (directory / 'tracee-go').exists():
        require(time.monotonic() < deadline, 'tracee handshake timed out')
        time.sleep(.01)


def verify_parent(raw, parent):
    _, closing, tail = raw.rpartition(')')
    fields = tail.split()
    require(closing and len(fields) >= 20 and fields[1] == str(parent),
            'tracee is not the tracer child')


def run(command, env, directory, budget, timeout, rss_limit):
    from common import atomic_json
    budget.check()
    started = time.monotonic()
    trace = directory / 'commit.trace'
    monitor = None
    identity = None
    samples, peak = 0, 0
    failure = None
    reaped = False
    with (directory / 'worker.log').open('xb') as log:
        tracer = subprocess.Popen(trace_command(command, trace), env=env, stdout=log,
                                  stderr=log, start_new_session=True)
        try:
            while tracer.poll() is None:
                require(time.monotonic()-started < timeout, 'diagnostic deadline exceeded')
                budget.check()
                require(not trace.exists() or trace.stat().st_size <= 1_048_576,
                        'trace size bound exceeded')
                handshake = directory / 'tracee-ready.json'
                if monitor is None and handshake.exists():
                    identity = json.loads(handshake.read_bytes())
                    require(type(identity['pid']) is int and identity['pid'] > 0,
                            'invalid tracee PID')
                    monitor = PhaseRss(identity['pid'])
                    require(monitor.identity == identity['start_ticks'], 'tracee identity changed')
                    verify_parent(Path(f"/proc/{identity['pid']}/stat").read_text(), tracer.pid)
                    value = monitor.snapshot()['rss_bytes']
                    require(value <= rss_limit, 'tracee RSS bound exceeded')
                    samples += 1
                    peak = max(peak, value)
                    # The child is still waiting, guaranteeing a live identity-
                    # bound observation before imports and the tiny workload.
                    (directory / 'tracee-go').touch(exist_ok=False)
                elif monitor is not None:
                    try:
                        value = monitor.snapshot()['rss_bytes']
                    except PhaseRssError:
                        try:
                            status = Path(f"/proc/{identity['pid']}/status").read_text()
                        except FileNotFoundError:
                            status = None
                        require(status is None or ('VmRSS:' not in status and 'VmHWM:' not in status),
                                'invalid live tracee counters')
                        # A disappearing process is accepted only with the tracer
                        # actually reaped, never merely after a polling timeout.
                        tracer.wait(timeout=.25)
                    else:
                        require(value <= rss_limit, 'tracee RSS bound exceeded')
                        samples += 1
                        peak = max(peak, value)
                try:
                    tracer.wait(timeout=.05)
                except subprocess.TimeoutExpired:
                    pass
            require(tracer.returncode == 0, 'traced worker failed')
            require(identity is not None and samples > 0, 'tracee observation missing')
            require(trace.stat().st_size <= 1_048_576, 'trace size bound exceeded')
        except BaseException as error:
            failure = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            if tracer.poll() is None:
                try:
                    os.killpg(tracer.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    tracer.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(tracer.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    tracer.wait(timeout=5)
            if monitor is not None:
                monitor.close()
            reaped = tracer.poll() is not None
            disappeared = None
            if identity is not None:
                path = Path(f"/proc/{identity['pid']}/stat")
                try:
                    disappeared = start_ticks(path.read_text()) != identity['start_ticks']
                except FileNotFoundError:
                    disappeared = True
            cleanup_complete = reaped and (identity is None or disappeared)
            atomic_json(directory / 'supervision.json', dict(returncode=tracer.returncode,
                tracer_reaped=reaped, tracee_identity=identity, tracee_disappeared=disappeared,
                elapsed_seconds=time.monotonic()-started, live_samples=samples,
                sampled_peak_rss_bytes=peak, failure=failure, cleanup_complete=cleanup_complete,
                scope='identity-bound Python tracee RSS; tracer is not the measured process'))
            if failure is None:
                require(cleanup_complete, 'tracee cleanup incomplete')


def verify_lifecycle(text, identity):
    import re
    pids = set()
    exits = {}
    for line in text.splitlines():
        match = re.fullmatch(r'(\d+)\s+\d+\.\d+\s+(.+)', line)
        require(match, 'trace lifecycle format differs')
        pid, event = int(match[1]), match[2]
        require(pid not in exits, 'event after process exit')
        pids.add(pid)
        end = re.fullmatch(r'\+\+\+ exited with (\d+) \+\+\+', event)
        if end:
            require(end[1] == '0', 'nonzero tracee exit')
            exits[pid] = 0
    require(identity['pid'] in pids and pids == set(exits), 'tracee exit record missing')
    return dict(processes=len(pids), all_exit_zero=True, main_pid=identity['pid'])
