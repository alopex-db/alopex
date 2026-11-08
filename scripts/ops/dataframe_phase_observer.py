"""Process resource windows for the pinned manual DataFrame workload.

CPU uses RUSAGE_SELF (all process threads). Linux /proc/PID/io includes waited
children; child membership/usage changes are rejected. Physical byte counters
are retained separately, including cancelled writes; no derived net I/O claim.
See https://man7.org/linux/man-pages/man2/getrusage.2.html and
https://man7.org/linux/man-pages/man5/proc_pid_io.5.html .
"""
import math
import os
from pathlib import Path
import resource
import sys

from phase_rss import PhaseRss


def require(value, message):
    if not value:
        raise RuntimeError(message)


def parse_io(text):
    wanted = {'read_bytes', 'write_bytes', 'cancelled_write_bytes'}
    result = {}
    for line in text.splitlines():
        key, separator, value = line.partition(':')
        if key not in wanted:
            continue
        require(separator and key not in result and value.strip().isdigit(), 'invalid I/O counter')
        result[key] = int(value)
    require(set(result) == wanted, 'missing I/O counter')
    return result


def counters():
    proc = Path('/proc') / str(os.getpid())
    values = parse_io((proc/'io').read_text())
    children = set()
    for task in (proc/'task').iterdir():
        children.update((task/'children').read_text().split())
    require(not children, 'DataFrame operation has child processes')
    return dict(io=values, waited_children=list(resource.getrusage(resource.RUSAGE_CHILDREN)))


class DataFrameObserver:
    def __init__(self, *, rss_limit, rss=None, snapshot=counters, usage=resource.getrusage):
        require(type(rss_limit) is int and rss_limit > 0, 'invalid RSS bound')
        if rss is None:
            require(sys.platform == 'linux', 'Linux process observation required')
            rss = PhaseRss(os.getpid())
        self.rss, self.snapshot, self.usage = rss, snapshot, usage
        self.limit, self.active, self.completed = rss_limit, None, []
        self.pending = None

    def begin(self, index):
        require(self.active is None and type(index) is int and index == len(self.completed), 'observation sequence differs')
        phase = 'sample-' + str(index)
        self.pending = dict(index=index)
        self.rss.begin(phase)
        before = self.snapshot()
        cpu = self.usage(resource.RUSAGE_SELF)
        self.active = (index, before, cpu.ru_utime, cpu.ru_stime)
        self.pending['before'] = dict(before, user_cpu_seconds=cpu.ru_utime, system_cpu_seconds=cpu.ru_stime)

    def finish(self, index):
        require(self.active is not None and self.active[0] == index, 'observation end differs')
        cpu = self.usage(resource.RUSAGE_SELF)
        after = self.snapshot()
        rss = self.rss.finish('sample-' + str(index))
        _, before, user_before, system_before = self.active
        self.pending['after'] = dict(after, user_cpu_seconds=cpu.ru_utime, system_cpu_seconds=cpu.ru_stime)
        self.pending['rss'] = rss
        require(before['waited_children'] == after['waited_children'], 'child usage changed during DataFrame phase')
        delta = dict(user_cpu_seconds=cpu.ru_utime-user_before, system_cpu_seconds=cpu.ru_stime-system_before)
        delta.update({key:after['io'][key]-before['io'][key] for key in before['io']})
        require(all(type(v) in (int,float) and math.isfinite(v) and v >= 0 for v in delta.values()), 'resource counter decreased or invalid')
        require(rss['kernel_reported_peak_rss_bytes'] <= self.limit, 'phase RSS bound exceeded')
        rss = dict(rss, scope='current process from pre-timer begin to post-timer finish; includes counter-read overhead')
        result = dict(index=index, before=self.pending['before'], after=self.pending['after'], delta=delta, rss=rss,
            scope='timed DataFrame operation plus observation-boundary overhead; excludes subsequent raw output and untimed oracle',
            io='Linux physical byte counters; cancelled writes retained separately; no net or disk-throughput claim')
        self.completed.append(result)
        self.active, self.pending = None, None
        return result

    def close(self):
        self.rss.close()
