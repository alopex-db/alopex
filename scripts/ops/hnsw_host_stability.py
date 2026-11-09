"""Best-effort host counters sampled outside the HNSW query timer.

Missing optional counters are explicit. These samples diagnose trial instability;
they are not an alternative performance acceptance metric.
"""

import resource
import time
from pathlib import Path


def _text(path):
    try:
        return Path(path).read_text()
    except (OSError, UnicodeError):
        return None


def _integers(text, prefix):
    if text is None:
        return None
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0] == prefix:
            try:
                return [int(value) for value in parts[1:]]
            except ValueError:
                return None
    return None


def _status_switches():
    status = _text('/proc/self/status')
    if status is None:
        return None
    values = {}
    for line in status.splitlines():
        for label in ('voluntary_ctxt_switches', 'nonvoluntary_ctxt_switches'):
            if line.startswith(label + ':'):
                try:
                    values[label] = int(line.split(':', 1)[1].strip())
                except ValueError:
                    return None
    return values if len(values) == 2 else None


def _cpu_stat():
    content = _text('/sys/fs/cgroup/cpu.stat')
    if content is None:
        return None
    result = {}
    for line in content.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                result[parts[0]] = int(parts[1])
            except ValueError:
                return None
    return result or None


def _frequency_residency(cpu):
    content = _text(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq/stats/time_in_state')
    if content is None:
        return None
    result = {}
    for line in content.splitlines():
        parts = line.split()
        if len(parts) != 2:
            return None
        try:
            frequency, duration = map(int, parts)
        except ValueError:
            return None
        result[str(frequency)] = duration
    return result or None


def snapshot(cpu):
    assert isinstance(cpu, int) and cpu >= 0
    usage = resource.getrusage(resource.RUSAGE_SELF)
    frequency = _text(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_cur_freq')
    try:
        frequency_khz = int(frequency.strip()) if frequency is not None else None
    except ValueError:
        frequency_khz = None
    return {
        'monotonic_ns': time.monotonic_ns(),
        'pinned_cpu': cpu,
        'cpu_ticks': _integers(_text('/proc/stat'), f'cpu{cpu}'),
        'cpu_stat': _cpu_stat(),
        'frequency_khz': frequency_khz,
        'frequency_residency': _frequency_residency(cpu),
        'context_switches': _status_switches(),
        'self_user_cpu_seconds': usage.ru_utime,
        'self_system_cpu_seconds': usage.ru_stime,
    }
