"""Conservative host storage estimates from fixed-cost filesystem counters."""
from pathlib import Path
import shutil


def required_filesystem_paths(root):
    """Include native Windows volumes exposed to a WSL-hosted runner."""
    return [Path(root).resolve(), *(
        path for letter in 'abcdefghijklmnopqrstuvwxyz'
        if (path := Path('/mnt') / letter).is_mount())]


class GeneratedOutputBudget:
    """Monitor declared host filesystems without walking source or build trees.

    Positive growth is deliberately summed: overlapping backing volumes can
    overestimate consumption, but a shrink on one volume cannot hide growth on
    another. The baseline inventory belongs to the producer, not this monitor.
    """

    def __init__(self, root, config):
        self.config = config
        if not 0 < config['operating_limit_bytes'] <= 40_000_000_000:
            raise ValueError('host operating limit must not exceed 40 GB')
        if not 0 < config['planned_growth_bytes'] <= 8_000_000_000:
            raise ValueError('invalid planned output growth')
        if not 0 <= config['project_baseline_bytes'] < (
                config['operating_limit_bytes'] - config['planned_growth_bytes']):
            raise ValueError('insufficient host operating headroom')
        filesystems = config.get('filesystems')
        if not isinstance(filesystems, list) or not filesystems:
            raise ValueError('explicit host filesystem baselines are required')
        self.baselines = {}
        for entry in filesystems:
            path = Path(entry['path']).resolve()
            used = entry['used_at_baseline']
            if path in self.baselines or not isinstance(used, int) or used < 0:
                raise ValueError('invalid or duplicate filesystem baseline')
            self.baselines[path] = used
        if not set(required_filesystem_paths(root)).issubset(self.baselines):
            raise ValueError('host filesystem coverage is incomplete')
        self.starting_used = {path: shutil.disk_usage(path).used for path in self.baselines}

    def reason(self):
        usage = {path: shutil.disk_usage(path) for path in self.baselines}
        growth = sum(max(0, entry.used - self.starting_used[path])
                     for path, entry in usage.items())
        estimate = self.config['project_baseline_bytes'] + sum(
            max(0, entry.used - self.baselines[path]) for path, entry in usage.items())
        self.latest = {'project_estimate_bytes': estimate,
                       'filesystem_free_bytes': min(entry.free for entry in usage.values()),
                       'filesystem_growth_bytes': growth,
                       'filesystems': [dict(path=str(path), used_bytes=entry.used,
                                            free_bytes=entry.free)
                                       for path, entry in usage.items()]}
        if self.latest['filesystem_free_bytes'] < 16 * 1024**3:
            return 'filesystem free-space reserve'
        if estimate >= self.config['operating_limit_bytes']:
            return 'project output operating reserve'
        if growth > self.config['planned_growth_bytes']:
            return 'planned generated-output reserve'
        return None
