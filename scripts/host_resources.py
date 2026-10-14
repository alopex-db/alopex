"""Observe the Windows host from WSL; never substitute guest RAM for host RAM."""
import base64
import json
from pathlib import Path
import selectors
import shutil
import subprocess
import time


class HostMonitor:
    def __init__(self):
        self.process = None
        self.selector = None
        self.pending = b''
        self.clock_anchor = None

    def _start(self):
        shell = shutil.which('powershell.exe')
        if shell is None:
            candidate = Path('/mnt/c/Windows') / 'System32/WindowsPowerShell/v1.0/powershell.exe'
            if candidate.is_file():
                shell = str(candidate)
        if shell is None:
            raise OSError('Windows host observation unavailable')
        script = Path(__file__).with_name('windows_host_observer.ps1').read_text()
        encoded = base64.b64encode(script.encode('utf-16-le')).decode('ascii')
        self.process = subprocess.Popen([shell, '-NoProfile', '-NonInteractive',
                                         '-EncodedCommand', encoded],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def read(self):
        starting = self.process is None
        if self.process is None:
            self._start()
        self.process.stdin.write(b'\n')
        self.process.stdin.flush()
        # No test starts until initialization yields an observation; runtime stays strict.
        timeout = 30 if starting else 5
        deadline = time.monotonic() + timeout
        while b'\n' not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise OSError(f'No host observation received within {timeout} seconds')
            data = self.process.stdout.read(4096)
            if not data:
                error = self.process.stderr.read(4096).decode('utf-8', errors='replace')
                raise OSError('Windows host observer stopped: ' + error)
            self.pending += data
            if len(self.pending) > 8192:
                raise ValueError('invalid host observer output')
        # Read the newest complete observation; queued older samples are not current load.
        while self.selector.select(0):
            data = self.process.stdout.read(4096)
            if not data:
                raise OSError('Windows host observer stopped')
            self.pending += data
            if len(self.pending) > 8192:
                raise ValueError('invalid host observer output')
        complete, self.pending = self.pending.rsplit(b'\n', 1)
        line = complete.split(b'\n')[-1]
        sample = json.loads(line)
        now = time.monotonic()
        if self.clock_anchor is None:
            self.clock_anchor = (now, sample['observed_elapsed_ms'])
        age = now - self.clock_anchor[0] - (sample['observed_elapsed_ms'] - self.clock_anchor[1]) / 1000
        if age > 5:
            raise OSError('Host observation is older than five seconds')
        if age < 0:
            # A guest pause must not classify a newly received host sample as stale.
            self.clock_anchor = (now, sample['observed_elapsed_ms'])
        sample['observation_age_seconds'] = max(0, age)
        memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        sample['wsl_available_bytes'] = int(memory['MemAvailable'].split()[0]) * 1024
        return sample

    def close(self):
        if self.process is None:
            return
        self.selector.close()
        # EOF tells the Windows process to exit, rather than leaving an interop worker.
        try:
            self.process.stdin.close()
            self.process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        finally:
            self.process.stdout.close()
            self.process.stderr.close()
