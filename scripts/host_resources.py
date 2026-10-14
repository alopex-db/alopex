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
        self.request_id = 0

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
        self.request_id += 1
        self.process.stdin.write(f'{self.request_id}\n'.encode('ascii'))
        self.process.stdin.flush()
        # No test starts until initialization yields an observation; runtime stays strict.
        timeout = 30 if starting else 5
        started = time.monotonic()
        deadline = started + timeout
        while True:
            while b'\n' not in self.pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self.selector.select(remaining):
                    raise OSError(f'No host observation received within {timeout} seconds')
                data = self.process.stdout.read(4096)
                if not data:
                    raise OSError('Windows host observer stopped')
                self.pending += data
                if len(self.pending) > 8192:
                    raise ValueError('invalid host observer output')
            line, self.pending = self.pending.split(b'\n', 1)
            sample = json.loads(line)
            if sample['request_id'] > self.request_id:
                raise ValueError('invalid host observation request')
            if sample['request_id'] != self.request_id:
                continue
            age = time.monotonic() - started
            if age > timeout:
                raise OSError(f'No host observation received within {timeout} seconds')
            break
        # A correlated response was sampled after this request, without comparing clocks.
        sample['observation_age_seconds'] = age
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
