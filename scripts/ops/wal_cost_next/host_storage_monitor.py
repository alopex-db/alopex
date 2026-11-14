"""Ten-second full-host samples and cancellation for one owned resolver run."""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "test-env"))
from common import Failure, require
from docker_backend import Docker

SAMPLE_SECONDS = 10


def kill_group(child):
    if child is not None and child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
            child.wait(timeout=2)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=2)


class HostMonitor:
    def __init__(self, profile_path, storage, baseline, ceiling):
        self.profile_path = Path(profile_path)
        self.storage = storage
        self.baseline = baseline
        self.ceiling = ceiling
        self.stage_baseline = None
        self.stage_limit = None
        self.failure = None
        self.active = None
        self.cleanup_mode = False
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.sample_lock = threading.Lock()
        self.thread = None
        self.next_due = None

    def sample(self):
        with self.sample_lock:
            return self._sample_locked()

    def _sample_locked(self):
        command = [sys.executable, "-B", str(Path(__file__).resolve().with_name("sample_host_storage.py")),
                   "--profile", str(self.profile_path)]
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = child.communicate(timeout=SAMPLE_SECONDS)
        except subprocess.TimeoutExpired as error:
            kill_group(child)
            raise Failure("MEASUREMENT", "full-host sample timeout") from error
        value = stdout.strip().decode("ascii", errors="strict") if len(stdout) <= 32 else ""
        require(child.returncode == 0 and not stderr and value.isascii() and
                value.isdecimal(), "MEASUREMENT")
        used = int(value)
        require(used >= 0 and used + self.storage["reserve_bytes"] <
                self.storage["stop_bytes"] and used - self.baseline <= self.ceiling,
                "CAPACITY")
        with self.lock:
            stage = self.stage_baseline
            limit = self.stage_limit
        if stage is not None:
            require(used - stage <= limit, "CAPACITY")
        return used

    def start(self):
        self.next_due = time.monotonic() + SAMPLE_SECONDS
        self.sample()  # A slow/incomplete inventory stops before Docker pull.
        self.thread = threading.Thread(target=self._loop, name="signed-index-host-monitor",
                                       daemon=True)
        self.thread.start()

    def _loop(self):
        due = self.next_due
        while not self.stopping.is_set():
            if self.stopping.wait(max(0, due - time.monotonic())):
                return
            try:
                self.sample()
            except BaseException as error:
                with self.lock:
                    first = self.failure is None
                    if first:
                        self.failure = error
                    child = self.active
                if first and not self.cleanup_mode:
                    kill_group(child)
            due += SAMPLE_SECONDS

    def check(self):
        with self.lock:
            error = self.failure
        if error is not None and not self.cleanup_mode:
            raise error

    def stage(self, baseline, limit):
        with self.lock:
            self.stage_baseline = baseline
            self.stage_limit = limit
        self.check()

    def run(self, command, *, timeout, log=None, capture=False, allow_nonzero=False):
        self.check()
        output = subprocess.PIPE if capture else open(log, "ab")
        if not capture:
            os.chmod(log, 0o600)
        try:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                     stdout=output, stderr=subprocess.PIPE if capture else output,
                                     start_new_session=True)
            with self.lock:
                self.active = child
            deadline = time.monotonic() + timeout
            try:
                if capture:
                    while True:
                        self.check()
                        remaining = deadline - time.monotonic()
                        require(remaining > 0, "TIMEOUT")
                        try:
                            stdout, stderr = child.communicate(timeout=min(remaining, 0.25))
                            break
                        except subprocess.TimeoutExpired:
                            continue
                    require(len(stdout) <= 8_000_000 and len(stderr) <= 1_000_000,
                            "COMMAND")
                    self.check()
                    if allow_nonzero:
                        return child.returncode, stdout.decode(), stderr.decode()
                    require(child.returncode == 0 and not stderr, "COMMAND")
                    return stdout.decode().strip()
                while child.poll() is None:
                    self.check()
                    remaining = deadline - time.monotonic()
                    require(remaining > 0, "TIMEOUT")
                    try:
                        child.wait(timeout=min(remaining, 0.25))
                    except subprocess.TimeoutExpired:
                        pass
                self.check()
                if allow_nonzero:
                    return child.returncode
                require(child.returncode == 0, "COMMAND")
            except BaseException:
                kill_group(child)
                raise
            finally:
                with self.lock:
                    if self.active is child:
                        self.active = None
        finally:
            if not capture:
                output.close()

    def stop(self):
        if not self.cleanup_mode:
            self.sample()
        self.stopping.set()
        if self.thread is not None:
            self.thread.join(timeout=SAMPLE_SECONDS + 2)
            require(not self.thread.is_alive(), "MEASUREMENT")
        self.check()


class MonitoredDocker(Docker):
    def __init__(self, profile, monitor):
        super().__init__(profile)
        self.monitor = monitor

    def call(self, *args, missing=False):
        code, stdout, stderr = self.monitor.run([*self.prefix, *args], timeout=30,
                                                capture=True, allow_nonzero=True)
        if code:
            if missing and ("No such container:" in stderr or
                            "no such volume" in stderr.lower() or
                            (args[:2] == ("image", "inspect") and
                             "No such image:" in stderr)):
                return None
            raise Failure("PREREQUISITE", stderr[:4096])
        return stdout.strip()
