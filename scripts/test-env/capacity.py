"""Fail-closed allocated-byte monitoring for explicitly inventoried host roots."""
from __future__ import annotations

import os
from pathlib import Path
import signal
import stat
import subprocess
import time

from common import Failure, record_cleanup_error, require


def allocated_bytes(roots):
    """Return a complete traversal, retrying descendant disappearance at most twice.

    Every retry discards the partial total and revisits all inventory roots.
    Missing roots, permission/I/O errors and persistent churn remain failures.
    This is not an atomic filesystem snapshot.
    """
    roots = tuple(Path(root) for root in roots)
    for attempt in range(3):
        try:
            return _allocated_bytes_once(roots)
        except FileNotFoundError:
            if attempt == 2:
                raise
            for root in roots:
                require(root.is_absolute() and root.exists(), "MEASUREMENT")


def _allocated_bytes_once(roots):
    seen, total = set(), 0
    for root in roots:
        root = Path(root)
        require(root.is_absolute() and root.exists(), "MEASUREMENT")
        pending = [root]
        while pending:
            path = pending.pop()
            info = path.lstat()  # Disappearance is an incomplete snapshot, not zero.
            identity = info.st_dev, info.st_ino
            if identity in seen:
                continue
            seen.add(identity)
            require(hasattr(info, "st_blocks"), "MEASUREMENT")
            total += info.st_blocks * 512
            if stat.S_ISLNK(info.st_mode):
                # The link's own allocation was counted above. A dangling
                # target occupies no storage; permission errors still fail.
                target = path.resolve(strict=False)
                try:
                    target.lstat()
                except FileNotFoundError:
                    continue
                pending.append(target)
            elif stat.S_ISDIR(info.st_mode):
                pending.extend(path.iterdir())
    return total


class Budget:
    def __init__(self, config, measure=allocated_bytes):
        self.config, self.measure = config, measure
        self.initial = None
        self.planned = None

    def used(self):
        try:
            value = self.measure(self.config["roots"])
            require(type(value) is int and value >= 0, "MEASUREMENT")
            return value
        except (OSError, ValueError) as error:
            raise Failure("MEASUREMENT", str(error)) from error

    def before(self, operation):
        value = self.used()
        planned = self.config["planned_bytes"][operation]
        require(value + planned + self.config["reserve_bytes"] < self.config["stop_bytes"], "CAPACITY")
        self.initial, self.planned = value, planned
        return value

    def check(self):
        require(self.initial is not None, "STATE")
        value = self.used()
        require(value + self.config["reserve_bytes"] < self.config["stop_bytes"], "CAPACITY")
        require(value - self.initial <= self.planned, "CAPACITY")
        return value

    def run(self, command, *, timeout, log, env=None, cancel=None):
        """Call before() first. cancel stops a daemon-owned operation as well."""
        self.check()
        with Path(log).open("ab") as output:
            os.chmod(log, 0o600)
            child = subprocess.Popen(command, stdout=output, stderr=output,
                                     env=env, start_new_session=True)
            deadline = time.monotonic() + timeout
            try:
                while child.poll() is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise Failure("TIMEOUT")
                    try:
                        child.wait(timeout=min(remaining, self.config["poll_seconds"]))
                    except subprocess.TimeoutExpired:
                        self.check()
                self.check()
                require(child.returncode == 0, "COMMAND")
            except BaseException as primary:
                try:
                    if cancel is not None:
                        cancel()
                except BaseException as error:
                    record_cleanup_error(primary, "cancel_owned_operation", error)
                try:
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGTERM)
                        try:
                            child.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait()
                except BaseException as error:
                    record_cleanup_error(primary, "reap_command", error)
                raise
