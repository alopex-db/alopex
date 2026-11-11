"""Fail-closed allocated-byte monitoring for explicitly inventoried host roots."""
from __future__ import annotations

import os
from pathlib import Path
import signal
import stat
import subprocess
import time

from common import Failure, record_cleanup_error, require

SUDO_BINARY = Path("/usr/bin/sudo")
DU_BINARY = Path("/usr/bin/du")


def allocated_bytes(roots, *, forbidden_root=None):
    """Return a complete traversal, retrying descendant disappearance at most twice.

    Every retry discards the partial total and revisits all inventory roots.
    Missing roots, permission/I/O errors and persistent churn remain failures.
    This is not an atomic filesystem snapshot.
    """
    roots = tuple(Path(root) for root in roots)
    for attempt in range(3):
        try:
            return _allocated_bytes_once(roots, forbidden_root=forbidden_root)
        except FileNotFoundError:
            if attempt == 2:
                raise
            for root in roots:
                require(root.is_absolute() and root.exists(), "MEASUREMENT")


def _allocated_bytes_once(roots, *, forbidden_root=None):
    seen, total = set(), 0
    for root in roots:
        root = Path(root)
        require(root.is_absolute() and root.exists(), "MEASUREMENT")
        pending = [root]
        while pending:
            path = pending.pop()
            if forbidden_root is not None:
                require(not path.resolve(strict=False).is_relative_to(forbidden_root), "MEASUREMENT")
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


def trusted_executable(path):
    """Pin a privileged command to an absolute, root-controlled path."""
    path = Path(path)
    require(path.is_absolute(), "MEASUREMENT")
    try:
        for parent in (Path("/"), *reversed(path.parents[:-1])):
            info = parent.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and
                    not info.st_mode & 0o022, "MEASUREMENT")
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
                not info.st_mode & 0o022 and info.st_mode & 0o111, "MEASUREMENT")
        return info.st_dev, info.st_ino, info.st_mode, info.st_uid
    except OSError as error:
        raise Failure("MEASUREMENT", str(error)) from error


def readonly_sudo_du_bytes(root, *, timeout):
    """Measure one daemon backing root with the host's existing read-only sudo."""
    root = Path(root)
    require(root.is_absolute() and root.is_dir() and not root.is_symlink(), "MEASUREMENT")
    root_info = root.stat()
    root_identity = root_info.st_dev, root_info.st_ino
    sudo_identity = trusted_executable(SUDO_BINARY)
    du_identity = trusted_executable(DU_BINARY)
    try:
        result = subprocess.run([str(SUDO_BINARY), "-n", str(DU_BINARY), "-sk", "--", str(root)],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                timeout=timeout, check=False)
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as error:
        raise Failure("MEASUREMENT", str(error)) from error
    require(result.returncode == 0 and not result.stderr and
            len(result.stdout.encode("utf-8")) <= 4096, "MEASUREMENT")
    fields = result.stdout.splitlines()
    require(len(fields) == 1, "MEASUREMENT")
    parts = fields[0].split("\t")
    require(len(parts) == 2 and parts[0].isascii() and parts[0].isdecimal() and
            parts[1] == str(root), "MEASUREMENT")
    current_root = root.stat()
    require((current_root.st_dev, current_root.st_ino) == root_identity and
            trusted_executable(SUDO_BINARY) == sudo_identity and
            trusted_executable(DU_BINARY) == du_identity, "MEASUREMENT")
    return int(parts[0]) * 1024


class Budget:
    def __init__(self, config, measure=allocated_bytes):
        self.config, self.measure = config, measure
        self.initial = None
        self.planned = None
        self.sudo_root = config.get("readonly_sudo_du_root")
        self.sudo_root_identity = None
        if self.sudo_root is not None:
            try:
                root = Path(self.sudo_root)
                require(root.is_absolute() and root.is_dir() and not root.is_symlink(), "MEASUREMENT")
                selected = root.resolve(strict=True)
                require(str(selected) == str(root), "MEASUREMENT")
                require(config["roots"].count(self.sudo_root) == 1, "MEASUREMENT")
                roots = [Path(item).resolve(strict=True) for item in config["roots"]]
                require(roots.count(selected) == 1 and
                        all(not (item.is_relative_to(selected) or selected.is_relative_to(item))
                            for item in roots if item != selected), "MEASUREMENT")
                info = root.stat()
                self.sudo_root_identity = (info.st_dev, info.st_ino)
            except (OSError, ValueError, TypeError) as error:
                raise Failure("MEASUREMENT", str(error)) from error

    def used(self):
        try:
            if self.sudo_root is None:
                value = self.measure(self.config["roots"])
            else:
                root = Path(self.sudo_root)
                info = root.stat()
                require((info.st_dev, info.st_ino) == self.sudo_root_identity and
                        not root.is_symlink(), "MEASUREMENT")
                direct_roots = [item for item in self.config["roots"] if item != self.sudo_root]
                if self.measure is allocated_bytes:
                    value = self.measure(direct_roots, forbidden_root=root)
                else:
                    value = self.measure(direct_roots)
                value += readonly_sudo_du_bytes(root, timeout=self.config["poll_seconds"])
                info = root.stat()
                require((info.st_dev, info.st_ino) == self.sudo_root_identity, "MEASUREMENT")
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
