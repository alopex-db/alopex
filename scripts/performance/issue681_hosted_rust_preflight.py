"""Check the exact existing Rust toolchain without installing or logging it."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import time


CASE = "HNSW_TOOLCHAIN"
VERSION = "1.90.0"
HOST = "x86_64-unknown-linux-gnu"
LIMIT = 4096


def require(value):
    if not value:
        raise ValueError("TOOLCHAIN")


def _binary(name, path):
    found = shutil.which(name, path=path)
    require(type(found) is str)
    selected = Path(found)
    require(selected.is_absolute() and selected.is_file())
    resolved = selected.resolve(strict=True)
    info = resolved.stat()
    require(stat.S_ISREG(info.st_mode) and
            info.st_uid in (0, os.geteuid()) and
            not (info.st_mode & 0o022))
    return selected


def _run(binary, environment, *args, allow_info_stderr=False):
    result = subprocess.run([str(binary), *args],
                            env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=10)
    if allow_info_stderr:
        stderr_ok = (len(result.stderr) <= LIMIT and
                     all(line.startswith("info: ")
                         for line in result.stderr.decode("ascii").splitlines()))
    else:
        stderr_ok = not result.stderr
    require(result.returncode == 0 and stderr_ok and
            0 < len(result.stdout) <= LIMIT)
    return result.stdout.decode("ascii")


def check():
    home = Path.home()
    require(home.is_absolute() and home.is_dir() and not home.is_symlink() and
            home.stat().st_uid == os.geteuid())
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    environment = {"HOME": str(home), "PATH": search, "LANG": "C",
                   "RUSTUP_AUTO_INSTALL": "0", "RUSTUP_TOOLCHAIN": VERSION}
    rustc = _binary("rustc", search)
    cargo = _binary("cargo", search)
    compiler = _run(rustc, environment, "-Vv").splitlines()
    require(compiler.count("release: " + VERSION) == 1 and
            compiler.count("host: " + HOST) == 1)
    package = _run(cargo, environment, "-V").strip()
    require(re.fullmatch(r"cargo " + re.escape(VERSION) +
                         r" \([0-9a-f]{9} [0-9]{4}-[0-9]{2}-[0-9]{2}\)",
                         package))


def check_admission():
    """Verify rustup/rustc/cargo binary health without requiring 1.90.0 preinstalled.

    Actual #681 build/measurement jobs install pinned 1.90.0 --profile minimal
    under the 40 GB storage watchdog and then enforce check().
    """
    home = Path.home()
    require(home.is_absolute() and home.is_dir() and not home.is_symlink() and
            home.stat().st_uid == os.geteuid())
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    environment = {"HOME": str(home), "PATH": search, "LANG": "C",
                   "RUSTUP_AUTO_INSTALL": "0"}
    rustup = _binary("rustup", search)
    rustc = _binary("rustc", search)
    cargo = _binary("cargo", search)
    manager = _run(rustup, environment, "-V", allow_info_stderr=True).strip()
    require(re.fullmatch(r"rustup [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)? \([^)]+\)",
                         manager))
    compiler = _run(rustc, environment, "-Vv").splitlines()
    require(any(re.fullmatch(r"release: [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?", line)
                for line in compiler) and
            compiler.count("host: " + HOST) == 1)
    package = _run(cargo, environment, "-V").strip()
    require(re.fullmatch(r"cargo [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)? \([^)]+\)",
                         package))


def _stop_process(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=20)
    else:
        process.wait()


def install_pinned_minimal(guard, log_path: Path, *, timeout: int = 300):
    """Install pinned Rust VERSION --profile minimal under storage guard, then verify check()."""
    require(callable(guard) and isinstance(log_path, Path) and
            log_path.is_absolute() and not log_path.exists() and
            not log_path.is_symlink() and type(timeout) is int and 1 <= timeout <= 600)
    check_admission()
    home = Path.home()
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    rustup = _binary("rustup", search)
    environment = {"HOME": str(home), "PATH": search, "LANG": "C",
                   "RUSTUP_AUTO_INSTALL": "0"}
    guard()
    fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o600)
        process = subprocess.Popen(
            [str(rustup), "toolchain", "install", VERSION,
             "--profile", "minimal", "--no-self-update"],
            env=environment, stdin=subprocess.DEVNULL,
            stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline)
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
                guard()
            require(process.returncode == 0)
            guard()
        except BaseException:
            _stop_process(process)
            raise
    check()


def run():
    try:
        check()
        return {"case": CASE, "outcome": "passed"}, 0
    except BaseException:
        return {"case": CASE, "outcome": "failed",
                "error_code": "TOOLCHAIN"}, 1


def main():
    outcome, status = run()
    print(json.dumps(outcome, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
