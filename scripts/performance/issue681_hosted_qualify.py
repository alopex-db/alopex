"""Qualify one hosted #681 PR job without exposing local runner details."""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys

import issue681_public_admission as admission

CASE = "HNSW_HOSTED_QUAL"
STOP_BYTES = 40_000_000_000
CEILING_BYTES = 50_000_000_000
BUILD_BYTES = 8_000_000_000
RESERVE_BYTES = 2_000_000_000
REPO = Path(__file__).resolve().parents[2]


def require(value):
    if not value:
        raise ValueError("QUALIFICATION")


def command(args, timeout=30):
    result = subprocess.run(args, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=timeout)
    require(result.returncode == 0 and not result.stderr and
            len(result.stdout) <= 4096)
    return result.stdout.strip()


def context(env, *, checkout=REPO):
    require(env.get("GITHUB_EVENT_NAME") == "pull_request" and
            env.get("GITHUB_REPOSITORY") == "alopex-db/alopex" and
            re.fullmatch(r"[0-9a-f]{40}",
                         env.get("EXPECTED_HEAD_SHA", "")) is not None and
            all(env.get(key, "").isascii() and
                env.get(key, "").isdecimal() and int(env[key]) > 0
                for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")))
    event_path = Path(env["GITHUB_EVENT_PATH"])
    require(event_path.is_absolute() and event_path.is_file() and
            not event_path.is_symlink() and event_path.stat().st_size <= 262_144)
    event = json.loads(event_path.read_bytes())
    pr = event["pull_request"]
    require(pr["draft"] is True and
            pr["head"]["sha"] == env["EXPECTED_HEAD_SHA"] and
            pr["head"]["repo"]["full_name"] == env["GITHUB_REPOSITORY"] and
            pr["base"]["ref"] == "integration/v0.8.17-candidate" and
            event["repository"]["full_name"] == env["GITHUB_REPOSITORY"] and
            env.get("GITHUB_REF") == f"refs/pull/{pr['number']}/merge" and
            checkout.is_dir() and not checkout.is_symlink() and
            command(["git", "-C", str(checkout), "rev-parse", "HEAD"]) ==
            env["EXPECTED_HEAD_SHA"] and
            command(["git", "-C", str(checkout), "status", "--porcelain",
                     "--untracked-files=all"]) == "")


def platform_security():
    require(sys.platform == "linux" and platform.machine() == "x86_64" and
            os.geteuid() != 0)
    status = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    require(status.get("CapEff") == "0000000000000000" and
            status.get("Seccomp") == "2" and
            status.get("NoNewPrivs") in ("0", "1"))
    mounts = [line.split() for line in
              Path("/proc/self/mountinfo").read_text().splitlines()
              if " - cgroup2 " in line and
              len(line.split()) > 5 and line.split()[4] == "/sys/fs/cgroup"]
    require(len(mounts) == 1 and mounts[0][3] == "/")
    selected = tuple(sorted(os.sched_getaffinity(0))[:4])
    require(len(selected) == 4)
    admission.current_allocation(selected)
    admission.child_counter_probe()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            require(not (proc.stat().st_uid == os.geteuid() and
                         (proc / "comm").read_text().strip() in
                         ("cargo", "rustc", "sccache")))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue


def mounted_allocation(raw=None, *, stat_fn=os.stat,
                       statvfs_fn=os.statvfs):
    """Upper-bound Alopex storage with all visible filesystem allocation."""
    if raw is None:
        raw = Path("/proc/self/mountinfo").read_text()
    lines = raw.splitlines()
    require(1 <= len(lines) <= 4096)
    devices = {}
    saw_root = False
    for line in lines:
        fields = line.split(" - ", 1)[0].split()
        require(len(fields) >= 6)
        mount = re.sub(r"\\([0-7]{3})",
                       lambda match: chr(int(match.group(1), 8)), fields[4])
        path = Path(mount)
        require(path.is_absolute() and path.exists() and
                not path.is_symlink())
        saw_root |= path == Path("/")
        device = stat_fn(path).st_dev
        vfs = statvfs_fn(path)
        require(all(type(value) is int for value in
                    (device, vfs.f_blocks, vfs.f_bfree, vfs.f_bavail,
                     vfs.f_frsize)) and
                vfs.f_frsize > 0 and
                0 <= vfs.f_bavail <= vfs.f_bfree <= vfs.f_blocks)
        allocation = (vfs.f_blocks - vfs.f_bfree) * vfs.f_frsize
        available = vfs.f_bavail * vfs.f_frsize
        if device in devices:
            previous = devices[device]
            devices[device] = (max(previous[0], allocation),
                               min(previous[1], available))
        else:
            devices[device] = (allocation, available)
    require(saw_root and 1 <= len(devices) <= 256)
    return sum(value[0] for value in devices.values()), {
        device: value[1] for device, value in devices.items()}


def storage(env):
    home = Path.home()
    workspace = Path(env["GITHUB_WORKSPACE"]).parent
    temporary = Path(env["RUNNER_TEMP"])
    for root in (home, workspace, temporary):
        require(root.is_absolute() and root.is_dir() and
                not root.is_symlink() and root.resolve(strict=True) == root and
                root.stat().st_uid == os.geteuid())
    used, available = mounted_allocation()
    require(available[os.stat(workspace).st_dev] >
            BUILD_BYTES + RESERVE_BYTES and
            available[os.stat(home).st_dev] > 1_000_000_000 and
            available[os.stat(temporary).st_dev] > 1_000_000_000)
    require(used + BUILD_BYTES + RESERVE_BYTES < STOP_BYTES and
            used + BUILD_BYTES + RESERVE_BYTES < CEILING_BYTES)


def run(env):
    code = "SOURCE"
    try:
        context(env)
        code = "PLATFORM"
        platform_security()
        code = "STORAGE"
        storage(env)
        return {"case": CASE, "outcome": "passed"}, 0
    except BaseException:
        return {"case": CASE, "outcome": "failed", "error_code": code}, 1


def main():
    result, status = run(os.environ)
    print(json.dumps(result, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
