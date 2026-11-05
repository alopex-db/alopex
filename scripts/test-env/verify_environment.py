"""Live permission and accounting probes; each writes only a tiny fixture."""
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import select
import time

from common import CAPABILITIES, read_json, require
from observe import process


def privileges(request):
    require(os.getuid() != 0, "POLICY")
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines())
    require(int(status["CapEff"], 16) == 0 and int(status["NoNewPrivs"]) == 0, "POLICY")
    require(int(status["Seccomp"]) == 2, "POLICY")
    label = Path("/proc/self/attr/current").read_text().strip()
    expected = request["security"]["lsm_profile"]
    if request["security"]["lsm_kind"] == "apparmor":
        require(label == expected + " (enforce)", "POLICY")
    else:
        require(label == expected, "POLICY")
    root = subprocess.run(["sudo", "-n", "id", "-u"], capture_output=True, text=True, timeout=5)
    require(root.returncode == 0 and root.stdout.strip() == "0", "POLICY")
    # CAP numbers are Linux ABI identifiers, not host CPU or architecture values.
    mask = sum(1 << bit for bit in (0, 1, 3, 6, 7, 29))
    require(int(status["CapBnd"], 16) == mask, "POLICY")
    root_status = subprocess.check_output(["sudo", "-n", "cat", "/proc/self/status"], text=True, timeout=5)
    root_fields = dict(line.split(":", 1) for line in root_status.splitlines())
    require(int(root_fields["CapBnd"], 16) == mask, "POLICY")
    return {"capabilities": list(CAPABILITIES), "sudo": True, "lsm_applied": True}


def mount_for(path, table=None):
    """Inspect mount flags separately from superblock flags (a RO bind may share a RW FS)."""
    table = Path("/proc/self/mountinfo").read_text() if table is None else table
    target = Path(path)
    matches = []
    for line in table.splitlines():
        left, right = line.split(" - ", 1)
        fields, fs = left.split(), right.split()
        mount = Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4]))
        if target == mount or mount in target.parents:
            matches.append((len(mount.parts), {"mount": str(mount), "filesystem": fs[0],
                            "options": fields[5].split(","), "super_options": fs[2].split(",")}))
    require(matches, "POLICY")
    depth = max(row[0] for row in matches)
    selected = [row[1] for row in matches if row[0] == depth]
    # Unexpected stacked mounts require investigation, not an arbitrary choice.
    require(len(selected) == 1, "POLICY")
    return selected[0]


def ownership(path, uid, gid, mode):
    info = Path(path).lstat()
    require(not stat.S_ISLNK(info.st_mode) and info.st_uid == uid and info.st_gid == gid and
            stat.S_IMODE(info.st_mode) == mode, "POLICY")


def file_layout(work, inputs=Path("/input"), tool=Path("/tool")):
    uid, gid = os.getuid(), os.getgid()
    manifest = read_json(inputs / "assets.json")
    executable = {item["path"] for item in manifest["files"] if item["role"] == "server"}
    for path in [inputs, *inputs.rglob("*")]:
        mode = 0o750 if path.is_dir() else 0o550 if str(path.relative_to(inputs)) in executable else 0o640
        ownership(path, 0, gid, mode)
        if path.is_file():
            with path.open("rb") as stream:
                stream.read(1)
            if str(path.relative_to(inputs)) in executable:
                require(os.access(path, os.X_OK), "POLICY")
    for path in [tool, *tool.rglob("*")]:
        ownership(path, 0, 0, 0o755 if path.is_dir() else 0o644)
    for path in [work, *(work / name for name in ("home", "tmp", "cache", "results"))]:
        ownership(path, uid, gid, 0o700)
    sudoers = subprocess.check_output(["sudo", "-n", "stat", "-c", "%u:%g:%a",
                                      "/etc/sudoers.d/alopex"], text=True, timeout=5)
    require(sudoers.strip() == "0:0:440", "POLICY")
    ownership(Path("/usr/bin/sudo"), 0, 0, 0o4755)
    mounts = {str(path): mount_for(path) for path in (inputs, work, Path("/usr/bin/sudo"), Path("/dev/shm"))}
    require("ro" in mounts[str(inputs)]["options"] and "rw" in mounts[str(work)]["options"], "POLICY")
    require("ro" not in mounts[str(work)]["super_options"], "POLICY")
    for path in (inputs, work):
        require("noexec" not in mounts[str(path)]["options"], "POLICY")
    require("nosuid" not in mounts["/usr/bin/sudo"]["options"] and
            "noexec" not in mounts["/usr/bin/sudo"]["options"], "POLICY")
    require(mounts["/dev/shm"]["filesystem"] == "tmpfs" and
            {"rw", "nosuid", "nodev", "noexec"} <= set(mounts["/dev/shm"]["options"]), "POLICY")
    return mounts


def files(work):
    mounts = file_layout(work)
    probe = work / "permission-probe"
    probe.mkdir(mode=0o700)
    path = probe / "data"
    try:
        with path.open("xb") as stream:
            stream.write(b"x")
            stream.flush()
            os.fsync(stream.fileno())
        path.rename(probe / "renamed")
        ownership(probe / "renamed", os.getuid(), os.getgid(), 0o600)
        # Drop all supplementary groups as well as the UID/GID. A sudo failure
        # must not be mistaken for the expected denial of the private file.
        denied = subprocess.run(["sudo", "-n", sys.executable, "-c", """
import json, os, pwd, sys
user = pwd.getpwnam('nobody')
assert user.pw_uid not in (0, int(sys.argv[2]))
os.setgroups([])
os.setgid(user.pw_gid)
os.setuid(user.pw_uid)
assert os.getuid() == user.pw_uid and os.getgid() == user.pw_gid and not os.getgroups()
try:
    open(sys.argv[1], 'rb').read(1)
except PermissionError:
    print(json.dumps({'other_user_denied': True}))
else:
    raise SystemExit(1)
""", str(probe / "renamed"), str(os.getuid())], capture_output=True, text=True, timeout=5)
        require(denied.returncode == 0 and json.loads(denied.stdout) == {"other_user_denied": True}, "POLICY")
        (probe / "renamed").unlink()
        result = subprocess.run(["sudo", "-n", "touch", "/input/.write-probe"],
                                capture_output=True, timeout=5)
        require(result.returncode != 0 and not Path("/input/.write-probe").exists(), "POLICY")
        # Prove work-volume execution rather than trusting mount metadata alone.
        script = probe / "executable"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o700)
        ownership(script, os.getuid(), os.getgid(), 0o700)
        require(subprocess.run([str(script)], capture_output=True, timeout=5).returncode == 0, "POLICY")
        return {"write_rename_fsync_unlink": True, "input_read_only": True,
                "other_user_denied": True, "work_exec": True, "ownership_modes": True, "mounts": mounts}
    finally:
        if probe.exists():
            for child in probe.iterdir():
                child.unlink()
            probe.rmdir()


def cpu_set(value):
    require(re.fullmatch(r"[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*", value) is not None, "POLICY")
    result = set()
    for part in value.split(","):
        ends = [int(number) for number in part.split("-")]
        first, last = ends[0], ends[-1]
        require(first <= last and last - first < 65536, "POLICY")
        result.update(range(first, last + 1))
    return result


def resource_limits(request, root=Path("/sys/fs/cgroup")):
    """Read kernel limits; Docker's requested values alone do not prove enforcement."""
    require((root / "cgroup.controllers").is_file(), "PREREQUISITE")
    limits = request["limits"]
    actual = {}
    for filename, key in (("memory.max", "memory_bytes"), ("memory.swap.max", "swap_bytes"),
                          ("pids.max", "pids")):
        value = (root / filename).read_text().strip()
        require(value.isdecimal() and int(value) == limits[key], "POLICY")
        actual[key] = int(value)
    expected = set(request["cpu"]["server"] + request["cpu"]["client"])
    effective = cpu_set((root / "cpuset.cpus.effective").read_text().strip())
    require(effective == expected and os.sched_getaffinity(0) == expected, "POLICY")
    affinity = {}
    for role in ("server", "client"):
        output = subprocess.check_output([sys.executable, "-c", """
import json, os, sys
os.sched_setaffinity(0, json.loads(sys.argv[1]))
print(json.dumps(sorted(os.sched_getaffinity(0))))
""", json.dumps(request["cpu"][role])], text=True, timeout=5)
        affinity[role] = json.loads(output)
        require(set(affinity[role]) == set(request["cpu"][role]), "POLICY")
    return {"kind": "live_kernel_limits", "cgroup_version": 2,
            "limits": actual, "effective_cpus": sorted(effective), "affinity": affinity}


def io_probe(work, seconds=5):
    path = work / "io-probe"
    child = subprocess.Popen([sys.executable, __file__, "helper", str(path)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True)
    deadline = time.monotonic() + seconds
    def line():
        remaining = deadline - time.monotonic()
        require(remaining > 0 and select.select([child.stdout], [], [], remaining)[0], "TIMEOUT")
        return child.stdout.readline().strip()
    try:
        require(line() == "ready", "MEASUREMENT")
        before = process(child.pid)
        child.stdin.write("write\n")
        child.stdin.flush()
        require(line() == "written", "MEASUREMENT")
        after = process(child.pid)
        require(before["start_ticks"] == after["start_ticks"], "MEASUREMENT")
        require(after["write_bytes"] - before["write_bytes"] >= 4096, "MEASUREMENT")
        child.stdin.write("stop\n")
        child.stdin.flush()
        require(child.wait(timeout=5) == 0, "CASE")
        return {"write_bytes": after["write_bytes"] - before["write_bytes"], "reaped": True}
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        path.unlink(missing_ok=True)


if __name__ == "__main__" and sys.argv[1] == "helper":
    print("ready", flush=True)
    require(input() == "write")
    with Path(sys.argv[2]).open("xb") as stream:
        stream.write(b"x" * 4096)
        stream.flush()
        os.fsync(stream.fileno())
    print("written", flush=True)
    require(input() == "stop")
