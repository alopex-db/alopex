"""Live permission and accounting probes; each writes only a tiny fixture."""
import json
import os
from pathlib import Path
import subprocess
import sys
import select
import time

from common import CAPABILITIES, require
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


def files(work):
    probe = work / "permission-probe"
    probe.mkdir(mode=0o700)
    path = probe / "data"
    try:
        with path.open("xb") as stream:
            stream.write(b"x")
            stream.flush()
            os.fsync(stream.fileno())
        path.rename(probe / "renamed")
        require((probe / "renamed").stat().st_mode & 0o777 == 0o600, "POLICY")
        (probe / "renamed").unlink()
        result = subprocess.run(["sudo", "-n", "touch", "/input/.write-probe"],
                                capture_output=True, timeout=5)
        require(result.returncode != 0 and not Path("/input/.write-probe").exists(), "POLICY")
        return {"write_rename_fsync_unlink": True, "input_read_only": True}
    finally:
        if probe.exists():
            for child in probe.iterdir():
                child.unlink()
            probe.rmdir()


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
