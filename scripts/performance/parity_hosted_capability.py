"""Probe a hosted parity runner without constructing or measuring a release run."""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import re
import select
import subprocess
import sys
import tempfile


class ProbeFailure(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def require(condition: bool, code: str) -> None:
    if not condition:
        raise ProbeFailure(code)


def command(*args: str) -> str:
    try:
        result = subprocess.run(args, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ProbeFailure("DOCKER") from error
    require(result.returncode == 0 and len(result.stdout) <= 1_000_000, "DOCKER")
    return result.stdout.strip()


def sibling_io(root: Path) -> None:
    require(root.is_absolute() and root.is_dir(), "COUNTER")
    code = (
        "import os,sys,time; f=open(sys.argv[1],'wb'); "
        "f.write(b'x'*4096); f.flush(); os.fsync(f.fileno()); "
        "print('READY',flush=True); time.sleep(10)"
    )
    try:
        with tempfile.TemporaryDirectory(prefix="alopex-parity-probe-", dir=root) as directory:
            target = Path(directory) / "io"
            child = subprocess.Popen([sys.executable, "-c", code, str(target)],
                                     stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL, text=True)
            try:
                require(child.stdout is not None and
                        bool(select.select([child.stdout], [], [], 5)[0]) and
                        child.stdout.readline().strip() == "READY" and
                        child.poll() is None, "COUNTER")
                proc = Path("/proc") / str(child.pid)
                status = (proc / "status").read_text()
                io = (proc / "io").read_text()
                uid = next((line.split()[1] for line in status.splitlines()
                            if line.startswith("Uid:")), None)
                written = next((line.split()[1] for line in io.splitlines()
                                if line.startswith("write_bytes:")), None)
                require(uid == str(os.getuid()) and written is not None and
                        int(written) >= 4096 and child.poll() is None, "COUNTER")
            finally:
                child.terminate()
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise ProbeFailure("COUNTER") from error


def probe() -> None:
    require(sys.platform == "linux" and platform.machine() == "x86_64" and
            os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and
            os.environ.get("PARITY_ISSUE_NUMBER") == "712", "IDENTITY")
    source = os.environ.get("GITHUB_SHA", "")
    require(re.fullmatch(r"[0-9a-f]{40}", source) is not None, "IDENTITY")
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True,
                                       timeout=5).strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise ProbeFailure("IDENTITY") from error
    require(head == source, "IDENTITY")
    kernel = Path("/boot/config-" + platform.release())
    lsm = Path("/sys/kernel/security/lsm")
    cgroup = Path("/sys/fs/cgroup/cgroup.controllers")
    try:
        require("CONFIG_TASK_IO_ACCOUNTING=y" in kernel.read_text() and
                "apparmor" in lsm.read_text().strip().split(",") and
                cgroup.is_file(), "PREREQUISITE")
    except OSError as error:
        raise ProbeFailure("PREREQUISITE") from error
    sibling_io(Path(os.environ.get("RUNNER_TEMP", "")))
    context = command("docker", "context", "show")
    try:
        selected = json.loads(command("docker", "context", "inspect", context))[0]
        endpoint = selected["Endpoints"]["docker"]["Host"]
        info = json.loads(command("docker", "info", "--format", "{{json .}}"))
    except (ValueError, KeyError, IndexError, TypeError) as error:
        raise ProbeFailure("DOCKER") from error
    require(type(endpoint) is str and endpoint.startswith("unix://") and
            info.get("OSType") == "linux" and
            info.get("Architecture") == "x86_64" and
            info.get("CgroupVersion") == "2" and
            "name=apparmor" in info.get("SecurityOptions", []) and
            any(option.startswith("name=seccomp,") for option in
                info.get("SecurityOptions", [])), "DOCKER")


def main() -> int:
    try:
        probe()
        print("PARITY_HOSTED_CAPABILITY_OBSERVED")
        return 0
    except ProbeFailure as error:
        print(f"PARITY_HOSTED_CAPABILITY_{error.code}", file=sys.stderr)
        return 1
    except Exception:
        print("PARITY_HOSTED_CAPABILITY_INTERNAL", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
