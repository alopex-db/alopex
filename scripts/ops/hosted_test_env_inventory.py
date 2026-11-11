"""Collect encrypted, read-only host facts for a possible #804 execution lane.

This never qualifies a host, starts a container, or changes Docker configuration.
Console output is a fixed outcome code; detailed facts are encrypted before upload.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def read_text(path: Path, limit: int = 2_000_000) -> dict:
    try:
        if not path.is_file() or path.stat().st_size > limit:
            return {"status": "missing_or_oversize"}
        return {"status": "read", "value": path.read_text(errors="replace")}
    except OSError:
        return {"status": "unreadable"}


def command(*args: str) -> dict:
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=15, check=False)
        return {"exit": proc.returncode, "stdout": proc.stdout[:2_000_000], "stderr": proc.stderr[:100_000]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error_type": type(error).__name__}


def storage(roots: list[Path], deadline: float) -> dict:
    pending = list(roots)
    seen: set[tuple[int, int]] = set()
    total = 0
    failures = []
    while pending:
        if time.monotonic() > deadline:
            return {"status": "timeout", "allocated_bytes": total, "failures": failures}
        path = pending.pop()
        try:
            info = path.lstat()
            identity = (info.st_dev, info.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            if path.is_symlink():
                pending.append(path.resolve(strict=True))
                continue
            total += info.st_blocks * 512
            if path.is_dir():
                with os.scandir(path) as entries:
                    pending.extend(Path(entry.path) for entry in entries)
        except (OSError, RuntimeError) as error:
            failures.append({"path": str(path), "error_type": type(error).__name__})
            if len(failures) > 100:
                return {"status": "too_many_errors", "allocated_bytes": total, "failures": failures}
    return {"status": "measured" if not failures else "incomplete", "allocated_bytes": total, "failures": failures}


def sibling_io() -> dict:
    with tempfile.TemporaryDirectory(prefix="alopex-io-probe-") as directory:
        target = Path(directory) / "probe"
        code = (
            "import os,sys,time; f=open(sys.argv[1],'wb'); f.write(b'x'*4096);"
            "f.flush(); os.fsync(f.fileno()); print('ready',flush=True); time.sleep(15)"
        )
        child = subprocess.Popen(
            [sys.executable, "-c", code, str(target)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            assert child.stdout is not None
            ready = child.stdout.readline().strip()
            result = {"ready": ready == "ready", "pid": child.pid}
            root = Path("/proc") / str(child.pid)
            for name in ("io", "stat", "status", "task", "exe"):
                path = root / name
                if name == "task":
                    result[name] = {"status": "read", "tids": [p.name for p in path.iterdir()]}
                elif name == "exe":
                    result[name] = {"status": "read", "target": str(path.resolve(strict=True))}
                else:
                    result[name] = read_text(path)
            if result["io"]["status"] == "read":
                values = dict(line.split(":", 1) for line in result["io"]["value"].splitlines() if ":" in line)
                result["write_bytes"] = int(values.get("write_bytes", "-1"))
            return result
        except (OSError, ValueError, RuntimeError) as error:
            return {"status": "failed", "error_type": type(error).__name__}
        finally:
            child.terminate()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)


def main() -> int:
    export = Path("native-inventory-export")
    export.mkdir(mode=0o700, exist_ok=True)
    temp = Path(os.environ["RUNNER_TEMP"])
    private = temp / "alopex-host-inventory.private.json"
    facts: dict = {"schema": "alopex.host-inventory/v1", "qualification": "not_assessed"}
    try:
        facts["identity"] = {
            "uname": platform.uname()._asdict(), "uid": os.geteuid(), "groups": os.getgroups(),
            "hostname": platform.node(), "runner_environment": os.environ.get("RUNNER_ENVIRONMENT"),
        }
        for key, path in {
            "kernel_config": Path("/boot/config-" + platform.release()),
            "proc_self_io": Path("/proc/self/io"),
            "proc_self_status": Path("/proc/self/status"),
            "proc_self_mountinfo": Path("/proc/self/mountinfo"),
            "cgroup_controllers": Path("/sys/fs/cgroup/cgroup.controllers"),
            "lsm": Path("/sys/kernel/security/lsm"),
            "apparmor_enabled": Path("/sys/module/apparmor/parameters/enabled"),
            "dmi_product_uuid": Path("/sys/class/dmi/id/product_uuid"),
        }.items():
            facts[key] = read_text(path)
        facts["sibling_io"] = sibling_io()
        facts["docker_context"] = command("docker", "context", "inspect")
        facts["docker_info"] = command("docker", "info", "--format", "{{json .}}")
        docker_root = None
        if facts["docker_info"].get("exit") == 0:
            try:
                docker_root = json.loads(facts["docker_info"]["stdout"]).get("DockerRootDir")
            except (ValueError, TypeError):
                pass
        roots = [Path(os.environ["GITHUB_WORKSPACE"]).parent, temp]
        for path in (Path.home() / ".cargo", Path.home() / ".cache", Path.home() / ".local"):
            if path.exists():
                roots.append(path)
        if docker_root:
            roots.append(Path(docker_root))
        facts["storage_roots"] = list(map(str, roots))
        facts["storage"] = storage(roots, time.monotonic() + 120)
        facts["docker_root_observed"] = bool(docker_root)
        private.write_text(json.dumps(facts, sort_keys=True) + "\n")
        private.chmod(0o600)
        encrypted = export / "private-evidence.cms"
        encryption = subprocess.run(
            ["openssl", "cms", "-encrypt", "-binary", "-aes256", "-outform", "DER",
             "-in", str(private), "-out", str(encrypted), ".github/ops/native-evidence-cert.pem"],
            capture_output=True, timeout=30, check=False,
        )
        if encryption.returncode != 0 or not encrypted.is_file():
            raise RuntimeError("encryption_failed")
        (export / "public.json").write_text(json.dumps({
            "schema": "alopex.host-inventory-public/v1", "case": "HOST_PREFLIGHT",
            "outcome": "collected_not_qualified", "source_commit": os.environ.get("GITHUB_SHA"),
        }, sort_keys=True) + "\n")
        print('{"case":"HOST_PREFLIGHT","outcome":"collected_not_qualified"}')
        return 0
    except BaseException:
        print('{"case":"HOST_PREFLIGHT","outcome":"collector_failed"}')
        return 1
    finally:
        try:
            private.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
