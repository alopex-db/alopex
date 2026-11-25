"""Bounded, read-only selected-runner diagnostic. Never grants run admission."""
from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys
import time

SCHEMA = "alopex.parity-selected-inventory/v1"
MAX_RECEIPT = 1_048_576
MAX_ENTRIES = 500_000
SECONDS = 180
PRIVATE_NAME = "parity-selected-inventory"
CODES = frozenset({"IDENTITY", "PROFILE", "STORAGE_COVERAGE", "STORAGE_READ", "TIMEOUT", "OVERSIZE", "RUNTIME", "PROCESS", "NETWORK"})
ROOT_CLASSES = frozenset({"workspace", "worktrees", "temporary", "installations", "caches", "podman", "windows"})


class Incomplete(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def require(ok: bool, code: str) -> None:
    if not ok:
        raise Incomplete(code)


def bounded_text(path: Path, limit: int = 65536) -> str:
    try:
        with path.open("rb") as stream:
            data = stream.read(limit + 1)
        require(len(data) <= limit, "OVERSIZE")
        return data.decode("utf-8", "replace")
    except OSError as error:
        raise Incomplete("STORAGE_READ") from error


def command(argv: list[str]) -> dict:
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=8, check=False)
        require(len(result.stdout) <= 65536 and len(result.stderr) <= 8192, "OVERSIZE")
        return {"exit": result.returncode,
                "stdout": result.stdout.decode("utf-8", "replace"),
                "stderr": result.stderr.decode("utf-8", "replace")}
    except subprocess.TimeoutExpired as error:
        raise Incomplete("TIMEOUT") from error
    except OSError as error:
        raise Incomplete("RUNTIME") from error


def allocation(roots: list[Path], deadline: float) -> dict:
    pending = roots.copy()
    seen: set[tuple[int, int]] = set()
    total = 0
    entries = 0
    while pending:
        require(time.monotonic() < deadline, "TIMEOUT")
        path = pending.pop()
        try:
            info = path.lstat()
            require(not stat.S_ISLNK(info.st_mode), "STORAGE_READ")
            identity = (info.st_dev, info.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            entries += 1
            require(entries <= MAX_ENTRIES, "OVERSIZE")
            total += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(path) as children:
                    pending.extend(Path(child.path) for child in children)
        except OSError as error:
            raise Incomplete("STORAGE_READ") from error
    return {"allocated_bytes": total, "entries": entries}


def mode_record(path: Path) -> dict:
    try:
        info = path.lstat()
    except OSError as error:
        raise Incomplete("RUNTIME") from error
    return {"path": str(path), "uid": info.st_uid, "gid": info.st_gid,
            "mode": stat.S_IMODE(info.st_mode), "type": stat.S_IFMT(info.st_mode)}


def service_process_observation() -> dict:
    deadline = time.monotonic() + 5
    checked = 0
    try:
        for entry in Path("/proc").iterdir():
            require(time.monotonic() < deadline, "RUNTIME")
            if not entry.name.isdecimal():
                continue
            checked += 1
            require(checked <= 1024, "RUNTIME")
            try:
                if bounded_text(entry / "comm", 128).strip() != "podman":
                    continue
                with (entry / "cmdline").open("rb") as stream:
                    cmdline = stream.read(8193)
                require(len(cmdline) <= 8192, "RUNTIME")
                if b"system\0service" not in cmdline and b"system service" not in cmdline:
                    continue
                if entry.stat().st_uid != os.getuid():
                    continue
                status = bounded_text(entry / "status", 8192)
                return {"status": "observed", "pid": int(entry.name),
                        "owner_uid": entry.stat().st_uid, "selection_basis": "same_uid",
                        "process_status": status}
            except FileNotFoundError:
                # Process exited during the bounded snapshot.
                continue
    except (OSError, Incomplete) as error:
        raise Incomplete("RUNTIME") from error
    return {"status": "same_uid_service_absent_from_proc_snapshot"}


def runtime_observation(info: dict) -> tuple[dict, Path]:
    try:
        graph = info["store"]["graphRoot"]
        require(isinstance(graph, str) and graph.startswith("/"), "RUNTIME")
        graph_root = Path(graph).resolve(strict=True)
        shim = shutil.which("docker")
        require(bool(shim), "RUNTIME")
        shim_target = Path(shim).resolve(strict=True)
        result = {"docker_shim": mode_record(Path(shim)),
                  "docker_shim_target": mode_record(shim_target),
                  "podman_binary": mode_record(Path("/usr/bin/podman")),
                  "graph_root": mode_record(graph_root)}
        socket = Path(os.environ.get("XDG_RUNTIME_DIR", "/nonexistent")) / "podman/podman.sock"
        result["service_socket"] = mode_record(socket) if socket.exists() else {"status": "absent"}
        result["service_process"] = service_process_observation()
        return result, graph_root
    except (KeyError, TypeError, ValueError, OSError) as error:
        raise Incomplete("RUNTIME") from error


def sibling_observation() -> dict:
    # Observe an existing same-UID process, without starting a test child.
    deadline = time.monotonic() + 5
    checked = 0
    try:
        for path in Path("/proc").iterdir():
            require(time.monotonic() < deadline, "TIMEOUT")
            if not path.name.isdecimal() or int(path.name) == os.getpid():
                continue
            checked += 1
            require(checked <= 1024, "PROCESS")
            try:
                status = bounded_text(path / "status", 8192)
            except Incomplete:
                # A process may exit between enumeration and observation.
                continue
            uid = next((line.split()[1] for line in status.splitlines()
                        if line.startswith("Uid:") and len(line.split()) > 1), None)
            if uid == str(os.getuid()):
                try:
                    io = bounded_text(path / "io", 8192)
                except Incomplete:
                    continue
                return {"status": "observed", "pid": int(path.name),
                        "process_status": status, "process_io": io}
    except (OSError, ValueError, Incomplete) as error:
        raise Incomplete("PROCESS") from error
    raise Incomplete("PROCESS")


def network_observation() -> dict:
    ip = shutil.which("ip")
    require(bool(ip), "NETWORK")
    try:
        route = command([ip, "-json", "route", "show"])
    except Incomplete as error:
        raise Incomplete("NETWORK") from error
    require(route["exit"] == 0, "NETWORK")
    try:
        return {"routes": route["stdout"],
                "tcp": bounded_text(Path("/proc/net/tcp"), 32768),
                "tcp6": bounded_text(Path("/proc/net/tcp6"), 32768),
                "peer_auth": "not_assessed_no_peer"}
    except Incomplete as error:
        raise Incomplete("NETWORK") from error


def observed_storage_paths() -> dict[str, list[Path]]:
    """Read current job paths and Git's linked worktrees, without creating any."""
    e = os.environ
    required = {"workspace": e.get("GITHUB_WORKSPACE"),
                "temporary": e.get("RUNNER_TEMP"),
                "cargo": e.get("CARGO_HOME", str(Path.home() / ".cargo")),
                "cache": e.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))}
    require(all(required.values()), "STORAGE_COVERAGE")
    observed: dict[str, list[Path]] = {}
    try:
        for key, value in required.items():
            path = Path(value)
            require(path.is_absolute() and path.exists() and
                    path.resolve(strict=True) == path, "STORAGE_COVERAGE")
            observed[key] = [path]
        worktrees = command(["git", "worktree", "list", "--porcelain"])
        require(worktrees["exit"] == 0, "STORAGE_COVERAGE")
        paths = [Path(line.removeprefix("worktree ")) for line in
                 worktrees["stdout"].splitlines() if line.startswith("worktree ")]
        require(1 <= len(paths) <= 128, "STORAGE_COVERAGE")
        require(all(path.is_absolute() and path.exists() and
                    path.resolve(strict=True) == path for path in paths),
                "STORAGE_COVERAGE")
        observed["worktrees"] = paths
        local = Path.home() / ".local"
        installs = []
        if local.is_dir():
            scanned = 0
            deadline = time.monotonic() + 5
            with os.scandir(local) as entries:
                for entry in entries:
                    scanned += 1
                    require(scanned <= 1024 and time.monotonic() < deadline,
                            "STORAGE_COVERAGE")
                    if entry.name.startswith("alopex-"):
                        installs.append(Path(entry.path).resolve(strict=True))
                        require(len(installs) <= 128, "STORAGE_COVERAGE")
        observed["installations"] = installs
    except (OSError, ValueError, Incomplete) as error:
        raise Incomplete("STORAGE_COVERAGE") from error
    return observed


def profile_roots(path: Path, mounts: str, graph_root: Path) -> list[Path]:
    try:
        info = path.lstat()
        parent = path.parent.stat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
                not info.st_mode & 0o077 and parent.st_uid == os.getuid() and
                not parent.st_mode & 0o077 and info.st_size <= 65536, "PROFILE")
        profile = json.loads(bounded_text(path))
    except (OSError, ValueError, TypeError) as error:
        raise Incomplete("PROFILE") from error
    require(isinstance(profile, dict), "PROFILE")
    # A preprovisioned profile cannot know a workflow_dispatch run ID. The
    # current job supplies that binding to the receipt, and the sealer verifies it.
    require(profile.get("schema") == "alopex.parity-selected-inventory-profile/v1" and
            profile.get("source_commit") == os.environ.get("GITHUB_SHA") and
            profile.get("runner_name") == os.environ.get("RUNNER_NAME") and
            profile.get("coverage_complete") is True, "PROFILE")
    rows = profile.get("storage_roots")
    require(isinstance(rows, list) and 1 <= len(rows) <= 64, "STORAGE_COVERAGE")
    roots = []
    classes = set()
    for row in rows:
        require(isinstance(row, dict) and set(row) == {"path", "class"} and
                row["class"] in ROOT_CLASSES and
                isinstance(row["path"], str), "STORAGE_COVERAGE")
        root = Path(row["path"])
        try:
            require(root.is_absolute() and root.is_dir() and not root.is_symlink() and
                    root.resolve(strict=True) == root and root not in roots,
                    "STORAGE_COVERAGE")
        except OSError as error:
            raise Incomplete("STORAGE_READ") from error
        roots.append(root)
        classes.add(row["class"])
    require(classes == ROOT_CLASSES and any(
        row["class"] == "podman" and (graph_root == Path(row["path"]) or
        graph_root.is_relative_to(Path(row["path"]))) for row in rows),
        "STORAGE_COVERAGE")
    # Every mounted Windows drive must have an explicit accounting root.
    windows_mounts = [Path(parts[4]) for line in mounts.splitlines()
                      if len(parts := line.split()) > 8 and
                      ((" - drvfs " in line and parts[4].startswith("/")) or
                       (" - 9p " in line and parts[4].startswith("/mnt/")))]
    # This maps declared Alopex roots to each visible Windows mount. The
    # profile's completeness assertion remains diagnostic, not admission proof.
    windows_roots = [Path(row["path"]) for row in rows if row["class"] == "windows"]
    require(bool(windows_mounts) and all(any(root == mount or
            root.is_relative_to(mount) for root in windows_roots) for mount in windows_mounts),
            "STORAGE_COVERAGE")
    observed = observed_storage_paths()
    by_class = {name: [Path(row["path"]) for row in rows if row["class"] == name]
                for name in ROOT_CLASSES}
    for key, expected_class in (("workspace", "workspace"), ("temporary", "temporary"),
                                ("cargo", "caches"), ("cache", "caches")):
        require(all(any(item == root or item.is_relative_to(root)
                        for root in by_class[expected_class]) for item in observed[key]),
                "STORAGE_COVERAGE")
    # A checkout may itself be a worktree; every other linked worktree must
    # have a declared worktree root. No stale or unaccounted path is ignored.
    require(all(any(item == root or item.is_relative_to(root) for root in
                    by_class["workspace"] + by_class["worktrees"])
                for item in observed["worktrees"]), "STORAGE_COVERAGE")
    require(all(any(item == root or item.is_relative_to(root) for root in
                    by_class["installations"]) for item in observed["installations"]),
            "STORAGE_COVERAGE")
    return roots


def collect() -> dict:
    e = os.environ
    require(e.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and
            e.get("PARITY_ISSUE_NUMBER") == "712" and
            e.get("RUNNER_ENVIRONMENT") == "self-hosted" and
            platform.system() == "Linux" and platform.machine() == "x86_64" and
            re.fullmatch(r"[0-9a-f]{40}", e.get("GITHUB_SHA", "")) is not None and
            e.get("GITHUB_SHA") == e.get("EXPECTED_INVENTORY_SHA"), "IDENTITY")
    head = command(["git", "rev-parse", "HEAD"])
    require(head["exit"] == 0 and head["stdout"].strip() == e["GITHUB_SHA"], "IDENTITY")
    deadline = time.monotonic() + SECONDS
    facts = {"schema": SCHEMA, "qualification": "not_assessed", "read_only": True,
             "source_commit": e["GITHUB_SHA"], "run_id": e["GITHUB_RUN_ID"],
             "run_attempt": e["GITHUB_RUN_ATTEMPT"], "runner_name": e.get("RUNNER_NAME"),
             "identity": {"uname": platform.uname()._asdict(), "uid": os.getuid(),
                          "groups": os.getgroups()},
             "coverage_basis": "owner_profile_and_visible_windows_mounts_not_admission",
             "mountinfo": bounded_text(Path("/proc/self/mountinfo")),
             "status": bounded_text(Path("/proc/self/status"))}
    try:
        facts["podman_version"] = command(["/usr/bin/podman", "version", "--format", "json"])
        facts["podman_info"] = command(["/usr/bin/podman", "info", "--format", "json"])
        require(facts["podman_version"]["exit"] == 0 and
                facts["podman_info"]["exit"] == 0, "RUNTIME")
        try:
            podman_info = json.loads(facts["podman_info"]["stdout"])
        except (ValueError, TypeError) as error:
            raise Incomplete("RUNTIME") from error
        facts["runtime"], graph_root = runtime_observation(podman_info)
        facts["lsm"] = bounded_text(Path("/sys/kernel/security/lsm"))
        profile = e.get("ALOPEX_PARITY_INVENTORY_PROFILE")
        require(bool(profile), "PROFILE")
        roots = profile_roots(Path(profile), facts["mountinfo"], graph_root)
        facts["storage_roots"] = [str(root) for root in roots]
        facts["storage"] = allocation(roots, deadline)
        require(facts["storage"]["allocated_bytes"] < 50_000_000_000,
                "STORAGE_COVERAGE")
        facts["sibling_process"] = sibling_observation()
        facts["network"] = network_observation()
    except Incomplete as error:
        facts.update(outcome="incomplete", error_code=error.code)
    else:
        facts["outcome"] = "collected_not_qualified"
    return facts


def main() -> int:
    private = Path(os.environ["RUNNER_TEMP"]) / PRIVATE_NAME
    os.umask(0o077)
    try:
        private.mkdir(mode=0o700)
        facts = collect()
        code = facts.get("error_code", "COLLECTED")
    except Incomplete as error:
        facts = {"schema": SCHEMA, "qualification": "not_assessed", "read_only": True,
                 "source_commit": os.environ.get("GITHUB_SHA"),
                 "run_id": os.environ.get("GITHUB_RUN_ID"),
                 "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
                 "runner_name": os.environ.get("RUNNER_NAME"),
                 "outcome": "incomplete", "error_code": error.code}
        code = error.code
    except Exception:
        facts = {"schema": SCHEMA, "qualification": "not_assessed", "read_only": True,
                 "source_commit": os.environ.get("GITHUB_SHA"),
                 "run_id": os.environ.get("GITHUB_RUN_ID"),
                 "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
                 "runner_name": os.environ.get("RUNNER_NAME"),
                 "outcome": "incomplete", "error_code": "INTERNAL"}
        code = "INTERNAL"
    try:
        payload = (json.dumps(facts, sort_keys=True) + "\n").encode()
        if len(payload) > MAX_RECEIPT:
            facts = {"schema": SCHEMA, "qualification": "not_assessed", "read_only": True,
                     "source_commit": os.environ.get("GITHUB_SHA"),
                     "run_id": os.environ.get("GITHUB_RUN_ID"),
                     "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
                     "runner_name": os.environ.get("RUNNER_NAME"),
                     "outcome": "incomplete", "error_code": "OVERSIZE"}
            payload = (json.dumps(facts, sort_keys=True) + "\n").encode()
            code = "OVERSIZE"
        with (private / "inventory.private.json").open("xb") as stream:
            stream.write(payload)
        print(f"PARITY_SELECTED_INVENTORY_{code}")
        return 0 if code == "COLLECTED" else 1
    except Exception:
        print("PARITY_SELECTED_INVENTORY_EVIDENCE", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
