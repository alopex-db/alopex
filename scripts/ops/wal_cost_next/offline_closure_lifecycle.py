"""Creation and inspection contract for a future exact-base offline dpkg lane.

These functions do not create, start, stop, or remove a container. The caller
must apply the selected host monitor and Run ownership/cleanup protocol.
"""
from __future__ import annotations

import json
import re

from capture_signed_indexes import read_json, digest
from resolve_builder_lock import BASE, PLATFORM, LockError

OFFLINE_UID_GID = "65534:65534"
KEEPER = ["-c", "sleep 900"]
OWNER = "org.alopex.test-env.owner=envctl"


def create_arguments(docker, profile: dict, run_id: str, token: str) -> list[str]:
    """Build a narrower create request from the already admitted #804 profile."""
    if (profile.get("platform") != PLATFORM or
            profile.get("execution_mode") != "native" or
            run_id != profile.get("run_id", "") + "-trace-lock" or
            re.fullmatch(r"[0-9a-f]{32}", token) is None or
            not profile.get("docker_context") or
            profile.get("security", {}).get("lsm_kind") != "apparmor" or
            profile["security"].get("lsm_profile") != "docker-default" or
            digest(profile["security"]["seccomp_profile"]) !=
            profile["security"].get("seccomp_sha256")):
        raise LockError("offline profile identity")
    limits = profile["limits"]
    cpus = sorted(set(profile["cpu"]["server"] + profile["cpu"]["client"]))
    if not cpus or limits.get("memory_bytes") != 1_610_612_736 or limits.get("swap_bytes") != 0 or limits.get("pids") != 256:
        raise LockError("offline resource profile")
    return ["create", "--name", "alopex-test-" + run_id, "--init",
            "--pull=never", "--platform", PLATFORM, "--network=none",
            "--ipc=private", "--cap-drop=ALL", "--user", OFFLINE_UID_GID,
            "--read-only", "--label", OWNER,
            "--label", "org.alopex.test-env.run=" + run_id,
            "--label", "org.alopex.test-env.token=" + token,
            "--memory", str(limits["memory_bytes"]),
            "--memory-swap", str(limits["memory_bytes"]),
            "--pids-limit", str(limits["pids"]),
            "--cpuset-cpus", ",".join(map(str, cpus)),
            "--log-driver=local", "--log-opt=max-size=1m",
            "--log-opt=max-file=1", "--log-opt=compress=false",
            *docker.policy(), "--entrypoint", "/bin/sh", BASE, *KEEPER]


def inspect_offline_container(docker, identity: str, profile: dict,
                              run_id: str, token: str, *, running: bool) -> dict:
    """Reject any authority, identity, resource, or lifecycle drift."""
    live = json.loads(docker.call("container", "inspect", identity))[0]
    host, config, state = live["HostConfig"], live["Config"], live["State"]
    normalized = lambda items: {item.removeprefix("CAP_").upper() for item in items or []}
    cpus = sorted(set(profile["cpu"]["server"] + profile["cpu"]["client"]))
    limits = profile["limits"]
    labels = config.get("Labels") or {}
    security = host.get("SecurityOpt") or []
    if (live["Id"] != identity or state["Running"] is not running or
            state.get("Paused", False) or host["Privileged"] or
            host["NetworkMode"] != "none" or host["IpcMode"] != "private" or
            host["PidMode"] not in ("", "private") or
            not host.get("ReadonlyRootfs") or host.get("Binds") or
            host.get("Mounts") or host.get("VolumesFrom") or host.get("Tmpfs") or
            host.get("Devices") or host.get("DeviceRequests") or
            host.get("PortBindings") or live["Mounts"] or
            normalized(host["CapDrop"]) != {"ALL"} or normalized(host["CapAdd"]) or
            config["User"] != OFFLINE_UID_GID or config["Image"] != BASE or
            config["Entrypoint"] != ["/bin/sh"] or config["Cmd"] != KEEPER or
            host["Memory"] != limits["memory_bytes"] or
            host["MemorySwap"] != limits["memory_bytes"] or
            host["PidsLimit"] != limits["pids"] or not host["Init"] or
            host["CpusetCpus"] != ",".join(map(str, cpus)) or
            host["LogConfig"] != {"Type": "local", "Config": {
                "max-size": "1m", "max-file": "1", "compress": "false"}} or
            labels.get("org.alopex.test-env.owner") != "envctl" or
            labels.get("org.alopex.test-env.run") != run_id or
            labels.get("org.alopex.test-env.token") != token or
            profile["security"]["lsm_kind"] != "apparmor" or
            profile["security"]["lsm_profile"] != "docker-default" or
            digest(profile["security"]["seccomp_profile"]) !=
            profile["security"]["seccomp_sha256"] or
            len(security) != 2 or "apparmor=docker-default" not in security or
            not any(value.startswith("seccomp=") and
                    json.loads(value.split("=", 1)[1]) ==
                    read_json(profile["security"]["seccomp_profile"])
                    for value in security)):
        raise LockError("offline container policy drift")
    image = json.loads(docker.call("image", "inspect", BASE))[0]
    if (live["Image"] != image["Id"] or image["Os"] != "linux" or
            image["Architecture"] != "amd64" or
            BASE not in image.get("RepoDigests", [])):
        raise LockError("selected base resolver unavailable")
    return live
