"""Creation and inspection contract for a future exact-base offline dpkg lane.

These functions do not create, start, stop, or remove a container. The caller
must apply the selected host monitor and Run ownership/cleanup protocol.
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import signal
import threading
from pathlib import Path

from capture_signed_indexes import (EXTRA_LIMIT, ORIGINAL_ALLOWANCE, STOP, RESERVE,
                                    read_json, digest, atomic_json, load_profile,
                                    image_lock, Budget, require)
from candidate_closure import SCHEMA, candidate_closure
from host_storage_monitor import HostMonitor, MonitoredDocker
from lifecycle import Run
from resolve_builder_lock import BASE, PLATFORM, LockError
from same_job_wrapper import same_host

OFFLINE_UID_GID = "65534:65534"
KEEPER = ["-c", "sleep 900"]
OWNER = "org.alopex.test-env.owner=envctl"
OFFLINE_GROWTH_LIMIT = 64_000_000
CANDIDATE_BYTES_LIMIT = 2_000_000


def verify_context(docker, profile: dict) -> None:
    """Bind both Docker operations and policy construction to this profile."""
    context = profile.get("docker_context")
    if (not context or docker.profile != profile or
            docker.prefix != ["docker", "--context", context]):
        raise LockError("offline Docker context drift")


def create_arguments(docker, profile: dict, run_id: str, token: str) -> list[str]:
    """Build a narrower create request from the already admitted #804 profile."""
    verify_context(docker, profile)
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
    if (not cpus or any(type(cpu) is not int or cpu < 0 for cpu in cpus) or
            type(limits.get("memory_bytes")) is not int or
            limits["memory_bytes"] <= 0 or
            type(limits.get("swap_bytes")) is not int or
            limits["swap_bytes"] < 0 or
            type(limits.get("pids")) is not int or limits["pids"] <= 0):
        raise LockError("offline resource profile")
    return ["create", "--name", "alopex-test-" + run_id, "--init",
            "--pull=never", "--platform", PLATFORM, "--network=none",
            "--ipc=private", "--cap-drop=ALL", "--user", OFFLINE_UID_GID,
            "--read-only", "--label", OWNER,
            "--label", "org.alopex.test-env.run=" + run_id,
            "--label", "org.alopex.test-env.token=" + token,
            "--memory", str(limits["memory_bytes"]),
            "--memory-swap", str(limits["memory_bytes"] + limits["swap_bytes"]),
            "--pids-limit", str(limits["pids"]),
            "--cpuset-cpus", ",".join(map(str, cpus)),
            "--log-driver=local", "--log-opt=max-size=1m",
            "--log-opt=max-file=1", "--log-opt=compress=false",
            *docker.policy(), "--entrypoint", "/bin/sh", BASE, *KEEPER]


def inspect_offline_container(docker, identity: str, profile: dict,
                              run_id: str, token: str, *, running: bool) -> dict:
    """Reject any authority, identity, resource, or lifecycle drift."""
    verify_context(docker, profile)
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
            host["MemorySwap"] != limits["memory_bytes"] + limits["swap_bytes"] or
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


def captured_inputs(profile: dict) -> tuple[dict, dict, str, dict[str, bytes], str, str]:
    """Read only the verified, owner-collected same-job capture bytes."""
    run = Run(profile["private_output"], profile["run_id"] + "-trace-resolve").load()
    if run.record["phase"] != "deleted":
        raise LockError("signed capture not deleted")
    run.verify_collection()
    proof = run.root / "proof"
    if (not proof.is_dir() or proof.is_symlink() or
            proof.stat().st_uid != os.getuid() or proof.stat().st_mode & 0o077):
        raise LockError("signed proof directory")
    base = read_json(proof / "base.json")
    auth = read_json(proof / "authentication.json")
    proof_hash = (proof / "proof.sha256").read_text().strip()
    canonical = json.dumps(auth, sort_keys=True, separators=(",", ":")).encode()
    if (proof_hash != hashlib.sha256(canonical).hexdigest() or
            auth.get("signature_verified") is not True):
        raise LockError("signed proof digest")
    index_map = read_json(proof / "indexes.json")
    if not isinstance(index_map, dict) or set(index_map) != {
            item["name"] for item in auth.get("indexes", [])}:
        raise LockError("signed index map")
    root = (run.root / "collected" / "packages").resolve(strict=True)
    indexes = {}
    for name, source in index_map.items():
        path = Path(source).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise LockError("signed index path")
        indexes[name] = path.read_bytes()
    apt_uris = (run.root / "collected" / "apt-uris.txt").read_text()
    resolver_sha = auth["resolver_sha256"]
    return base, auth, proof_hash, indexes, apt_uris, resolver_sha


def admit_offline(profile: dict, prior: dict, module, private: Path):
    """Recheck selected identity and the *cumulative* #804 + diagnostic budget."""
    require(private.resolve() == Path(os.environ["RUNNER_TEMP"]).resolve() /
            "alopex-804-qualification" and
            profile == load_profile(private / "profile.json") and
            profile["run_id"] == "hosted-804-" + os.environ["GITHUB_RUN_ID"] and
            profile["private_output"] == str(private / "runs") and
            profile["platform"] == PLATFORM and
            profile["execution_mode"] == "native" and
            profile["docker_context"] == prior["context"] and
            profile["storage"] == prior["storage"], "IDENTITY")
    same_host(prior, module.admission_facts())
    security = profile["security"]
    require(security["lsm_kind"] == "apparmor" and
            security["lsm_profile"] == "docker-default" and
            security["seccomp_sha256"] == module.POLICY_DIGEST and
            Path(security["seccomp_profile"]) == private / "seccomp.json" and
            digest(private / "seccomp.json") == module.POLICY_DIGEST, "POLICY")
    lock, _ = image_lock(profile)
    require(Path(profile["image_lock"]) == private / "image.lock.json" and
            digest(private / "image.lock.json") == module.LOCK_DIGEST and
            lock["base"] == BASE, "IDENTITY")
    budget = Budget(profile["storage"])
    used = budget.used()
    baseline = prior["baseline_bytes"]
    require(baseline < 29_000_000_000 and
            used - baseline <= ORIGINAL_ALLOWANCE + EXTRA_LIMIT and
            used + OFFLINE_GROWTH_LIMIT + RESERVE < STOP, "CAPACITY")
    for root in (Path(os.environ["GITHUB_WORKSPACE"]),
                 Path(prior["docker_info"]["DockerRootDir"])):
        stat = os.statvfs(root)
        require(stat.f_bavail * stat.f_frsize > OFFLINE_GROWTH_LIMIT + RESERVE,
                "CAPACITY")
    return budget, lock


def run_candidate_lifecycle(profile: dict, prior: dict, private: Path, module,
                            signed_receipt: dict, *, admission=admit_offline,
                            monitor_factory=HostMonitor,
                            docker_factory=MonitoredDocker) -> dict:
    """Own one offline container from preflight through collection and deletion.

    The caller has already qualified #804, captured indexes and sealed a passed
    signed-index receipt. No production trust pin is set or formal lock emitted.
    """
    if (signed_receipt.get("outcome") != "passed" or
            threading.current_thread() is not threading.main_thread()):
        raise LockError("offline candidate prerequisites")
    budget, _ = admission(profile, prior, module, private)
    base, auth, proof_hash, indexes, apt_uris, resolver_sha = captured_inputs(profile)
    used = budget.used()
    baseline = prior["baseline_bytes"]
    if (used - baseline > ORIGINAL_ALLOWANCE + EXTRA_LIMIT or
            used + OFFLINE_GROWTH_LIMIT + RESERVE >= STOP):
        raise LockError("offline host capacity")
    run_id = profile["run_id"] + "-trace-lock"
    monitor = monitor_factory(private / "profile.json", profile["storage"],
                              baseline, ORIGINAL_ALLOWANCE + EXTRA_LIMIT)
    docker = None
    run = None
    identity = None
    primary = None
    candidate = None
    cleanup_error = None
    try:
        monitor.start()
        monitor.stage(used, OFFLINE_GROWTH_LIMIT)
        docker = docker_factory(profile, monitor)
        docker.prerequisites()
        image = json.loads(docker.call("image", "inspect", BASE))[0]
        if (image["Os"] != "linux" or image["Architecture"] != "amd64" or
                BASE not in image.get("RepoDigests", [])):
            raise LockError("selected base image unavailable")
        run = Run(profile["private_output"], run_id).create({
            "base": BASE, "image_id": image["Id"], "proof_sha256": proof_hash,
            "resolver_sha256": resolver_sha})
        name = "alopex-test-" + run_id
        run.intent("container", name)
        identity = docker.call(*create_arguments(docker, profile, run_id,
                                                 run.record["ownership_token"]))
        if re.fullmatch(r"[0-9a-f]{64}", identity) is None:
            raise LockError("ambiguous offline create")
        run.resource("container", name, identity)
        inspect_offline_container(docker, identity, profile, run_id,
                                  run.record["ownership_token"], running=False)
        docker.call("start", identity)
        inspect_offline_container(docker, identity, profile, run_id,
                                  run.record["ownership_token"], running=True)
        run.transition("running")
        from offline_closure_adapter import ExactBaseDpkg
        comparator = ExactBaseDpkg(docker, monitor, identity, profile, run_id,
                                   run.record["ownership_token"])
        prior_handler = signal.getsignal(signal.SIGALRM)
        def timeout(_signum, _frame):
            raise LockError("offline closure timeout")
        signal.signal(signal.SIGALRM, timeout)
        try:
            signal.setitimer(signal.ITIMER_REAL, profile["limits"]["run_seconds"])
            candidate = candidate_closure(base, auth, proof_hash, indexes,
                                          apt_uris, resolver_sha, comparator.compare)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, prior_handler)
        if (candidate.get("schema") != SCHEMA or
                candidate.get("approval") != "unapproved" or
                candidate.get("formal_lock") is not False):
            raise LockError("candidate output boundary")
        if len(json.dumps(candidate).encode()) > CANDIDATE_BYTES_LIMIT:
            raise LockError("candidate output capacity")
    except BaseException as error:
        primary = error
    finally:
        monitor.cleanup_mode = True
        if run is not None and docker is not None:
            try:
                if identity is not None:
                    item = {"kind": "container", "name": "alopex-test-" + run_id,
                            "id": identity}
                    live = docker.inspect(item)
                    if (live is None or live["id"] != identity or
                            live["labels"].get("org.alopex.test-env.run") != run_id or
                            live["labels"].get("org.alopex.test-env.token") !=
                            run.record["ownership_token"]):
                        raise LockError("offline cleanup ownership")
                    if live["running"]:
                        docker.stop(identity)
                if run.record["phase"] in ("preparing", "running"):
                    run.transition("stopped")
                def collect(staging):
                    result = {"schema": "alopex.trace-closure-result/v1",
                              "status": "candidate" if primary is None else "failed",
                              "error_code": None if primary is None else
                              getattr(primary, "code", "COMMAND")}
                    (staging / "result.json").write_text(json.dumps(result) + "\n")
                    if candidate is not None:
                        (staging / "candidate.private.json").write_text(
                            json.dumps(candidate, sort_keys=True) + "\n")
                run.collect(collect)
                run.clean(docker.inspect, docker.remove)
            except BaseException as error:
                cleanup_error = error
                atomic_json(run.root / "cleanup-error.private.json", {
                    "error": repr(error)[:4096],
                    "original_error": repr(primary)[:4096]})
        try:
            monitor.stop()
            if budget.used() + RESERVE >= STOP:
                raise LockError("offline post-cleanup capacity")
        except BaseException as error:
            cleanup_error = cleanup_error or error
    if cleanup_error is not None:
        raise LockError("offline cleanup failed") from cleanup_error
    if primary is not None:
        raise primary
    return candidate
