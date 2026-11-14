"""Same-job, exact-profile Ubuntu index capture for the #682 tracer builder.

Only called after the #804 qualification receipt has been verified by the WAL
wrapper. The product image lock is read only. No package is downloaded.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys

HERE = Path(__file__).resolve().parent
TEST_ENV = HERE.parents[1] / "test-env"
sys.path.insert(0, str(TEST_ENV))

from capacity import Budget  # noqa: E402
from common import (CAPABILITIES, Failure, atomic_json, digest, load_profile,  # noqa: E402
                    private_dir, read_json, require)
from docker_backend import Docker, OWNER  # noqa: E402
from envctl import image_lock, recover  # noqa: E402
from lifecycle import Run  # noqa: E402
from host_storage_monitor import HostMonitor, MonitoredDocker  # noqa: E402
from resolve_builder_lock import BASE, PLATFORM  # noqa: E402

EXTRA_LIMIT = 2_500_000_000
CAPTURE_LIMIT = 250_000_000
ORIGINAL_ALLOWANCE = 7_500_000_000
RESERVE = 1_000_000_000
STOP = 40_000_000_000


def admissible(profile, prior, module, private):
    """Check exact archived policy and fresh full-host budget before Docker growth."""
    require(private.resolve() == Path(os.environ["RUNNER_TEMP"]).resolve() /
            "alopex-804-qualification", "IDENTITY")
    require(profile == load_profile(private / "profile.json"), "IDENTITY")
    require(profile["run_id"] == "hosted-804-" + os.environ["GITHUB_RUN_ID"] and
            profile["private_output"] == str(private / "runs") and
            profile["platform"] == PLATFORM and profile["execution_mode"] == "native" and
            profile["docker_context"] == prior["context"] and
            profile["storage"] == prior["storage"], "IDENTITY")
    security, limits = profile["security"], profile["limits"]
    require(security["lsm_kind"] == "apparmor" and
            security["lsm_profile"] == "docker-default" and
            security["seccomp_sha256"] == module.POLICY_DIGEST and
            Path(security["seccomp_profile"]) == private / "seccomp.json" and
            digest(private / "seccomp.json") == module.POLICY_DIGEST and
            limits == {"memory_bytes": 1_610_612_736, "swap_bytes": 0,
                       "pids": 256, "case_seconds": 60, "run_seconds": 900,
                       "stop_seconds": 10}, "POLICY")
    lock, _ = image_lock(profile)
    require(Path(profile["image_lock"]) == private / "image.lock.json" and
            digest(private / "image.lock.json") == module.LOCK_DIGEST and
            lock["base"] == BASE, "IDENTITY")
    facts = module.admission_facts()
    # The caller checks the same-host facts, including the complete storage
    # scope. Here enforce the capture's *additional* allowance at both points.
    require(facts["storage"] == prior["storage"] and
            prior["baseline_bytes"] < 29_000_000_000, "MEASUREMENT")
    budget = Budget(profile["storage"])
    used = budget.used()
    require(used <= prior["baseline_bytes"] + ORIGINAL_ALLOWANCE and
            used + EXTRA_LIMIT + RESERVE < STOP, "CAPACITY")
    for root in (Path(os.environ["GITHUB_WORKSPACE"]),
                 Path(prior["docker_info"]["DockerRootDir"])):
        stat = os.statvfs(root)
        require(stat.f_bavail * stat.f_frsize > EXTRA_LIMIT + RESERVE, "CAPACITY")
    capture_storage = dict(profile["storage"])
    capture_storage["planned_bytes"] = {**capture_storage["planned_bytes"],
                                         "signed_capture": EXTRA_LIMIT}
    budget = Budget(capture_storage)
    budget.before("signed_capture")
    return budget, lock


def inspect_resolver(docker, identity, profile, token):
    """Inspect before start; product Docker.validate_container is network-none."""
    live = json.loads(docker.call("container", "inspect", identity))[0]
    host, config = live["HostConfig"], live["Config"]
    normalize = lambda values: {value.removeprefix("CAP_").upper() for value in values or []}
    require(not host["Privileged"] and host["NetworkMode"] == "bridge" and
            host["IpcMode"] == "private" and host["PidMode"] in ("", "private") and
            not host.get("Binds") and not host.get("Devices") and
            not host.get("PortBindings") and not live["Mounts"] and
            normalize(host["CapDrop"]) == {"ALL"} and
            normalize(host["CapAdd"]) == set(CAPABILITIES) and
            config["User"] == "root" and config["Image"] == BASE and
            config["Entrypoint"] == ["/bin/sh"] and
            config["Cmd"] == ["/resolution/resolve.sh"] and
            host["Memory"] == profile["limits"]["memory_bytes"] and
            host["MemorySwap"] == profile["limits"]["memory_bytes"] and
            host["PidsLimit"] == profile["limits"]["pids"] and
            host["Init"] is True and
            host["CpusetCpus"] == ",".join(map(str, sorted(set(
                profile["cpu"]["server"] + profile["cpu"]["client"])))) and
            host["LogConfig"]["Type"] == "local" and
            host["LogConfig"]["Config"] == {"max-size": "1m", "max-file": "1",
                                             "compress": "false"}, "POLICY")
    labels = config.get("Labels") or {}
    require(labels.get("org.alopex.test-env.owner") == "envctl" and
            labels.get("org.alopex.test-env.token") == token, "POLICY")
    security = host.get("SecurityOpt") or []
    require(len(security) == 2 and "apparmor=docker-default" in security and
            any(value.startswith("seccomp=") and
                json.loads(value.split("=", 1)[1]) ==
                read_json(profile["security"]["seccomp_profile"])
                for value in security), "POLICY")
    return live


def capture(module, prior, private):
    profile = read_json(private / "profile.json", private=True)
    budget, _ = admissible(profile, prior, module, private)
    docker = Docker(profile)
    docker.prerequisites()
    docker.policy()
    run_id = profile["run_id"] + "-trace-resolve"
    run = Run(profile["private_output"], run_id).create({
        "base": BASE, "image_lock_sha256": digest(private / "image.lock.json"),
        "resolver_sha256": digest(HERE / "capture_ubuntu_indexes.sh")})
    name = "alopex-test-" + run_id
    monitor = HostMonitor(private / "profile.json", profile["storage"],
                          budget.initial, EXTRA_LIMIT)
    docker = MonitoredDocker(profile, monitor)
    try:
        monitor.start()
        monitor.run([*docker.prefix, "pull", "--platform", PLATFORM, BASE],
                    timeout=profile["limits"]["run_seconds"], log=run.root / "pull.log")
        image = json.loads(docker.call("image", "inspect", BASE))[0]
        require(image["Os"] == "linux" and image["Architecture"] == "amd64" and
                BASE in image.get("RepoDigests", []), "IDENTITY")
        capture_storage = dict(profile["storage"])
        capture_storage["planned_bytes"] = {**capture_storage["planned_bytes"],
                                             "capture_files": CAPTURE_LIMIT}
        capture_budget = Budget(capture_storage)
        capture_budget.before("capture_files")
        monitor.stage(capture_budget.initial, CAPTURE_LIMIT)
        atomic_json(run.root / "capture-budget.private.json", {
            "allocated_before_bytes": capture_budget.initial,
            "capture_limit_bytes": CAPTURE_LIMIT})
        bootstrap = private_dir(run.root / "bootstrap")
        shutil.copyfile(HERE / "capture_ubuntu_indexes.sh", bootstrap / "resolve.sh")
        certificates = ssl.create_default_context().get_ca_certs(binary_form=True)
        require(bool(certificates), "PREREQUISITE")
        (bootstrap / "ca.pem").write_text("".join(ssl.DER_cert_to_PEM_cert(cert)
                                                   for cert in certificates))
        bootstrap.chmod(0o755)
        for path in bootstrap.iterdir():
            path.chmod(0o644)
        atomic_json(run.root / "bootstrap.private.json", {
            "resolver_sha256": digest(bootstrap / "resolve.sh"),
            "ca_sha256": digest(bootstrap / "ca.pem")})
        capture_budget.check()
        limits = profile["limits"]
        cpus = sorted(set(profile["cpu"]["server"] + profile["cpu"]["client"]))
        args = ["create", "--name", name, "--pull=never", "--platform", PLATFORM,
                "--init", "--network=bridge", "--ipc=private", "--cap-drop=ALL",
                "--user=root", "--label", OWNER,
                "--label", "org.alopex.test-env.run=" + run_id,
                "--label", "org.alopex.test-env.token=" + run.record["ownership_token"],
                "--memory", str(limits["memory_bytes"]), "--memory-swap", str(limits["memory_bytes"]),
                "--cpuset-cpus", ",".join(map(str, cpus)), "--pids-limit", str(limits["pids"]),
                "--log-driver=local", "--log-opt=max-size=1m", "--log-opt=max-file=1",
                "--log-opt=compress=false"]
        for capability in CAPABILITIES:
            args.extend(["--cap-add", capability])
        args.extend([*docker.policy(), "--entrypoint", "/bin/sh", BASE,
                     "/resolution/resolve.sh"])
        run.intent("container", name)
        identity = docker.call(*args)
        run.resource("container", name, identity)
        inspect_resolver(docker, identity, profile, run.record["ownership_token"])
        budget.check()
        monitor.run([*docker.prefix, "cp", str(bootstrap), identity + ":/resolution"],
                    timeout=limits["case_seconds"], log=run.root / "stage.log")
        run.transition("running")
        monitor.run([*docker.prefix, "start", "--attach", identity],
                    timeout=limits["run_seconds"], log=run.root / "container.log")
        state = json.loads(docker.call("container", "inspect", identity))[0]["State"]
        require(state["ExitCode"] == 0 and not state["OOMKilled"], "COMMAND")
        run.transition("stopped")

        def copy(destination):
            monitor.run([*docker.prefix, "cp", identity + ":/resolution/results/.",
                         str(destination)], timeout=limits["case_seconds"],
                        log=run.root / "collect.log")
            (destination / "result.json").write_text('{"capture":"complete"}\n')

        run.collect(copy)
        collected = run.root / "collected"
        actual = sum(path.stat().st_size for path in collected.rglob("*") if path.is_file())
        require(actual <= CAPTURE_LIMIT and
                (collected / "capture.status").read_text() == "capture-complete\n", "CAPACITY")
        monitor.run([sys.executable, "-B", str(HERE / "verify_captured_indexes.py"),
                     "--capture", str(collected), "--resolver-script",
                     str(HERE / "capture_ubuntu_indexes.sh"), "--output",
                     str(run.root / "proof")], timeout=300, log=run.root / "proof.log")
        require((run.root / "proof" / "proof.sha256").is_file(), "COLLECT")
        capture_budget.check()
        budget.check()
        run.clean(docker.inspect, docker.remove)
        budget.check()
        monitor.stop()
        return run.root
    except BaseException as error:
        monitor.cleanup_mode = True
        atomic_json(run.root / "failure.json", {
            "error_code": getattr(error, "code", "COMMAND"),
            "detail": repr(error)[:4096]})
        run.load()
        if run.record["phase"] == "collected":
            run.clean(docker.inspect, docker.remove)
        else:
            recover(run, docker)
        raise
    finally:
        if not monitor.stopping.is_set():
            monitor.cleanup_mode = True
            monitor.stop()
