"""Manual managed-VM image construction. Never run on a self-hosted machine."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time


def daemon_configuration(work, mount):
    return {
        "data-root": str(mount / "data"),
        "exec-root": str(mount / "daemon-state"),
        "pidfile": str(work / "daemon.pid"),
        "hosts": ["unix://" + str(work / "daemon.sock")],
        "bridge": "none", "iptables": False, "ip6tables": False,
        "ip-forward": False, "ip-masq": False,
        "storage-driver": "overlay2",
        "features": {"containerd-snapshotter": False},
    }


def check_inputs(value):
    bounds = value["bounds"]
    for name in ("disk_bytes", "fetch_bytes", "build_bytes", "probe_bytes",
                 "collect_bytes", "reserve_bytes", "operation_seconds"):
        if type(bounds[name]) is not int or bounds[name] <= 0:
            raise ValueError("invalid construction bound")
    maximum = sum(bounds[name] for name in ("disk_bytes", "fetch_bytes", "build_bytes",
                                          "probe_bytes", "collect_bytes", "reserve_bytes"))
    if maximum >= 40_000_000_000:
        raise ValueError("construction cannot fit the storage stop budget")
    if value["platform"] not in value["image_lock"]["platforms"]:
        raise ValueError("image platform not locked")
    policy = value["seccomp_text"].encode()
    if hashlib.sha256(policy).hexdigest() != value["seccomp_sha256"]:
        raise ValueError("security policy digest mismatch")
    if json.loads(policy).get("defaultAction") != "SCMP_ACT_ERRNO":
        raise ValueError("unexpected security policy")
    return maximum, policy


PROBE = r'''
import json, os, pathlib, subprocess, sys
sys.path.insert(0, "/tool")
import verify_environment as verify
request = json.loads(os.environ["ALOPEX_IMAGE_PROBE"])
os.umask(0o077)
subprocess.run(["sudo", "-n", "chown", str(os.getuid()) + ":" + str(os.getgid()), "/work"], check=True, capture_output=True)
subprocess.run(["sudo", "-n", "chmod", "0700", "/work"], check=True, capture_output=True)
result = {
    "privileges": verify.privileges(request),
    "kernel_limits": verify.resource_limits(request),
    "volume_io": verify.io_probe(pathlib.Path("/work")),
}
print(json.dumps(result, sort_keys=True))
'''


def construct(repo, work, export, inputs, recipient):
    sys.path.insert(0, str(repo / "scripts/test-env"))
    from capacity import Budget, allocated_bytes
    from common import atomic_json, digest, private_dir, require
    from docker_backend import Docker
    from lifecycle import Run

    maximum, policy = check_inputs(inputs)
    require(os.geteuid() == 0 and os.environ.get("RUNNER_KIND") == "github-hosted", "POLICY")
    require(os.environ.get("GITHUB_ACTIONS") == "true", "POLICY")
    require(platform.machine() == inputs["expected_machine"], "IDENTITY")
    require(not work.exists() and not export.exists(), "STATE")
    private_dir(work)
    private_dir(export)
    mount = work.parent / (work.name + "-mount")
    require(not mount.exists(), "STATE")
    private_dir(mount)
    roots = [repo.parent, work, export]
    # No checkout, caches or artifacts from another job are reused. Include
    # the provider's action cache and both users' possible tool caches anyway.
    for root in (repo.parent.parent / "_actions", Path(os.environ["OPERATOR_HOME"]) / ".cargo",
                 Path(os.environ["OPERATOR_HOME"]) / ".cache", Path.home() / ".cargo",
                 Path.home() / ".cache"):
        if root.exists():
            roots.append(root)
    before = allocated_bytes(roots)
    require(before + maximum < 40_000_000_000, "CAPACITY")
    atomic_json(work / "storage-before.json", {"allocated_bytes": before, "roots": list(map(str, roots))})
    cpus = sorted(os.sched_getaffinity(0))
    require(len(cpus) >= 2, "PREREQUISITE")
    disk = work / "docker.ext4"
    disk.touch(mode=0o600)
    lock = work / "image.lock.json"
    atomic_json(lock, inputs["image_lock"])
    seccomp = work / "seccomp.json"
    seccomp.write_bytes(policy)
    seccomp.chmod(0o600)
    private_dir(work / "cache")
    private_dir(work / "results")
    private_dir(work / "docker-cli")
    atomic_json(work / "docker-cli/config.json", {"auths": {}})
    os.environ["DOCKER_CONFIG"] = str(work / "docker-cli")
    os.environ["DOCKER_TMPDIR"] = str(mount / "tmp")
    os.environ["TMPDIR"] = str(work / "temporary")
    private_dir(work / "temporary")
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="safe.directory", GIT_CONFIG_VALUE_0=str(repo))
    bounds = inputs["bounds"]
    storage = {"roots": list(map(str, roots)), "runtime_storage_roots": [str(disk)],
               "stop_bytes": 40_000_000_000, "reserve_bytes": bounds["reserve_bytes"],
               "poll_seconds": 2,
               "planned_bytes": {"fetch": bounds["fetch_bytes"], "build": bounds["build_bytes"],
                                 "runtime": bounds["probe_bytes"], "accept": bounds["probe_bytes"],
                                 "collect": bounds["collect_bytes"], "bootstrap": bounds["disk_bytes"]}}
    budget = Budget(storage)
    context = "alopex-native-image"
    profile = {"schema": "alopex.test-env-profile/v1", "run_id": "native-image-probe",
               "docker_context": context, "platform": inputs["platform"], "execution_mode": "native",
               "image_lock": str(lock), "assets": str(work / "not-yet-constructed-assets.json"),
               "private_output": str(work / "results"), "cache": str(work / "cache"),
               "cpu": {"server": [cpus[0]], "client": [cpus[1]]},
               "limits": inputs["limits"], "storage": storage,
               "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                            "seccomp_profile": str(seccomp), "seccomp_sha256": inputs["seccomp_sha256"]}}
    atomic_json(work / "profile.json", profile)
    atomic_json(work / "daemon.json", daemon_configuration(work, mount))
    daemon = None
    mounted = False
    run = None
    docker = None
    failure = None
    cleanup_errors = []
    public = {"schema": "alopex.native-image-probe/v1", "status": "failed"}

    def bounded(command, phase="bootstrap", seconds=60, name="bootstrap"):
        budget.before(phase)
        budget.run(command, timeout=seconds, log=work / (name + ".log"))

    def cleanup_command(command, name):
        # Stopping or unmounting must also work when accounting has failed.
        with (work / (name + ".log")).open("ab") as output:
            subprocess.run(command, stdout=output, stderr=output, check=True, timeout=30)

    try:
        budget.before("bootstrap")
        with disk.open("r+b") as stream:
            stream.truncate(bounds["disk_bytes"])
        bounded(["mkfs.ext4", "-q", "-F", "-m", "0", str(disk)])
        bounded(["mount", "-o", "loop", str(disk), str(mount)])
        mounted = True
        private_dir(mount / "tmp")
        daemon_log = (work / "daemon.log").open("wb")
        (work / "daemon.log").chmod(0o600)
        daemon = subprocess.Popen(["dockerd", "--config-file", str(work / "daemon.json")],
                                  stdout=daemon_log, stderr=daemon_log, start_new_session=True)
        daemon_log.close()
        deadline = time.monotonic() + 60
        while True:
            require(daemon.poll() is None and time.monotonic() < deadline, "PREREQUISITE")
            budget.check()
            ready = subprocess.run(["docker", "--host", "unix://" + str(work / "daemon.sock"),
                                    "info", "--format", "{{json .}}"],
                                   capture_output=True, text=True, timeout=5)
            if ready.returncode == 0:
                info = json.loads(ready.stdout)
                break
            time.sleep(1)
        require(Path(info["DockerRootDir"]).resolve() == (mount / "data").resolve(), "IDENTITY")
        require(info["Driver"] == "overlay2" and info["OSType"] == "linux", "IDENTITY")
        atomic_json(work / "daemon-info.json", info)
        bounded(["docker", "context", "create", context, "--docker", "host=unix://" + str(work / "daemon.sock")])
        docker = Docker(profile)
        docker.prerequisites()
        controller = [sys.executable, str(repo / "scripts/test-env/envctl.py")]
        for command, phase in ((["doctor", "--phase", "preflight"], "runtime"),
                               (["fetch"], "fetch"), (["build"], "build")):
            bounded(controller + command + ["--profile", str(work / "profile.json")],
                    phase, bounds["operation_seconds"], command[0])
        image = json.loads((work / "cache/image.json").read_text())
        run = Run(profile["private_output"], profile["run_id"]).create(image)
        budget.before("runtime")
        input_volume = docker.create_volume(run, "input")
        work_volume = docker.create_volume(run, "work")
        name = "alopex-test-" + run.run_id
        run.intent("container", name)
        args = docker.arguments(run.run_id, image["id"], input_volume, work_volume, run.record["ownership_token"])
        request = {k: profile[k] for k in ("cpu", "limits", "security")}
        args[-1:-1] = ["--entrypoint", "python3", "--env", "ALOPEX_IMAGE_PROBE=" + json.dumps(request)]
        args += ["-c", PROBE]
        identity = docker.call(*args)
        run.resource("container", name, identity)
        atomic_json(run.root / "inspection.json", docker.validate_container(name))
        run.transition("running")
        budget.run([*docker.prefix, "start", "--attach", name], timeout=profile["limits"]["case_seconds"],
                   log=run.root / "probe.log", cancel=lambda: docker.stop(name))
        details = json.loads((run.root / "probe.log").read_text())
        require(details["volume_io"]["reaped"] and details["volume_io"]["write_bytes"] >= 4096, "CASE")
        atomic_json(run.root / "probe-result.json", {"status": "passed", "evidence": details})
        public.update(image_digest=image["id"][7:], controller_commit=image["controller_commit"],
                      controller_digest=image["controller_digest"], status="passed",
                      checks={"isolated_container": True, "guest_sudo": True,
                              "work_volume_process_io": True, "kernel_limits": True})
    except BaseException as error:
        failure = error
        atomic_json(work / "failure.json", {"error": repr(error)})
    finally:
        if run is not None and docker is not None:
            try:
                for item in run.record["resources"]:
                    if item["kind"] == "container":
                        docker.stop(item["name"])
                if run.record["phase"] in ("preparing", "running"):
                    run.transition("stopped")
                def copy_evidence(destination):
                    result = run.root / "probe-result.json"
                    if result.exists():
                        shutil.copyfile(result, destination / "result.json")
                    else:
                        atomic_json(destination / "result.json", {"status": "failed"})
                    for filename in ("probe.log", "inspection.json"):
                        if (run.root / filename).exists():
                            shutil.copyfile(run.root / filename, destination / filename)
                run.collect(copy_evidence)
                run.clean(docker.inspect, docker.remove)
            except BaseException as error:
                cleanup_errors.append(repr(error))
        if failure is None and not cleanup_errors:
            try:
                image_id = public["image_digest"]
                size = int(docker.call("image", "inspect", "sha256:" + image_id, "--format", "{{.Size}}"))
                require(size * 2 + 64 * 1024 * 1024 < bounds["collect_bytes"], "CAPACITY")
                bounded([*docker.prefix, "save", "--output", str(export / "environment-image.tar"),
                         "sha256:" + image_id], "collect", 300, "save-image")
                public["archive_sha256"] = digest(export / "environment-image.tar")
            except BaseException as error:
                failure = error
                atomic_json(work / "export-failure.json", {"error": repr(error)})
        if daemon is not None:
            try:
                if daemon.poll() is None:
                    os.killpg(daemon.pid, signal.SIGTERM)
                    try:
                        daemon.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(daemon.pid, signal.SIGKILL)
                        daemon.wait(timeout=10)
                        cleanup_errors.append("daemon graceful shutdown timeout")
            except BaseException as error:
                cleanup_errors.append(repr(error))
        if mounted:
            try:
                cleanup_command(["umount", str(mount)], "unmount")
                mounted = False
            except BaseException as error:
                cleanup_errors.append(repr(error))
        if not mounted and not cleanup_errors:
            try:
                disk.unlink(missing_ok=True)
                mount.rmdir()
            except OSError as error:
                cleanup_errors.append(repr(error))
        if cleanup_errors:
            atomic_json(work / "cleanup-errors.json", cleanup_errors)
        public["status"] = "passed" if failure is None and not cleanup_errors else "failed"
        public["owned_resources_cleaned"] = not mounted and not cleanup_errors
        public["full_acceptance"] = False
        public["performance_qualification"] = False
        atomic_json(work / "storage-after.json", {"allocated_bytes": allocated_bytes(roots)})
        # Keep detailed settings and logs confidential even in a public repo.
        budget.before("collect")
        archive = work / "private-evidence.tar.gz"
        selected = []
        candidates = [*work.iterdir(), *(work / "cache").glob("*.log"), *(work / "cache").glob("image.json")]
        for name in sorted(candidates):
            if name.name in (archive.name, "docker.ext4", "cache", "daemon-state"):
                continue
            if name.is_file() or name.name == "results":
                for item in ([name] if not name.is_dir() else [name, *name.rglob("*")]):
                    mode = item.lstat().st_mode
                    require(stat.S_ISREG(mode) or stat.S_ISDIR(mode), "POLICY")
                    selected.append(item)
        # Bound the uncompressed archive, encrypted copy and exported image.
        upper = sum(item.stat().st_size + 8192 for item in selected) + 10240
        exported = allocated_bytes([export])
        require(exported + 2 * upper < bounds["collect_bytes"], "CAPACITY")
        with tarfile.open(archive, "w:gz") as output:
            for name in selected:
                budget.check()
                output.add(name, arcname=str(name.relative_to(work)), recursive=False)
        require(archive.stat().st_size <= upper, "CAPACITY")
        bounded(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER",
                 "-in", str(archive), "-out", str(export / "private-evidence.cms"), str(recipient)],
                "collect", 30, "encrypt-evidence")
        atomic_json(export / "result.json", public)
        for path in [export, *export.iterdir()]:
            os.chown(path, int(os.environ["OPERATOR_UID"]), int(os.environ["OPERATOR_GID"]))
            path.chmod(0o700 if path.is_dir() else 0o600)
    return 0 if public["status"] == "passed" else 1


def main():
    if os.environ.get("RUNNER_KIND") != "github-hosted" or os.geteuid() != 0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    try:
        os.umask(0o077)
        repo = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
        temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
        inputs = json.loads(gzip.decompress(base64.b64decode(os.environ.pop("NATIVE_IMAGE_INPUTS"), validate=True)))
        result = construct(repo, temporary / "alopex-native-private", repo / "native-export",
                           inputs, repo / ".github/ops/native-evidence-cert.pem")
        print(json.dumps({"status": "passed" if result == 0 else "failed", "error_code": None if result == 0 else "CONSTRUCTION"}))
        return result
    except BaseException:
        print('{"status":"failed","error_code":"CONSTRUCTION"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
