"""Qualify #804 on one hosted runner, with read-only admission before Docker growth.

Console and uploaded public output contain fixed outcomes only. Machine details,
profile, logs, and exact paths are encrypted with the repository evidence cert.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))
sys.path.insert(0, str(HERE))

from capacity import Budget, DU_BINARY, SUDO_BINARY, trusted_executable  # noqa: E402
from common import (CASES, Failure, atomic_json, digest, elf_signature, load_profile,  # noqa: E402
                    private_dir, read_json, require, verify_assets)
from docker_backend import Docker  # noqa: E402
from envctl import image_lock, source_identity  # noqa: E402
from hosted_test_env_inventory import read_text  # noqa: E402

PRODUCT = "a4158b873963dd79c7844953d64a8b511ee75fde"
ASSET_RUN = 43110932395
ASSET_RUN_HEAD = "39657522a2712f8019880826d28e82567d040b70"
ASSET_ARTIFACT_ID = 13895989882
ASSET_ZIP_DIGEST = "2f8c652140ec7f7ae75f7e7553d1227e1dbe0fef3e007e4310f7e83fe658e720"
HARNESS = "5c3d068b9ce4d60fbc1608924882df13b0a27568"
ASSETS_DIGEST = "36e16e155e96982f094482ae53615789ae7eab4fcf7ef0d0c77cf282d464cb6e"
LOCK_DIGEST = "701265c8e64f2bbb6fad61db0a65e5980189379a1c4f1f666a39dc6c31b63978"
POLICY_DIGEST = "536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74"
CONTROLLER_DIGEST = "624a29073d242fcc07b5c899f9a7bb8d3903c834a88b9bbd158145df0d8457ec"
EXPECTED_DOCKER_VERSION = "28.0.4"
DOCKER_BINARY = Path("/usr/bin/docker")
PLANNED = {"fetch": 1_000_000_000, "build": 3_000_000_000,
           "runtime": 500_000_000, "accept": 1_000_000_000, "collect": 500_000_000}
CERT = REPO / ".github/ops/native-evidence-cert.pem"
INPUT = REPO / ".github/ops/qualification"
EXPORT = REPO / "native-qualification-export"
EXPECTED_TEMP_SNAPSHOT = None
PRIVATE_STAGE = "initial"


def checked_command(args, *, timeout=30):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True,
                            text=True, timeout=timeout, check=False)
    require(result.returncode == 0, "PREREQUISITE")
    return result.stdout.strip()


def verified_inputs(assets_root):
    """Check immutable candidate bytes before using them as container inputs."""
    require(source_identity()["controller_digest"] == CONTROLLER_DIGEST, "IDENTITY")
    manifest = assets_root / "assets/assets.json"
    result = assets_root / "result.json"
    require(digest(manifest) == ASSETS_DIGEST, "IDENTITY")
    published = json.loads(result.read_text())
    require(published == {"schema": "alopex.native-assets/v1", "status": "passed",
                          "source": {"commit": PRODUCT,
                                     "tree": "99de240db970a91057ea60852bd2688c1d879fc8",
                                     "profile": "release"},
                          "assets_digest": ASSETS_DIGEST,
                          "performance_qualification": False}, "IDENTITY")
    checked = verify_assets(manifest, "linux/amd64")
    require(checked["source_commit"] == PRODUCT and checked["harness_commit"] == HARNESS,
            "IDENTITY")
    by_role = {item["role"]: assets_root / "assets" / item["path"]
               for item in checked["files"]}
    require(elf_signature(by_role["server"]) == (2, 1, 62) and
            elf_signature(by_role["library"]) == (2, 1, 62) and
            by_role["wheel"].name == "alopex-0.8.17-cp38-abi3-linux_x86_64.whl",
            "IDENTITY")
    with zipfile.ZipFile(by_role["wheel"]) as wheel:
        shared = [name for name in wheel.namelist()
                  if name.startswith("alopex/") and name.endswith(".so")]
        require(shared, "IDENTITY")
        require(all(wheel.read(name)[:20][0:4] == b"\x7fELF" and
                    int.from_bytes(wheel.read(name)[:20][18:20], "little") == 62
                    for name in shared), "IDENTITY")
    require(digest(INPUT / "abi-image.lock.json") == LOCK_DIGEST and
            digest(INPUT / "seccomp-default.json") == POLICY_DIGEST, "IDENTITY")
    lock = json.loads((INPUT / "abi-image.lock.json").read_text())
    variant = lock["platforms"]["linux/amd64"]
    require(len(variant["files"]) == 62 and len(variant["packages"]) == 152 and
            all(item["url"].startswith("https://") and item["size"] > 0
                for item in variant["files"]), "IDENTITY")
    policy = json.loads((INPUT / "seccomp-default.json").read_text())
    require(policy["defaultAction"] == "SCMP_ACT_ERRNO" and policy["syscalls"], "POLICY")
    return manifest


def verified_controller():
    require(shutil.which("docker") == str(DOCKER_BINARY), "PREREQUISITE")
    trusted_executable(DOCKER_BINARY)
    identity = source_identity()
    expected = os.environ.get("EXPECTED_CONTROLLER_SHA")
    require(expected is not None and len(expected) == 40 and
            all(letter in "0123456789abcdef" for letter in expected) and
            identity["controller_commit"] == expected == os.environ.get("GITHUB_SHA") and
            identity["controller_digest"] == CONTROLLER_DIGEST, "IDENTITY")
    require(digest(INPUT / "abi-image.lock.json") == LOCK_DIGEST and
            digest(INPUT / "seccomp-default.json") == POLICY_DIGEST, "IDENTITY")


def discover_temp_entries(areas=(Path("/tmp"), Path("/var/tmp"))):
    """Inventory only top-level runner-owned or Alopex-named temp entries."""
    roots, snapshot = [], []
    try:
        for area in areas:
            if not area.exists():
                continue
            require(area.is_dir(), "MEASUREMENT")
            with os.scandir(area) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    if info.st_uid == os.getuid() or "alopex" in entry.name.lower():
                        actual = Path(entry.path).resolve(strict=True)
                        roots.append(actual)
                        snapshot.append([str(area), entry.name, info.st_dev, info.st_ino,
                                         info.st_uid, str(actual)])
    except OSError as error:
        raise Failure("MEASUREMENT", str(error)) from error
    return roots, sorted(snapshot)


def temp_scope():
    runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    require(all(Path(os.environ[key]).resolve(strict=True) == runner_temp
                for key in ("TMPDIR", "TMP", "TEMP")) and
            Path(tempfile.gettempdir()).resolve(strict=True) == runner_temp,
            "MEASUREMENT")
    return discover_temp_entries()


def verify_temp_snapshot(expected):
    _, observed = temp_scope()
    require(observed == expected, "MEASUREMENT")


def admissible_root(path, temporary_roots):
    """Include snapshotted temporary FIFOs/sockets without opening their streams."""
    try:
        if not path.is_absolute() or path.resolve(strict=True) != path:
            return False
        mode = path.lstat().st_mode
        return (stat.S_ISDIR(mode) or stat.S_ISREG(mode) or
                (str(path) in temporary_roots and
                 (stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode))))
    except (OSError, RuntimeError):
        return False


def checkout_growth(private):
    global PRIVATE_STAGE
    PRIVATE_STAGE = "checkout_receipt"
    receipt = read_json(private / "checkout-preflight.private.json", private=True)
    require(receipt.get("expected_controller_sha") == os.environ.get("EXPECTED_CONTROLLER_SHA"),
            "IDENTITY")
    root = Path(receipt["workspace_root"])
    require(root == Path(os.environ["GITHUB_WORKSPACE"]).parent and root.is_absolute() and
            not root.is_symlink(), "MEASUREMENT")
    PRIVATE_STAGE = "checkout_temp_snapshot"
    verify_temp_snapshot(receipt["temp_snapshot"])
    PRIVATE_STAGE = "checkout_commands"
    trusted_executable(DU_BINARY)
    trusted_executable(SUDO_BINARY)
    PRIVATE_STAGE = "checkout_root_shape"
    roots = [Path(name) for name in receipt["roots"]]
    docker_root = Path(receipt["docker_root"])
    temporary_roots = {row[-1] for row in receipt["temp_snapshot"]}
    require(docker_root in roots and all(admissible_root(item, temporary_roots)
                                         for item in roots),
            "MEASUREMENT")
    PRIVATE_STAGE = "checkout_workspace_coverage"
    require(any(root == item or root.is_relative_to(item) for item in roots), "MEASUREMENT")
    PRIVATE_STAGE = "checkout_root_overlap"
    require(all(not (first.is_relative_to(second) or second.is_relative_to(first))
                for index, first in enumerate(roots) for second in roots[index + 1:]),
            "MEASUREMENT")
    def measure(path):
        args = ([str(SUDO_BINARY), "-n"] if path == docker_root else []) + [
            str(DU_BINARY), "-sk" if path == docker_root else "-skL", "--", str(path)]
        result = subprocess.run(args, stdin=subprocess.DEVNULL,
                                capture_output=True, text=True,
                                timeout=60 if path == docker_root else 10, check=False)
        fields = result.stdout.strip().split("\t")
        require(result.returncode == 0 and not result.stderr and len(fields) == 2 and
                fields[0].isascii() and fields[0].isdecimal() and fields[1] == str(path),
                "MEASUREMENT")
        return int(fields[0]) * 1024
    PRIVATE_STAGE = "checkout_workspace_measurement"
    workspace_bytes = measure(root)
    PRIVATE_STAGE = "checkout_aggregate_measurement"
    aggregate = sum(measure(path) for path in roots)
    PRIVATE_STAGE = "checkout_capacity"
    require(0 <= workspace_bytes - receipt["allocated_bytes"] <= 1_000_000_000 and
            0 <= aggregate - receipt["aggregate_allocated_bytes"] <= 1_000_000_000 and
            aggregate + 7_500_000_000 < 40_000_000_000, "CAPACITY")


def gh_api(path, *, binary=False, timeout=45):
    """Read exact GitHub metadata/artifact bytes without logging the token."""
    trusted_executable(Path("/usr/bin/gh"))
    require(os.environ.get("GITHUB_REPOSITORY") == "alopex-db/alopex" and
            bool(os.environ.get("GH_TOKEN")), "PREREQUISITE")
    result = subprocess.run(["/usr/bin/gh", "api", "repos/alopex-db/alopex" + path],
                            stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=timeout, check=False)
    require(result.returncode == 0 and len(result.stdout) <=
            (60_000_000 if binary else 2_000_000), "PREREQUISITE")
    return result.stdout if binary else json.loads(result.stdout)


def acquire_assets(private, assets_root):
    """Verify run/artifact identity, then hash the original ZIP before extraction."""
    run = gh_api(f"/actions/runs/{ASSET_RUN}")
    require(run["status"] == "completed" and run["conclusion"] == "success" and
            run["head_sha"] == ASSET_RUN_HEAD and run["run_attempt"] == 1 and
            run["event"] == "workflow_dispatch", "IDENTITY")
    jobs = gh_api(f"/actions/runs/{ASSET_RUN}/jobs?per_page=100")
    require(jobs["total_count"] == len(jobs["jobs"]) == 1 and
            jobs["jobs"][0]["conclusion"] == "success" and
            all(step["conclusion"] == "success" for step in jobs["jobs"][0]["steps"]),
            "IDENTITY")
    listing = gh_api(f"/actions/runs/{ASSET_RUN}/artifacts?per_page=100")
    require(listing["total_count"] == len(listing["artifacts"]) == 1, "IDENTITY")
    artifact = listing["artifacts"][0]
    require(artifact["id"] == ASSET_ARTIFACT_ID and
            artifact["name"] == "native-product-assets" and
            artifact["digest"] == "sha256:" + ASSET_ZIP_DIGEST and
            artifact["workflow_run"]["head_sha"] == ASSET_RUN_HEAD and
            not artifact["expired"] and 0 < artifact["size_in_bytes"] < 60_000_000,
            "IDENTITY")
    raw = gh_api(f"/actions/artifacts/{ASSET_ARTIFACT_ID}/zip", binary=True, timeout=180)
    require(0 < len(raw) < 60_000_000 and
            hashlib.sha256(raw).hexdigest() == ASSET_ZIP_DIGEST, "IDENTITY")
    archive = private / "candidate.original.zip"
    archive.write_bytes(raw)
    archive.chmod(0o600)
    require(not assets_root.exists(), "STATE")
    private_dir(assets_root)
    with zipfile.ZipFile(archive) as package:
        entries = package.infolist()
        names = [entry.filename for entry in entries]
        require(0 < len(entries) <= 32 and len(names) == len(set(names)) and
                sum(entry.file_size for entry in entries) <= 268_435_456 and
                {"result.json", "private-evidence.cms", "assets/assets.json"} <= set(names),
                "ARCHIVE")
        for entry in entries:
            name = PurePosixPath(entry.filename)
            mode = entry.external_attr >> 16
            require(not name.is_absolute() and ".." not in name.parts and
                    "\\" not in entry.filename and
                    (entry.filename in ("result.json", "private-evidence.cms") or
                     entry.filename.startswith("assets/")) and
                    stat.S_IFMT(mode) != stat.S_IFLNK and
                    entry.file_size <= 268_435_456, "ARCHIVE")
            target = assets_root.joinpath(*name.parts)
            if entry.is_dir():
                private_dir(target)
                continue
            private_dir(target.parent)
            with package.open(entry) as source, target.open("xb") as output:
                copied = 0
                while chunk := source.read(1_048_576):
                    copied += len(chunk)
                    require(copied <= entry.file_size, "ARCHIVE")
                    output.write(chunk)
            target.chmod(0o600)
            require(copied == entry.file_size, "ARCHIVE")
    verified_inputs(assets_root)
    atomic_json(private / "acquisition.private.json", {
        "run": ASSET_RUN, "head": ASSET_RUN_HEAD,
        "artifact_id": ASSET_ARTIFACT_ID, "zip_sha256": ASSET_ZIP_DIGEST,
        "assets_digest": ASSETS_DIGEST})


def sibling_observation():
    """Prove live same-UID process I/O and child-list visibility before Docker."""
    with tempfile.TemporaryDirectory(prefix="alopex-804-io-") as directory:
        fixture = Path(directory) / "fixture"
        code = ("import os,sys,time; f=open(sys.argv[1],'wb'); f.write(b'x'*4096);"
                "f.flush(); os.fsync(f.fileno()); print('ready',flush=True); time.sleep(15)")
        child = subprocess.Popen([sys.executable, "-c", code, str(fixture)],
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 text=True)
        try:
            require(child.stdout is not None and child.stdout.readline().strip() == "ready",
                    "MEASUREMENT")
            proc = Path("/proc") / str(child.pid)
            values = {}
            for name in ("io", "stat", "status"):
                values[name] = (proc / name).read_text()
            values["exe"] = str((proc / "exe").resolve(strict=True))
            tasks = list((proc / "task").iterdir())
            require(tasks and all((task / "children").is_file() and
                                  (task / "children").read_text() is not None
                                  for task in tasks), "MEASUREMENT")
            counters = dict(line.split(":", 1) for line in values["io"].splitlines() if ":" in line)
            require(int(counters.get("write_bytes", "-1")) >= 4096, "MEASUREMENT")
            return {"pid": child.pid, "exe": values["exe"], "task_count": len(tasks),
                    "write_bytes": int(counters["write_bytes"]),
                    "status": "observed"}
        finally:
            child.terminate()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)


def admission_facts():
    """Collect only read-only host facts and a small same-UID I/O fixture."""
    require(sys.platform == "linux" and platform.machine() == "x86_64" and
            os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "PREREQUISITE")
    kernel = read_text(Path("/boot/config-" + platform.release()))
    lsm = read_text(Path("/sys/kernel/security/lsm"))
    apparmor = read_text(Path("/sys/module/apparmor/parameters/enabled"))
    require(kernel.get("status") == "read" and "CONFIG_TASK_IO_ACCOUNTING=y" in kernel["value"] and
            lsm.get("status") == "read" and "apparmor" in lsm["value"].split(",") and
            apparmor.get("status") == "read" and apparmor["value"].strip() == "Y", "PREREQUISITE")
    sibling = sibling_observation()
    dmi = read_text(Path("/sys/class/dmi/id/product_uuid"))
    mountinfo = read_text(Path("/proc/self/mountinfo"))
    require(dmi.get("status") == "read" and mountinfo.get("status") == "read",
            "PREREQUISITE")
    context = checked_command(["docker", "context", "show"])
    inspection = json.loads(checked_command(["docker", "context", "inspect", context]))[0]
    endpoint = inspection["Endpoints"]["docker"]["Host"]
    require(endpoint.startswith("unix://"), "PREREQUISITE")
    docker_info = json.loads(checked_command(["docker", "info", "--format", "{{json .}}"]))
    require(docker_info.get("ServerVersion") == EXPECTED_DOCKER_VERSION and
            docker_info.get("OSType") == "linux" and docker_info.get("Architecture") == "x86_64" and
            docker_info.get("CgroupVersion") == "2" and
            "name=apparmor" in docker_info.get("SecurityOptions", []) and
            any(value.startswith("name=seccomp,") for value in docker_info.get("SecurityOptions", [])) and
            docker_info.get("NCPU", 0) >= 2, "PREREQUISITE")
    docker_root = Path(docker_info["DockerRootDir"])
    require(docker_root.is_absolute() and docker_root.is_dir() and
            str(docker_root.resolve(strict=True)) == str(docker_root), "MEASUREMENT")
    cpu_set = sorted(os.sched_getaffinity(0))
    require(len(cpu_set) >= 2, "PREREQUISITE")
    memory_limit = Path("/sys/fs/cgroup/memory.max").read_text().strip()
    require(memory_limit == "max" or int(memory_limit) >= 1_610_612_736, "PREREQUISITE")
    temp_roots, temp_snapshot = temp_scope()
    temporary_roots = {str(root) for root in temp_roots}
    roots = [Path(os.environ["GITHUB_WORKSPACE"]).parent, Path.home(),
             Path(os.environ["RUNNER_TEMP"]), docker_root, *temp_roots]
    roots = list(dict.fromkeys(str(root.resolve(strict=True)) for root in roots))
    require(roots.count(str(docker_root)) == 1, "MEASUREMENT")
    root_metadata = []
    for root in roots:
        entry = Path(root)
        metadata = entry.lstat()
        require(admissible_root(entry, temporary_roots), "MEASUREMENT")
        root_metadata.append({"path": root, "uid": metadata.st_uid, "gid": metadata.st_gid,
                              "mode": stat.S_IMODE(metadata.st_mode), "dev": metadata.st_dev,
                              "ino": metadata.st_ino})
        if entry == docker_root:
            require(metadata.st_uid == 0 and not metadata.st_mode & 0o022, "POLICY")
    storage = {"roots": roots, "runtime_storage_roots": [str(docker_root)],
               "readonly_sudo_du_root": str(docker_root), "stop_bytes": 40_000_000_000,
               "reserve_bytes": 1_000_000_000, "poll_seconds": 10,
               "readonly_sudo_du_timeout_seconds": 60,
               "planned_bytes": PLANNED}
    baseline = Budget(storage).used()
    # Evidence encryption adds a final bounded copy after envctl collection.
    require(baseline + sum(PLANNED.values()) + PLANNED["collect"] +
            storage["reserve_bytes"] <
            storage["stop_bytes"], "CAPACITY")
    disk = os.statvfs(docker_root)
    require(disk.f_bavail * disk.f_frsize > sum(PLANNED.values()) +
            PLANNED["collect"] +
            storage["reserve_bytes"], "CAPACITY")
    return {"context": context, "docker_info": docker_info, "storage": storage,
            "baseline_bytes": baseline, "cpus": cpu_set[:2], "sibling": sibling,
            "kernel": kernel, "lsm": lsm, "apparmor": apparmor,
            "dmi": dmi, "mountinfo": mountinfo, "root_metadata": root_metadata,
            "temp_snapshot": temp_snapshot}


def create_profile(private, manifest, facts):
    lock = private / "image.lock.json"
    policy = private / "seccomp.json"
    shutil.copyfile(INPUT / "abi-image.lock.json", lock)
    shutil.copyfile(INPUT / "seccomp-default.json", policy)
    lock.chmod(0o600)
    policy.chmod(0o600)
    profile = {"schema": "alopex.test-env-profile/v1",
               "run_id": "hosted-804-" + os.environ["GITHUB_RUN_ID"],
               "docker_context": facts["context"], "platform": "linux/amd64",
               "execution_mode": "native", "image_lock": str(lock), "assets": str(manifest),
               "private_output": str(private / "runs"), "cache": str(private / "cache"),
               "cpu": {"server": [facts["cpus"][0]], "client": [facts["cpus"][1]]},
               "limits": {"memory_bytes": 1_610_612_736, "swap_bytes": 0, "pids": 256,
                          "case_seconds": 60, "run_seconds": 900, "stop_seconds": 10},
               "storage": facts["storage"],
               "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                            "seccomp_profile": str(policy), "seccomp_sha256": POLICY_DIGEST}}
    path = private / "profile.json"
    atomic_json(path, profile)
    path.chmod(0o600)
    load_profile(path)
    image_lock(profile)
    Docker(profile).prerequisites()
    Docker(profile).policy()
    return path


def same_runner(previous, current):
    require(current["context"] == previous["context"] and
            current["docker_info"]["DockerRootDir"] ==
            previous["docker_info"]["DockerRootDir"] and
            current["dmi"] == previous["dmi"] and
            current["kernel"] == previous["kernel"] and
            current["baseline_bytes"] <= previous["baseline_bytes"] + 1_000_000_000,
            "IDENTITY")
    require(current["temp_snapshot"] == previous["temp_snapshot"], "MEASUREMENT")


def envctl(profile, *args):
    if EXPECTED_TEMP_SNAPSHOT is not None and args[0] not in ("collect", "clean"):
        verify_temp_snapshot(EXPECTED_TEMP_SNAPSHOT)
    command = [sys.executable, "-B", str(REPO / "scripts/test-env/envctl.py"),
               *args, "--profile", str(profile)]
    timeout = 2000 if args and args[0] == "accept" else 960
    result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                            text=True, timeout=timeout, check=False)
    try:
        output = json.loads(result.stdout)
    except ValueError as error:
        raise Failure("COMMAND") from error
    require(result.returncode == 0 and output == {"status": "passed", "error_code": None},
            output.get("error_code") if output.get("error_code") in
            ("CAPACITY", "MEASUREMENT", "POLICY", "IDENTITY", "PREREQUISITE", "TIMEOUT")
            else "COMMAND")
    if EXPECTED_TEMP_SNAPSHOT is not None and args[0] not in ("collect", "clean"):
        verify_temp_snapshot(EXPECTED_TEMP_SNAPSHOT)


def try_collect_clean(profile, *, probe=False):
    suffix = "-probe" if probe else ""
    config = json.loads(Path(profile).read_text())
    root = Path(config["private_output"]) / (config["run_id"] + suffix)
    if not (root / "run.json").exists():
        return []
    try:
        phase = read_json(root / "run.json", private=True)["phase"]
        if phase == "deleted":
            return []
        flags = ["--probe"] if probe else []
        if phase != "collected":
            envctl(profile, "collect", *flags)
        envctl(profile, "clean", *flags)
        return []
    except BaseException as error:
        # Retain resources when stop or collection could not be verified.
        return [{"phase": "probe" if probe else "accept",
                 "error_code": error.code if isinstance(error, Failure) else "INTERNAL",
                 "detail": repr(error)}]


def execute(profile):
    envctl(profile, "doctor", "--phase", "preflight")
    envctl(profile, "fetch")
    envctl(profile, "build")
    primary = None
    try:
        envctl(profile, "doctor", "--phase", "runtime")
        envctl(profile, "collect", "--probe")
        envctl(profile, "clean", "--probe")
        envctl(profile, "accept")
        envctl(profile, "collect")
        envctl(profile, "export-public")
        result = json.loads(Path(profile).read_text())
        public = json.loads((Path(result["private_output"]) / result["run_id"] / "public.json").read_text())
        rows = public.get("cases", [])
        require(public.get("status") == "passed" and len(rows) == len(CASES) and
                {row["id"] for row in rows} == set(CASES) and
                all(row["status"] == "passed" for row in rows), "INCOMPLETE")
        envctl(profile, "clean")
        return public
    except BaseException as error:
        primary = error
        raise
    finally:
        errors = [*try_collect_clean(profile, probe=True), *try_collect_clean(profile)]
        if errors:
            config = json.loads(Path(profile).read_text())
            atomic_json(Path(config["private_output"]) / "cleanup-errors.private.json", errors)
            if primary is None:
                raise Failure("CLEANUP")


def encrypt_evidence(private, public, budget=None):
    private_dir(EXPORT)
    if budget is not None:
        budget.before("collect")
    files = [path for path in sorted(private.rglob("*")) if path.is_file()]
    require(sum(path.stat().st_size for path in files) <= 200_000_000, "CAPACITY")
    archive = private / "evidence.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        for path in files:
            bundle.add(path, arcname=str(path.relative_to(private)), recursive=False)
    encrypted = EXPORT / "private-evidence.cms"
    result = subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes256",
                             "-outform", "DER", "-in", str(archive), "-out", str(encrypted),
                             str(CERT)], capture_output=True, timeout=60, check=False)
    require(result.returncode == 0 and encrypted.is_file(), "COMMAND")
    archive.unlink()
    if EXPECTED_TEMP_SNAPSHOT is not None:
        verify_temp_snapshot(EXPECTED_TEMP_SNAPSHOT)
    if budget is not None:
        budget.check()
    (EXPORT / "public.json").write_text(json.dumps(public, sort_keys=True) + "\n")


def main(argv=None):
    global EXPECTED_TEMP_SNAPSHOT, PRIVATE_STAGE
    argv = sys.argv[1:] if argv is None else argv
    if argv not in (["--admission-only"], ["--acquire-assets"], ["--execute"]):
        print('{"case":"HOST_QUALIFICATION","outcome":"invalid_phase"}')
        return 1
    admission_only = argv == ["--admission-only"]
    private = private_dir(Path(os.environ["RUNNER_TEMP"]) / "alopex-804-qualification")
    public = {"schema": "alopex.hosted-test-env-qualification/v1",
              "case": "HOST_QUALIFICATION", "outcome": "failed", "error_code": "INCOMPLETE"}
    try:
        assets_root = Path(os.environ["ALOPEX_804_ASSETS"])
        PRIVATE_STAGE = "controller_identity"
        verified_controller()
        if admission_only:
            checkout_growth(private)
            PRIVATE_STAGE = "admission_facts"
            facts = admission_facts()
            EXPECTED_TEMP_SNAPSHOT = facts["temp_snapshot"]
            atomic_json(private / "admission.private.json", facts)
            create_profile(private, assets_root / "assets/assets.json", facts)
            public = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "admitted_not_qualified",
                      "error_code": None}
        elif argv == ["--acquire-assets"]:
            previous = read_json(private / "admission.private.json", private=True)
            require((private / "profile.json").is_file(), "PREREQUISITE")
            facts = admission_facts()
            same_runner(previous, facts)
            EXPECTED_TEMP_SNAPSHOT = previous["temp_snapshot"]
            acquire_assets(private, assets_root)
            verify_temp_snapshot(previous["temp_snapshot"])
            public = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "assets_verified_not_qualified",
                      "error_code": None, "asset_digest": ASSETS_DIGEST}
        else:
            previous = read_json(private / "admission.private.json", private=True)
            profile = private / "profile.json"
            require(profile.is_file(), "PREREQUISITE")
            acquisition = read_json(private / "acquisition.private.json", private=True)
            require(acquisition == {"run": ASSET_RUN, "head": ASSET_RUN_HEAD,
                                    "artifact_id": ASSET_ARTIFACT_ID,
                                    "zip_sha256": ASSET_ZIP_DIGEST,
                                    "assets_digest": ASSETS_DIGEST} and
                    digest(private / "candidate.original.zip") == ASSET_ZIP_DIGEST,
                    "IDENTITY")
            manifest = verified_inputs(assets_root)
            current = admission_facts()
            facts = current
            same_runner(previous, current)
            EXPECTED_TEMP_SNAPSHOT = previous["temp_snapshot"]
            configured = load_profile(profile)
            require(configured["assets"] == str(manifest) and
                    configured["storage"] == previous["storage"], "IDENTITY")
            accepted = execute(profile)
            final_bytes = Budget(current["storage"]).used()
            verify_temp_snapshot(previous["temp_snapshot"])
            atomic_json(private / "storage-after.private.json", {"allocated_bytes": final_bytes})
            require(final_bytes < 40_000_000_000, "CAPACITY")
            public = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "passed",
                      "error_code": None, "source_commit": PRODUCT,
                      "asset_digest": ASSETS_DIGEST,
                      "cases": [{"id": row["id"], "status": row["status"],
                                 "error_code": row["error_code"]}
                                for row in accepted["cases"]]}
        return_code = 0
    except BaseException as error:
        code = error.code if isinstance(error, Failure) else "INTERNAL"
        public["error_code"] = code
        atomic_json(private / "failure.private.json", {"error_code": code,
                                                        "detail": repr(error),
                                                        "stage": PRIVATE_STAGE})
        return_code = 1
    try:
        encrypt_evidence(private, public,
                         Budget(facts["storage"]) if "facts" in locals() else None)
    except BaseException as error:
        private_dir(EXPORT)
        (EXPORT / "public.json").write_text(json.dumps({
            "schema": "alopex.hosted-test-env-qualification/v1",
            "case": "HOST_QUALIFICATION", "outcome": "evidence_failed",
            "error_code": error.code if isinstance(error, Failure) else "INTERNAL"},
            sort_keys=True) + "\n")
        print('{"case":"HOST_QUALIFICATION","outcome":"evidence_failed"}')
        return 1
    print(json.dumps({"case": "HOST_QUALIFICATION", "outcome": public["outcome"],
                      "error_code": public["error_code"]}, sort_keys=True))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
