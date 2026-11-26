"""Join one qualified hosted job to one audited public finish."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import time

import issue681_hosted_acquire_all as acquire
import issue681_hosted_build_guard as capacity
import issue681_hosted_build_join as build_join
import issue681_hosted_identity as identity_gate
import issue681_hosted_measure_join as measure_join
import issue681_hosted_qualify as qualify
import issue681_hosted_storage as scoped_storage
import issue681_hosted_rust_preflight as rust
import issue681_public_admission as public_admission
import issue681_public_transport as transport

ERROR = "PUBLIC_JOB"
CASE = "HNSW_PUBLIC_JOB"
SOFT_BUDGET_SECONDS = 160 * 60
ACQUIRE_BUDGET_SECONDS = 25 * 60
HOSTED_RUNNER_KIND = "github_hosted_ubuntu2404"


def require(value):
    if not value:
        raise ValueError(ERROR)


def _empty_private(path):
    build_join._private_dir(path)
    require(not any(path.iterdir()))


def _owned(path, parent, prefix, identity):
    require(path.parent == parent and path.name.startswith(prefix))
    if identity is None:
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700)
        identity = (info.st_dev, info.st_ino)
    acquire._remove_owned(path, identity)


def _public_snapshot(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size <= transport.MAX_BYTES)
        raw = stream.read(transport.MAX_BYTES + 1)
        require(len(raw) == info.st_size)
        return raw


def _execute(env, workspace: Path, task_parent: Path, home_parent: Path,
             profile: dict, finish: str, qualify_job):
    """Return a private, audited public-file handle; never print host data."""
    owned = []
    try:
        require(type(env) is dict and isinstance(workspace, Path) and
                workspace.is_absolute() and workspace.is_dir() and
                not workspace.is_symlink() and
                workspace.resolve(strict=True) == workspace and
                workspace == qualify.REPO and
                isinstance(task_parent, Path) and
                isinstance(home_parent, Path) and
                finish in ("commit", "rollback") and
                build_join._disjoint((workspace, task_parent,
                                      home_parent)) and
                Path.home() in home_parent.parents)
        _empty_private(task_parent)
        _empty_private(home_parent)
        manifest = scoped_storage.make_manifest(
            workspace, Path(os.environ.get("RUNNER_TEMP", str(task_parent))),
            task_parent=task_parent, home_parent=home_parent)
        outcome, status = qualify_job(env)
        require(status == 0 and outcome ==
                {"case": qualify.CASE, "outcome": "passed"})
        outcome, status = rust.run()
        require(status == 0 and outcome ==
                {"case": rust.CASE, "outcome": "passed"})

        task = Path(tempfile.mkdtemp(prefix="issue681-job-", dir=task_parent))
        owned.append((task, task_parent, "issue681-job-", None))
        owned[-1] = (*owned[-1][:3], acquire._identity(task))
        home = Path(tempfile.mkdtemp(prefix="issue681-home-", dir=home_parent))
        owned.append((home, home_parent, "issue681-home-", None))
        owned[-1] = (*owned[-1][:3], acquire._identity(home))
        for name in ("acquire", "build", "measure"):
            (task / name).mkdir(mode=0o700)
            _empty_private(task / name)
        acquired = acquire.acquire_all(workspace, task / "acquire", home,
                                       manifest=manifest)
        built = build_join.build(acquired, workspace, task / "build")
        result = measure_join.measure(acquired, built, workspace,
                                      task / "measure", profile, finish)
        public = result["public"]
        require(type(result) is dict and result["finish"] == finish and
                result["guard"] is acquired["guard"] and
                isinstance(public, Path) and public.is_relative_to(task) and
                public.is_file() and not public.is_symlink() and
                public.stat().st_uid == os.geteuid())
        raw = _public_snapshot(public)
        digest = hashlib.sha256(raw).hexdigest()
        pins = built["pair"]["pins"]
        require(transport.validate(raw, expected_sha256=digest,
                                   expected_pins=pins, finish=finish) == raw)
        acquired["guard"]()
        return {"case": CASE, "outcome": "passed", "public": public,
                "public_sha256": digest, "pins": copy.deepcopy(pins),
                "task_root": task, "home_root": home}
    except BaseException:
        for path, parent, prefix, identity in reversed(owned):
            try:
                _owned(path, parent, prefix, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None


def execute(env, workspace: Path, task_parent: Path, home_parent: Path,
            profile: dict, finish: str):
    """Retain the Draft-PR qualification route for its original use case."""
    return _execute(env, workspace, task_parent, home_parent, profile, finish,
                    qualify.run)


def execute_hosted_measure(env, workspace: Path, task_parent: Path,
                           home_parent: Path, profile: dict, finish: str, *,
                           expected_job: str = "hnsw_release_measure",
                           soft_budget_seconds: int = SOFT_BUDGET_SECONDS,
                           acquire_budget_seconds: int = ACQUIRE_BUDGET_SECONDS,
                           on_stage=None,
                           on_substage=None):
    """Execute one admitted GitHub-hosted VM #681 measurement finish.

    UC-V0817-681-HOSTED-MEASURE-WIRE-01 / UC-V0817-681-ACQUIRE-GUARD-DIAG-01:
    1. Caller has already passed identity_gate.run(env, expected_job=...).
    2. Install pinned Rust 1.90.0 --profile minimal under HostStorageGuard.
    3. Acquire exact pinned sources, Nim parser, and cargo vendor --locked.
    4. Build both release arms offline and verify vendor/source immutability.
    5. Run 1k/10k/100k paired measurement under Admission + HostStorageGuard.
    6. Snapshot and transport.validate() canonical public JSON.
    """
    owned = []
    stage = "init"
    started = time.monotonic()

    def notify(stage_name):
        if on_stage is not None:
            on_stage(stage_name)

    def check_budget():
        require(time.monotonic() - started < soft_budget_seconds)

    try:
        require(type(env) is dict and isinstance(workspace, Path) and
                workspace.is_absolute() and workspace.is_dir() and
                not workspace.is_symlink() and
                workspace.resolve(strict=True) == workspace and
                workspace == qualify.REPO and
                isinstance(task_parent, Path) and
                isinstance(home_parent, Path) and
                finish in ("commit", "rollback") and
                expected_job in identity_gate.ALLOWED_JOBS and
                build_join._disjoint((workspace, task_parent,
                                      home_parent)) and
                Path.home() in home_parent.parents)
        _empty_private(task_parent)
        _empty_private(home_parent)
        manifest = scoped_storage.make_manifest(
            workspace, Path(env.get("RUNNER_TEMP", str(task_parent))),
            task_parent=task_parent, home_parent=home_parent)

        task = Path(tempfile.mkdtemp(prefix="issue681-job-", dir=task_parent))
        owned.append((task, task_parent, "issue681-job-", acquire._identity(task)))
        home = Path(tempfile.mkdtemp(prefix="issue681-home-", dir=home_parent))
        owned.append((home, home_parent, "issue681-home-", acquire._identity(home)))
        for name in ("acquire", "build", "measure"):
            (task / name).mkdir(mode=0o700)
            _empty_private(task / name)

        stage = "toolchain_install"
        check_budget()
        install_guard = capacity.HostStorageGuard(workspace, manifest=manifest)
        rust.install_pinned_minimal(
            install_guard, task / "rustup-install.private.log")
        notify("toolchain_install")

        stage = "acquire"
        check_budget()
        acquire_kwargs = {"manifest": manifest, "vendor_mode": "cargo_vendor_locked"}
        if on_substage is not None:
            acquire_kwargs["on_substage"] = on_substage
            acquire_kwargs["stage_budget_seconds"] = acquire_budget_seconds
        acquired = acquire.acquire_all(
            workspace, task / "acquire", home, **acquire_kwargs)
        notify("acquire")

        stage = "build"
        check_budget()
        built = build_join.build(acquired, workspace, task / "build")
        notify("build")

        stage = "measure"
        check_budget()
        result = measure_join.measure(acquired, built, workspace,
                                      task / "measure", profile, finish)
        notify("measure")

        stage = "audit"
        public = result["public"]
        require(type(result) is dict and result["finish"] == finish and
                result["guard"] is acquired["guard"] and
                isinstance(public, Path) and public.is_relative_to(task) and
                public.is_file() and not public.is_symlink() and
                public.stat().st_uid == os.geteuid())
        raw = _public_snapshot(public)
        digest = hashlib.sha256(raw).hexdigest()
        pins = built["pair"]["pins"]
        require(transport.validate(raw, expected_sha256=digest,
                                   expected_pins=pins, finish=finish) == raw)
        acquired["guard"]()
        notify("audit")
        return {"case": CASE, "outcome": "passed", "public": public,
                "public_bytes": raw, "public_sha256": digest,
                "pins": copy.deepcopy(pins), "finish": finish,
                "task_root": task, "home_root": home}
    except BaseException:
        for path, parent, prefix, identity in reversed(owned):
            try:
                _owned(path, parent, prefix, identity)
            except BaseException:
                pass
        raise ValueError(f"{ERROR}:{stage}") from None


def run_hosted_measure_cli(env=None):
    """CLI entry point for .github/workflows/alopex-performance.yml hnsw_release_measure."""
    env = dict(os.environ if env is None else env)
    stages = {
        "admission": "skipped_prereq",
        "toolchain_install": "skipped_prereq",
        "acquire": "skipped_prereq",
        "build": "skipped_prereq",
        "measure": "skipped_prereq",
        "audit": "skipped_prereq",
        "export": "skipped_prereq",
    }
    finish = env.get("HNSW_FINISH", "")
    if finish not in ("commit", "rollback"):
        print(json.dumps({"case": CASE, "outcome": "failed",
                          "error_code": "INVALID_FINISH", "stages": stages},
                         sort_keys=True), flush=True)
        return 1

    admission_outcome, admission_status = identity_gate.run(
        env, expected_job="hnsw_release_measure")
    print(json.dumps(admission_outcome, sort_keys=True), flush=True)
    if admission_status != 0 or admission_outcome.get("outcome") != "passed":
        stages["admission"] = "failed"
        print(json.dumps({"case": CASE, "outcome": "failed",
                          "error_code": "ADMISSION", "stages": stages},
                         sort_keys=True), flush=True)
        return 1
    stages["admission"] = "passed"

    runner_temp = Path(env["RUNNER_TEMP"]).resolve(strict=True)
    workspace = Path(env["GITHUB_WORKSPACE"]).resolve(strict=True)
    home_root = Path.home().resolve(strict=True)
    task_parent = Path(tempfile.mkdtemp(prefix="alopex-681-task-",
                                        dir=runner_temp))
    os.chmod(task_parent, 0o700)
    home_parent = Path(tempfile.mkdtemp(prefix=".alopex-681-home-",
                                        dir=home_root))
    os.chmod(home_parent, 0o700)

    acquire_substages = {}
    failure_detail = {"substage": None, "detail_code": None}

    def on_stage(name):
        stages[name] = "passed"
        print(json.dumps({"case": CASE, "stage": name, "status": "passed"},
                         sort_keys=True), flush=True)

    def on_substage(name, status, detail_code, elapsed_s):
        acquire_substages[name] = status
        payload = {
            "case": CASE,
            "stage": "acquire",
            "substage": name,
            "status": status,
            "elapsed_s": elapsed_s,
        }
        if status == "failed":
            safe_code = (detail_code if detail_code in acquire.ALLOWED_DETAIL_CODES
                         else acquire.ERROR)
            payload["detail_code"] = safe_code
            failure_detail["substage"] = name
            failure_detail["detail_code"] = safe_code
        print(json.dumps(payload, sort_keys=True), flush=True)

    profile = {
        "schema": public_admission.SCHEMA,
        "runner_kind": env.get("RUNNER_KIND", HOSTED_RUNNER_KIND),
        "cpu_count": 4,
        "additional_bytes": qualify.RESERVE_BYTES,
    }
    try:
        result = execute_hosted_measure(
            env, workspace, task_parent, home_parent, profile, finish,
            expected_job="hnsw_release_measure", on_stage=on_stage,
            on_substage=on_substage)
        export_dir = runner_temp / "issue681-public-export"
        export_dir.mkdir(mode=0o700, exist_ok=True)
        os.chmod(export_dir, 0o700)
        export_file = export_dir / f"issue681-public-{finish}.json"
        if export_file.exists():
            export_file.unlink()
        fd = os.open(export_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(result["public_bytes"])
            stream.flush()
            os.fsync(stream.fileno())
        stages["export"] = "passed"
        print(json.dumps({
            "case": CASE,
            "outcome": "passed",
            "finish": finish,
            "public_sha256": result["public_sha256"],
            "stages": stages,
        }, sort_keys=True), flush=True)
        return 0
    except ValueError as exc:
        token = str(exc)
        failed_stage = (token.split(":", 1)[1]
                        if token.startswith(ERROR + ":") else "unknown")
        if failed_stage in stages:
            stages[failed_stage] = "failed"
        summary = {
            "case": CASE,
            "outcome": "failed",
            "error_code": ERROR,
            "failed_stage": failed_stage,
            "stages": stages,
        }
        if acquire_substages:
            summary["acquire_substages"] = acquire_substages
        if failure_detail["substage"] is not None:
            summary["failed_substage"] = failure_detail["substage"]
        if failure_detail["detail_code"] is not None:
            summary["detail_code"] = failure_detail["detail_code"]
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 1
    except BaseException:
        print(json.dumps({
            "case": CASE,
            "outcome": "failed",
            "error_code": ERROR,
            "failed_stage": "export",
            "stages": stages,
        }, sort_keys=True), flush=True)
        return 1
    finally:
        for directory in (task_parent, home_parent):
            try:
                if directory.exists():
                    shutil.rmtree(directory)
            except BaseException:
                pass


if __name__ == "__main__":
    raise SystemExit(run_hosted_measure_cli())
