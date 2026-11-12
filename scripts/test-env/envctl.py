#!/usr/bin/env python3
"""Build and operate SQL acceptance containers from verified offline inputs."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.parse
import urllib.error
import urllib.request
import contextlib
import hashlib
import time
import traceback

from capacity import Budget
from common import (ERRORS, Failure, atomic_json, contained, digest, load_profile,
                    private_dir, public_result, read_json, require, sha256, verify_assets, host_lock,
                    record_cleanup_error)
from docker_backend import Docker, OWNER
from lifecycle import Run

HERE = Path(__file__).resolve().parent


def fixed_error_site(error):
    """Private source location without exception text or runner paths."""
    for frame in reversed(traceback.extract_tb(error.__traceback__)):
        source = Path(frame.filename)
        if source.is_relative_to(HERE) and source.name != "common.py":
            return {"module": source.name, "function": frame.name, "line": frame.lineno}
    return None


class PrivateParser(argparse.ArgumentParser):
    def error(self, message):
        print(json.dumps({"status": "failed", "error_code": "INPUT"}))
        raise SystemExit(64)


def source_identity():
    repo = HERE.parents[1]
    pending = subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain", "--", "scripts/test-env"], text=True)
    require(not pending.strip(), "PREREQUISITE")
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    files = [*HERE.glob("*.py"), *HERE.glob("*.sh"), *HERE.glob("tests/*.py"), HERE / "Dockerfile",
             HERE / ".dockerignore", HERE / "requirements.lock"]
    content = {str(path.relative_to(HERE)): digest(path) for path in sorted(files)}
    return {"controller_commit": commit,
            "controller_digest": hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()}


def image_lock(profile):
    lock = read_json(profile["image_lock"])
    require(lock.get("schema") == "alopex.test-env-image/v1")
    require("@sha256:" in lock["base"])
    sha256(lock["base"].rsplit("@sha256:", 1)[1])
    variant = lock.get("platforms", {}).get(profile["platform"])
    require(variant is not None, "PREREQUISITE")
    require(variant.get("files") and variant.get("packages"), "PREREQUISITE")
    names = set()
    for item in variant["files"]:
        require(item["path"] not in names and item["size"] > 0)
        contained(Path(profile["cache"]), item["path"])
        names.add(item["path"])
        sha256(item["sha256"])
        require(urllib.parse.urlsplit(item["url"]).scheme == "https")
    return lock, variant


class LockedRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, hosts):
        self.hosts = set(hosts)

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlsplit(newurl)
        require(parsed.scheme == "https" and parsed.hostname in self.hosts, "POLICY")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def fetch(profile, docker, budget):
    budget.before("fetch")
    lock, variant = image_lock(profile)
    cache = private_dir(profile["cache"])
    opener = urllib.request.build_opener(LockedRedirect(variant["allowed_hosts"]))
    for item in variant["files"]:
        destination = contained(cache, item["path"])
        private_dir(destination.parent)
        if destination.exists() and digest(destination) == item["sha256"]:
            continue
        parsed = urllib.parse.urlsplit(item["url"])
        require(parsed.hostname in variant["allowed_hosts"], "POLICY")
        temporary = destination.with_name(destination.name + ".pending")
        for attempt in range(3):
            received = 0
            try:
                with opener.open(item["url"], timeout=30) as response, temporary.open("wb") as stream:
                    temporary.chmod(0o600)
                    while block := response.read(1024 * 1024):
                        received += len(block)
                        require(received <= item["size"], "IDENTITY")
                        budget.check()
                        stream.write(block)
                require(received == item["size"] and digest(temporary) == item["sha256"], "IDENTITY")
                temporary.replace(destination)
                break
            except (TimeoutError, urllib.error.URLError) as error:
                timed_out = isinstance(error, TimeoutError) or isinstance(
                    getattr(error, "reason", None), TimeoutError)
                if not timed_out or attempt == 2:
                    raise
                budget.check()
                time.sleep(1 << attempt)
            finally:
                temporary.unlink(missing_ok=True)
    budget.run([*docker.prefix, "pull", "--platform", profile["platform"], lock["base"]],
               timeout=profile["limits"]["run_seconds"], log=cache / "fetch.log")


def build(profile, docker, budget):
    budget.before("build")
    lock, variant = image_lock(profile)
    source = source_identity()
    cache = private_dir(profile["cache"])
    context = cache / "context"
    if context.exists():
        shutil.rmtree(context)
    private_dir(context)
    try:
        for path in HERE.glob("*.py"):
            shutil.copyfile(path, context / path.name)
        shutil.copytree(HERE / "tests", context / "tests", ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(HERE / "Dockerfile", context / "Dockerfile")
        for item in variant["files"]:
            source_file = contained(cache, item["path"])
            require(source_file.is_file() and source_file.stat().st_size == item["size"] and
                    digest(source_file) == item["sha256"], "IDENTITY")
            target = contained(context, item["path"])
            private_dir(target.parent)
            shutil.copyfile(source_file, target)
        atomic_json(context / "image-manifest.json", variant)
        budget.run([*docker.prefix, "build", "--network=none", "--pull=false",
                    "--platform", profile["platform"], "--build-arg", "BASE=" + lock["base"],
                    "--iidfile", str(cache / "image.id"), str(context)],
                   timeout=profile["limits"]["run_seconds"], log=cache / "build.log")
        image = (cache / "image.id").read_text().strip()
        require(image.startswith("sha256:"))
        sha256(image[7:])
        atomic_json(cache / "image.json", {"id": image, "lock_digest": digest(profile["image_lock"]),
                                          "platform": profile["platform"], **source})
    finally:
        shutil.rmtree(context, ignore_errors=True)


def verified_image(profile, docker):
    docker.prerequisites()
    image = read_json(Path(profile["cache"]) / "image.json", private=True)
    require(image["lock_digest"] == digest(profile["image_lock"]) and
            image["platform"] == profile["platform"], "IDENTITY")
    require(image["controller_digest"] == source_identity()["controller_digest"], "IDENTITY")
    actual = json.loads(docker.call("image", "inspect", image["id"]))[0]
    actual_platform = actual["Os"] + "/" + actual["Architecture"]
    require(profile["platform"].startswith(actual_platform), "IDENTITY")
    return image["id"]


def runtime_request(profile, cases):
    return {"schema": "alopex.test-env-request/v1", "cases": cases,
            "cpu": profile["cpu"], "limits": profile["limits"],
            "security": {"lsm_kind": profile["security"]["lsm_kind"],
                         "lsm_profile": profile["security"]["lsm_profile"]}}


def stage(profile, docker, budget, run, image, inputs, work, cases):
    """Copy through a bounded, owned helper; no host bind mounts or sockets."""
    manifest = verify_assets(profile["assets"], profile["platform"])
    bundle = private_dir(run.root / "bundle")
    for item in manifest["files"]:
        target = contained(bundle, item["path"])
        private_dir(target.parent)
        shutil.copyfile(contained(Path(profile["assets"]).parent, item["path"]), target)
    atomic_json(bundle / "assets.json", manifest)
    atomic_json(bundle / "request.json", runtime_request(profile, cases))
    helper = "alopex-stage-" + run.run_id
    command = ["create", "--name", helper, "--pull=never", "--network=none", "--user", "root",
               "--cap-drop=ALL", "--cap-add=CHOWN", "--cap-add=DAC_OVERRIDE", "--cap-add=FOWNER",
               "--label", OWNER, "--label", "org.alopex.test-env.run=" + run.run_id,
               "--label", "org.alopex.test-env.token=" + run.record["ownership_token"],
               "--memory", str(profile["limits"]["memory_bytes"]),
               "--memory-swap", str(profile["limits"]["memory_bytes"]),
               "--pids-limit", str(profile["limits"]["pids"]),
               "--mount", f"type=volume,src={inputs},dst=/input,volume-nocopy",
               "--mount", f"type=volume,src={work},dst=/work,volume-nocopy",
               *docker.policy(), "--entrypoint", "python3", image, "/tool/stage.py"]
    run.intent("container", helper)
    identity = docker.call(*command)
    run.resource("container", helper, identity)
    primary = None
    try:
        budget.run([*docker.prefix, "cp", str(bundle) + "/.", helper + ":/input"],
                   timeout=profile["limits"]["case_seconds"], log=run.root / "stage.log",
                   cancel=lambda: docker.stop(helper))
        budget.run([*docker.prefix, "start", "--attach", helper],
                   timeout=profile["limits"]["case_seconds"], log=run.root / "stage.log",
                   cancel=lambda: docker.stop(helper))
        state = json.loads(docker.call("container", "inspect", helper))[0]["State"]
        require(state["ExitCode"] == 0 and not state["OOMKilled"], "CASE")
    except BaseException as error:
        primary = error
        raise
    finally:
        cleanup_failure = None
        for operation, action in (("stop_stage", lambda: docker.stop(helper)),
                                  ("remove_stage_copy", lambda: shutil.rmtree(bundle))):
            try:
                action()
            except BaseException as error:
                if primary is not None:
                    record_cleanup_error(primary, operation, error)
                else:
                    cleanup_failure = cleanup_failure or Failure("CLEANUP")
                    record_cleanup_error(cleanup_failure, operation, error)
        if cleanup_failure is not None:
            raise cleanup_failure


def execute(profile, docker, budget, cases, *, probe=False, expected_cases=None):
    budget.before("runtime" if probe else "accept")
    image = verified_image(profile, docker)
    assets = verify_assets(profile["assets"], profile["platform"])
    run_id = profile["run_id"] + ("-probe" if probe else "")
    run = Run(profile["private_output"], run_id).create({
        "image_digest": image[7:], "assets_digest": digest(profile["assets"]),
        "harness_digest": next(f["sha256"] for f in assets["files"] if f["role"] == "harness"),
        **source_identity()})
    run.record["expected_cases"] = list(expected_cases if expected_cases is not None else cases)
    run.save()
    name = "alopex-test-" + run_id
    primary = None
    try:
        inputs = docker.create_volume(run, "input")
        work = docker.create_volume(run, "work")
        stage(profile, docker, budget, run, image, inputs, work, cases)
        run.intent("container", name)
        identity = docker.call(*docker.arguments(run_id, image, inputs, work, run.record["ownership_token"]))
        run.resource("container", name, identity)
        docker.validate_container(name)
        run.transition("running")
        budget.run([*docker.prefix, "start", "--attach", name],
                   timeout=profile["limits"]["run_seconds"], log=run.root / "container.log",
                   cancel=lambda: docker.stop(name))
        live = json.loads(docker.call("container", "inspect", name))[0]
        atomic_json(run.root / "exit.json", live["State"])
        require(live["State"]["ExitCode"] == 0 and not live["State"]["OOMKilled"], "CASE")
    except BaseException as error:
        primary = error
        try:
            atomic_json(run.root / "failure.json", {"error_code": getattr(error, "code", "INTERRUPTED" if isinstance(error, KeyboardInterrupt) else "INTERNAL"),
                                                    "detail": error.detail if isinstance(error, Failure) else repr(error),
                                                    "cleanup_errors": getattr(error, "cleanup_errors", [])})
        except BaseException as diagnostic_error:
            record_cleanup_error(error, "persist_failure", diagnostic_error)
        raise
    finally:
        stopped = True
        cleanup_errors = []
        for item in run.record["resources"]:
            if item["kind"] == "container":
                try:
                    if item["id"]:
                        docker.stop(item["id"])
                    else:
                        stopped = False
                        cleanup_errors.append({"operation": "stop_container", "name": item["name"],
                                               "error_code": "IDENTITY"})
                except BaseException as error:
                    stopped = False
                    cleanup_errors.append({"operation": "stop_container", "name": item["name"],
                                           "error_code": getattr(error, "code", "CLEANUP"),
                                           "detail": error.detail if isinstance(error, Failure) else repr(error)})
        try:
            if cleanup_errors:
                atomic_json(run.root / "cleanup-errors.json", cleanup_errors)
            if stopped and run.record["phase"] in ("preparing", "running"):
                run.transition("stopped")
            elif primary is None:
                atomic_json(run.root / "failure.json", {"error_code": "CLEANUP", "cleanup_errors": cleanup_errors})
                raise Failure("CLEANUP")
        except BaseException as error:
            if primary is None:
                raise
            record_cleanup_error(primary, "persist_stop_state", error)
    return run


def recover(run, docker):
    """Reconcile interrupted creates only through persisted ownership intents."""
    if run.record["phase"] in ("collected", "deleted"):
        return
    for item in run.record["resources"]:
        live = docker.inspect(item)
        if live is None:
            # A timed-out create can publish after inspect. Keep the intent
            # unresolved until an owned identity can be bound and stopped.
            require(item["id"] is not None, "CLEANUP")
            continue
        require(live["labels"].get("org.alopex.test-env.run") == run.run_id and
                live["labels"].get("org.alopex.test-env.owner") == "envctl" and
                live["labels"].get("org.alopex.test-env.token") == run.record["ownership_token"], "CLEANUP")
        require(item["id"] in (None, live["id"]), "IDENTITY")
        if item["id"] is None:
            run.resource(item["kind"], item["name"], live["id"])
        if item["kind"] == "container":
            docker.stop(live["id"])
    if run.record["phase"] in ("preparing", "running"):
        if not (run.root / "failure.json").exists():
            atomic_json(run.root / "failure.json", {"error_code": "INTERRUPTED"})
        run.transition("stopped")


def collect(profile, docker, budget, run_id):
    budget.before("collect")
    run = Run(profile["private_output"], run_id).load()
    recover(run, docker)
    container = next((i for i in run.record["resources"]
                      if i["kind"] == "container" and i["name"] == "alopex-test-" + run_id), None)

    def copy(destination):
        if container and container["id"]:
            live = docker.inspect(container)
            require(live and live["id"] == container["id"] and not live["running"], "COLLECT")
            log = run.root / "collect.log"
            offset = log.stat().st_size if log.exists() else 0
            try:
                budget.run([*docker.prefix, "cp", container["id"] + ":/work/results/.", str(destination)],
                           timeout=profile["limits"]["case_seconds"], log=log)
            except Failure as error:
                # Capacity, measurement and timeout failures must retain their
                # original category; only a missing result directory has the
                # special recovery path below.
                atomic_json(run.root / "collect-copy-error.json", {
                    "error_code": error.code, "detail": error.detail,
                })
                if error.code != "COMMAND":
                    raise
                with log.open("rb") as stream:
                    stream.seek(offset)
                    message = stream.read(65536).decode("utf-8", errors="replace")
                missing = "Could not find the file /work/results" in message
                if not (missing and (run.root / "failure.json").exists()):
                    raise Failure("COLLECT", message) from error
                atomic_json(destination / "result.json", {"status": "failed", "cases": [],
                                                          "error_code": "INCOMPLETE"})
        else:
            # A failed stage has no result producer. Preserve controller evidence.
            require((run.root / "failure.json").exists(), "COLLECT")
            atomic_json(destination / "result.json", {"cases": [], "status": "failed",
                                                      "error_code": "INCOMPLETE"})
        if not (destination / "result.json").exists():
            # Startup can create the result directory, then be interrupted
            # before the supervisor writes its first report. A successful cp
            # of that empty/partial directory is still recoverable only when
            # the controller has already recorded a failed execution.
            require((run.root / "failure.json").exists(), "COLLECT")
            atomic_json(destination / "result.json", {"cases": [], "status": "failed",
                                                      "error_code": "INCOMPLETE"})
        for name in ("failure.json", "exit.json", "container.log", "stage.log", "collect-copy-error.json",
                     "cleanup-errors.json"):
            if (run.root / name).exists():
                shutil.copyfile(run.root / name, destination / name)
        if (run.root / "failure.json").exists():
            result = read_json(destination / "result.json")
            result["status"] = "failed"
            result["controller_error"] = read_json(run.root / "failure.json")["error_code"]
            atomic_json(destination / "result.json", result)
    run.collect(copy)


def acceptance(profile, docker, budget, cases):
    """R01 uses a fresh container and volumes; collect remains idempotent."""
    live_cases = [case for case in cases if case != "R01"]
    if not live_cases:
        live_cases = ["E01", "E02", "E04", "S01", "S02", "S03"]
    run = execute(profile, docker, budget, live_cases, expected_cases=cases if len(cases) > 1 else live_cases + (["R01"] if "R01" in cases else []))
    collect(profile, docker, budget, run.run_id)
    run.load()
    if "R01" not in cases:
        return
    repeat = dict(profile, run_id=profile["run_id"] + "-repeat")
    # Save the dependency before starting, so interrupted repeat runs remain
    # discoverable and are never silently orphaned by clean.
    run.record["repeat_run"] = repeat["run_id"]
    run.save()
    execute(repeat, Docker(repeat), Budget(repeat["storage"]), live_cases)
    collect(repeat, Docker(repeat), Budget(repeat["storage"]), repeat["run_id"])
    first = read_json(run.root / "collected/result.json")
    other = Run(profile["private_output"], repeat["run_id"]).load()
    second = read_json(other.root / "collected/result.json")
    require(run.record["identities"] == other.record["identities"], "IDENTITY")
    signature = lambda report: [(row["id"], row["status"], row.get("error_code")) for row in report["cases"]]
    require(first["status"] == second["status"] == "passed" and signature(first) == signature(second), "CASE")
    first["cases"].append({"id": "R01", "status": "passed", "error_code": None,
                           "evidence": {"first": digest(run.root / "collection.json"),
                                        "repeat": digest(other.root / "collection.json")}})
    atomic_json(run.root / "acceptance.json", first)
    run.record["acceptance_digest"] = digest(run.root / "acceptance.json")
    run.save()


def main(argv=None):
    parser = PrivateParser(description=__doc__)
    parser.add_argument("command", choices=("doctor", "resolve-lock", "fetch", "build", "accept", "run",
                                           "collect", "export-public", "clean"))
    parser.add_argument("--profile", required=True)
    parser.add_argument("--phase", choices=("preflight", "runtime"), default="preflight")
    parser.add_argument("--case")
    kind = parser.add_mutually_exclusive_group()
    kind.add_argument("--probe", action="store_true")
    kind.add_argument("--resolution", action="store_true")
    args = parser.parse_args(argv)
    output = None
    guard = contextlib.ExitStack()
    try:
        p = load_profile(args.profile)
        guard.enter_context(host_lock())
        output = private_dir(p["private_output"])
        docker = Docker(p)
        sudo_root = p["storage"].get("readonly_sudo_du_root")
        if sudo_root is not None:
            docker.prerequisites()
            info = json.loads(docker.call("info", "--format", "{{json .}}"))
            require(info.get("DockerRootDir") == sudo_root and info.get("OSType") == "linux",
                    "MEASUREMENT")
        budget = Budget(p["storage"])
        command = args.command
        require(not args.resolution or command in ("collect", "clean"))
        run_id = p["run_id"] + ("-resolve" if args.resolution else "-probe" if args.probe else "")
        if command == "doctor":
            if args.phase == "preflight":
                for operation in ("fetch", "build", "runtime", "accept", "collect"):
                    budget.before(operation)
                info = json.loads(docker.call("info", "--format", "{{json .}}"))
                require(info["OSType"] == "linux", "PREREQUISITE")
                docker.prerequisites()
                docker.policy()
                image_lock(p)
            else:
                execute(p, docker, budget, ["E01", "E02", "E04"], probe=True)
        elif command == "resolve-lock":
            from resolve_image import resolve
            resolve(p, docker, budget)
        elif command == "fetch":
            fetch(p, docker, budget)
        elif command == "build":
            build(p, docker, budget)
        elif command in ("accept", "run"):
            cases = [args.case] if command == "run" else list(__import__("common").CASES)
            require(all(c in __import__("common").CASES for c in cases))
            acceptance(p, docker, budget, cases)
        elif command == "collect":
            collect(p, docker, budget, run_id)
        elif command == "clean":
            run = Run(output, run_id).load()
            if "repeat_run" in run.record:
                require(run.record["repeat_run"] == run_id + "-repeat", "STATE")
                Run(output, run.record["repeat_run"]).load().clean(docker.inspect, docker.remove)
            run.clean(docker.inspect, docker.remove)
        elif command == "export-public":
            run = Run(output, run_id).load()
            require(run.record["phase"] in ("collected", "deleted"), "STATE")
            run.verify_collection()
            result = read_json(run.root / "collected/result.json")
            if "acceptance_digest" in run.record:
                require(digest(run.root / "acceptance.json") == run.record["acceptance_digest"], "IDENTITY")
                result = read_json(run.root / "acceptance.json")
            complete_ids = [row["id"] for row in result.get("cases", [])]
            expected = run.record.get("expected_cases", [])
            if not expected or len(complete_ids) != len(set(complete_ids)) or set(complete_ids) != set(expected):
                result["status"] = "failed"
                for case in set(expected) - set(complete_ids):
                    result["cases"].append({"id": case, "status": "not_run", "error_code": "INCOMPLETE"})
            result.update(run.record["identities"])
            atomic_json(run.root / "public.json", public_result(result))
        print(json.dumps({"status": "passed", "error_code": None}))
        return 0
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        code = error.code if isinstance(error, Failure) else (
            "INPUT" if isinstance(error, (KeyError, ValueError, TypeError)) else "INTERNAL")
        if isinstance(error, KeyboardInterrupt):
            code = "INTERRUPTED"
        if output is not None:
            try:
                atomic_json(output / "last-error.json", {
                    "error_code": code,
                    "detail": error.detail if isinstance(error, Failure) else repr(error),
                    "error_site": fixed_error_site(error),
                    "cleanup_errors": getattr(error, "cleanup_errors", []),
                })
            except BaseException:
                # A private diagnostic sink can itself fail. Never replace the
                # original status or leak its path/exception via a traceback.
                pass
        print(json.dumps({"status": "failed", "error_code": code}))
        return ERRORS[code]
    finally:
        guard.close()


if __name__ == "__main__":
    sys.exit(main())
