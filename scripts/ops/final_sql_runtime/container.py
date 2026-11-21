"""Owned Docker callbacks for the fixed final SQL operation.

The caller supplies a previously reviewed private test-env profile per cell.
This adapter only narrows its time limits and substitutes the staged, pinned
assets manifest. It never constructs an image or changes Docker policy.
"""
from __future__ import annotations

import copy
import json
import math
from pathlib import Path
import sys
import time

from . import worker


def _runtime():
    # The test-env program is an existing script directory, not a Python package.
    directory = str(Path(__file__).resolve().parents[2] / "test-env")
    if directory not in sys.path:
        sys.path.insert(0, directory)
    import common
    import capacity
    import docker_backend
    import envctl
    import lifecycle
    return common, capacity, docker_backend, envctl, lifecycle


def owned_run_id(kind, config):
    """Separate four preflight runs from the same coordinates in the matrix."""
    if kind not in ("preflight", "cell") or type(config) is not dict:
        raise ValueError("INPUT")
    coordinate = config.get("id")
    if type(coordinate) is not str or not coordinate.startswith("sql-") or len(coordinate) > 36:
        raise ValueError("INPUT")
    return kind + "-" + coordinate


class DeadlineBudget:
    """Limit every owned command across staging, execution and collection."""

    def __init__(self, base, deadline, clock):
        self.base, self.deadline, self.clock = base, deadline, clock

    def remaining(self):
        value = self.deadline - self.clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise TimeoutError("RUN_DEADLINE")
        return value

    def before(self, operation):
        self.remaining()
        result = self.base.before(operation)
        self.remaining()
        return result

    def check(self):
        self.remaining()
        result = self.base.check()
        self.remaining()
        return result

    def run(self, command, *, timeout, log, env=None, cancel=None):
        remaining = self.remaining()
        return self.base.run(command, timeout=min(timeout, remaining), log=log,
                             env=env, cancel=cancel)


def verify_collected(run, config, kind, assets_digest, *, common):
    """Accept only a complete, hash-bound SQL report from an owned collection."""
    run.verify_collection()
    root = run.root / "collected"
    report = common.read_json(root / "result.json")
    common.require(report.get("schema") == "alopex.native-sql-measurement/v1" and
                   report.get("status") == "passed" and
                   report.get("phase") == kind == config.get("phase") and
                   report.get("assets_digest") == assets_digest and
                   report.get("source") == config["source"] and
                   all(report.get(key) == config[key] for key in ("case", "engine", "trial")), "CASE")
    exit_state = common.read_json(root / "exit.json")
    common.require(exit_state.get("ExitCode") == 0 and exit_state.get("OOMKilled") is False,
                   "CASE")
    for name in ("environment.json", "generated-permissions.json"):
        common.require((root / name).is_file(), "COLLECT")
        common.read_json(root / name)
    cell = root / "cell"
    common.require(common.digest(cell / "status.json") == report.get("cell_status_sha256"), "IDENTITY")
    status = common.read_json(cell / "status.json")
    common.require(status.get("status") == "passed" and status.get("worker_error") is None and
                   status.get("stop_error") is None and
                   status.get("server_stopped") is True and
                   type(status.get("server_identity")) is dict and
                   type(status["server_identity"].get("pid")) is int and
                   status["server_identity"]["pid"] > 0 and
                   status.get("measurement", {}).get("phase") == kind, "CASE")
    result_path, raw_path = cell / "measurement.json", cell / "raw.ndjson"
    result = common.read_json(result_path)
    common.require(common.digest(result_path) == status["measurement"]["result_sha256"] and
                   common.digest(raw_path) == result["raw_sha256"] ==
                   status["measurement"]["raw_sha256"], "IDENTITY")
    common.require(result["trial"] == config["trial"] and
                   result["subject_identity"]["source"] == config["source"], "IDENTITY")
    raw = [json.loads(line) for line in raw_path.read_text().splitlines()]
    resource_digest = common.digest(cell / "resources.json")
    worker.verify_cell(result, raw, config, resource_digest)
    if kind == "preflight":
        exact_path = cell / "exact-result.json"
        common.require(exact_path.is_file(), "COLLECT")
        exact = common.read_json(exact_path)
        common.require(set(exact) == {"rows", "result"} and
                       config["case"] == "select-all" and
                       common.digest(exact_path) == status["measurement"].get("exact_result_sha256"),
                       "CASE")
        common.require(worker.validate_exact("select-all", exact["result"], exact["rows"]) ==
                       result["result_sha256"], "CASE")
        return exact
    common.require(kind == "cell", "INPUT")
    return {"cell_result": result, "raw": raw, "resource_digest": resource_digest}


class ContainerCallbacks:
    """Bind execute, collect and cleanup to one owned run at a time."""

    def __init__(self, profile_for, *, runtime=None, clock=time.monotonic):
        self.profile_for = profile_for
        self.common, self.capacity, self.docker_backend, self.envctl, self.lifecycle = (
            runtime if runtime is not None else _runtime())
        self.clock = clock
        self.active = None

    def execute(self, staged, config, *, kind, worker_timeout_seconds, run_timeout_seconds):
        common = self.common
        common.require(self.active is None and kind in ("preflight", "cell"), "STATE")
        common.require(config.get("phase") == kind, "IDENTITY")
        common.require(type(worker_timeout_seconds) in (int, float) and
                       type(run_timeout_seconds) in (int, float) and
                       0 < worker_timeout_seconds <= 300 and
                       worker_timeout_seconds <= run_timeout_seconds <= 420, "INPUT")
        staged = Path(staged).resolve(strict=True)
        common.require(staged.name == "assets.json" and staged.is_file(), "INPUT")
        common.require(common.read_json(staged.parent / "measurement.json") == config,
                       "IDENTITY")
        profile_path = Path(self.profile_for(kind, config)).resolve(strict=True)
        profile = copy.deepcopy(common.load_profile(profile_path))
        common.require(Path(profile["assets"]).resolve(strict=True) == staged, "IDENTITY")
        common.require(profile["run_id"] == owned_run_id(kind, config), "IDENTITY")
        common.require(Path(profile["private_output"]).is_dir() and
                       profile["limits"]["case_seconds"] >= worker_timeout_seconds and
                       profile["limits"]["run_seconds"] >= run_timeout_seconds, "POLICY")
        profile["limits"]["case_seconds"] = math.ceil(worker_timeout_seconds)
        profile["limits"]["run_seconds"] = math.ceil(run_timeout_seconds)
        common.require(profile["limits"]["case_seconds"] > 0 and
                       profile["limits"]["run_seconds"] > 0, "INPUT")
        common.verify_assets(staged, profile["platform"])
        base_docker = self.docker_backend.Docker

        class MeasurementDocker(base_docker):
            def arguments(self, run_id, image, input_volume, work_volume, token):
                args = super().arguments(run_id, image, input_volume, work_volume, token)
                common.require(args[-1] == image, "IDENTITY")
                return [*args[:-1], "--entrypoint", "python3", image,
                        "-B", "/input/controller.py"]

            def validate_container(self, name):
                live = super().validate_container(name)
                common.require(live["Config"]["Entrypoint"] == ["python3"] and
                               live["Config"]["Cmd"] == ["-B", "/input/controller.py"], "IDENTITY")
                return live

        docker = MeasurementDocker(profile)
        budget = DeadlineBudget(self.capacity.Budget(profile["storage"]),
                                self.clock() + run_timeout_seconds, self.clock)
        self.active = (profile, docker, budget, config, kind)
        self.envctl.execute(profile, docker, budget, [])
        budget.remaining()
        return profile["run_id"]

    def collect(self, execution, kind, row):
        self.common.require(self.active is not None, "STATE")
        profile, docker, budget, config, expected_kind = self.active
        self.common.require(execution == profile["run_id"] and kind == expected_kind and
                            all(config[key] == row[key] for key in row), "IDENTITY")
        self.envctl.collect(profile, docker, budget, profile["run_id"])
        budget.remaining()
        run = self.lifecycle.Run(profile["private_output"], profile["run_id"]).load()
        return verify_collected(run, config, kind, self.common.digest(profile["assets"]),
                                common=self.common)

    def cleanup(self, staged, kind, row):
        if self.active is None:
            return
        profile, docker, budget, config, expected_kind = self.active
        self.common.require(kind == expected_kind and
                            all(config[key] == row[key] for key in row) and
                            Path(staged).resolve() == Path(profile["assets"]).resolve(), "IDENTITY")
        run = self.lifecycle.Run(profile["private_output"], profile["run_id"])
        if not run.path.exists():
            self.active = None
            return
        run.load()
        if run.record["phase"] not in ("collected", "deleted"):
            self.envctl.collect(profile, docker, getattr(budget, "base", budget), profile["run_id"])
            run.load()
        run.clean(docker.inspect, docker.remove)
        self.active = None
