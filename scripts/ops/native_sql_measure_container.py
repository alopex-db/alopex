"""Run a measurement with the qualified Docker policy and durable collection."""
from pathlib import Path

from common import digest, read_json, require
from docker_backend import Docker
from lifecycle import Run
import envctl

from native_sql_measure_worker import verify_cell


class MeasurementDocker(Docker):
    def arguments(self, run_id, image, input_volume, work_volume, token):
        args = super().arguments(run_id, image, input_volume, work_volume, token)
        require(args[-1] == image, "IDENTITY")
        return [*args[:-1], "--entrypoint", "python3", image, "-B", "/input/controller.py"]

    def validate_container(self, name):
        live = super().validate_container(name)
        require(live["Config"]["Entrypoint"] == ["python3"] and
                live["Config"]["Cmd"] == ["-B", "/input/controller.py"], "IDENTITY")
        return live


def failed_worker_was_recovered(run, config, assets_digest):
    """Distinguish a reported worker failure from an infrastructure command failure."""
    if run.record["phase"] != "deleted":
        return False
    run.verify_collection()
    root = run.root / "collected"
    report = read_json(root / "result.json")
    if report.get("schema") != "alopex.native-sql-measurement/v1" or report.get("status") != "failed":
        return False
    require(report["assets_digest"] == assets_digest and report["source"] == config["source"] and
            all(report[key] == config[key] for key in ("case", "engine", "trial")), "IDENTITY")
    status_path = root / "cell/status.json"
    require(digest(status_path) == report["cell_status_sha256"], "IDENTITY")
    status = read_json(status_path)
    return (status["status"] == "failed" and status["worker_error"] is not None and
            status["stop_error"] is None and not (root / "cleanup-errors.json").exists())


def verify_collected(run, config, assets_digest):
    run.verify_collection()
    root = run.root / "collected"
    report = read_json(root / "result.json")
    require(report["schema"] == "alopex.native-sql-measurement/v1" and
            report["status"] == "passed" and report["assets_digest"] == assets_digest and
            report["source"] == config["source"] and
            all(report[key] == config[key] for key in ("case", "engine", "trial")), "CASE")
    cell = root / "cell"
    require(digest(cell / "status.json") == report["cell_status_sha256"], "IDENTITY")
    status = read_json(cell / "status.json")
    require(status["status"] == "passed" and status["worker_error"] is None and
            status["stop_error"] is None, "CASE")
    result = read_json(cell / "measurement.json")
    require(digest(cell / "measurement.json") == status["measurement"]["result_sha256"] and
            digest(cell / "raw.ndjson") == result["raw_sha256"] == status["measurement"]["raw_sha256"], "IDENTITY")
    require(result["trial"] == config["trial"] and result["subject_identity"]["source"] == config["source"], "IDENTITY")
    import json
    raw = [json.loads(line) for line in (cell / "raw.ndjson").read_text().splitlines()]
    verify_cell(result, raw, config, digest(cell / "resources.json"))
    return {"case": config["case"], "engine": config["engine"], "trial": config["trial"],
            "source": config["source"], "status": "passed",
            "result_sha256": digest(cell / "measurement.json"), "raw_sha256": result["raw_sha256"]}


def execute_cell(profile, budget, config, *, verify=None, recovered=None):
    verify = verify_collected if verify is None else verify
    recovered = failed_worker_was_recovered if recovered is None else recovered
    docker = MeasurementDocker(profile)
    run = Run(profile["private_output"], profile["run_id"])
    original = None
    verified = None
    try:
        envctl.execute(profile, docker, budget, [])
        envctl.collect(profile, docker, budget, profile["run_id"])
        run.load()
        verified = verify(run, config, digest(profile["assets"]))
    except BaseException as error:
        original = error
    finally:
        if run.path.exists():
            try:
                run.load()
                if run.record["phase"] not in ("collected", "deleted"):
                    envctl.collect(profile, docker, budget, profile["run_id"])
                    run.load()
                run.clean(docker.inspect, docker.remove)
            except BaseException as error:
                if original is None:
                    original = error
                else:
                    from common import record_cleanup_error
                    record_cleanup_error(original, "measurement_collection_cleanup", error)
    if original is not None:
        # docker start --attach returns the worker's nonzero status as COMMAND.
        # Continue independent cells only after a source-bound failure report,
        # successful stop, hash-verified collection and complete owned cleanup.
        if (getattr(original, "code", None) == "COMMAND" and
                not getattr(original, "cleanup_errors", []) and run.path.exists() and
                recovered(run, config, digest(profile["assets"]))):
            from common import Failure
            raise Failure("CASE", "worker failure; original command evidence retained") from original
        raise original
    return verified
