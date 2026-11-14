"""Private #712 copy of frozen #831 collection; paired diagnostic schema only."""
import json
from pathlib import Path

from common import digest, read_json, require
from docker_backend import Docker
from lifecycle import Run
import envctl

from diagnostic_worker import verify_cell
from diagnostic import expected_client_records

SOURCE = {"commit": "894843360824e324c80b7a078bb3a1e63240e9bc",
          "tree": "c3e79b91f584d6549f84eb99aefb89ddf4b6b5ab", "profile": "release"}
STOP_BYTES = 40_000_000_000
ADDITIONAL_BYTES = 4_000_000_000
RESERVE_BYTES = 1_000_000_000


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
    if report.get("schema") != "alopex.sql-diagnostic-measurement/v1" or report.get("status") != "failed":
        return False
    require(report["assets_digest"] == assets_digest and report["source"] == config["source"] and
            all(report[key] == config[key] for key in ("case", "engine", "trial", "modes")), "IDENTITY")
    status_path = root / "pair/status.json"
    require(digest(status_path) == report["cell_status_sha256"], "IDENTITY")
    status = read_json(status_path)
    return (status["status"] == "failed" and status["worker_error"] is not None and
            status["stop_error"] is None and not (root / "cleanup-errors.json").exists())


def verify_collected(run, config, assets_digest):
    run.verify_collection()
    root = run.root / "collected"
    report = read_json(root / "result.json")
    require(report["schema"] == "alopex.sql-diagnostic-measurement/v1" and
            report["status"] == "passed" and report["assets_digest"] == assets_digest and
            report["source"] == config["source"] and
            all(report[key] == config[key] for key in ("case", "engine", "trial", "modes")), "CASE")
    pair = root / "pair"
    require(digest(pair / "status.json") == report["cell_status_sha256"], "IDENTITY")
    status = read_json(pair / "status.json")
    require(status["status"] == "passed" and status["worker_error"] is None and
            status["stop_error"] is None, "CASE")
    require(digest(pair / "server-timing.json") == report["server_timing_sha256"], "IDENTITY")
    servers = read_json(pair / "server-timing.json")
    require(type(servers) is list and len(servers) <= 4_000 and
            all(type(row) is dict and set(row) == {"trace_id", "stage", "duration_ns"}
                for row in servers), "EVIDENCE_BOUND")
    summaries, clients = [], []
    for mode in config["modes"]:
        cell = pair / mode
        hashes = status["measurement"]["cells"][mode]
        result = read_json(cell / "measurement.json")
        require(digest(cell / "measurement.json") == hashes["result_sha256"] and
                digest(cell / "raw.ndjson") == result["raw_sha256"] == hashes["raw_sha256"] and
                digest(cell / "client.ndjson") == hashes["client_sha256"], "IDENTITY")
        require(result["trial"] == config["trial"] and result["mode"] == mode and
                result["subject_identity"]["source"] == config["source"] and
                result["pair_server"] == status["measurement"]["server"] and
                result["fixture_sha256"] == status["measurement"]["fixture_sha256"], "IDENTITY")
        raw = [json.loads(line) for line in (cell / "raw.ndjson").read_text().splitlines()]
        verify_cell(result, raw, config, digest(pair / "resources.json"))
        require(len(raw) == 21 and (cell / "client.ndjson").stat().st_size <= 2_000_000 and
                (pair / "server-timing.json").stat().st_size <= 5_000_000, "EVIDENCE_BOUND")
        for row in raw:
            require({(p["pid"], p["start_ticks"]) for p in row["resources"]["server_processes"]} ==
                    {(result["pair_server"]["pid"], result["pair_server"]["start_ticks"])}, "IDENTITY")
        cell_clients = [json.loads(line) for line in (cell / "client.ndjson").read_text().splitlines()]
        require(len(cell_clients) == expected_client_records(mode), "EVIDENCE_BOUND")
        measured_timers = {row.get("sample"): (row.get("outer_ns"), row.get("pair_ordinal"))
                           for row in cell_clients
                           if type(row) is dict and row.get("phase") == "measured"}
        require(len(measured_timers) == 21 and
                all(type(measured_timers.get(sample, (None, None))[0]) is int and
                    measured_timers[sample] == (round(row["seconds"] * 1_000_000_000),
                                                row.get("pair_ordinal")) and
                    type(row.get("pair_ordinal")) is int
                    for sample, row in enumerate(raw)), "IDENTITY")
        clients.extend(cell_clients)
        summaries.append({"case": config["case"], "trial": config["trial"], "mode": mode,
                          "result_sha256": result["result_sha256"],
                          "raw_seconds": [row["seconds"] for row in raw],
                          "raw_ordinals": [row["pair_ordinal"] for row in raw],
                          "raw_sha256": result["raw_sha256"],
                          "pair_server": result["pair_server"],
                          "fixture_sha256": result["fixture_sha256"],
                          "pair_order": result["pair_order"]})
    return summaries, clients, servers


def execute_pair(profile, budget, config, *, verify=None, recovered=None):
    require(config["source"] == SOURCE and config["engine"] == "alopex-server" and
            config["case"] in ("aggregate", "ordered") and config["modes"] == ["on", "off"] and
            type(config["trial"]) is int and config["trial"] in range(3) and
            profile["limits"]["case_seconds"] == 300 and
            profile["limits"]["run_seconds"] == 420 and
            profile["storage"]["poll_seconds"] == budget.config["poll_seconds"] == 10 and
            profile["storage"]["stop_bytes"] == STOP_BYTES and
            profile["storage"]["reserve_bytes"] == RESERVE_BYTES, "POLICY")
    verify = verify_collected if verify is None else verify
    recovered = failed_worker_was_recovered if recovered is None else recovered
    docker = MeasurementDocker(profile)
    run = Run(profile["private_output"], profile["run_id"])
    original = None
    verified = None
    initial = budget.used()
    require(initial + ADDITIONAL_BYTES + RESERVE_BYTES < STOP_BYTES, "CAPACITY")
    try:
        envctl.execute(profile, docker, budget, [])
        envctl.collect(profile, docker, budget, profile["run_id"])
        run.load()
        verified = verify(run, config, digest(profile["assets"]))
        final = budget.used()
        require(final - initial <= ADDITIONAL_BYTES and
                final + RESERVE_BYTES < STOP_BYTES, "CAPACITY")
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
    run.load()
    require(run.record["phase"] == "deleted" and
            not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
    run.verify_collection()
    return verified
