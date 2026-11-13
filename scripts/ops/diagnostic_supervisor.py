"""Private diagnostic copy of frozen #831 SQL supervisor; candidate 1951 only."""
from __future__ import annotations

import json
import os
import re
import signal
from pathlib import Path
import sys
from diagnostic import audit_inner_events, expected_client_records

SOURCE = {"commit": "894843360824e324c80b7a078bb3a1e63240e9bc",
          "tree": "c3e79b91f584d6549f84eb99aefb89ddf4b6b5ab", "profile": "release"}
TARGET = "alopex_sql_private_timing"
STAGES = {"transaction_setup", "route_plan", "execute_multi",
          "completion_and_commit", "result_map", "execute_and_map", "response_build"}
TIMING = re.compile(
    r"\bINFO (?:http_request\{[^{}\r\n]*\}: )?alopex_sql_private_timing: sql diagnostic stage "
    r"trace_id=([0-9a-f-]{36}) stage=\"?([a-z_]+)\"? duration_ns=([0-9]+)\s*$")
INNER_TARGET = "alopex_sql_private_inner"
INNER_PREFIX = (r"\bINFO (?:http_request\{[^{}\r\n]*\}: )?"
                r"alopex_sql_private_inner: ")
INNER_STAGE = re.compile(INNER_PREFIX +
    r"sql (?:inner )?diagnostic stage trace_id=([0-9a-f-]{36}) stage=\"([a-z_]+)\" duration_ns=([0-9]+)\s*$")
INNER_ROUTE = re.compile(INNER_PREFIX +
    r"sql diagnostic route trace_id=([0-9a-f-]{36}) statement_index=([0-9]+) route=\"([a-z_]+)\" root=\"([a-z_]+)\"\s*$")
INNER_BRANCH = re.compile(INNER_PREFIX +
    r"sql diagnostic (branch|plan) trace_id=([0-9a-f-]{36}) branch=\"([a-z_]+)\"\s*$")


def server_environment(base, mode):
    if mode not in ("on", "off"):
        raise ValueError("INPUT")
    value = dict(base)
    value.pop("ALOPEX_SQL_TIMING", None)
    value["NO_COLOR"] = "1"
    value["RUST_LOG"] = "warn"
    if mode == "on":
        value["ALOPEX_SQL_TIMING"] = "1"
        value["RUST_LOG"] = "warn,alopex_sql_private_timing=info"
    return value


def worker_config(config, output, state, docs, product):
    if config.get("source") != SOURCE or config.get("case") not in ("aggregate", "ordered") or \
            config.get("mode") not in ("on", "off") or type(config.get("trial")) is not int or \
            config["trial"] not in range(3):
        raise ValueError("IDENTITY")
    return dict(config, schema="alopex.sql-diagnostic-cell/v1",
                raw=str(output / "raw.ndjson"), result=str(output / "measurement.json"),
                client=str(output / "client.ndjson"), data=str(state),
                docs_harness_root=str(docs), product_harness_root=str(product))


def require_client_inventory(clients, mode):
    if type(clients) is not list or len(clients) != expected_client_records(mode):
        raise ValueError("EVIDENCE_BOUND")
    return clients


def parse_server_timing(path, mode):
    """Reduce a bounded server log to the three fixed timing fields only."""
    if mode not in ("on", "off") or path.is_symlink() or not path.is_file() or \
            path.stat().st_size > 8_000_000:
        raise ValueError("TRACE_BOUND")
    events = []
    with path.open("r", encoding="utf-8", errors="replace") as stream:
        for line in stream:
            if TARGET not in line:
                continue
            match = TIMING.search(line)
            if match is None or mode == "off" or len(events) >= 4_000:
                raise ValueError("TRACE_FORMAT")
            trace_id, stage, duration = match.groups()
            from uuid import UUID
            try:
                if str(UUID(trace_id)) != trace_id or UUID(trace_id).version != 4:
                    raise ValueError("TRACE_FORMAT")
            except ValueError as error:
                raise ValueError("TRACE_FORMAT") from error
            elapsed = int(duration)
            if stage not in STAGES or elapsed > 300_000_000_000:
                raise ValueError("TRACE_FORMAT")
            events.append({"trace_id": trace_id, "stage": stage,
                           "duration_ns": elapsed})
    return events


def parse_inner_diagnostics(path, mode, clients, servers):
    """Optional private reduction for a new native asset, separate from frozen timing.

    A later controller can call this after parse_server_timing using the same
    server.log, client.ndjson and server-timing.json, then seal the returned
    schema in a private artifact. The 1951 path need not call it.
    """
    if mode not in ("on", "off") or path.is_symlink() or not path.is_file() or \
            path.stat().st_size > 8_000_000:
        raise ValueError("INNER_BOUND")
    events = []
    with path.open("r", encoding="utf-8", errors="replace") as stream:
        for line in stream:
            if INNER_TARGET not in line:
                continue
            if len(events) >= 8_000:
                raise ValueError("INNER_BOUND")
            if match := INNER_STAGE.search(line):
                trace_id, stage, duration = match.groups()
                events.append({"kind": "stage", "trace_id": trace_id,
                               "stage": stage, "duration_ns": int(duration)})
            elif match := INNER_ROUTE.search(line):
                trace_id, index, route, root = match.groups()
                events.append({"kind": "route", "trace_id": trace_id,
                               "statement_index": int(index), "route": route, "root": root})
            elif match := INNER_BRANCH.search(line):
                _, trace_id, branch = match.groups()
                events.append({"kind": "branch", "trace_id": trace_id, "branch": branch})
            else:
                raise ValueError("INNER_FORMAT")
    return audit_inner_events(events, mode, clients, servers)


def run_owned_server(start, measure, save):
    """Preserve worker failure and cleanup failure independently."""
    server = None
    report = {"status": "failed", "worker_error": None, "stop_error": None}
    try:
        server, endpoint = start()
        report["measurement"] = measure(server, endpoint)
    except BaseException as error:
        report["worker_error"] = repr(error)
    finally:
        if server is not None:
            try:
                server.stop()
            except BaseException as error:
                report["stop_error"] = repr(error)
        if report["worker_error"] is None and report["stop_error"] is None:
            report["status"] = "passed"
        save(report)
    return report["status"] == "passed"


def main():
    sys.path.insert(0, "/tool")
    from common import atomic_json, contained, digest, private_dir, read_json, require, verify_assets, elf_signature
    from entrypoint import WORK, INPUT, alopex, prepare, command
    from observe import process
    from permissions import private_tree
    from verify_environment import privileges, files, resource_limits

    os.umask(0o077)
    require(os.getuid() != 0 and sys.platform == "linux", "POLICY")
    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    request = read_json(INPUT / "request.json")
    config = read_json(INPUT / "measurement.json")
    require(config["schema"] == "alopex.sql-diagnostic-supervisor/v1" and
            config.get("source") == SOURCE and config.get("engine") == "alopex-server" and
            config.get("case") in ("aggregate", "ordered") and
            config.get("mode") in ("on", "off") and
            type(config.get("trial")) is int and config["trial"] in range(3) and
            request["limits"]["case_seconds"] == 300 and
            request["limits"]["run_seconds"] == 420, "INPUT")
    manifest = read_json(INPUT / "assets.json")
    assets = verify_assets(INPUT / "assets.json", manifest["platform"])
    entries = {role: next(item for item in assets["files"] if item["role"] == role)
               for role in ("server", "wheel", "receipt", "measurement_worker", "measurement_controller", "measurement_config")}
    require(entries["measurement_controller"]["path"] == "controller.py" and
            entries["measurement_worker"]["path"] == "worker.py" and
            entries["measurement_config"]["path"] == "measurement.json", "IDENTITY")
    require({(item["role"], item["path"]) for item in assets["files"]}.issuperset(
        {("diagnostic_measure", "diagnostic_measure.py"),
         ("diagnostic_adapter", "diagnostic.py")}), "IDENTITY")
    require(digest(Path(__file__)) == entries["measurement_controller"]["sha256"], "IDENTITY")
    require(elf_signature(contained(INPUT, entries["server"]["path"])) == elf_signature(sys.executable), "IDENTITY")
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS",
                "TOKIO_WORKER_THREADS", "POLARS_MAX_THREADS"):
        os.environ[key] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    atomic_json(WORK / "results/environment.json", {
        "privileges": privileges(request), "files": files(WORK), "limits": resource_limits(request),
    })
    python, env = prepare(assets, request)
    receipt = read_json(contained(INPUT, entries["receipt"]["path"]))
    require(receipt["source"] == config["source"] == SOURCE, "IDENTITY")
    output = private_dir(WORK / "results/cell")
    state = private_dir(WORK / "databases/cell")

    def start():
        return alopex(output, request, assets,
                      server_environment(env, config["mode"]), state_directory=state)

    def measure(server, ports):
        def artifact(role):
            item = entries[role]
            return {"path": str(contained(INPUT, item["path"])), "sha256": item["sha256"]}
        subject = {"schema": "alopex.performance-subject/v1", "version": receipt["version"],
                   "source": receipt["source"], "cpu": request["cpu"],
                   "build_receipt": artifact("receipt"), "python_wheel": artifact("wheel")}
        pid = server.child.pid
        identity = {"pid": pid, "start_ticks": process(pid)["start_ticks"]}
        worker_env = dict(env)
        url = f'http://127.0.0.1:{ports["http"]}'
        subject["server"] = dict(identity, binary=artifact("server"), url=url)
        worker_env["ALOPEX_PERF_SERVER_URL"] = url
        subject_path, resources = output / "subject.json", output / "resources.json"
        atomic_json(subject_path, subject)
        atomic_json(resources, {"schema": "alopex.sql-resource-servers/v1", "servers": {config["engine"]: identity}})
        worker_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(subject_path),
                          ALOPEX_PERF_SUBJECT_SHA256=digest(subject_path),
                          ALOPEX_PERF_SQL_RESOURCE_MANIFEST=str(resources))
        worker = worker_config(config, output, state,
                               contained(WORK / "harness", config["docs_subdirectory"]),
                               contained(WORK / "harness", config["product_subdirectory"]))
        worker_path = output / "worker.json"
        atomic_json(worker_path, worker)
        command([str(python), "-B", str(INPUT / "worker.py"), str(worker_path)],
                output / "worker.log", request["limits"]["case_seconds"], worker_env)
        value = read_json(output / "measurement.json")
        require(value["case"] == config["case"] and value["engine"] == config["engine"] and
                value["trial"] == config["trial"] and value["mode"] == config["mode"] and
                value["subject_identity"]["source"] == config["source"] and
                value["raw_sha256"] == digest(output / "raw.ndjson"), "IDENTITY")
        clients = require_client_inventory(
            [json.loads(line) for line in (output / "client.ndjson").read_text().splitlines()],
            config["mode"])
        require(all(digest(contained(INPUT, item["path"])) == item["sha256"] for item in assets["files"]), "IDENTITY")
        return {"result_sha256": digest(output / "measurement.json"),
                "raw_sha256": value["raw_sha256"],
                "client_sha256": digest(output / "client.ndjson")}

    passed = run_owned_server(start, measure, lambda report: atomic_json(output / "status.json", report))
    if passed:
        try:
            atomic_json(output / "server-timing.json",
                        parse_server_timing(output / "server.log", config["mode"]))
        except BaseException:
            passed = False
            status = read_json(output / "status.json")
            status.update(status="failed", worker_error="TRACE_FORMAT")
            atomic_json(output / "status.json", status)
    atomic_json(WORK / "results/generated-permissions.json", {
        "database": private_tree(state), "results": private_tree(output),
    })
    atomic_json(WORK / "results/result.json", {
        "schema": "alopex.sql-diagnostic-measurement/v1", "status": "passed" if passed else "failed",
        "case": config["case"], "engine": config["engine"], "trial": config["trial"],
        "mode": config["mode"],
        "source": config["source"], "assets_digest": digest(INPUT / "assets.json"),
        "cell_status_sha256": digest(output / "status.json"),
        "server_timing_sha256": digest(output / "server-timing.json") if passed else None,
    })
    return 0 if passed else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("measurement supervisor interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
