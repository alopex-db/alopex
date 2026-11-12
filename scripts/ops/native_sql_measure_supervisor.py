"""Supervise one SQL performance cell inside the qualified private container."""
from __future__ import annotations

import json
import os
import signal
from pathlib import Path
import sys


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
    from entrypoint import WORK, INPUT, alopex, postgres, prepare, command
    from observe import process
    from permissions import private_tree
    from verify_environment import privileges, files, resource_limits

    os.umask(0o077)
    require(os.getuid() != 0 and sys.platform == "linux", "POLICY")
    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    request = read_json(INPUT / "request.json")
    config = read_json(INPUT / "measurement.json")
    require(config["schema"] == "alopex.native-sql-supervisor/v1", "INPUT")
    manifest = read_json(INPUT / "assets.json")
    assets = verify_assets(INPUT / "assets.json", manifest["platform"])
    entries = {role: next(item for item in assets["files"] if item["role"] == role)
               for role in ("server", "wheel", "receipt", "measurement_worker", "measurement_controller", "measurement_config")}
    require(entries["measurement_controller"]["path"] == "controller.py" and
            entries["measurement_worker"]["path"] == "worker.py" and
            entries["measurement_config"]["path"] == "measurement.json", "IDENTITY")
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
    require(receipt["source"] == config["source"], "IDENTITY")
    output = private_dir(WORK / "results/cell")
    state = private_dir(WORK / "databases/cell")

    def start():
        if config["engine"] == "postgresql":
            return postgres(output, request, state_directory=state), None
        require(config["engine"] == "alopex-server", "INPUT")
        return alopex(output, request, assets, env, state_directory=state)

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
        if config["engine"] == "alopex-server":
            url = f'http://127.0.0.1:{ports["http"]}'
            subject["server"] = dict(identity, binary=artifact("server"), url=url)
            worker_env["ALOPEX_PERF_SERVER_URL"] = url
        else:
            worker_env["ALOPEX_PERF_POSTGRES_DSN"] = f'host={state / "socket"} dbname=acceptance user=alopex'
        subject_path, resources = output / "subject.json", output / "resources.json"
        atomic_json(subject_path, subject)
        atomic_json(resources, {"schema": "alopex.sql-resource-servers/v1", "servers": {config["engine"]: identity}})
        worker_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(subject_path),
                          ALOPEX_PERF_SUBJECT_SHA256=digest(subject_path),
                          ALOPEX_PERF_SQL_RESOURCE_MANIFEST=str(resources))
        worker = dict(config, schema="alopex.native-sql-measurement-cell/v1",
                      raw=str(output / "raw.ndjson"), result=str(output / "measurement.json"), data=str(state),
                      docs_harness_root=str(contained(WORK / "harness", config["docs_subdirectory"])),
                      product_harness_root=str(contained(WORK / "harness", config["product_subdirectory"])))
        worker_path = output / "worker.json"
        atomic_json(worker_path, worker)
        command([str(python), "-B", str(INPUT / "worker.py"), str(worker_path)],
                output / "worker.log", request["limits"]["case_seconds"], worker_env)
        value = read_json(output / "measurement.json")
        require(value["case"] == config["case"] and value["engine"] == config["engine"] and
                value["trial"] == config["trial"] and value["subject_identity"]["source"] == config["source"] and
                value["raw_sha256"] == digest(output / "raw.ndjson"), "IDENTITY")
        require(all(digest(contained(INPUT, item["path"])) == item["sha256"] for item in assets["files"]), "IDENTITY")
        return {"result_sha256": digest(output / "measurement.json"), "raw_sha256": value["raw_sha256"]}

    passed = run_owned_server(start, measure, lambda report: atomic_json(output / "status.json", report))
    atomic_json(WORK / "results/generated-permissions.json", {
        "database": private_tree(state), "results": private_tree(output),
    })
    atomic_json(WORK / "results/result.json", {
        "schema": "alopex.native-sql-measurement/v1", "status": "passed" if passed else "failed",
        "case": config["case"], "engine": config["engine"], "trial": config["trial"],
        "source": config["source"], "assets_digest": digest(INPUT / "assets.json"),
        "cell_status_sha256": digest(output / "status.json"),
    })
    return 0 if passed else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("measurement supervisor interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
