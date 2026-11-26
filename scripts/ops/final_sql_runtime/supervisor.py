"""Supervise one SQL performance cell inside the qualified private container."""
from __future__ import annotations

import importlib.util
import os
import signal
from pathlib import Path
import sys


def run_owned_server(start, measure, save):
    """Preserve worker failure and cleanup failure independently."""
    server = None
    report = {"status": "failed", "worker_error": None, "stop_error": None,
              "server_identity": None, "server_stopped": False}
    try:
        server, endpoint = start()
        report["server_identity"] = {"pid": server.child.pid}
        report["measurement"] = measure(server, endpoint)
    except BaseException as error:
        report["worker_error"] = repr(error)
    finally:
        if server is not None:
            try:
                server.stop()
                report["server_stopped"] = server.child.poll() is not None
                if not report["server_stopped"]:
                    report["stop_error"] = "server remained running after stop"
            except BaseException as error:
                report["stop_error"] = repr(error)
        if report["worker_error"] is None and report["stop_error"] is None and report["server_stopped"]:
            report["status"] = "passed"
        save(report)
    return report["status"] == "passed"


def validate_phase(config, require):
    """Keep the staged phase and the worker phase identical and bounded."""
    require(config.get("schema") == "alopex.native-sql-supervisor/v1", "INPUT")
    phase = config.get("phase")
    require(phase in ("preflight", "cell") and
            (phase != "preflight" or config.get("case") == "select-all"), "INPUT")
    require(config.get("subject") in ("published", "before", "candidate", "postgresql") and
            config.get("engine") == ("postgresql" if config.get("subject") == "postgresql"
                                     else "alopex-server") and
            type(config.get("trial")) is int and config["trial"] in range(3), "INPUT")
    require(config.get("id") ==
            f"sql-{config['case']}-{config['trial']}-{config['subject']}", "INPUT")
    return phase


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
    phase = validate_phase(config, require)
    manifest = read_json(INPUT / "assets.json")
    assets = verify_assets(INPUT / "assets.json", manifest["platform"])
    roles = ("server", "wheel", "receipt", "measurement_worker", "measurement_controller", "measurement_config")
    require(all(sum(item["role"] == role for item in assets["files"]) == 1 for role in roles), "IDENTITY")
    entries = {role: next(item for item in assets["files"] if item["role"] == role)
               for role in roles}
    require(entries["measurement_controller"]["path"] == "controller.py" and
            entries["measurement_worker"]["path"] == "worker.py" and
            entries["measurement_config"]["path"] == "measurement.json", "IDENTITY")
    require(digest(Path(__file__)) == entries["measurement_controller"]["sha256"], "IDENTITY")
    require(digest(INPUT / "measurement.json") == entries["measurement_config"]["sha256"], "IDENTITY")
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
        if phase == "preflight":
            worker["exact_result"] = str(output / "exact-result.json")
        else:
            worker["public"] = str(output / "public.ndjson")
        worker_path = output / "worker.json"
        atomic_json(worker_path, worker)
        command([str(python), "-B", str(INPUT / "worker.py"), str(worker_path)],
                output / "worker.log", request["limits"]["case_seconds"], worker_env)
        value = read_json(output / "measurement.json")
        require(value["case"] == config["case"] and value["engine"] == config["engine"] and
                value["trial"] == config["trial"] and value["subject_identity"]["source"] == config["source"] and
                value["raw_sha256"] == digest(output / "raw.ndjson"), "IDENTITY")
        result = {"result_sha256": digest(output / "measurement.json"),
                  "raw_sha256": value["raw_sha256"], "phase": phase}
        if phase == "preflight":
            spec = importlib.util.spec_from_file_location("pinned_measurement_worker", INPUT / "worker.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            exact_path = output / "exact-result.json"
            exact = read_json(exact_path)
            require(set(exact) == {"rows", "result"} and
                    module.validate_exact("select-all", exact["result"], exact["rows"]) ==
                    value["result_sha256"], "IDENTITY")
            result["exact_result_sha256"] = digest(exact_path)
        else:
            require(not (output / "exact-result.json").exists(), "IDENTITY")
            require((output / "public.ndjson").is_file(), "IDENTITY")
            result["public_sha256"] = digest(output / "public.ndjson")
        require(all(digest(contained(INPUT, item["path"])) == item["sha256"] for item in assets["files"]), "IDENTITY")
        return result

    passed = run_owned_server(start, measure, lambda report: atomic_json(output / "status.json", report))
    atomic_json(WORK / "results/generated-permissions.json", {
        "database": private_tree(state), "results": private_tree(output),
    })
    atomic_json(WORK / "results/result.json", {
        "schema": "alopex.native-sql-measurement/v1", "status": "passed" if passed else "failed",
        "case": config["case"], "engine": config["engine"], "trial": config["trial"],
        "phase": phase,
        "source": config["source"], "assets_digest": digest(INPUT / "assets.json"),
        "cell_status_sha256": digest(output / "status.json"),
    })
    return 0 if passed else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("measurement supervisor interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
