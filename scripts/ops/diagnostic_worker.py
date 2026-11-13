"""One task-private #712 diagnostic pair, copied from the frozen SQL worker."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys

ISOLATED = {"aggregate": 34250, "ordered": 34250}
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes", "rss_after_bytes",
            "read_bytes", "write_bytes", "cancelled_write_bytes")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def verify_cell(result, raw, config, resource_digest):
    """Reject missing samples, altered boundaries and incomplete resource cells."""
    case, engine = config["case"], config["engine"]
    require(case in ISOLATED, "case outside independent SQL lane")
    require(engine == "alopex-server", "unknown SQL engine")
    require(type(config["trial"]) is int and config["trial"] in range(3), "invalid trial")
    require(result["case"] == case and result["engine"] == engine and
            result["server_client"] is True, "measurement boundary mismatch")
    require(result["warmups"] == 3 and result["samples"] == len(raw) == 21,
            "sample count mismatch")
    require(result["fixture_rows"] == ISOLATED.get(case, 10000), "fixture size mismatch")
    require(len(result["result_sha256"]) == 64 and
            all(c in "0123456789abcdef" for c in result["result_sha256"]), "missing result identity")
    for index, row in enumerate(raw):
        if case in ISOLATED:
            require(row["sample"] == index, "sample sequence mismatch")
        else:
            require(row["index"] == index and row["phase"] == "measure", "sample sequence mismatch")
        require(number(row["seconds"]) and row["seconds"] > 0, "invalid elapsed time")
        resource = row["resources"]
        require(resource["schema"] == "alopex.sql-operation-resources/v1" and
                resource["status"] == "observed" and
                resource["coverage"] == "stable_pid_namespace_allocation" and
                resource["manifest_sha256"] == resource_digest, "unqualified resource observation")
        client, servers = resource["client"], resource["server_processes"]
        require(servers and len({p["pid"] for p in servers}) == len(servers) and
                client["pid"] not in {p["pid"] for p in servers}, "process scope mismatch")
        for process in [client, *servers]:
            require(type(process["pid"]) is int and process["pid"] > 0 and
                    type(process["start_ticks"]) is int and process["start_ticks"] > 0,
                    "missing process identity")
            require(all(number(process[field]) for field in COUNTERS), "invalid resource counter")
    median = statistics.median(row["seconds"] for row in raw) * 1000
    require(math.isclose(result["latency_p50_ms"], median, rel_tol=1e-12), "summary differs from raw")


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def enforce_complete_result_order(module):
    """Keep the archived SQL and checksum, but reject reordered/duplicated rows."""
    original = module.validate

    def validate(case, result, rows):
        if case == "select-all":
            expected = [{"id": index, "value": float(index)} for index in range(0, rows, 2)]
            require(type(result) is list and len(result) == len(expected), "incomplete full SQL result")
            for actual, wanted in zip(result, expected):
                require(type(actual) is dict and set(actual) == set(wanted) and
                        all(type(actual[key]) is type(value) and actual[key] == value
                            for key, value in wanted.items()), "full SQL result order/value/type mismatch")
        return original(case, result, rows)

    module.validate = validate


def main(config):
    require(sys.platform == "linux" and os.geteuid() != 0, "native unprivileged Linux worker required")
    require(config["schema"] == "alopex.sql-diagnostic-pair/v1", "unknown pair schema")
    require(config["case"] in ISOLATED, "case outside independent SQL lane")
    require(config["engine"] == "alopex-server", "unknown SQL engine")
    require(config.get("modes") == ["on", "off"], "invalid diagnostic modes")
    require(type(config["trial"]) is int and config["trial"] in range(3), "invalid trial")
    os.umask(0o077)
    paths = {mode: {name: Path(config["outputs"][mode][name])
                    for name in ("result", "raw", "client")} for mode in config["modes"]}
    require(all(not path.exists() for cell in paths.values() for path in cell.values()),
            "immutable pair output already exists")
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS",
                "TOKIO_WORKER_THREADS", "POLARS_MAX_THREADS"):
        require(os.environ.get(key) == "1", "thread policy mismatch")
    docs = Path(config["docs_harness_root"]) / "scripts/performance"
    product = Path(config["product_harness_root"]) / "scripts/performance"
    for root, files in ((docs, config["docs_files"]), (product, config["product_files"])):
        require(files, "missing harness inventory")
        actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
        require(actual == set(files), "harness inventory mismatch")
        for name, expected in files.items():
            path = root / name
            require(path.resolve().is_relative_to(root.resolve()) and not path.is_symlink(), "harness path mismatch")
            require(hashlib.sha256(path.read_bytes()).hexdigest() == expected, "harness bytes mismatch")
    sys.path.insert(0, str(docs))
    from subject_identity import verify_subject, cpu_assignment
    from sql_resources import ENV
    require(os.environ.get("ALOPEX_PERF_SUBJECT_MANIFEST") and os.environ.get("ALOPEX_PERF_SUBJECT_SHA256")
            and os.environ.get(ENV), "qualified subject and resource manifests required")
    subject = verify_subject(require_server=config["engine"] == "alopex-server")
    require(subject["source"] == config["source"], "subject source mismatch")
    allocation = cpu_assignment(subject)
    os.sched_setaffinity(0, set(allocation["client"]))
    require(os.sched_getaffinity(0) == set(allocation["client"]), "client affinity mismatch")
    resource_digest = hashlib.sha256(Path(os.environ[ENV]).read_bytes()).hexdigest()
    sys.path.insert(0, str(product))
    parity = load_module("fixed_product_parity", product / "parity_v0811_measure.py")
    archived = docs / "server-sql-isolated-v0816.py"
    require(hashlib.sha256(archived.read_bytes()).hexdigest() ==
            "856a5def988dfa60f0343c6686ebccbe260e67fde644b5451f6063a8faa1b093",
            "archived SQL harness mismatch")
    isolated = load_module("private_diagnostic_sql", Path(__file__).with_name("diagnostic_measure.py"))
    from diagnostic import pair_order
    results, clients = isolated.measure_pair(parity, config["case"], config["trial"],
                                             {mode: paths[mode]["raw"] for mode in config["modes"]})
    expected_server = subject["server"]
    server_identity = (expected_server["pid"], expected_server["start_ticks"])
    for mode in config["modes"]:
        raw = [json.loads(line) for line in paths[mode]["raw"].read_text().splitlines()]
        verify_cell(results[mode], raw, config, resource_digest)
        require(all({(server["pid"], server["start_ticks"])
                     for server in row["resources"]["server_processes"]} == {server_identity}
                    for row in raw), "pair server identity changed")
        require(results[mode]["result_sha256"] == results["on"]["result_sha256"],
                "pair fixture oracle changed")
    require(hashlib.sha256(Path(os.environ[ENV]).read_bytes()).hexdigest() == resource_digest,
            "resource manifest changed")
    for mode in config["modes"]:
        result = results[mode]
        result.update(trial=config["trial"], subject_identity=subject,
                      mode=mode, raw_sha256=hashlib.sha256(paths[mode]["raw"].read_bytes()).hexdigest(),
                      pair_server={"pid": server_identity[0], "start_ticks": server_identity[1]},
                      fixture_sha256=results["on"]["result_sha256"],
                      pair_order=[list(pair_order(config["trial"], index)) for index in range(21)])
        with paths[mode]["client"].open("x") as stream:
            for row in clients[mode]:
                stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        with paths[mode]["result"].open("x") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")


if __name__ == "__main__":
    main(json.loads(Path(sys.argv[1]).read_text()))
