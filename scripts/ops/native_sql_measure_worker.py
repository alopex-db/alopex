"""One fixed SQL performance cell; all output stays in the private work volume."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys

ISOLATED = {"aggregate": 34250, "ordered": 34250, "select-all": 34250, "insert-rollback": 1}
WRITES = {"insert-commit", "batch-insert-commit", "random-get-hit", "random-get-miss",
          "random-update-commit", "random-delete-commit"}
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
    require(case in ISOLATED or case in WRITES, "unknown SQL case")
    require(engine in ("alopex-server", "postgresql"), "unknown SQL engine")
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
    if case in WRITES:
        require(result["operations_per_sample"] ==
                (100 if case in ("batch-insert-commit", "random-get-hit", "random-get-miss") else 1),
                "request count mismatch")
        require(len(result["input_sha256"]) == 64 and
                all(c in "0123456789abcdef" for c in result["input_sha256"]), "missing input identity")


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(config):
    require(sys.platform == "linux" and os.geteuid() != 0, "native unprivileged Linux worker required")
    require(config["schema"] == "alopex.native-sql-measurement-cell/v1", "unknown cell schema")
    require(config["case"] in ISOLATED or config["case"] in WRITES, "unknown SQL case")
    require(config["engine"] in ("alopex-server", "postgresql"), "unknown SQL engine")
    require(type(config["trial"]) is int and config["trial"] in range(3), "invalid trial")
    os.umask(0o077)
    output = Path(config["result"])
    raw_path = Path(config["raw"])
    require(not output.exists() and not raw_path.exists(), "immutable cell output already exists")
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
    if config["case"] in ISOLATED:
        sys.path.insert(0, str(product))
        parity = load_module("fixed_product_parity", product / "parity_v0811_measure.py")
        isolated = load_module("fixed_isolated_sql", docs / "server-sql-isolated-v0816.py")
        result = isolated.measure(parity, config["engine"], config["case"], raw_path)
    else:
        module = load_module("fixed_write_sql", docs / "write_kv_sql.py")
        result = module.run(config["engine"], config["case"], Path(config["data"]), raw_path)
    raw = [json.loads(line) for line in raw_path.read_text().splitlines()]
    verify_cell(result, raw, config, resource_digest)
    require(hashlib.sha256(Path(os.environ[ENV]).read_bytes()).hexdigest() == resource_digest,
            "resource manifest changed")
    result.update(trial=config["trial"], subject_identity=subject,
                  raw_sha256=hashlib.sha256(raw_path.read_bytes()).hexdigest())
    with output.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")


if __name__ == "__main__":
    main(json.loads(Path(sys.argv[1]).read_text()))
