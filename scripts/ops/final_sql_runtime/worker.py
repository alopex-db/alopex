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
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes", "rss_after_bytes",
            "read_bytes", "write_bytes", "cancelled_write_bytes")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def expected(case):
    require(case in ISOLATED, "excluded SQL case")
    if case == "aggregate":
        return [{"count": 17125}]
    if case == "ordered":
        return [{"id": i, "value": float(i)} for i in range(16)]
    if case == "select-all":
        return [{"id": i, "value": float(i)} for i in range(0, 34250, 2)]
    return [{"id": 3}]


def result_digest(case):
    return hashlib.sha256(json.dumps(expected(case), sort_keys=True).encode()).hexdigest()


def validate_exact(case, result, rows):
    require(rows == 34250 and type(result) is list, "fixture or result shape mismatch")
    wanted = expected(case)
    require(len(result) == len(wanted), "incomplete SQL result")
    for actual, reference in zip(result, wanted):
        require(type(actual) is dict and set(actual) == set(reference) and
                all(type(actual[key]) is type(value) and actual[key] == value
                    for key, value in reference.items()), "SQL result order/value/type mismatch")
    return result_digest(case)


def verify_cell(result, raw, config, resource_digest):
    """Reject missing samples, altered boundaries and incomplete resource cells."""
    case, engine = config["case"], config["engine"]
    require(case in ISOLATED, "case outside independent SQL lane")
    require(engine == ("postgresql" if config["subject"] == "postgresql" else "alopex-server")
            and config["subject"] in ("published", "before", "candidate", "postgresql"),
            "subject engine mismatch")
    require(type(config["trial"]) is int and config["trial"] in range(3), "invalid trial")
    require(result["case"] == case and result["engine"] == engine and
            result["server_client"] is True, "measurement boundary mismatch")
    require(result["warmups"] == 3 and result["samples"] == len(raw) == 21,
            "sample count mismatch")
    require(result["fixture_rows"] == ISOLATED.get(case, 10000), "fixture size mismatch")
    require(result["result_sha256"] == result_digest(case), "SQL result mismatch")
    require(result["sql"] == {"aggregate": "SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
                              "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
                              "select-all": "SELECT id, value FROM bench WHERE enabled = true",
                              "insert-rollback": "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"}[case],
            "SQL operation mismatch")
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


def enforce_complete_result_order(module, *, capture_exact=False):
    """Keep archived SQL while checking every result and optionally retaining one."""
    captured = {"raw": None}

    def checked(case, result, rows):
        checksum = validate_exact(case, result, rows)
        if capture_exact and captured["raw"] is None:
            require(case == "select-all", "preflight must select all rows")
            raw = json.dumps({"rows": rows, "result": result},
                             sort_keys=True, allow_nan=False).encode()
            require(len(raw) <= 2_000_000, "preflight result exceeds bound")
            captured["raw"] = raw
        return checksum

    module.validate = checked
    return captured


def main(config):
    require(sys.platform == "linux" and os.geteuid() != 0, "native unprivileged Linux worker required")
    require(config["schema"] == "alopex.native-sql-measurement-cell/v1", "unknown cell schema")
    require(config["case"] in ISOLATED, "case outside independent SQL lane")
    phase = config.get("phase")
    require(phase in ("preflight", "cell") and
            (phase != "preflight" or config["case"] == "select-all"),
            "invalid measurement phase")
    require(config.get("subject") in ("published", "before", "candidate", "postgresql") and
            config["engine"] == ("postgresql" if config["subject"] == "postgresql" else "alopex-server"),
            "subject engine mismatch")
    require(type(config["trial"]) is int and config["trial"] in range(3), "invalid trial")
    os.umask(0o077)
    output = Path(config["result"])
    raw_path = Path(config["raw"])
    exact_path = Path(config["exact_result"]) if phase == "preflight" else None
    require(not output.exists() and not raw_path.exists(), "immutable cell output already exists")
    require(exact_path is None or not exact_path.exists(), "immutable preflight output already exists")
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
    isolated = load_module("fixed_isolated_sql", docs / "server-sql-isolated-v0816.py")
    captured = enforce_complete_result_order(isolated, capture_exact=phase == "preflight")
    result = isolated.measure(parity, config["engine"], config["case"], raw_path)
    raw = [json.loads(line) for line in raw_path.read_text().splitlines()]
    verify_cell(result, raw, config, resource_digest)
    require(hashlib.sha256(Path(os.environ[ENV]).read_bytes()).hexdigest() == resource_digest,
            "resource manifest changed")
    if exact_path is not None:
        require(captured["raw"] is not None, "missing exact preflight result")
        with exact_path.open("xb") as stream:
            stream.write(captured["raw"])
    result.update(trial=config["trial"], subject_identity=subject,
                  raw_sha256=hashlib.sha256(raw_path.read_bytes()).hexdigest())
    with output.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")


if __name__ == "__main__":
    main(json.loads(Path(sys.argv[1]).read_text()))
