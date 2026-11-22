"""One private, source-pinned four-case pair on a qualified Linux server.

The supervisor authenticates the configuration and kills this process at its
hard deadline. This worker independently checks the live subject, harness bytes,
resource observations, and pair identity before publishing cell records.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import stat
import statistics
import sys

from . import adapter, oracle, runner


COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes",
            "rss_after_bytes", "read_bytes", "write_bytes", "cancelled_write_bytes")
THREADS = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "POLARS_MAX_THREADS")


def require(condition, code):
    if not condition:
        raise ValueError(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def _hex(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    require(spec is not None and spec.loader is not None, "HARNESS_MODULE")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _harness(root, files):
    root = Path(root) / "scripts/performance"
    require(type(files) is dict and files and all(type(name) is str and
            type(value) is str and _hex(value) for name, value in files.items()),
            "HARNESS_INVENTORY")
    require(root.is_dir() and not root.is_symlink(), "HARNESS_INVENTORY")
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
    require(actual == set(files), "HARNESS_INVENTORY")
    for name, expected in files.items():
        path = root / name
        require(path.resolve().is_relative_to(root.resolve()) and not path.is_symlink() and
                digest(path.read_bytes()) == expected, "HARNESS_BYTES")
    return root


def _paths(config):
    require(type(config.get("outputs")) is dict and set(config["outputs"]) == {"on", "off"},
            "OUTPUT_PATHS")
    paths = {}
    for mode in ("on", "off"):
        names = config["outputs"][mode]
        require(type(names) is dict and set(names) == {"result", "raw", "client"}, "OUTPUT_PATHS")
        paths[mode] = {name: Path(value) for name, value in names.items()}
    flat = [p for cell in paths.values() for p in cell.values()]
    require(len({str(p.resolve()) for p in flat}) == 6 and all(p.is_absolute() and
            not p.exists() and not p.is_symlink() and p.name not in ("", ".", "..")
            for p in flat), "OUTPUT_PATHS")
    for path in flat:
        info = path.parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700, "OUTPUT_PARENT")
    parents = {paths[mode][kind].parent for mode in ("on", "off")
               for kind in ("result", "raw", "client")}
    require(parents == {paths["on"]["raw"].parent,
                        paths["off"]["raw"].parent} and
            paths["on"]["raw"].parent != paths["off"]["raw"].parent and
            paths["on"]["raw"].parent.parent == paths["off"]["raw"].parent.parent and
            paths["on"]["raw"].parent.name == "on" and
            paths["off"]["raw"].parent.name == "off", "OUTPUT_PATHS")
    output = paths["on"]["raw"].parent.parent
    info = output.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700, "OUTPUT_PARENT")
    return paths


def _resource(row, resource_digest, server_identity, client_identity, server_tree):
    require(type(row) is dict and row.get("schema") == "alopex.sql-operation-resources/v1" and
            row.get("status") == "observed" and
            row.get("coverage") == "stable_pid_namespace_allocation" and
            row.get("manifest_sha256") == resource_digest, "RESOURCE_COVERAGE")
    client, servers = row.get("client"), row.get("server_processes")
    require(type(client) is dict and type(servers) is list and
            1 <= len(servers) <= 64 and all(type(server) is dict for server in servers),
            "RESOURCE_SCOPE")
    for process in (client, *servers):
        require(type(process.get("pid")) is int and process["pid"] > 0 and
                type(process.get("start_ticks")) is int and process["start_ticks"] > 0 and
                all(type(process.get(key)) in (int, float) and math.isfinite(process[key]) and
                    process[key] >= 0 for key in COUNTERS), "RESOURCE_COUNTERS")
    observed_tree = frozenset((server.get("pid"), server.get("start_ticks"))
                              for server in servers)
    require(len(observed_tree) == len(servers) and server_identity in observed_tree and
            len({server.get("pid") for server in servers}) == len(servers) and
            client.get("pid") not in {server.get("pid") for server in servers} and
            (server_tree is None or server_tree == observed_tree), "SERVER_IDENTITY")
    identity = (client["pid"], client["start_ticks"])
    require(client_identity is None or client_identity == identity, "CLIENT_IDENTITY")
    return identity, observed_tree


def _verify_cell(summary, raw_bytes, config, mode, resource_digest, server_identity,
                 client_identity, server_tree):
    case, trial = config["case"], config["trial"]
    require(type(summary) is dict and summary.get("case") == case and
            summary.get("engine") == "alopex-server" and summary.get("server_client") is True and
            summary.get("warmups") == 3 and summary.get("samples") == 21 and
            summary.get("fixture_rows") == (1 if case == "insert-rollback" else oracle.ROWS) and
            summary.get("result_sha256") == oracle.ORACLE_SHA256[case] and
            summary.get("rollback_sha256") == (oracle.ROLLBACK_SHA256 if case == "insert-rollback" else None),
            "RESULT_IDENTITY")
    require(raw_bytes.endswith(b"\n") and len(raw_bytes.splitlines()) == 21 and
            len(raw_bytes) <= 1_000_000, "RAW_INVENTORY")
    seconds, ordinals = [], []
    for index, line in enumerate(raw_bytes.splitlines()):
        require(len(line) <= 50_000, "RAW_BOUND")
        row = json.loads(line)
        ordinal = 2 * index + adapter.pair_order(trial, index).index(mode)
        require(type(row) is dict and set(row) == {"sample", "seconds", "pair_ordinal", "resources"} and
                row["sample"] == index and row["pair_ordinal"] == ordinal and
                type(row["seconds"]) in (int, float) and math.isfinite(row["seconds"]) and
                row["seconds"] > 0, "RAW_SAMPLE")
        client_identity, server_tree = _resource(row["resources"], resource_digest,
                                                 server_identity, client_identity, server_tree)
        seconds.append(row["seconds"])
        ordinals.append(ordinal)
    require(type(summary.get("latency_p50_ms")) in (int, float) and
            math.isclose(summary["latency_p50_ms"], statistics.median(seconds) * 1000,
                         rel_tol=1e-12), "RESULT_LATENCY")
    return seconds, ordinals, client_identity, server_tree


def _write_private(path, value):
    with runner._open_private_raw(path) as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def run_pair(config, *, subject, observer, parity, connect, remote_type, txn_mode,
             resource_digest, manifest_path=None):
    """Execute a pair with already verified live dependencies; return audit cells."""
    require(type(config) is dict and config.get("schema") == "alopex.sql-four-case-pair/v1" and
            config.get("case") in adapter.CASES and config.get("engine") == "alopex-server" and
            config.get("modes") == ["on", "off"] and type(config.get("trial")) is int and
            config["trial"] in range(3), "PAIR_CONFIG")
    require(type(config.get("source")) is dict and config["source"] == subject.get("source") and
            _hex(config.get("assets_digest")) and type(config.get("same_job_id")) is str and
            bool(config["same_job_id"]) and _hex(config.get("fixture_sha256")), "SUBJECT_BINDING")
    server = subject.get("server")
    require(type(server) is dict and type(server.get("pid")) is int and
            type(server.get("start_ticks")) is int and server.get("url") ==
            os.environ.get("ALOPEX_PERF_SERVER_URL"), "SUBJECT_SERVER")
    require(observer is not None and callable(getattr(observer, "begin", None)) and
            callable(getattr(observer, "end", None)) and
            observer.manifest_sha256 == resource_digest and
            (observer.pid, observer.start) == (server["pid"], server["start_ticks"]),
            "RESOURCE_OBSERVER")
    paths = _paths(config)
    output = paths["on"]["raw"].parent.parent
    timing_root = output.resolve() / "public-timing"
    require(not timing_root.exists() and not timing_root.is_symlink(), "TIMING_ROOT")
    timing_root.mkdir(mode=0o700)
    timing_root.chmod(0o700)
    summaries, clients = runner.measure_pair(parity, config["case"], config["trial"],
        {mode: paths[mode]["raw"] for mode in ("on", "off")}, connect=connect,
        remote_type=remote_type, url=server["url"], txn_mode=txn_mode,
        observer=observer, timing_root=timing_root, evidence_mode="public")
    require(type(summaries) is dict and set(summaries) == {"on", "off"} and
            type(clients) is dict and set(clients) == {"on", "off"}, "PAIR_RESULT")
    identity = (server["pid"], server["start_ticks"])
    cells = {}
    client_identity = None
    server_tree = None
    for mode in ("on", "off"):
        raw = paths[mode]["raw"].read_bytes()
        seconds, ordinals, client_identity, server_tree = _verify_cell(
            summaries[mode], raw, config, mode, resource_digest, identity,
            client_identity, server_tree)
        require(type(clients[mode]) is list and
                len(clients[mode]) == adapter.expected_client_records(config["case"], mode),
                "CLIENT_INVENTORY")
        cells[mode] = {"case": config["case"], "trial": config["trial"], "mode": mode,
                       "result_sha256": summaries[mode]["result_sha256"],
                       "raw_seconds": seconds, "raw_ordinals": ordinals,
                       "raw_sha256": digest(raw),
                       "pair_server": {"pid": identity[0], "start_ticks": identity[1]},
                       "fixture_sha256": config["fixture_sha256"],
                       "pair_order": [list(adapter.pair_order(config["trial"], i)) for i in range(21)],
                       "source": config["source"], "assets_digest": config["assets_digest"],
                       "same_job_id": config["same_job_id"],
                       "rollback_sha256": summaries[mode]["rollback_sha256"],
                       "cleanup_passed": True}
    require(summaries["on"]["result_sha256"] == summaries["off"]["result_sha256"],
            "PAIR_ORACLE")
    if manifest_path is not None:
        require(digest(Path(manifest_path).read_bytes()) == resource_digest,
                "RESOURCE_MANIFEST_CHANGED")
    for mode in ("on", "off"):
        _write_private(paths[mode]["client"], "".join(json.dumps(row, sort_keys=True,
            allow_nan=False) + "\n" for row in clients[mode]))
        _write_private(paths[mode]["result"], json.dumps(cells[mode], sort_keys=True,
            allow_nan=False) + "\n")
    return cells


def main(config):
    require(sys.platform == "linux" and os.geteuid() != 0, "LINUX_NONROOT")
    require(all(os.environ.get(key) == "1" for key in THREADS), "THREAD_POLICY")
    os.umask(0o077)
    docs = _harness(config["docs_harness_root"], config["docs_files"])
    product = _harness(config["product_harness_root"], config["product_files"])
    sys.path.insert(0, str(docs))
    from subject_identity import cpu_assignment, verify_subject
    from sql_resources import ENV, OperationResources
    require(os.environ.get("ALOPEX_PERF_SUBJECT_MANIFEST") and
            os.environ.get("ALOPEX_PERF_SUBJECT_SHA256") and os.environ.get(ENV),
            "QUALIFIED_MANIFESTS")
    subject = verify_subject(require_server=True)
    allocation = cpu_assignment(subject)
    os.sched_setaffinity(0, set(allocation["client"]))
    require(os.sched_getaffinity(0) == set(allocation["client"]), "CLIENT_AFFINITY")
    resource_digest = digest(Path(os.environ[ENV]).read_bytes())
    observer = OperationResources.from_environment("alopex-server", None)
    require(observer is not None, "RESOURCE_OBSERVER")
    sys.path.insert(0, str(product))
    parity = _load_module("four_case_product_parity", product / "parity_v0811_measure.py")
    import alopex
    result = run_pair(config, subject=subject, observer=observer, parity=parity,
        connect=alopex.connect, remote_type=alopex.RemoteDatabase,
        txn_mode=alopex.TxnMode.READ_WRITE, resource_digest=resource_digest,
        manifest_path=os.environ[ENV])
    return result


if __name__ == "__main__":
    main(json.loads(Path(sys.argv[1]).read_text()))
