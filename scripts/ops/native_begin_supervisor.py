"""Run one #684 fixture with separate seed/probe processes in the test container."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sys
import zipfile


def require(value, message):
    if not value:
        raise ValueError(message)


def probes(rows):
    require(type(rows) is int and rows in (0, 100, 1000), "unsupported row count")
    result = [dict(mode=mode, operation=operation, samples=6, repeat=0)
              for mode in ("ro", "rw") for operation in ("noop", "query")]
    result.append(dict(mode="auto", operation="query", samples=6, repeat=0))
    if rows == 1000:
        result.extend(dict(mode="rw", operation="noop", samples=1, repeat=n) for n in (1, 2))
    return result


def extension_digest(wheel):
    with zipfile.ZipFile(wheel) as archive:
        entries = [item for item in archive.infolist()
                   if item.filename.startswith("alopex/_alopex") and item.filename.endswith(".so")]
        require(len(entries) == 1 and 0 < entries[0].file_size <= 256_000_000,
                "wheel extension inventory or bound differs")
        with archive.open(entries[0]) as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_begin_assets(manifest_path, platform=None):
    """Accept the normal product or an exact-source wheel-only begin input."""
    from common import contained, digest, verify_assets

    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema") == "alopex.test-env-assets/v1":
        return verify_assets(manifest_path, platform or manifest["platform"])
    require(manifest.get("schema") == "alopex.native-wheel-assets/v1" and
            manifest.get("platform") == "linux/amd64" and
            (platform is None or platform == manifest["platform"]) and
            isinstance(manifest.get("source_commit"), str) and
            isinstance(manifest.get("source_tree"), str), "wheel-only identity differs")
    files = manifest.get("files")
    require(isinstance(files, list) and len(files) in (3, 7), "wheel-only roles differ")
    roles = {item["role"] for item in files}
    expected = {"wheel", "library", "receipt"}
    if len(files) == 7:
        expected |= {"harness", "measurement_controller", "measurement_worker", "measurement_config"}
    require(roles == expected and len({item["path"] for item in files}) == len(files),
            "wheel-only roles differ")
    require(sum(item["size"] for item in files) < 100_000_000,
            "wheel-only input exceeds bound")
    for item in files:
        path = contained(manifest_path.parent, item["path"])
        require(path.is_file() and not path.is_symlink() and
                path.stat().st_size == item["size"] and digest(path) == item["sha256"],
                "wheel-only asset differs")
    by_role = {item["role"]: item for item in files}
    receipt = json.loads(contained(manifest_path.parent, by_role["receipt"]["path"]).read_text())
    source = {"commit": manifest["source_commit"], "tree": manifest["source_tree"],
              "profile": "release"}
    require(receipt.get("schema") == "alopex.performance-wheel-build/v1" and
            receipt.get("source") == source and
            receipt.get("python_wheel_sha256") == by_role["wheel"]["sha256"] and
            receipt.get("parser_sha256") == by_role["library"]["sha256"],
            "wheel-only receipt differs")
    extension_digest(contained(manifest_path.parent, by_role["wheel"]["path"]))
    return manifest


def prepare_wheel_only(assets, request):
    """Install the verified wheel without the SQL server or harness inputs."""
    from common import atomic_json, contained, private_dir
    from entrypoint import INPUT, WORK, command
    from permissions import private_tree

    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    venv = WORK / "venv"
    command([sys.executable, "-m", "venv", str(venv)],
            WORK / "results/prepare.log", request["limits"]["case_seconds"])
    python = venv / "bin/python"
    wheel = next(item for item in assets["files"] if item["role"] == "wheel")
    command([str(python), "-m", "pip", "install", "--no-index", "--find-links=/tool/wheelhouse",
             "--no-cache-dir", "psycopg==3.2.10", str(contained(INPUT, wheel["path"]))],
            WORK / "results/prepare.log", request["limits"]["case_seconds"])
    env = dict(os.environ)
    libraries = {str(contained(INPUT, item["path"]).parent)
                 for item in assets["files"] if item["role"] == "library"}
    env["LD_LIBRARY_PATH"] = ":".join(sorted(libraries))
    atomic_json(WORK / "results/prepared-permissions.json",
                {"venv": private_tree(venv, configure=True, external_targets=[sys.executable])})
    return python, env


def number(value, positive=False):
    return (type(value) in (int, float) and math.isfinite(value) and
            (value > 0 if positive else value >= 0))


def verify_events(events, config, expected_extension, spec=None):
    require(events and events[0].get("extension_sha256") == expected_extension,
            "loaded extension differs from supplied wheel")
    require(all(events[0].get(key) == config[key] for key in ("rows", "indexes")),
            "fixture identity differs")
    if spec is None:
        require(len(events) == 1 and events[0].get("event") == "seeded" and
                events[0].get("all_rows_unchanged") is True, "seed validation missing")
        return
    require(len(events) == spec["samples"] + 2 and events[0].get("event") == "ready" and
            all(events[0].get(key) == spec[key] for key in ("mode", "operation")),
            "probe identity or coverage differs")
    require(events[0]["initial_cache"]["entries"] == 0, "reader is not fresh")
    for iteration, sample in enumerate(events[1:-1]):
        require(sample.get("event") == "sample" and sample.get("iteration") == iteration and
                sample.get("first_after_open") is (iteration == 0), "sample order differs")
        timings = {"begin_ns", "query_ns", "rollback_ns", "auto_total_ns"}
        active = ({"auto_total_ns"} if spec["mode"] == "auto" else
                  {"begin_ns", "rollback_ns"} | ({"query_ns"} if spec["operation"] == "query" else set()))
        require(all(type(sample.get(key)) is int and sample[key] > 0 for key in active) and
                all(sample.get(key) is None for key in timings - active), "invalid phase timing")
        snapshots = {"before", "after_finish"}
        if spec["mode"] != "auto": snapshots.add("after_begin")
        if spec["operation"] == "query": snapshots.add("after_query")
        for key in snapshots:
            snapshot = sample[key]
            require(number(snapshot["rss_bytes"], positive=True) and
                    snapshot["rss_bytes"] < 2 * 1024**3 and
                    all(number(snapshot[name]) for name in ("allocated_bytes", "logical_bytes")),
                    "invalid RSS or storage observation")
        require(all(sample.get(key) is None for key in
                    {"after_begin", "after_query"} - snapshots), "unexpected phase snapshot")
        if iteration:
            require(sample["cache_before"]["entries"] == config["indexes"], "cache not explicitly warmed")
            if config["rows"] and config["indexes"] and spec["operation"] == "query":
                require(sample["cache_after"]["hits"] > sample["cache_before"]["hits"], "missing cache hit")
    final = events[-1]
    require(final.get("event") == "complete" and final.get("all_rows_unchanged") is True and
            all(number(final.get(key), positive=True) for key in
                ("peak_before_validation_bytes", "peak_after_validation_bytes")),
            "missing final validation or lifetime RSS")


def run_fixture(config, execute, output, expected_extension):
    """Persist every attempted process, including its original failure, before returning."""
    records = []
    try:
        specs = [None, *probes(config["rows"])]
        for index, spec in enumerate(specs):
            record = {"ordinal": index, "spec": spec, "status": "failed"}
            records.append(record)
            try:
                raw = execute(index, spec)
                events = [json.loads(line) for line in raw.read_text().splitlines()]
                verify_events(events, config, expected_extension, spec)
                record.update(status="passed", raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())
            except BaseException as error:
                record["error"] = repr(error)
                raise
            finally:
                output(records)
    finally:
        output(records)
    return records


def main():
    sys.path.insert(0, "/tool")
    from common import atomic_json, contained, digest, private_dir, read_json
    from entrypoint import INPUT, WORK, command, prepare
    from permissions import private_tree
    from verify_environment import privileges, files, resource_limits

    os.umask(0o077)
    require(sys.platform == "linux" and os.getuid() != 0, "normal Linux user required")
    request = read_json(INPUT / "request.json")
    config = read_json(INPUT / "measurement.json")
    require(config["schema"] == "alopex.native-begin-supervisor/v1" and
            type(config["indexes"]) is int and config["indexes"] in (0, 1, 2) and
            type(config["trial"]) is int and config["trial"] in (0, 1, 2), "invalid fixture configuration")
    probes(config["rows"])
    manifest = read_json(INPUT / "assets.json")
    assets = verify_begin_assets(INPUT / "assets.json")
    entries = {}
    for role in ("wheel", "receipt", "measurement_controller", "measurement_worker", "measurement_config"):
        matches = [item for item in assets["files"] if item["role"] == role]
        require(len(matches) == 1, "asset role inventory differs")
        entries[role] = matches[0]
    for role, name in (("measurement_controller", "controller.py"),
                       ("measurement_worker", "worker.py"), ("measurement_config", "measurement.json")):
        require(entries[role]["path"] == name, "operator path differs")
    require(digest(Path(__file__)) == entries["measurement_controller"]["sha256"], "controller identity differs")
    receipt = read_json(contained(INPUT, entries["receipt"]["path"]))
    require(receipt["source"] == config["source"] and
            receipt["python_wheel_sha256"] == entries["wheel"]["sha256"], "product identity differs")
    expected_extension = extension_digest(contained(INPUT, entries["wheel"]["path"]))
    require(expected_extension == config["extension_sha256"], "configured extension differs")
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS",
                "TOKIO_WORKER_THREADS", "POLARS_MAX_THREADS"):
        os.environ[key] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    atomic_json(WORK / "results/environment.json", {
        "privileges": privileges(request), "files": files(WORK), "limits": resource_limits(request)})
    python, env = (prepare_wheel_only(assets, request)
                   if assets["schema"] == "alopex.native-wheel-assets/v1"
                   else prepare(assets, request))
    os.sched_setaffinity(0, request["cpu"]["client"])
    require(os.sched_getaffinity(0) == set(request["cpu"]["client"]), "CPU affinity differs")
    output = private_dir(WORK / "results/cell")
    state = private_dir(WORK / "databases/cell")
    db = state / "db.alopex"
    # Each child independently verifies inherited affinity before importing the probe.
    launcher = ("import os,runpy,sys; "
                "expected=set(map(int,sys.argv.pop(1).split(','))); "
                "assert os.sched_getaffinity(0)==expected, 'CPU affinity differs'; "
                "sys.argv=sys.argv[1:]; runpy.run_path(sys.argv[0],run_name='__main__')")
    def execute(index, spec):
        raw = output / f"process-{index}.ndjson"
        args = [str(python), "-B", "-c", launcher, ",".join(map(str, request["cpu"]["client"])),
                str(INPUT / "worker.py"), "--manual", "--path", str(db),
                "--rows", str(config["rows"]), "--indexes", str(config["indexes"])]
        if spec is None:
            args += ["--action", "seed", "--samples", "1"]
        else:
            args += ["--action", "probe", "--mode", spec["mode"], "--operation", spec["operation"],
                     "--samples", str(spec["samples"])]
        command(args, raw, request["limits"]["case_seconds"], env)
        return raw
    report = dict(schema="alopex.native-begin-measurement/v1", status="failed", source=config["source"],
                  rows=config["rows"], indexes=config["indexes"], trial=config["trial"],
                  assets_digest=digest(INPUT / "assets.json"), extension_sha256=expected_extension,
                  worker_error=None, finalization_error=None)
    try:
        run_fixture(config, execute, lambda records: atomic_json(output / "processes.json", records), expected_extension)
        require(all(digest(contained(INPUT, item["path"])) == item["sha256"] for item in assets["files"]),
                "input changed during measurement")
        report["status"] = "passed"
    except BaseException as error:
        report["worker_error"] = repr(error)
    finally:
        try:
            atomic_json(WORK / "results/generated-permissions.json", {
                "database": private_tree(state), "results": private_tree(output)})
            report["processes_sha256"] = digest(output / "processes.json")
        except BaseException as error:
            report.update(status="failed", finalization_error=repr(error))
        atomic_json(WORK / "results/result.json", report)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("begin supervisor interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
