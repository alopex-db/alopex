"""Fail-closed, private supervisor for one qualified four-case pair.

The caller must supply a source-bound #804 server launcher and staged package.
This module deliberately has no container construction or asset acquisition path.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import threading
import time

from . import adapter, log_parser, oracle, worker as pair_worker

WORKER_SECONDS = 300
PAIR_SECONDS = 420
CLIENT_BYTES = 2_000_000
RAW_BYTES = 1_000_000
RESULT_BYTES = 100_000
WORKER_LOG_BYTES = 1_000_000
PACKAGE_FILE_BYTES = 2_000_000


def require(value, code):
    if not value:
        raise ValueError(code)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def _hex(value, length):
    return type(value) is str and len(value) == length and all(c in "0123456789abcdef" for c in value)


def _private_file(path, limit):
    path = Path(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise ValueError("EVIDENCE_BOUND") from error
    with os.fdopen(fd, "rb") as stream:
        first = os.fstat(stream.fileno())
        require(stat.S_ISREG(first.st_mode) and first.st_uid == os.geteuid() and
                stat.S_IMODE(first.st_mode) == 0o600 and first.st_size <= limit,
                "EVIDENCE_BOUND")
        value = stream.read(limit + 1)
        last = os.fstat(stream.fileno())
        require(len(value) == first.st_size and len(value) <= limit and
                (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns) ==
                (last.st_dev, last.st_ino, last.st_size, last.st_mtime_ns),
                "EVIDENCE_BOUND")
    return value


def _json_file(path, limit):
    try:
        return json.loads(_private_file(path, limit))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("EVIDENCE_FORMAT") from error


def _ndjson(path, limit, expected):
    data = _private_file(path, limit)
    require(data.endswith(b"\n") and len(data.splitlines()) == expected, "EVIDENCE_INCOMPLETE")
    try:
        return [json.loads(line) for line in data.splitlines()], data
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("EVIDENCE_FORMAT") from error


def verify_package(package_root, package_files):
    """Require the complete staged package to match a trusted pin mapping."""
    root = Path(package_root)
    package = root / "four_case_diagnostic"
    require(type(package_files) is dict and package_files and
            {"four_case_diagnostic/__init__.py",
             "four_case_diagnostic/worker.py"} <= set(package_files) and
            all(type(name) is str and name.startswith("four_case_diagnostic/") and
                name.endswith(".py") and ".." not in Path(name).parts and
                _hex(value, 64) for name, value in package_files.items()) and
            root.is_dir() and package.is_dir() and not root.is_symlink() and
            not package.is_symlink(), "WORKER_PACKAGE")
    require({path.name for path in root.iterdir()} == {"four_case_diagnostic"},
            "WORKER_PACKAGE")
    actual = set()
    for path in package.rglob("*"):
        require(not path.is_symlink(), "WORKER_PACKAGE")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
        else:
            require(path.is_dir(), "WORKER_PACKAGE")
    require(actual == set(package_files), "WORKER_PACKAGE")
    for name, expected in package_files.items():
        path = root / name
        require(digest(_private_file(path, PACKAGE_FILE_BYTES)) == expected,
                "WORKER_PACKAGE")


def _terminate_worker(child):
    if child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            child.wait()


def _group_members(group):
    """Read the Linux process table; inability to prove emptiness is failure."""
    require(os.path.isdir("/proc"), "WORKER_CLEANUP")
    members = []
    try:
        for name in os.listdir("/proc"):
            if not name.isdecimal():
                continue
            try:
                line = Path("/proc") / name / "stat"
                fields = line.read_text().rsplit(")", 1)[1].split()
            except FileNotFoundError:
                continue
            if int(fields[2]) == group and int(fields[3]) == group:
                members.append(int(name))
    except (OSError, IndexError, ValueError) as error:
        raise ValueError("WORKER_CLEANUP") from error
    return members


def _clear_group(group):
    """Terminate descendants after leader exit and prove the session is empty."""
    if not _group_members(group):
        return False
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if not _group_members(group):
                return True
            time.sleep(0.05)
    require(not _group_members(group), "WORKER_CLEANUP")
    return True


def _run_worker(argv, log, seconds, env, cwd):
    """Own, time-bound, and reap the entire worker process group."""
    require(0 < seconds <= WORKER_SECONDS, "PAIR_TIMEOUT")
    fd = os.open(log, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        child = subprocess.Popen(argv, stdout=stream, stderr=stream, env=env,
                                 cwd=str(cwd),
                                 start_new_session=True)
        try:
            code = child.wait(timeout=seconds)
            orphaned = _clear_group(child.pid)
            require(not orphaned, "WORKER_ORPHAN")
            require(code == 0, "WORKER_EXIT")
        except BaseException as error:
            try:
                _terminate_worker(child)
                _clear_group(child.pid)
            except BaseException:
                error.worker_cleanup_error = "WORKER_CLEANUP"
            raise


def validate_config(config, source, assets_digest, job_id):
    require(type(config) is dict and config.get("schema") == "alopex.sql-four-case-pair/v1" and
            config.get("engine") == "alopex-server" and config.get("case") in adapter.CASES and
            config.get("modes") == ["on", "off"] and type(config.get("trial")) is int and
            config["trial"] in range(3) and config.get("source") == source and
            type(source) is dict and set(source) == {"commit", "tree", "profile"} and
            _hex(source["commit"], 40) and _hex(source["tree"], 40) and
            source["profile"] == "release" and config.get("assets_digest") == assets_digest and
            _hex(assets_digest, 64) and config.get("same_job_id") == job_id and
            type(job_id) is str and job_id.isdecimal() and
            _hex(config.get("fixture_sha256"), 64), "PAIR_CONFIG")


def worker_config(config, output, docs, product):
    """Create only new, private output names for the package worker."""
    output = Path(output)
    require(output.is_dir() and not output.is_symlink() and
            stat.S_IMODE(output.stat().st_mode) == 0o700, "OUTPUT_PARENT")
    require(all(not (output / name).exists() and not (output / name).is_symlink()
                for name in ("server.log", "worker.log", "worker.json")),
            "OUTPUT_STATE")
    paths = {}
    for mode in ("on", "off"):
        cell = output / mode
        require(cell.is_dir() and not cell.is_symlink() and
                stat.S_IMODE(cell.stat().st_mode) == 0o700, "OUTPUT_PARENT")
        paths[mode] = {"raw": str(cell / "raw.ndjson"),
                       "client": str(cell / "client.ndjson"),
                       "result": str(cell / "measurement.json")}
        require(all(not Path(value).exists() for value in paths[mode].values()), "OUTPUT_STATE")
    return dict(config, outputs=paths, docs_harness_root=str(docs),
                product_harness_root=str(product))


def _verify_subject(env, config, identity, endpoint):
    subject_path = Path(env["ALOPEX_PERF_SUBJECT_MANIFEST"])
    subject_data = _private_file(subject_path, RESULT_BYTES)
    require(digest(subject_data) == env["ALOPEX_PERF_SUBJECT_SHA256"], "SUBJECT_BINDING")
    subject = json.loads(subject_data)
    server = subject.get("server") if type(subject) is dict else None
    require(type(subject) is dict and subject.get("source") == config["source"] and
            type(server) is dict and
            all(server.get(key) == value for key, value in identity.items()) and
            server.get("url") == endpoint and env["ALOPEX_PERF_SERVER_URL"] == endpoint,
            "SUBJECT_BINDING")
    resource = _json_file(env["ALOPEX_PERF_SQL_RESOURCE_MANIFEST"], RESULT_BYTES)
    servers = resource.get("servers") if type(resource) is dict else None
    require(type(resource) is dict and resource.get("schema") ==
            "alopex.sql-resource-servers/v1" and
            type(servers) is dict and set(servers) in
            ({"alopex-server"}, {"alopex-server", "postgresql"}) and
            servers["alopex-server"] == identity, "SUBJECT_BINDING")
    if "postgresql" in servers:
        pg = servers["postgresql"]
        require(type(pg) is dict and set(pg) == {"pid", "start_ticks"} and
                all(type(value) is int and value > 0 for value in pg.values()) and
                pg != identity and pg["pid"] != identity["pid"], "SUBJECT_BINDING")
    return digest(_private_file(env["ALOPEX_PERF_SQL_RESOURCE_MANIFEST"], RESULT_BYTES))


def verify_output(config, output, identity, resource_digest):
    """Verify both cells and join the complete client inventory to the server log."""
    output = Path(output)
    clients = {}
    cells = {}
    tree = None
    client_identity = None
    for mode in ("on", "off"):
        root = output / mode
        cell = _json_file(root / "measurement.json", RESULT_BYTES)
        require(type(cell) is dict and cell.get("case") == config["case"] and
                cell.get("trial") == config["trial"] and cell.get("mode") == mode and
                cell.get("source") == config["source"] and
                cell.get("assets_digest") == config["assets_digest"] and
                cell.get("same_job_id") == config["same_job_id"] and
                cell.get("fixture_sha256") == config["fixture_sha256"] and
                cell.get("pair_server") == identity and cell.get("cleanup_passed") is True,
                "CELL_IDENTITY")
        require(set(cell) == {"case", "trial", "mode", "source", "assets_digest",
                              "same_job_id", "fixture_sha256", "pair_server",
                              "cleanup_passed", "result_sha256", "rollback_sha256",
                              "raw_sha256", "raw_seconds", "raw_ordinals", "pair_order"} and
                cell["result_sha256"] == oracle.ORACLE_SHA256[config["case"]] and
                cell["rollback_sha256"] == (oracle.ROLLBACK_SHA256 if
                config["case"] == "insert-rollback" else None) and
                cell["pair_order"] == [list(adapter.pair_order(config["trial"], i))
                                       for i in range(21)], "CELL_IDENTITY")
        raw, raw_bytes = _ndjson(root / "raw.ndjson", RAW_BYTES, 21)
        require(cell.get("raw_sha256") == digest(raw_bytes) and
                cell.get("raw_seconds") == [row.get("seconds") for row in raw] and
                cell.get("raw_ordinals") == [row.get("pair_ordinal") for row in raw],
                "RAW_IDENTITY")
        clients[mode], _ = _ndjson(root / "client.ndjson", CLIENT_BYTES,
                                   adapter.expected_client_records(config["case"], mode))
        measured = {row.get("sample"): row for row in clients[mode]
                    if type(row) is dict and row.get("phase") == "measured"}
        require(len(measured) == 21 and all(
            type(measured.get(index, {}).get("outer_ns")) is int and
            measured[index]["outer_ns"] == round(row["seconds"] * 1_000_000_000) and
            measured[index].get("pair_ordinal") == row["pair_ordinal"]
            for index, row in enumerate(raw)), "CLIENT_RAW_JOIN")
        for index, row in enumerate(raw):
            require(type(row) is dict and set(row) ==
                    {"sample", "seconds", "pair_ordinal", "resources"} and
                    row["sample"] == index and row["pair_ordinal"] ==
                    2 * index + adapter.pair_order(config["trial"], index).index(mode),
                    "RAW_IDENTITY")
            try:
                client_identity, tree = pair_worker._resource(
                    row["resources"], resource_digest,
                    (identity["pid"], identity["start_ticks"]), client_identity, tree)
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError("RESOURCE_IDENTITY") from error
        cells[mode] = cell
    require(cells["on"]["result_sha256"] == cells["off"]["result_sha256"] and
            cells["on"]["rollback_sha256"] == cells["off"]["rollback_sha256"],
            "PAIR_IDENTITY")
    events = log_parser.parse_server_timing(output / "server.log", config["case"],
                                            config["trial"], clients["on"], clients["off"])
    return {"cells": cells, "clients": clients, "server_timing": events,
            "server": identity, "client_identity": client_identity}


def run_owned_pair(config, output, *, source, assets_digest, job_id, start,
                   python, package_root, package_files, docs, product,
                   subject_environment, identity_of, monotonic=time.monotonic):
    """Run a staged package worker against one caller-qualified owned server.

    ``start`` owns the unchanged #804 runtime policy. This module kills and
    reaps its worker process group on timeout. SIGALRM provides a hard
    420-second cap over startup, worker and owned-server shutdown.
    """
    validate_config(config, source, assets_digest, job_id)
    require(type(subject_environment) is dict and isinstance(package_root, (str, Path)),
            "WORKER_PACKAGE")
    verify_package(package_root, package_files)
    output = Path(output)
    require(output.is_dir() and not output.is_symlink() and
            output.parent.is_dir() and not output.parent.is_symlink() and
            output.parent.stat().st_uid == os.geteuid() and
            stat.S_IMODE(output.parent.stat().st_mode) == 0o700 and
            output.stat().st_uid == os.geteuid() and
            stat.S_IMODE(output.stat().st_mode) == 0o700,
            "OUTPUT_PARENT")
    worker = worker_config(config, output, docs, product)
    require(os.name == "posix" and threading.current_thread() is threading.main_thread() and
            signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), "PAIR_TIMER")
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expired(_signum, _frame):
        raise TimeoutError("PAIR_TIMEOUT")

    previous_umask = os.umask(0o077)
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, PAIR_SECONDS)
    report = {"status": "failed", "primary_error": None,
              "worker_cleanup_error": None, "stop_error": None}
    server = None
    identity = None
    resource_digest = None
    worker_completed = False
    began = monotonic()
    try:
        server, endpoint = start()
        identity = identity_of(server)
        require(type(identity) is dict and set(identity) == {"pid", "start_ticks"} and
                all(type(v) is int and v > 0 for v in identity.values()) and
                type(endpoint) is str and endpoint.startswith("http://127.0.0.1:"),
                "SERVER_IDENTITY")
        require(monotonic() - began < PAIR_SECONDS, "PAIR_TIMEOUT")
        env = dict(subject_environment)
        require(env.get("ALOPEX_PERF_SERVER_URL") == endpoint and
                env.get("ALOPEX_PERF_SUBJECT_MANIFEST") and
                env.get("ALOPEX_PERF_SUBJECT_SHA256") and
                env.get("ALOPEX_PERF_SQL_RESOURCE_MANIFEST"), "SUBJECT_BINDING")
        resource_digest = _verify_subject(env, config, identity, endpoint)
        worker_path = output / "worker.json"
        fd = os.open(worker_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(worker, stream, sort_keys=True)
            stream.write("\n")
        for key in ("PYTHONHOME", "PYTHONSTARTUP", "PYTHONUSERBASE",
                    "PYTHONINSPECT", "PYTHONPATH"):
            env.pop(key, None)
        env["PYTHONPATH"] = str(Path(package_root).resolve())
        env["PYTHONNOUSERSITE"] = "1"
        argv = [str(python), "-B", "-P", "-m", "four_case_diagnostic.worker",
                str(worker_path)]
        verify_package(package_root, package_files)
        _run_worker(argv, output / "worker.log",
                    min(WORKER_SECONDS, PAIR_SECONDS - (monotonic() - began)), env,
                    Path(package_root).resolve())
        worker_completed = True
        require(monotonic() - began < PAIR_SECONDS, "PAIR_TIMEOUT")
        require(len(_private_file(output / "worker.log", WORKER_LOG_BYTES)) <= WORKER_LOG_BYTES,
                "EVIDENCE_BOUND")
    except BaseException as error:
        report["worker_cleanup_error"] = getattr(error, "worker_cleanup_error", None)
        code = error.args[0] if type(error) is ValueError and error.args else None
        allowed = {"WORKER_PACKAGE", "OUTPUT_PARENT", "OUTPUT_STATE", "SERVER_IDENTITY",
                   "SUBJECT_BINDING", "PAIR_TIMEOUT", "EVIDENCE_BOUND"}
        report["primary_error"] = ("PAIR_TIMEOUT" if isinstance(error, TimeoutError) else
                                   "WORKER_TIMEOUT" if isinstance(error, subprocess.TimeoutExpired)
                                   else code if code in allowed else "PAIR_FAILED")
    finally:
        if server is not None:
            try:
                server.stop()
                require(server.child.poll() is not None, "SERVER_STOP")
            except BaseException:
                report["stop_error"] = "SERVER_STOP"
        if monotonic() - began > PAIR_SECONDS and report["primary_error"] is None:
            report["primary_error"] = "PAIR_TIMEOUT"
        if worker_completed and report["primary_error"] is None and \
                report["worker_cleanup_error"] is None and report["stop_error"] is None:
            try:
                verify_package(package_root, package_files)
                require(_verify_subject(subject_environment, config, identity, endpoint) ==
                        resource_digest, "SUBJECT_BINDING")
                report["measurement"] = verify_output(config, output, identity, resource_digest)
            except BaseException:
                report["primary_error"] = "EVIDENCE_INCOMPLETE"
        if report["primary_error"] is None and report["worker_cleanup_error"] is None and \
                report["stop_error"] is None:
            report["status"] = "passed"
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        os.umask(previous_umask)
    return report


def run_collected_pair(config, run, proof, receipt):
    """Verify one *deleted* owned container run and its complete pair evidence.

    The caller must first attest #804, execute and collect via its pinned
    Docker/Run adapter, and invoke ``Run.clean``. This does not accept a host
    process or URL handoff. The report's PID and loopback URL are meaningful
    only inside the container namespaces recorded by the pinned entrypoint.
    """
    from .stage_pair import validate_stage_binding

    root = Path(run.root)
    collected = root / "collected"
    # The in-container output is copied to the run collection. The staging
    # boundary is checked here without creating a host-side pair directory.
    validate_stage_binding(config, collected, proof, receipt)
    require(all((collected / mode).is_dir() and
                not (collected / mode).is_symlink() and
                (collected / mode).stat().st_uid == os.geteuid() and
                stat.S_IMODE((collected / mode).stat().st_mode) == 0o700
                for mode in ("on", "off")), "OUTPUT_PARENT")
    require(run.record.get("phase") == "deleted" and
            not (root / "cleanup-errors.json").exists() and
            run.record.get("identities") == {
                "image_digest": proof["image_digest"],
                "assets_digest": config["assets_digest"],
                "source": config["source"],
                "job_id": config["same_job_id"]}, "CLEANUP")
    run.verify_collection()
    report = _json_file(collected / "result.json", RESULT_BYTES)
    require(type(report) is dict and set(report) == {
                "schema", "status", "config", "image_digest", "profile_sha256",
                "server", "client", "resource_manifest_sha256", "cleanup"} and
            report["schema"] == "alopex.sql-four-case-container-pair/v1" and
            report["status"] == "passed" and report["config"] == config and
            report["image_digest"] == proof["image_digest"] and
            report["profile_sha256"] == proof["profile_sha256"], "PAIR_FAILED")
    server, client = report["server"], report["client"]
    require(type(server) is dict and type(client) is dict and
            set(server) == {"pid", "start_ticks", "url", "pid_namespace", "net_namespace"} and
            set(client) == {"pid", "start_ticks", "pid_namespace", "net_namespace"} and
            report["cleanup"] == {"server_reaped": True,
                                  "worker_reaped": True,
                                  "process_groups_empty": True} and
            _hex(report["resource_manifest_sha256"], 64), "PAIR_IDENTITY")
    from .container_pair_entrypoint import validate_process_pair
    validate_process_pair({key: server[key] for key in client}, client,
                          server["url"])
    identity = {"pid": server["pid"], "start_ticks": server["start_ticks"]}
    require(digest(_private_file(collected / "resources.json", RESULT_BYTES)) ==
            report["resource_manifest_sha256"], "RESOURCE_IDENTITY")
    _verify_subject({"ALOPEX_PERF_SUBJECT_MANIFEST": str(collected / "subject.json"),
                     "ALOPEX_PERF_SUBJECT_SHA256": digest(_private_file(
                         collected / "subject.json", RESULT_BYTES)),
                     "ALOPEX_PERF_SQL_RESOURCE_MANIFEST": str(collected / "resources.json"),
                     "ALOPEX_PERF_SERVER_URL": server["url"]},
                    config, identity, server["url"])
    verified = verify_output(config, collected, identity,
                             report["resource_manifest_sha256"])
    require(verified["client_identity"] ==
            (client["pid"], client["start_ticks"]), "CLIENT_IDENTITY")
    return {"status": "passed", "server": identity,
            "server_timing": verified["server_timing"],
            "collection_verified": True, "owned_resources_cleaned": True}
