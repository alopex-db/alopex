"""Non-root supervisor. All detailed outputs stay in the private work volume."""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import unittest
import tarfile
import zipfile

from common import (Failure, atomic_json, contained, digest, private_dir, read_json,
                    require, verify_assets, elf_signature)
from verify_environment import files, io_probe, privileges, resource_limits
from verify_capacity import active_faults
from permissions import private_tree

WORK, INPUT = Path("/work"), Path("/input")
PG = Path("/usr/lib/postgresql/16/bin")


def command(argv, log, seconds, env=None):
    with Path(log).open("ab") as stream:
        child = subprocess.Popen(argv, stdout=stream, stderr=stream, env=env, start_new_session=True)
        try:
            require(child.wait(timeout=seconds) == 0, "CASE")
        except BaseException:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
            raise


class Server:
    def __init__(self, argv, directory, request, *, postgres=False, env=None, data_directory=None):
        self.directory, self.request, self.postgres = directory, request, postgres
        self.data_directory = data_directory if data_directory is not None else directory / "data"
        self.log = (directory / "server.log").open("ab")
        self.child = subprocess.Popen(argv, stdout=self.log, stderr=self.log,
                                      env=env, start_new_session=True,
                                      preexec_fn=lambda: os.sched_setaffinity(0, request["cpu"]["server"]))

    def ready(self, probe):
        deadline = time.monotonic() + self.request["limits"]["case_seconds"]
        while time.monotonic() < deadline:
            require(self.child.poll() is None, "CASE")
            if probe():
                require(os.sched_getaffinity(self.child.pid) == set(self.request["cpu"]["server"]), "POLICY")
                return
            time.sleep(0.05)
        raise Failure("TIMEOUT")

    def stop(self):
        seconds = self.request["limits"]["stop_seconds"]
        try:
            require(self.child.poll() is None, "CASE")
            if self.postgres:
                command([str(PG / "pg_ctl"), "-D", str(self.data_directory), "-m", "fast",
                         "-w", "-t", str(seconds), "stop"], self.directory / "stop.log", seconds + 1)
            else:
                self.child.send_signal(signal.SIGTERM)
            require(self.child.wait(timeout=seconds) == 0, "CASE")
        except BaseException:
            if self.child.poll() is None:
                os.killpg(self.child.pid, signal.SIGKILL)
                self.child.wait()
            raise
        finally:
            self.log.close()


def pg_ready(directory):
    return subprocess.run([str(PG / "pg_isready"), "-h", str(directory / "socket"),
                           "-U", "alopex", "-d", "acceptance"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3).returncode == 0


def postgres(directory, request, *, state_directory=None):
    state = state_directory if state_directory is not None else directory
    data, sock = state / "data", private_dir(state / "socket")
    seconds = request["limits"]["case_seconds"]
    command([str(PG / "initdb"), "-D", str(data), "--username=alopex", "--auth-local=peer",
             "--auth-host=reject", "--no-locale", "--encoding=UTF8"], directory / "initdb.log", seconds)
    settings = {"listen_addresses": "''", "unix_socket_directories": "'" + str(sock) + "'",
                "unix_socket_permissions": "0700", "fsync": "on", "synchronous_commit": "on",
                "autovacuum": "off", "max_parallel_workers_per_gather": "0"}
    with (data / "postgresql.conf").open("a") as config:
        config.write("\n" + "\n".join(k + " = " + v for k, v in settings.items()) + "\n")
    hba = data / "pg_hba.conf"
    reject = "local all all reject\nhost all all all reject\n"
    hba.write_text("local postgres alopex peer\n" + reject)
    server = Server([str(PG / "postgres"), "-D", str(data)], directory, request,
                    postgres=True, data_directory=data)
    try:
        server.ready(lambda: pg_ready(state))
        command([str(PG / "psql"), "-h", str(sock), "-U", "alopex", "-d", "postgres",
                 "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE acceptance OWNER alopex"],
                directory / "bootstrap.log", seconds)
        command([str(PG / "psql"), "-h", str(sock), "-U", "alopex", "-d", "postgres",
                 "-v", "ON_ERROR_STOP=1", "-c", "CREATE ROLE denied_role LOGIN"],
                directory / "bootstrap.log", seconds)
    finally:
        server.stop()
    hba.write_text("local acceptance alopex peer\n" + reject)
    server = Server([str(PG / "postgres"), "-D", str(data)], directory, request,
                    postgres=True, data_directory=data)
    try:
        server.ready(lambda: pg_ready(state))
    except BaseException:
        server.stop()
        raise
    return server


def open_tcp(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def alopex(directory, request, assets, env, *, state_directory=None):
    state = state_directory if state_directory is not None else directory
    server_entry = next(item for item in assets["files"] if item["role"] == "server")
    # These are container-local service ports, not host resource identifiers.
    ports = request.get("ports", {"http": 8080, "grpc": 9090, "admin": 8081})
    config = directory / "alopex.toml"
    config.write_text("\n".join([
        f'http_bind = "127.0.0.1:{ports["http"]}"',
        f'grpc_bind = "127.0.0.1:{ports["grpc"]}"',
        f'admin_bind = "127.0.0.1:{ports["admin"]}"',
        f'data_dir = "{state / "data"}"',
        'copy_allowed_dirs = []', 'auth_mode = { type = "none" }',
    ]) + "\n")
    server = Server([str(contained(INPUT, server_entry["path"])), "--config", str(config)],
                    directory, request, env=env)
    try:
        server.ready(lambda: all(open_tcp(port) for port in ports.values()))
    except BaseException:
        server.stop()
        raise
    return server, ports


def prepare(assets, request):
    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    venv = WORK / "venv"
    command([sys.executable, "-m", "venv", str(venv)], WORK / "results/prepare.log", request["limits"]["case_seconds"])
    python = venv / "bin/python"
    wheel = next(item for item in assets["files"] if item["role"] == "wheel")
    command([str(python), "-m", "pip", "install", "--no-index", "--find-links=/tool/wheelhouse",
             "--no-cache-dir", "psycopg==3.2.10", str(contained(INPUT, wheel["path"]))],
            WORK / "results/prepare.log", request["limits"]["case_seconds"])
    env = dict(os.environ)
    libraries = {str(contained(INPUT, item["path"]).parent) for item in assets["files"] if item["role"] == "library"}
    env["LD_LIBRARY_PATH"] = ":".join(sorted(libraries))
    archive_entry = next(item for item in assets["files"] if item["role"] == "harness")
    archive = contained(INPUT, archive_entry["path"])
    harness = private_dir(WORK / "harness")
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as source:
            source.extractall(harness)
    else:
        with tarfile.open(archive) as source:
            source.extractall(harness, filter="data")
    modules = list(harness.rglob("sql_resources.py"))
    require(len(modules) == 1, "IDENTITY")
    env["ALOPEX_TEST_RESOURCE_MODULE"] = str(modules[0])
    # Archive and wheel modes are inputs, not the execution access policy.
    # Configure them before loading any supplied harness code or client module.
    layout = {"venv": private_tree(venv, configure=True, external_targets=[sys.executable]),
              "harness": private_tree(harness, configure=True)}
    atomic_json(WORK / "results/prepared-permissions.json", layout)
    return python, env


def sql_case(case, request, assets, python, env):
    results = []
    for engine in ("postgresql", "alopex"):
        directory = private_dir(WORK / "results" / (engine + "-" + case))
        # Keep sparse/preallocated DB files on the original work volume.
        # Result collection copies only diagnostics and observations, not WAL
        # capacity or database pages expanded by the Docker archive stream.
        state = private_dir(WORK / "databases" / (engine + "-" + case))
        server, ports = None, None
        error = None
        try:
            if engine == "postgresql":
                server = postgres(directory, request, state_directory=state)
            else:
                server, ports = alopex(directory, request, assets, env, state_directory=state)
            if engine == "postgresql":
                from verify_environment import ownership
                ownership(state / "socket/.s.PGSQL.5432", os.getuid(), os.getgid(), 0o700)
            config = {"case": case, "engine": engine, "server_pid": server.child.pid,
                      "pids": request["limits"]["pids"], "cpu": request["cpu"]["client"],
                      "socket": str(state / "socket"),
                      "url": f'http://127.0.0.1:{ports["http"]}' if ports else None,
                      "result": str(directory / "worker.json")}
            atomic_json(directory / "worker-config.json", config)
            command([str(python), "/tool/verify_sql.py", str(directory / "worker-config.json")],
                    directory / "worker.log", request["limits"]["case_seconds"], env)
            results.append(read_json(directory / "worker.json"))
        except BaseException as exc:
            error = exc
        finally:
            if server:
                try:
                    server.stop()
                except BaseException as stop_error:
                    atomic_json(directory / "stop-error.json", {"error": repr(stop_error)})
                    if error is None:
                        error = stop_error
        if error:
            raise error
        atomic_json(directory / "generated-permissions.json", {
            "database": private_tree(state), "results": private_tree(directory),
        })
    return results


def contract_case(case):
    sys.path.insert(0, "/tool/tests")
    import test_contracts
    groups = {"E03": ["Archives", "Assets"], "E06": ["Capacity"], "E07": ["Accounting"],
              "F02": ["Collection"], "P01": ["Privacy"]}
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(getattr(test_contracts, name))
                               for name in groups[case])
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream).run(suite)
    require(result.wasSuccessful() and result.testsRun > 0 and not result.skipped, "CASE")
    return {"kind": "contract_regression", "tests": result.testsRun}


def failure_case(request):
    directory = private_dir(WORK / "results/faults")
    # Each failure is expected, bounded, and reaped before the next starts.
    for name, code in (("worker", "raise SystemExit(7)"),
                       ("timeout", "import time; time.sleep(30)")):
        try:
            command([sys.executable, "-c", code], directory / (name + ".log"), 1)
        except (Failure, subprocess.TimeoutExpired):
            pass
        else:
            raise Failure("CASE")
    child = subprocess.Popen([str(PG / "postgres"), "-D", str(directory / "absent")],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        require(child.wait(timeout=request["limits"]["case_seconds"]) != 0, "CASE")
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    return {"worker_failure": True, "timeout": True, "db_start_failure": True}


def main():
    os.umask(0o077)
    require(os.getuid() != 0, "POLICY")
    # Environment/contract cases run before SQL preparation as well. Create
    # their private HOME/TMPDIR/cache before Python can fall back to /tmp.
    for name in ("home", "tmp", "cache", "results"):
        private_dir(WORK / name)
    request = read_json(INPUT / "request.json")
    manifest = read_json(INPUT / "assets.json")
    assets = verify_assets(INPUT / "assets.json", manifest["platform"])
    native = elf_signature(sys.executable)
    for item in assets["files"]:
        if item["role"] in ("server", "library"):
            require(elf_signature(contained(INPUT, item["path"])) == native, "IDENTITY")
    before = {item["path"]: item["sha256"] for item in assets["files"]}
    result_dir = private_dir(WORK / "results")
    report = {"schema": "alopex.test-env-result/v1", "cases": [], "status": "failed"}
    python, env = None, None
    for case in request["cases"]:
        row = {"id": case, "status": "failed", "error_code": None}
        try:
            if case == "E01":
                row["evidence"] = privileges(request)
            elif case == "E02":
                row["evidence"] = io_probe(WORK, request["limits"]["case_seconds"])
            elif case == "E04":
                row["evidence"] = files(WORK)
            elif case == "E06":
                row["evidence"] = {"kernel": resource_limits(request), "watchdog": active_faults(WORK),
                                   "contracts": contract_case(case)}
            elif case in ("E03", "E07", "F02", "P01"):
                row["evidence"] = contract_case(case)
            elif case in ("E05", "S01", "S02", "S03"):
                if python is None:
                    python, env = prepare(assets, request)
                row["evidence"] = sql_case(case, request, assets, python, env)
            elif case == "F01":
                row["evidence"] = failure_case(request)
            else:
                # A missing live case is never silently promoted from unit tests.
                raise Failure("INCOMPLETE")
            row["status"] = "passed"
        except BaseException as error:
            row["error_code"] = getattr(error, "code", "CASE")
            row["detail"] = error.detail if isinstance(error, Failure) else repr(error)
        report["cases"].append(row)
        atomic_json(result_dir / "result.json", report)
        if row["status"] != "passed":
            break
    passed = (len(report["cases"]) == len(request["cases"]) and
              all(row["status"] == "passed" for row in report["cases"]))
    try:
        require(all(digest(contained(INPUT, name)) == expected for name, expected in before.items()), "IDENTITY")
    except Exception as error:
        passed = False
        report["input_error"] = repr(error)
    report["status"] = "passed" if passed else "failed"
    atomic_json(result_dir / "result.json", report)
    return 0 if passed else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise Failure("INTERRUPTED")
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
