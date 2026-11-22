"""Offline pair lifecycle tests; no container or live server is started."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from . import adapter, oracle, public_timing, supervisor
from .test_log_parser import pair


SOURCE = {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"}
ASSETS = "c" * 64
IDENTITY = {"pid": 201, "start_ticks": 301}
ENDPOINT = "http://127.0.0.1:8080"
COUNTERS = {"user_cpu_seconds": 0.1, "system_cpu_seconds": 0.2,
            "rss_before_bytes": 1024, "rss_after_bytes": 2048,
            "read_bytes": 0, "write_bytes": 0, "cancelled_write_bytes": 0}


def private_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True) + "\n")
    path.chmod(0o600)


class Child:
    def poll(self):
        return 0


class Server:
    child = Child()

    def __init__(self, stop_error=False):
        self.stopped = False
        self.stop_error = stop_error

    def stop(self):
        self.stopped = True
        if self.stop_error:
            raise RuntimeError("private stop detail")


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.output = self.root / "pair"
        self.output.mkdir(mode=0o700)
        for mode in ("on", "off"):
            (self.output / mode).mkdir(mode=0o700)
        self.package = self.root / "package" / "four_case_diagnostic"
        self.package.mkdir(parents=True)
        self.package.parent.chmod(0o700)
        self.package.chmod(0o700)
        (self.package / "worker.py").write_text("# staged by trusted caller\n")
        (self.package / "worker.py").chmod(0o600)
        (self.package / "public_timing.py").write_text("# staged by trusted caller\n")
        (self.package / "public_timing.py").chmod(0o600)
        (self.package / "__init__.py").write_text("")
        (self.package / "__init__.py").chmod(0o600)
        self.package_files = {f"four_case_diagnostic/{name}":
                              supervisor.digest((self.package / name).read_bytes())
                              for name in ("__init__.py", "worker.py", "public_timing.py")}
        self.subject = self.root / "subject.json"
        private_json(self.subject, {"source": SOURCE, "server": dict(IDENTITY, url=ENDPOINT)})
        self.resource = self.root / "resources.json"
        private_json(self.resource, {"schema": "alopex.sql-resource-servers/v1",
                                     "servers": {"alopex-server": IDENTITY}})
        self.env = {"ALOPEX_PERF_SERVER_URL": ENDPOINT,
                    "ALOPEX_PERF_SUBJECT_MANIFEST": str(self.subject),
                    "ALOPEX_PERF_SUBJECT_SHA256": supervisor.digest(self.subject.read_bytes()),
                    "ALOPEX_PERF_SQL_RESOURCE_MANIFEST": str(self.resource)}
        self.server = Server()

    def config(self, case):
        return {"schema": "alopex.sql-four-case-pair/v1", "engine": "alopex-server",
                "case": case, "trial": 0, "modes": ["on", "off"],
                "source": SOURCE, "assets_digest": ASSETS, "same_job_id": "123",
                "fixture_sha256": "d" * 64}

    def emit(self, case, *, omit_log=False, changed_resource=False,
             missing_client=False, changed_client=False, malformed_resource=None):
        clients, lines = pair(case)
        timing_root = self.output / "public-timing"
        timing_root.mkdir(mode=0o700, exist_ok=True)
        for mode in ("on", "off"):
            for row in clients[mode]:
                if row["phase"] == "measured":
                    index = row["sample"]
                    row["outer_ns"] = round(0.001 * (index + 1) * 1_000_000_000)
                    row["pair_ordinal"] = 2 * index + adapter.pair_order(0, index).index(mode)
        if not omit_log:
            path = self.output / "server.log"
            path.write_text("".join(lines))
            path.chmod(0o600)
        resource_digest = supervisor.digest(self.resource.read_bytes())
        for mode in ("on", "off"):
            root = self.output / mode
            with public_timing.TimingCellWriter(timing_root, case, 0, mode) as timing:
                for index in range(21):
                    timing.append(index,
                        2 * index + adapter.pair_order(0, index).index(mode),
                        0.001 * (index + 1))
            with (root / "client.ndjson").open("w") as stream:
                for row in clients[mode]:
                    stream.write(json.dumps(row) + "\n")
            (root / "client.ndjson").chmod(0o600)
            seconds, ordinals = [], []
            with (root / "raw.ndjson").open("w") as stream:
                for index in range(21):
                    ordinal = 2 * index + adapter.pair_order(0, index).index(mode)
                    seconds.append(0.001 * (index + 1))
                    ordinals.append(ordinal)
                    identity = dict(IDENTITY)
                    if changed_resource and mode == "off" and index == 7:
                        identity["start_ticks"] += 1
                    row = {"sample": index, "seconds": seconds[-1],
                           "pair_ordinal": ordinal,
                           "resources": {"schema": "alopex.sql-operation-resources/v1",
                                         "status": "observed",
                                         "coverage": "stable_pid_namespace_allocation",
                                         "manifest_sha256": resource_digest,
                                         "client": ({"pid": 101, "start_ticks":
                                                     999 if changed_client and mode == "off" and
                                                     index == 7 else 401, **COUNTERS}
                                                    if not missing_client else None),
                                         "server_processes": [dict(identity, **COUNTERS)]}}
                    if malformed_resource and mode == "off" and index == 7:
                        if malformed_resource == "schema":
                            row["resources"]["schema"] = "wrong"
                        elif malformed_resource == "counter":
                            del row["resources"]["server_processes"][0]["read_bytes"]
                    stream.write(json.dumps(row) + "\n")
            (root / "raw.ndjson").chmod(0o600)
            cell = dict(case=case, trial=0, mode=mode, source=SOURCE,
                        assets_digest=ASSETS, same_job_id="123", fixture_sha256="d" * 64,
                        pair_server=IDENTITY, cleanup_passed=True,
                        result_sha256=oracle.ORACLE_SHA256[case],
                        rollback_sha256=oracle.ROLLBACK_SHA256 if case == "insert-rollback" else None,
                        pair_order=[list(adapter.pair_order(0, i)) for i in range(21)],
                        raw_sha256=supervisor.digest((root / "raw.ndjson").read_bytes()),
                        raw_seconds=seconds, raw_ordinals=ordinals)
            private_json(root / "measurement.json", cell)

    def run_pair(self, case, command):
        with patch.object(supervisor, "_run_worker", side_effect=command):
            return supervisor.run_owned_pair(
                self.config(case), self.output, source=SOURCE, assets_digest=ASSETS,
                job_id="123", start=lambda: (self.server, ENDPOINT),
                python="/venv/bin/python", package_root=self.package.parent,
                package_files=self.package_files, docs=self.root, product=self.root,
                subject_environment=self.env, identity_of=lambda server: IDENTITY)

    def test_complete_read_and_insert_pair_log_inventory(self):
        for case, count in (("aggregate", 2590), ("insert-rollback", 203)):
            with self.subTest(case=case):
                def command(argv, log, seconds, env, cwd):
                    self.assertEqual(argv[1:5], ["-B", "-P", "-m",
                                                "four_case_diagnostic.worker"])
                    self.assertEqual(seconds, 300)
                    self.assertEqual(env["PYTHONPATH"], str(self.package.parent.resolve()))
                    self.assertEqual(env["PYTHONNOUSERSITE"], "1")
                    self.assertEqual(cwd, self.package.parent.resolve())
                    self.emit(case)
                    private_json(log, {})
                report = self.run_pair(case, command)
                self.assertEqual(report["status"], "passed")
                self.assertEqual(len(report["measurement"]["server_timing"]), count)
                self.assertTrue(self.server.stopped)
                for path in self.output.rglob("*"):
                    if path.is_file():
                        path.unlink()
                self.server = Server()

    def test_timeout_and_stop_error_retained_separately(self):
        self.server = Server(stop_error=True)
        report = self.run_pair("aggregate", lambda *args: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("worker", 300)))
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["primary_error"], "WORKER_TIMEOUT")
        self.assertEqual(report["stop_error"], "SERVER_STOP")
        self.assertNotIn("private stop detail", str(report))

    def test_missing_log_or_changed_server_identity_rejects_success(self):
        for omit_log, changed_resource in ((True, False), (False, True)):
            with self.subTest(omit_log=omit_log):
                def command(argv, log, seconds, env, cwd):
                    self.emit("insert-rollback", omit_log=omit_log,
                              changed_resource=changed_resource)
                    private_json(log, {})
                report = self.run_pair("insert-rollback", command)
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["primary_error"], "EVIDENCE_INCOMPLETE")
                self.assertTrue(self.server.stopped)
                for path in self.output.rglob("*"):
                    if path.is_file():
                        path.unlink()
                self.server = Server()

    def test_missing_or_changed_client_resource_rejects_success(self):
        for missing_client, changed_client in ((True, False), (False, True)):
            with self.subTest(missing_client=missing_client):
                def command(argv, log, seconds, env, cwd):
                    self.emit("insert-rollback", missing_client=missing_client,
                              changed_client=changed_client)
                    private_json(log, {})
                report = self.run_pair("insert-rollback", command)
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["primary_error"], "EVIDENCE_INCOMPLETE")
                for path in self.output.rglob("*"):
                    if path.is_file():
                        path.unlink()
                self.server = Server()

    def test_malformed_resource_schema_or_counter_rejects_success(self):
        for malformed in ("schema", "counter"):
            with self.subTest(malformed=malformed):
                def command(argv, log, seconds, env, cwd):
                    self.emit("insert-rollback", malformed_resource=malformed)
                    private_json(log, {})
                report = self.run_pair("insert-rollback", command)
                self.assertEqual(report["primary_error"], "EVIDENCE_INCOMPLETE")
                self.assertEqual(report["status"], "failed")
                for path in self.output.rglob("*"):
                    if path.is_file():
                        path.unlink()
                self.server = Server()

    def test_inherited_umask_cannot_expose_server_log(self):
        previous = os.umask(0o022)
        self.addCleanup(os.umask, previous)
        def start():
            with (self.output / "server.log").open("ab"):
                pass
            return self.server, ENDPOINT
        with patch.object(supervisor, "_run_worker", side_effect=subprocess.TimeoutExpired(
                "worker", 300)):
            report = supervisor.run_owned_pair(
                self.config("aggregate"), self.output, source=SOURCE, assets_digest=ASSETS,
                job_id="123", start=start, python="/venv/bin/python",
                package_root=self.package.parent, package_files=self.package_files,
                docs=self.root, product=self.root, subject_environment=self.env,
                identity_of=lambda server: IDENTITY)
        self.assertEqual(report["primary_error"], "WORKER_TIMEOUT")
        self.assertEqual((self.output / "server.log").stat().st_mode & 0o777, 0o600)
        observed = os.umask(0o022)
        self.assertEqual(observed, 0o022)

    def test_import_shadow_file_rejected(self):
        shadow = self.package.parent / "sitecustomize.py"
        shadow.write_text("raise RuntimeError('shadow')\n")
        with self.assertRaisesRegex(ValueError, "WORKER_PACKAGE"):
            self.run_pair("aggregate", lambda *args: None)

    def test_orphan_descendant_is_cleaned_and_pair_fails(self):
        class ExitedChild:
            pid = 990
            def wait(self, timeout=None):
                return 0
            def poll(self):
                return 0
        with patch.object(supervisor.subprocess, "Popen", return_value=ExitedChild()), \
                patch.object(supervisor.os, "killpg") as killpg, \
                patch.object(supervisor, "_group_members", side_effect=[[991], [], []]):
            with self.assertRaisesRegex(ValueError, "WORKER_ORPHAN"):
                supervisor._run_worker(["python"], self.output / "worker.log", 300, {},
                                       self.package.parent)
        killpg.assert_called_once_with(990, supervisor.signal.SIGTERM)

    def test_worker_timeout_kills_and_reaps_process_group(self):
        class TimedChild:
            pid = 987
            calls = 0

            def poll(self):
                return None

            def wait(self, timeout=None):
                self.calls += 1
                if self.calls == 1:
                    raise subprocess.TimeoutExpired("worker", timeout)
                return -15

        child = TimedChild()
        log = self.output / "worker.log"
        with patch.object(supervisor.subprocess, "Popen", return_value=child) as popen, \
                patch.object(supervisor.os, "killpg") as killpg, \
                patch.object(supervisor, "_group_members", return_value=[]):
            with self.assertRaises(subprocess.TimeoutExpired):
                supervisor._run_worker(["python", "-m", "four_case_diagnostic.worker"],
                                       log, 300, {}, self.package.parent)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(popen.call_args.kwargs["cwd"], str(self.package.parent))
        self.assertEqual(popen.call_args.kwargs["stderr"], popen.call_args.kwargs["stdout"])
        killpg.assert_called_once_with(987, supervisor.signal.SIGTERM)
        self.assertEqual(child.calls, 2)
        self.assertEqual(log.stat().st_mode & 0o777, 0o600)

    def test_worker_escalates_to_kill_after_unresponsive_term(self):
        class StuckChild:
            pid = 988
            calls = 0

            def poll(self):
                return None

            def wait(self, timeout=None):
                self.calls += 1
                if self.calls < 3:
                    raise subprocess.TimeoutExpired("worker", timeout)
                return -9

        child = StuckChild()
        with patch.object(supervisor.subprocess, "Popen", return_value=child), \
                patch.object(supervisor.os, "killpg") as killpg, \
                patch.object(supervisor, "_group_members", return_value=[]):
            with self.assertRaises(subprocess.TimeoutExpired):
                supervisor._run_worker(["python"], self.output / "worker.log", 300, {},
                                       self.package.parent)
        self.assertEqual(killpg.call_args_list[0].args,
                         (988, supervisor.signal.SIGTERM))
        self.assertEqual(killpg.call_args_list[1].args,
                         (988, supervisor.signal.SIGKILL))
        self.assertEqual(child.calls, 3)

    def test_worker_timeout_preserves_cleanup_failure(self):
        class BrokenChild:
            pid = 989

            def wait(self, timeout=None):
                raise subprocess.TimeoutExpired("worker", timeout)

            def poll(self):
                raise RuntimeError("private cleanup detail")

        with patch.object(supervisor.subprocess, "Popen", return_value=BrokenChild()):
            with self.assertRaises(subprocess.TimeoutExpired) as raised:
                supervisor._run_worker(["python"], self.output / "worker.log", 300, {},
                                       self.package.parent)
        self.assertEqual(raised.exception.worker_cleanup_error, "WORKER_CLEANUP")

    def test_tampered_or_extra_package_file_rejected_before_server_start(self):
        for mutate in (lambda: (self.package / "worker.py").write_text("changed"),
                       lambda: (self.package / "extra.py").write_text("extra")):
            with self.subTest(mutate=mutate):
                mutate()
                with self.assertRaisesRegex(ValueError, "WORKER_PACKAGE"):
                    self.run_pair("aggregate", lambda *args: None)
                self.assertFalse(self.server.stopped)
                (self.package / "worker.py").write_text("# staged by trusted caller\n")
                (self.package / "worker.py").chmod(0o600)
                (self.package / "extra.py").unlink(missing_ok=True)

    def test_nonprivate_output_parent_rejected_before_server_start(self):
        self.root.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "OUTPUT_PARENT"):
            self.run_pair("aggregate", lambda *args: None)
        self.assertFalse(self.server.stopped)

    def test_optional_postgres_resource_binding(self):
        private_json(self.resource, {"schema": "alopex.sql-resource-servers/v1",
                                     "servers": {"alopex-server": IDENTITY,
                                                 "postgresql": {"pid": 202, "start_ticks": 302}}})
        self.env["ALOPEX_PERF_SUBJECT_SHA256"] = supervisor.digest(self.subject.read_bytes())
        self.assertEqual(supervisor._verify_subject(self.env, self.config("aggregate"),
                         IDENTITY, ENDPOINT), supervisor.digest(self.resource.read_bytes()))
        private_json(self.resource, {"schema": "alopex.sql-resource-servers/v1",
                                     "servers": {"alopex-server": {"pid": 201, "start_ticks": 999},
                                                 "postgresql": {"pid": 202, "start_ticks": 302}}})
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            supervisor._verify_subject(self.env, self.config("aggregate"), IDENTITY, ENDPOINT)


if __name__ == "__main__":
    unittest.main()
