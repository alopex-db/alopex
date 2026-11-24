"""Focused checks for the pinned v0816 harness and runtime consumer boundary."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import worker

HARNESS = (Path(__file__).resolve().parents[3].parent.parent / "alopex-docs" / "scripts/performance"
           / "server-sql-isolated-v0816.py")


def resource():
    process = {key: 0 for key in worker.COUNTERS}
    return {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
            "coverage": "stable_pid_namespace_allocation", "manifest_sha256": "a" * 64,
            "client": dict(process, pid=1, start_ticks=1),
            "server_processes": [dict(process, pid=2, start_ticks=2)]}


def cell(case="aggregate", subject="candidate"):
    engine = "postgresql" if subject == "postgresql" else "alopex-server"
    raw = [{"sample": i, "seconds": .001, "resources": resource()} for i in range(21)]
    sql = {"aggregate": "SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
           "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
           "select-all": "SELECT id, value FROM bench WHERE enabled = true",
           "insert-rollback": "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"}[case]
    result = {"case": case, "engine": engine, "server_client": True, "warmups": 3,
              "samples": 21, "fixture_rows": worker.ISOLATED[case], "sql": sql,
              "result_sha256": worker.result_digest(case), "latency_p50_ms": 1.0}
    return result, raw, {"case": case, "engine": engine, "subject": subject, "trial": 0}


class WorkerTests(unittest.TestCase):
    def test_public_capture_at_timed_call_and_private_equality(self):
        _, _, config = cell()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public.ndjson"
            raw_path = Path(directory) / "raw.ndjson"
            calls = []

            class Connection:
                def execute_sql(self, sql):
                    calls.append(sql)
                    if sql.startswith("SELECT"):
                        return [{"count": 17125}]
                    return []

                def close(self):
                    calls.append("close")

            class ResourceObserver:
                @staticmethod
                def from_environment(*_):
                    return types.SimpleNamespace(begin=lambda: None,
                                                 end=lambda _: resource())
            alopex = types.ModuleType("alopex")
            alopex.RemoteDatabase = Connection
            alopex.connect = lambda *_, **__: Connection()
            parity = types.SimpleNamespace(
                _populate_sql=lambda *args: calls.append("populate"),
                SQL_AGGREGATE_QUERY="SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
                SQL_STREAM_QUERY="SELECT id, value FROM bench WHERE enabled = true")
            isolated = types.SimpleNamespace(
                validate=worker.validate_exact, OperationResources=ResourceObserver,
                measure=lambda *_: self.fail("private debug harness was called"))
            with patch.dict(sys.modules, {"alopex": alopex}), \
                 patch.dict("os.environ", {"ALOPEX_PERF_SERVER_URL": "http://127.0.0.1"}):
                result = worker.public_measure(parity, isolated, config, raw_path, path,
                                                clock=iter([1.0, 1.001] * 21).__next__)
            public = worker.read_public(path, config)
            raw = [json.loads(line) for line in raw_path.read_text().splitlines()]
            self.assertEqual(len(public), 21)
            self.assertTrue(all(item == worker.public_record(config, index,
                            raw[index]["seconds"], raw[index]["resources"])
                            for index, item in enumerate(public)))
            self.assertEqual(result["samples"], 21)
            self.assertEqual(calls.count(parity.SQL_AGGREGATE_QUERY), 25)
            self.assertEqual(calls[-1], "close")

    def test_public_record_allowlist_excludes_private_identifiers(self):
        _, raw, config = cell()
        private = raw[0]["resources"]
        private["local_path"] = "/private/secret"
        private["error"] = "exception: /private/secret"
        private["client"]["pid"] = 9988
        private["server_processes"][0]["hostname"] = "private-host"
        row = worker.public_record(config, 0, .001, private)
        self.assertEqual(tuple(row), worker.PUBLIC_FIELDS)
        encoded = json.dumps(row)
        self.assertNotIn("/private/", encoded)
        self.assertNotIn("exception", encoded)
        self.assertFalse(any("pid" in key for key in row))
        self.assertNotIn("hostname", encoded)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "public.ndjson"
            valid = [worker.public_record(config, i, .001, resource()) for i in range(21)]
            path.write_text("".join(json.dumps(item, separators=(",", ":")) + "\n"
                                    for item in valid))
            self.assertEqual(worker.read_public(path, config), valid)
            valid[0]["local_path"] = "/private/secret"
            path.write_text("".join(json.dumps(item, separators=(",", ":")) + "\n"
                                    for item in valid))
            with self.assertRaises(ValueError):
                worker.read_public(path, config)

    def test_public_insert_harness_rolls_back_every_call(self):
        _, _, config = cell("insert-rollback")
        rollbacks = []

        class Transaction:
            def execute_sql(self, sql):
                self.sql = sql
                return [{"id": 3}]

            def rollback(self):
                rollbacks.append(self.sql)

        class Connection:
            def execute_sql(self, sql):
                if sql == "SELECT id, value FROM perf_items ORDER BY id":
                    return [{"id": 1, "value": "old"}]
                return []

            def begin(self, mode):
                return Transaction()

            def close(self):
                pass

        class ResourceObserver:
            @staticmethod
            def from_environment(*_):
                return types.SimpleNamespace(begin=lambda: None, end=lambda _: resource())

        alopex = types.ModuleType("alopex")
        alopex.RemoteDatabase = Connection
        alopex.TxnMode = types.SimpleNamespace(READ_WRITE="read_write")
        alopex.connect = lambda *_, **__: Connection()
        isolated = types.SimpleNamespace(validate=worker.validate_exact,
                                         OperationResources=ResourceObserver,
                                         measure=lambda *_: self.fail("private harness called"))
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(sys.modules, {"alopex": alopex}), \
             patch.dict("os.environ", {"ALOPEX_PERF_SERVER_URL": "http://127.0.0.1"}):
            root = Path(directory)
            result = worker.public_measure(types.SimpleNamespace(), isolated, config,
                                           root / "raw.ndjson", root / "public.ndjson",
                                           clock=iter([1.0, 1.001] * 21).__next__)
            self.assertEqual(len(worker.read_public(root / "public.ndjson", config)), 21)
        self.assertEqual(len(rollbacks), 25)
        self.assertEqual(set(rollbacks),
                         {"INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"})
        self.assertEqual(result["result_sha256"], worker.result_digest("insert-rollback"))

    def test_public_postgresql_client_uses_version_and_observed_pid(self):
        _, _, config = cell("aggregate", "postgresql")
        observed_pids = []

        class Cursor:
            description = True

            def __init__(self, sql):
                self.sql = sql

            def fetchone(self):
                return {"server_version": "16.14"}

            def fetchall(self):
                return [{"count": 17125}]

        class Connection:
            info = types.SimpleNamespace(backend_pid=72)

            def execute(self, sql):
                return Cursor(sql)

            def close(self):
                pass

        class ResourceObserver:
            @staticmethod
            def from_environment(engine, pid):
                observed_pids.append((engine, pid))
                return types.SimpleNamespace(begin=lambda: None, end=lambda _: resource())

        psycopg = types.ModuleType("psycopg")
        psycopg.connect = lambda *_, **__: Connection()
        rows = types.ModuleType("psycopg.rows")
        rows.dict_row = object()
        isolated = types.SimpleNamespace(validate=worker.validate_exact,
                                         OperationResources=ResourceObserver,
                                         measure=lambda *_: self.fail("private harness called"))
        parity = types.SimpleNamespace(
            _populate_sql=lambda *_: None,
            SQL_AGGREGATE_QUERY="SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
            SQL_STREAM_QUERY="SELECT id, value FROM bench WHERE enabled = true")
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(sys.modules, {"psycopg": psycopg, "psycopg.rows": rows}), \
             patch.dict("os.environ", {"ALOPEX_PERF_POSTGRES_DSN": "private socket"}):
            root = Path(directory)
            result = worker.public_measure(parity, isolated, config,
                                           root / "raw.ndjson", root / "public.ndjson",
                                           clock=iter([1.0, 1.001] * 21).__next__)
            self.assertEqual(len(worker.read_public(root / "public.ndjson", config)), 21)
        self.assertEqual(observed_pids, [("postgresql", 72)])
        self.assertEqual(result["engine"], "postgresql")

    def test_preflight_captures_one_exact_validated_result(self):
        module = types.SimpleNamespace()
        captured = worker.enforce_complete_result_order(module, capture_exact=True)
        wanted = worker.expected("select-all")
        self.assertEqual(module.validate("select-all", wanted, 34250),
                         worker.result_digest("select-all"))
        self.assertEqual(json.loads(captured["raw"]),
                         {"rows": 34250, "result": wanted})
        first = captured["raw"]
        self.assertEqual(module.validate("select-all", wanted, 34250),
                         worker.result_digest("select-all"))
        self.assertIs(captured["raw"], first)
        with self.assertRaises(ValueError):
            module.validate("select-all", wanted[:-1], 34250)
        self.assertIs(captured["raw"], first)

    def test_v0816_harness_uses_exact_oracle_for_all_included_cases(self):
        identity = types.ModuleType("subject_identity")
        identity.verify_subject = lambda **_: None
        identity.worker_command = lambda command, _: command
        resources = types.ModuleType("sql_resources")
        resources.ENV = "RESOURCE_ENV"
        resources.OperationResources = object
        with patch.dict(sys.modules, {"subject_identity": identity, "sql_resources": resources}):
            module = worker.load_module("pinned_v0816_sql_test", HARNESS)
        worker.enforce_complete_result_order(module)
        for case in worker.ISOLATED:
            wanted = worker.expected(case)
            self.assertEqual(module.validate(case, wanted, 34250), worker.result_digest(case))
            changed = [dict(row) for row in wanted]
            changed[0][next(iter(changed[0]))] = "wrong type"
            bads = [wanted[:-1], changed]
            if len(wanted) > 1:
                bads.append(list(reversed(wanted)))
            for bad in bads:
                with self.subTest(case=case), self.assertRaises(ValueError):
                    module.validate(case, bad, 34250)
        for excluded in ("drop-sequence", "insert-commit", "kv-write"):
            with self.assertRaises(ValueError):
                module.validate(excluded, [], 34250)
        source = HARNESS.read_text()
        self.assertIn('target.rollback()', source)
        self.assertIn('Rollback changed fixture', source)

    def test_raw_resource_and_engine_fail_closed(self):
        for case in worker.ISOLATED:
            result, raw, config = cell(case)
            worker.verify_cell(result, raw, config, "a" * 64)
        result, raw, config = cell()
        edits = [lambda r, w, c: w.pop(),
                 lambda r, w, c: w[1].update(sample=0),
                 lambda r, w, c: w[0]["resources"].update(status="failed"),
                 lambda r, w, c: w[0]["resources"]["client"].pop("read_bytes"),
                 lambda r, w, c: w[0]["resources"]["server_processes"][0].update(pid=1),
                 lambda r, w, c: r.update(result_sha256="b" * 64),
                 lambda r, w, c: r.update(warmups=2),
                 lambda r, w, c: r.update(sql="SELECT 1"),
                 lambda r, w, c: c.update(engine="postgresql")]
        for edit in edits:
            r, w, c = copy.deepcopy((result, raw, config))
            edit(r, w, c)
            with self.subTest(edit=edit), self.assertRaises((ValueError, KeyError)):
                worker.verify_cell(r, w, c, "a" * 64)


if __name__ == "__main__":
    unittest.main()
