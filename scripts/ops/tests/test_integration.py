"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

"""Fake boundary tests for the copied worker's separate diagnostic measure()."""
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
import uuid


HERE = Path(__file__).resolve().parents[1]


@contextlib.contextmanager
def fake_environment(*, drop_callback=False, wrong_result=False, leaked_field=False):
    old = {name: sys.modules.get(name) for name in ("alopex", "subject_identity", "sql_resources")}
    old_url = os.environ.get("ALOPEX_PERF_SERVER_URL")
    calls = []
    observes = []

    class Remote:
        def __init__(self, sink=None):
            self.sink = sink

        def execute_sql(self, sql):
            calls.append(sql)
            if self.sink and not drop_callback:
                timing = {"schema": "alopex.sql-client-timing/v1", "trace_id": str(uuid.uuid4()),
                          "bind_ns": 1, "transport_ns": 2, "parse_ns": 1, "rows_ns": 1,
                          "residual_ns": 1, "total_ns": 6, "response_chars": 12, "result_rows": 1}
                if leaked_field:
                    timing["sql"] = sql
                self.sink(timing)
            if sql == "AGGREGATE_SQL":
                return [{"count": 0 if wrong_result else 17125}]
            if sql.startswith("SELECT id, COALESCE"):
                return [{"id": i, "value": float(i)} for i in range(16)]
            return []

        def close(self):
            pass

    class Observer:
        @staticmethod
        def from_environment(engine, backend):
            assert engine == "alopex-server" and backend is None
            return Observer()

        def begin(self):
            observes.append("begin")
            return 1

        def end(self, token):
            assert token == 1
            observes.append("end")
            return {"schema": "alopex.sql-operation-resources/v1", "status": "observed"}

    sys.modules["alopex"] = types.SimpleNamespace(RemoteDatabase=Remote,
        connect=lambda url, **options: Remote(options.get("sql_timing_sink")))
    sys.modules["subject_identity"] = types.SimpleNamespace(verify_subject=lambda: None,
                                                               worker_command=lambda *args: args)
    sys.modules["sql_resources"] = types.SimpleNamespace(ENV="FAKE_RESOURCE", OperationResources=Observer)
    os.environ["ALOPEX_PERF_SERVER_URL"] = "http://fake.invalid"
    try:
        spec = importlib.util.spec_from_file_location("diagnostic_measure_test", HERE / "diagnostic_measure.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        parity = types.SimpleNamespace(SQL_AGGREGATE_QUERY="AGGREGATE_SQL",
            _populate_sql=lambda conn, engine, rows: (conn.execute_sql("CREATE TABLE bench"),
                                                       conn.execute_sql("INSERT INTO bench VALUES (...)")))
        yield module, parity, calls, observes
    finally:
        for name, previous in old.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
        if old_url is None:
            os.environ.pop("ALOPEX_PERF_SERVER_URL", None)
        else:
            os.environ["ALOPEX_PERF_SERVER_URL"] = old_url


class IntegratedMeasureTests(unittest.TestCase):
    def test_adjacent_pair_uses_one_fixture_and_no_off_callback(self):
        with fake_environment() as (measure, parity, calls, observes):
            remote = sys.modules["alopex"].RemoteDatabase
            original = remote.execute_sql
            measured_order = []
            def observed_sql(connection, sql):
                if sql == "AGGREGATE_SQL":
                    measured_order.append("on" if connection.sink else "off")
                return original(connection, sql)
            remote.execute_sql = observed_sql
            with tempfile.TemporaryDirectory() as directory:
                paths = {mode: Path(directory) / f"{mode}.ndjson" for mode in ("on", "off")}
                result, clients = measure.measure_pair(parity, "aggregate", 0, paths)
                raw = {mode: [json.loads(line) for line in paths[mode].read_text().splitlines()]
                       for mode in paths}
            self.assertEqual([len(raw[mode]) for mode in paths], [21, 21])
            self.assertEqual(calls.count("AGGREGATE_SQL"), 50)
            self.assertEqual(calls.count("CREATE TABLE bench"), 1)
            self.assertEqual(len(clients["on"]), 28)
            self.assertEqual(len(clients["off"]), 25)
            self.assertTrue(all("trace_id" not in row for row in clients["off"]))
            self.assertEqual(result["on"]["result_sha256"], result["off"]["result_sha256"])
            self.assertEqual(len(observes), 84)
            from diagnostic import pair_order
            self.assertEqual(measured_order[-42:],
                             [mode for sample in range(21) for mode in pair_order(0, sample)])

    def test_on_and_off_preserve_raw_boundary(self):
        for mode in ("on", "off"):
            with self.subTest(mode=mode), fake_environment() as (measure, parity, calls, observes):
                with tempfile.TemporaryDirectory() as directory:
                    result, client = measure.measure_diagnostic(parity, "aggregate", mode, 0,
                                                                  Path(directory) / "raw.ndjson")
                    raw = [json.loads(line) for line in (Path(directory) / "raw.ndjson").read_text().splitlines()]
                self.assertEqual(len(raw), 21)
                self.assertEqual(observes, [item for _ in range(21) for item in ("begin", "end")])
                self.assertEqual((result["warmups"], result["samples"]), (3, 21))
                self.assertEqual(len(client), 28 if mode == "on" else 25)
                self.assertEqual(sum(row["phase"] == "setup" for row in client), 3 if mode == "on" else 0)
                self.assertEqual(sum(row["phase"] == "measured" for row in client), 21)
                self.assertTrue(all("sql" not in row and "url" not in row and "path" not in row
                                    for row in client))
                self.assertNotIn("sql", result)
                self.assertEqual(calls.count("AGGREGATE_SQL"), 25)

    def test_fail_closed_on_missing_callback_or_changed_result(self):
        for options in ({"drop_callback": True}, {"wrong_result": True}, {"leaked_field": True}):
            with self.subTest(options=options), fake_environment(**options) as (measure, parity, _, _):
                with tempfile.TemporaryDirectory() as directory:
                    with self.assertRaises((AssertionError, ValueError)):
                        measure.measure_diagnostic(parity, "aggregate", "on", 0,
                                                   Path(directory) / "raw.ndjson")

    def test_pair_fails_closed_on_callback_or_oracle_change(self):
        for options in ({"drop_callback": True}, {"wrong_result": True}, {"leaked_field": True}):
            with self.subTest(options=options), fake_environment(**options) as (measure, parity, _, _):
                with tempfile.TemporaryDirectory() as directory:
                    paths = {mode: Path(directory) / f"{mode}.ndjson" for mode in ("on", "off")}
                    with self.assertRaises((AssertionError, ValueError)):
                        measure.measure_pair(parity, "aggregate", 0, paths)

    def test_pair_rejects_existing_output_before_samples(self):
        with fake_environment() as (measure, parity, _, observes):
            with tempfile.TemporaryDirectory() as directory:
                paths = {mode: Path(directory) / f"{mode}.ndjson" for mode in ("on", "off")}
                paths["on"].write_text("existing")
                with self.assertRaises(FileExistsError):
                    measure.measure_pair(parity, "aggregate", 0, paths)
                self.assertEqual(paths["on"].read_text(), "existing")
                self.assertEqual(observes, [])


if __name__ == "__main__":
    unittest.main()
