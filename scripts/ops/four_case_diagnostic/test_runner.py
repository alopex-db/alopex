"""Offline pair tests at the public remote and callback boundary."""
import json
import os
from pathlib import Path
import stat
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

from . import adapter, operation, public_attribution, public_timing, runner


class Remote:
    def __init__(self, state, sink, mode):
        self.state, self.sink, self.mode = state, sink, mode

    def execute_sql(self, sql):
        self.state["sql"].append((self.mode, sql))
        if self.sink:
            self.sink({"schema": "alopex.sql-client-timing/v1", "trace_id": str(uuid.uuid4()),
                       "bind_ns": 1, "transport_ns": 2, "parse_ns": 1, "rows_ns": 1,
                       "residual_ns": 1, "total_ns": 6, "response_chars": 12, "result_rows": 1})
        if sql == "AGGREGATE_SQL":
            return [{"count": 17_125}]
        if sql == "STREAM_SQL":
            return [{"id": i, "value": float(i)} for i in range(0, 34_250, 2)]
        if sql == operation.INSERT_SQL:
            return [{"id": 3}]
        if sql == operation.ROLLBACK_SQL:
            return [{"id": 1, "value": "old"}]
        if sql.startswith("SELECT id, COALESCE"):
            return [{"id": i, "value": float(i)} for i in range(16)]
        return []

    def begin(self, mode):
        self.state["transactions"].append((self.mode, "begin", mode))
        parent = self

        class Transaction:
            def execute_sql(self, sql):
                return parent.execute_sql(sql)

            def rollback(self):
                parent.state["transactions"].append((parent.mode, "rollback"))

        return Transaction()

    def close(self):
        self.state["closed"].append(self.mode)


class Observer:
    def __init__(self):
        self.events = []

    def begin(self):
        self.events.append("begin")
        return 1

    def end(self, token):
        assert token == 1
        if hasattr(self, "timing_root"):
            written = sum(len(path.read_text().splitlines())
                          for path in self.timing_root.iterdir())
            assert written == self.events.count("end") + 1
        self.events.append("end")
        counters = {"user_cpu_seconds": 0.1, "system_cpu_seconds": 0.2,
                    "rss_before_bytes": 1024, "rss_after_bytes": 2048,
                    "read_bytes": 0, "write_bytes": 0,
                    "cancelled_write_bytes": 0}
        return {"schema": "alopex.sql-operation-resources/v1",
                "status": "observed", "coverage": "stable_pid_namespace_allocation",
                "manifest_sha256": "a" * 64,
                "client": {"pid": 101, "start_ticks": 301, **counters},
                "server_processes": [{"pid": 201, "start_ticks": 401, **counters}]}


class RunnerTests(unittest.TestCase):
    def run_case(self, case, evidence_mode="public"):
        state = {"sql": [], "transactions": [], "closed": []}
        observer = Observer()

        def connect(url, **options):
            self.assertEqual(url, "same-qualified-server")
            self.assertTrue(options["keep_alive"])
            mode = "on" if "sql_timing_sink" in options else "off"
            return Remote(state, options.get("sql_timing_sink"), mode)

        def populate(connection, engine, rows):
            self.assertEqual((engine, rows), ("alopex", 34_250))
            connection.execute_sql("CREATE TABLE bench")
            for _ in range(343):
                connection.execute_sql("INSERT INTO bench VALUES (...)")

        module = types.SimpleNamespace(_populate_sql=populate,
                                       SQL_AGGREGATE_QUERY="AGGREGATE_SQL",
                                       SQL_STREAM_QUERY="STREAM_SQL")
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            debug_root = Path(directory).resolve() if evidence_mode == "debug" else None
            if debug_root is None:
                paths = {mode: Path(directory) / f"{mode}.ndjson" for mode in ("on", "off")}
                timing_root = Path(directory).resolve() / "public-timing"
                timing_root.mkdir(mode=0o700)
                operation_root = Path(directory).resolve() / "public-operations"
                operation_root.mkdir(mode=0o700)
                observer.timing_root = timing_root
            else:
                paths = {}
                timing_root = operation_root = None
                for mode in ("on", "off"):
                    parent = debug_root / mode
                    parent.mkdir(mode=0o700)
                    paths[mode] = parent / "raw.ndjson"
            summary, clients = runner.measure_pair(module, case, 0, paths,
                connect=connect, remote_type=Remote, url="same-qualified-server",
                txn_mode="read_write", observer=observer, timing_root=timing_root,
                operation_root=operation_root,
                evidence_mode=evidence_mode, debug_root=debug_root)
            raw = {mode: [json.loads(line) for line in paths[mode].read_text().splitlines()]
                   for mode in paths}
            timed = ({mode: public_timing._read_cell(
                timing_root / public_timing.cell_filename(case, 0, mode), case, 0, mode)
                for mode in paths} if timing_root is not None else None)
            attributed = ({mode: public_attribution._read_operation_cell(
                operation_root / public_attribution.operation_filename(case, 0, mode),
                case, 0, mode) for mode in paths} if operation_root is not None else None)
            if debug_root is not None:
                self.assertFalse((debug_root / "public-timing").exists())
            self.assertEqual({stat.S_IMODE(path.stat().st_mode) for path in paths.values()},
                             {0o600})
        self.assertEqual(state["closed"], ["off", "on"])
        self.assertEqual(len(observer.events), 84)
        for mode in ("on", "off"):
            self.assertEqual(len(raw[mode]), 21)
            if timed is not None:
                self.assertEqual([row["elapsed_seconds"] for row in timed[mode]],
                                 [row["seconds"] for row in raw[mode]])
                self.assertEqual(len(attributed[mode]), 21)
            self.assertEqual(len(clients[mode]), adapter.expected_client_records(case, mode))
            self.assertEqual((summary[mode]["warmups"], summary[mode]["samples"]), (3, 21))
            self.assertEqual([row["pair_ordinal"] for row in raw[mode]],
                [2 * sample + adapter.pair_order(0, sample).index(mode) for sample in range(21)])
            measured = [row for row in clients[mode] if row["phase"] == "measured"]
            self.assertEqual([row["pair_ordinal"] for row in measured],
                             [row["pair_ordinal"] for row in raw[mode]])
            self.assertEqual([row["outer_ns"] for row in measured],
                             [round(row["seconds"] * 1e9) for row in raw[mode]])
        self.assertTrue(all("trace_id" not in row for row in clients["off"]))
        self.assertEqual(sum(row["phase"] == "setup" for row in clients["on"]),
                         adapter.SETUP[case])
        return state, clients

    def test_select_all_exact_order_and_shared_fixture(self):
        state, clients = self.run_case("select-all")
        self.assertEqual(sum(sql == "STREAM_SQL" for _, sql in state["sql"]), 50)
        self.assertEqual(sum(sql == "CREATE TABLE bench" for _, sql in state["sql"]), 1)
        self.assertEqual(sum(row["phase"] == "measured" for row in clients["on"]), 21)

    def test_explicit_local_debug_runs_same_workload_without_public_timing(self):
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false", "CI": "false"}):
            state, clients = self.run_case("select-all", evidence_mode="debug")
        self.assertEqual(sum(sql == "STREAM_SQL" for _, sql in state["sql"]), 50)
        self.assertEqual(sum(row["phase"] == "measured" for row in clients["on"]), 21)

    def test_debug_mode_rejects_ci_and_mixed_public_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            paths = {}
            for mode in ("on", "off"):
                parent = root / mode
                parent.mkdir(mode=0o700)
                paths[mode] = parent / "raw.ndjson"
            arguments = dict(connect=lambda *a, **k: self.fail("connected"),
                             remote_type=Remote, url="same-qualified-server",
                             observer=Observer(), evidence_mode="debug", debug_root=root)
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}):
                with self.assertRaisesRegex(ValueError, "DEBUG_MODE"):
                    runner.measure_pair(None, "aggregate", 0, paths, **arguments)
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false", "CI": "false"}):
                arguments["timing_root"] = root
                with self.assertRaisesRegex(ValueError, "DEBUG_MODE"):
                    runner.measure_pair(None, "aggregate", 0, paths, **arguments)
                arguments["evidence_mode"] = "unknown"
                with self.assertRaisesRegex(ValueError, "EVIDENCE_MODE"):
                    runner.measure_pair(None, "aggregate", 0, paths, **arguments)

    def test_ordered_exact_values_and_shared_fixture(self):
        state, _ = self.run_case("ordered")
        self.assertEqual(sum(sql.startswith("SELECT id, COALESCE") for _, sql in state["sql"]), 50)

    def test_insert_rollback_and_postcheck(self):
        state, clients = self.run_case("insert-rollback")
        self.assertEqual(len(state["transactions"]), 100)
        self.assertEqual(len([row for row in clients["on"] if row["phase"] == "postcheck"]), 1)
        self.assertEqual(len([row for row in clients["off"] if row["phase"] == "postcheck"]), 1)
        self.assertEqual(sum(sql == operation.ROLLBACK_SQL for _, sql in state["sql"]), 2)

    def test_postcheck_validation_is_outside_sql_operation(self):
        state = {"sql": [], "transactions": [], "closed": []}
        connection = Remote(state, None, "off")
        with patch.object(operation.oracle, "rollback", side_effect=AssertionError(
                "oracle ran inside SQL operation")):
            result = operation.postcheck("insert-rollback", connection)
            with self.assertRaisesRegex(AssertionError, "oracle ran inside SQL operation"):
                operation.oracle.rollback(result)
        self.assertEqual(result, [{"id": 1, "value": "old"}])

    def test_postcheck_rejects_unrestored_fixture(self):
        state = {"sql": [], "transactions": [], "closed": []}

        class BrokenRemote(Remote):
            def execute_sql(self, sql):
                if sql == operation.ROLLBACK_SQL:
                    return [{"id": 1, "value": "changed"}]
                return super().execute_sql(sql)

        connection = BrokenRemote(state, None, "off")
        result = operation.postcheck("insert-rollback", connection)
        from . import oracle
        with self.assertRaisesRegex(ValueError, "ROLLBACK"):
            oracle.rollback(result)

    def test_raw_file_rejects_unsafe_parent_symlink_and_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "private"
            private.mkdir(mode=0o700)
            target = private / "raw.ndjson"
            with runner._open_private_raw(target) as stream:
                stream.write("sealed\n")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                runner._open_private_raw(target)
            self.assertEqual(target.read_text(), "sealed\n")
            unsafe = Path(directory) / "unsafe"
            unsafe.mkdir(mode=0o755)
            with self.assertRaisesRegex(ValueError, "RAW_PARENT"):
                runner._open_private_raw(unsafe / "raw.ndjson")
            alias = Path(directory) / "alias"
            alias.symlink_to(private, target_is_directory=True)
            with self.assertRaises(OSError):
                runner._open_private_raw(alias / "new.ndjson")
            link = private / "link.ndjson"
            link.symlink_to(target)
            with self.assertRaises(FileExistsError):
                runner._open_private_raw(link)

    def test_cleanup_error_retains_primary_failure(self):
        state = {"sql": [], "transactions": [], "closed": []}

        class BrokenRemote(Remote):
            def close(self):
                super().close()
                raise RuntimeError("close failed")

        def connect(url, **options):
            return BrokenRemote(state, None, "on" if "sql_timing_sink" in options else "off")

        module = types.SimpleNamespace(_populate_sql=lambda *args: None,
                                       SQL_AGGREGATE_QUERY="AGGREGATE_SQL",
                                       SQL_STREAM_QUERY="STREAM_SQL")
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            paths = {mode: Path(directory) / f"{mode}.ndjson" for mode in ("on", "off")}
            timing_root = Path(directory).resolve() / "public-timing"
            timing_root.mkdir(mode=0o700)
            operation_root = Path(directory).resolve() / "public-operations"
            operation_root.mkdir(mode=0o700)
            with self.assertRaises(BaseExceptionGroup) as caught:
                runner.measure_pair(module, "aggregate", 0, paths, connect=connect,
                                    remote_type=Remote, url="same-qualified-server",
                                    observer=Observer(), timing_root=timing_root,
                                    operation_root=operation_root)
        self.assertTrue(any("setup callback count mismatch" in str(error)
                            for error in caught.exception.exceptions))
        self.assertEqual(sum("close failed" in str(error)
                             for error in caught.exception.exceptions), 2)
        self.assertEqual(state["closed"], ["off", "on"])


if __name__ == "__main__":
    unittest.main()
