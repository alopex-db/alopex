"""Tiny SQL and fake handles protect composite timing and validation contracts."""
import copy
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sqlite_composite_profile as d


class Fake:
    def __init__(self, events, fail=None):
        self.events, self.fail = events, fail

    def execute_sql(self, sql):
        self.events.append(sql)
        if self.fail and sql.startswith(self.fail):
            raise ValueError("injected operation failure")
        if sql.startswith("SELECT COUNT"):
            return [{"count": 2}]
        if sql.startswith("SELECT id, COALESCE"):
            return [{"id": i, "value": float(i)} for i in range(3)]

    def begin(self, mode):
        self.events.append("begin")
        return self

    def savepoint(self, name): self.events.append("savepoint")
    def rollback_to(self, name): self.events.append("rollback_to")
    def release(self, name): self.events.append("release")
    def rollback(self): self.events.append("rollback")
    def prepare(self, sql):
        self.events.append("prepare")
        return self
    def bind(self, index, value): self.events.append(("bind", index, value))
    def execute(self):
        self.events.append("execute")
        if self.fail == "prepared":
            raise ValueError("prepared failure")
        return [{"id": 0}]
    def finalize(self): self.events.append("finalize")


class Contracts(unittest.TestCase):
    def test_original_and_checked_operation_order(self):
        outputs = []
        with patch.dict(sys.modules, {"alopex": SimpleNamespace(TxnMode=SimpleNamespace(READ_WRITE="rw"))}):
            for checked in (False, True):
                events = []
                client = Fake(events)
                if checked:
                    result, intervals = d.checked_composite(client, "alopex", 3)
                    d.validate_result(result, 3)
                    self.assertEqual(intervals, {})
                else:
                    self.assertIsNone(d.original._sql_workload(client, "alopex", "sql-sqlite-curated", 3))
                outputs.append([x.replace(" AS value", "") if isinstance(x, str) else x for x in events])
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[1][-7:], ["prepare", ("bind", 1, 0), "execute", "finalize",
                                         "begin", "INSERT INTO perf_child VALUES (1, 1, 1)", "rollback"])
        self.assertEqual(outputs[1].count("rollback"), 2)

    def test_phase_clocks_are_optional_and_ordered(self):
        with patch.dict(sys.modules, {"alopex": SimpleNamespace(TxnMode=SimpleNamespace(READ_WRITE="rw"))}):
            result, phases = d.checked_composite(Fake([]), "alopex", 3, iter(range(10)).__next__)
        self.assertEqual(tuple(phases), d.PHASES)
        self.assertEqual(list(phases.values()), [1] * 5)
        d.validate_result(result, 3)

    def test_prepared_failure_finalizes(self):
        events = []
        with self.assertRaisesRegex(ValueError, "prepared failure"):
            d.prepared(Fake(events, "prepared"), "alopex")
        self.assertEqual(events[-1], "finalize")

    def test_savepoint_failure_rolls_back(self):
        events = []
        with patch.dict(sys.modules, {"alopex": SimpleNamespace(TxnMode=SimpleNamespace(READ_WRITE="rw"))}):
            with self.assertRaisesRegex(ValueError, "injected"):
                d.savepoint_cycle(Fake(events, "UPDATE"), "alopex", 3)
        self.assertEqual(events[-1], "rollback")

    def test_typed_results_reject_wrong_values_types_order_and_length(self):
        expected = {"aggregate": [(2,)], "ordered": [(0, 0.0), (1, 1.0), (2, 2.0)], "prepared": [(0,)]}
        d.validate_result(expected, 3)
        for phase, bad in [("aggregate", [(True,)]), ("prepared", [(1,)]), ("ordered", [(0, 0)]),
                           ("ordered", [(1, 1.0), (0, 0.0), (2, 2.0)]),
                           ("ordered", [(0, 0.0), (1, 1.0), (1, 1.0)])]:
            actual = copy.deepcopy(expected); actual[phase] = bad
            with self.assertRaises(RuntimeError): d.validate_result(actual, 3)

    def test_three_row_sqlite_state_savepoint_and_constraint_preflight(self):
        with closing(sqlite3.connect(":memory:", isolation_level=None)) as connection:
            d.original._populate_sql(connection, "sqlite", 3)
            d.original._prepare_sql_workload(connection, "sqlite", "sql-sqlite-curated")
            initial = d.validate_state(connection, "sqlite", 3)
            with self.assertRaisesRegex(RuntimeError, "constraint violation was accepted"):
                d.validate_constraints(connection, "sqlite")
            connection.execute("PRAGMA foreign_keys=ON")
            d.validate_constraints(connection, "sqlite")
            d.validate_savepoint(connection, "sqlite", 3)
            result, _ = d.checked_composite(connection, "sqlite", 3)
            d.validate_result(result, 3)
            self.assertEqual(d.validate_state(connection, "sqlite", 3), initial)

    def test_unrelated_error_is_not_constraint_acceptance(self):
        with patch.object(d.original, "_sql_transaction", side_effect=ValueError("parser unavailable")):
            with self.assertRaisesRegex(RuntimeError, "unexpected constraint-preflight"):
                d.validate_constraints(None, "sqlite")

    def test_measure_validates_and_releases_outside_timer(self):
        events = []
        class Result:
            def __del__(self): events.append("release")
        def clock(): events.append("clock"); return len(events)
        def operation(): events.append("operation"); return Result()
        count = d.measure(operation, lambda actual: events.append("validate"), lambda: events.append("state"),
                          warmups=0, samples=2, clock=clock, record=lambda event: events.append("record"))
        self.assertEqual(count, 2)
        self.assertEqual(events, ["clock", "operation", "clock", "validate", "state", "record", "release"] * 2)

    def test_ci_rejects_before_native_import_or_work_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            run = subprocess.run([sys.executable, "-B", d.__file__, "--manual", "--surface", "embedded",
                                  "--rows", "3", "--operation", "alopex-original", "--work", str(work)],
                                 env=dict(os.environ, CI="true"), capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("forbidden in CI", run.stderr)
            self.assertFalse(work.exists())


if __name__ == "__main__":
    unittest.main()
