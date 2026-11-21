"""Focused checks for the pinned v0816 harness and runtime consumer boundary."""
import copy
import hashlib
import json
from pathlib import Path
import sys
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
