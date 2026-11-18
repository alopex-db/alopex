"""Focused offline rejection tests for the private four-case evidence contract."""
import copy
import hashlib
import json
import unittest
import uuid

from . import adapter, audit, operation, oracle


BINDING = {"source": {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"},
           "assets_digest": "c" * 64, "controller_sha256": "d" * 64,
           "operator_sha256": "e" * 64, "same_job_id": "job-1",
           "qualification_passed": True, "security_passed": True, "cleanup_passed": True}
FIXTURES = {case: "f" * 64 if case == "insert-rollback" else "1" * 64 for case in audit.CASES}


def checked(cells, clients, servers, raw, manifests, *, binding=BINDING):
    return audit.audit(cells, clients, servers, binding=binding,
                       expected_binding=BINDING, expected_fixtures=FIXTURES,
                       raw_by_cell=raw, resource_manifests=manifests)


def evidence():
    cells, clients, servers, raw, manifests = [], [], [], {}, {}
    for case in audit.CASES:
        for trial in range(3):
            pair_server = {"pid": 101 + trial, "start_ticks": 201 + trial}
            fixture = "f" * 64 if case == "insert-rollback" else "1" * 64
            manifests[(case, trial)] = "9" * 64
            for mode in ("on", "off"):
                ordinals = [2 * sample + audit.pair_order(trial, sample).index(mode)
                            for sample in range(21)]
                raw_rows = []
                for sample in range(21):
                    def process(pid, ticks):
                        return {"pid": pid, "start_ticks": ticks,
                                **{name: 1 for name in audit.COUNTERS}}
                    raw_rows.append({"sample": sample, "seconds": .001,
                                     "pair_ordinal": ordinals[sample],
                                     "resources": {"schema": "alopex.sql-operation-resources/v1",
                                                   "status": "observed", "coverage": "stable_pid_namespace_allocation",
                                                   "manifest_sha256": manifests[(case, trial)],
                                                   "client": process(301 + trial + (mode == "on"), 401 + trial),
                                                   "server_processes": [process(pair_server["pid"], pair_server["start_ticks"])]}})
                raw_bytes = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode()
                                     for row in raw_rows)
                raw[(case, trial, mode)] = raw_bytes
                cells.append({"case": case, "trial": trial, "mode": mode,
                              "result_sha256": oracle.ORACLE_SHA256[case],
                              "raw_seconds": [.001] * 21, "raw_ordinals": ordinals,
                              "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(), "pair_server": pair_server,
                              "fixture_sha256": fixture,
                              "pair_order": [list(audit.pair_order(trial, i)) for i in range(21)],
                              "source": BINDING["source"], "assets_digest": BINDING["assets_digest"],
                              "same_job_id": BINDING["same_job_id"],
                              "rollback_sha256": oracle.ROLLBACK_SHA256 if case == "insert-rollback" else None,
                              "cleanup_passed": True})
                for phase, count in (("setup", audit.SETUP[case] if mode == "on" else 0),
                                     ("precheck", 1), ("warmup", 3), ("measured", 21),
                                     ("postcheck", audit.POSTCHECK[case])):
                    for sample in range(count):
                        row = {"cell_id": f"{case}-{trial}-{mode}", "case": case,
                               "trial": trial, "mode": mode, "phase": phase, "sample": sample}
                        if phase != "setup":
                            row.update(outer_ns=1_000_000,
                                       result_sha256=oracle.ROLLBACK_SHA256 if phase == "postcheck"
                                       else oracle.ORACLE_SHA256[case])
                        if phase == "measured":
                            row["pair_ordinal"] = ordinals[sample]
                        if mode == "on":
                            trace_id = str(uuid.uuid4())
                            row.update(trace_id=trace_id, bind_ns=10, transport_ns=100,
                                       parse_ns=10, rows_ns=10, residual_ns=10, total_ns=140,
                                       response_chars=20, result_rows=1)
                            if phase != "setup":
                                row["wrapper_ns"] = 999_860
                            stages = (audit.SESSION_STAGES if case == "insert-rollback" and
                                      phase in ("precheck", "warmup", "measured") else audit.READ_STAGES)
                            for stage in stages:
                                servers.append({"trace_id": trace_id, "stage": stage,
                                                "duration_ns": 60 if stage == "execute_and_map" else 5})
                        clients.append(row)
    return cells, clients, servers, raw, manifests


class ContractTests(unittest.TestCase):
    def test_raw_and_resource_evidence_required(self):
        cells, clients, servers, raw, manifests = evidence()
        with self.assertRaisesRegex(ValueError, "raw_inventory"):
            checked(cells, clients, servers, {}, manifests)
        key = ("aggregate", 0, "on")
        raw[key] = raw[key].replace(b'"seconds":0.001', b'"seconds":0.002', 1)
        with self.assertRaisesRegex(ValueError, "raw_digest"):
            checked(cells, clients, servers, raw, manifests)
        cells, clients, servers, raw, manifests = evidence()
        key = ("aggregate", 0, "on")
        rows = [json.loads(line) for line in raw[key].splitlines()]
        rows[0]["resources"]["client"].pop("write_bytes")
        raw[key] = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode()
                            for row in rows)
        next(cell for cell in cells if (cell["case"], cell["trial"], cell["mode"]) == key)["raw_sha256"] = \
            hashlib.sha256(raw[key]).hexdigest()
        with self.assertRaisesRegex(ValueError, "resource_counters"):
            checked(cells, clients, servers, raw, manifests)
        rows[0]["resources"]["client"]["write_bytes"] = 1
        rows[0]["resources"]["server_processes"][0]["start_ticks"] += 1
        raw[key] = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode()
                            for row in rows)
        next(cell for cell in cells if (cell["case"], cell["trial"], cell["mode"]) == key)["raw_sha256"] = \
            hashlib.sha256(raw[key]).hexdigest()
        with self.assertRaisesRegex(ValueError, "resource_pid"):
            checked(cells, clients, servers, raw, manifests)

    def test_inner_case_and_timer_containment(self):
        trace_id = str(uuid.uuid4())
        clients = [{"trace_id": trace_id, "case": "aggregate", "phase": "measured"}]
        servers = [{"trace_id": trace_id, "stage": "execute_and_map", "duration_ns": 100}]
        events = [{"kind": "stage", "trace_id": trace_id, "stage": "worker_execute_plan", "duration_ns": 90},
                  {"kind": "route", "trace_id": trace_id, "statement_index": 0,
                   "route": "local_only", "root": "aggregate"},
                  {"kind": "branch", "trace_id": trace_id, "branch": "aggregate_streaming_global"}]
        self.assertEqual(len(adapter.audit_inner_events(events, "on", clients, servers)["measured"]), 1)
        wrong = copy.deepcopy(events)
        wrong[2]["branch"] = "ordered_limit_primary_key"
        with self.assertRaisesRegex(ValueError, "INNER_CASE"):
            adapter.audit_inner_events(wrong, "on", clients, servers)
        wrong = copy.deepcopy(events)
        wrong[0]["duration_ns"] = 101
        with self.assertRaisesRegex(ValueError, "INNER_TIMER"):
            adapter.audit_inner_events(wrong, "on", clients, servers)

    def test_complete_inventory(self):
        cells, clients, servers, raw, manifests = evidence()
        self.assertEqual((len(cells), len(clients), len(servers)), (24, 3720, 23994))
        result = checked(cells, clients, servers, raw, manifests)
        self.assertEqual((result["measured_uuids"], result["measured_server_events"]), (252, 1827))
        self.assertFalse(result["performance_qualification"])

    def test_rejects_correlation_and_stage_drift(self):
        for name, change in (
            ("missing UUID", lambda c, s: s.pop()),
            ("extra stage", lambda c, s: s.append(copy.copy(s[0]))),
            ("missing session stage", lambda c, s: next(s.pop(i) for i, row in enumerate(s)
                                                     if row["stage"] == "session_completion")),
            ("wrong phase", lambda c, s: c[0].update(phase="measured")),
            ("off trace", lambda c, s: c[-1].update(trace_id=str(uuid.uuid4()))),
            ("bad timer", lambda c, s: c[0].update(total_ns=141)),
        ):
            with self.subTest(name=name):
                cells, clients, servers, raw, manifests = evidence()
                change(clients, servers)
                with self.assertRaises(ValueError):
                    checked(cells, clients, servers, raw, manifests)

    def test_rejects_identity_rollback_cleanup_and_privacy(self):
        for field, value in (("source", {"commit": "0" * 40, "tree": "b" * 40, "profile": "release"}),
                             ("assets_digest", "0" * 64), ("same_job_id", "stale"),
                             ("rollback_sha256", "0" * 64), ("cleanup_passed", False)):
            cells, clients, servers, raw, manifests = evidence()
            target = next(cell for cell in cells if cell["case"] == "insert-rollback")
            target[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                checked(cells, clients, servers, raw, manifests)
        cells, clients, servers, raw, manifests = evidence()
        clients[0]["sql"] = "private"
        with self.assertRaisesRegex(ValueError, "client_fields"):
            checked(cells, clients, servers, raw, manifests)
        stale = copy.deepcopy(BINDING)
        stale["controller_sha256"] = "0" * 64
        cells, clients, servers, raw, manifests = evidence()
        with self.assertRaisesRegex(ValueError, "binding_identity"):
            checked(cells, clients, servers, raw, manifests, binding=stale)

    def test_exact_oracles(self):
        self.assertEqual(oracle.operation("aggregate", [{"count": 17125}], 34250),
                         oracle.ORACLE_SHA256["aggregate"])
        with self.assertRaisesRegex(ValueError, "ORACLE"):
            oracle.operation("select-all", list(reversed(oracle.expected("select-all"))), 34250)
        self.assertEqual(oracle.rollback([{"id": 1, "value": "old"}]), oracle.ROLLBACK_SHA256)
        with self.assertRaisesRegex(ValueError, "ROLLBACK"):
            oracle.rollback([{"id": 1, "value": "changed"}])

    def test_insert_operation_keeps_begin_and_rollback_outside_sql_callback(self):
        calls = []

        class Transaction:
            def execute_sql(self, sql):
                calls.append(("sql", sql))
                return [{"id": 3}]

            def rollback(self):
                calls.append(("rollback",))

        class Connection:
            def execute_sql(self, sql):
                calls.append(("setup", sql))
                return []

            def begin(self, mode):
                calls.append(("begin", mode))
                return Transaction()

        connection = Connection()
        adapter = operation.prepare("insert-rollback", None, connection,
                                    lambda work: work(), txn_mode="read_write")
        self.assertEqual(len(calls), 3)
        self.assertEqual(adapter(connection), [{"id": 3}])
        self.assertEqual([row[0] for row in calls[3:]], ["begin", "sql", "rollback"])
        self.assertEqual(calls[4][1], operation.INSERT_SQL)


if __name__ == "__main__":
    unittest.main()
