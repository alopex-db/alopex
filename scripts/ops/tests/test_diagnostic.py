"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import copy
import hashlib
import unittest
import uuid

from audit import audit
from diagnostic import CellAdapter, STAGES, cell_order


ORACLE = {case: hashlib.sha256(case.encode()).hexdigest() for case in ("aggregate", "ordered")}


def fixture():
    cells, clients, servers = [], [], []
    for case in ("aggregate", "ordered"):
        for trial in range(3):
            for mode in cell_order(trial):
                cells.append({"case": case, "trial": trial, "mode": mode,
                              "result_sha256": ORACLE[case], "raw_seconds": [.001 + trial * .00001] * 21,
                              "raw_sha256": "a" * 64})
                for phase, count in (("precheck", 1), ("warmup", 3), ("measured", 21)):
                    for sample in range(count):
                        base = {"cell_id": f"{case}-{trial}-{mode}", "case": case,
                                "trial": trial, "mode": mode, "phase": phase,
                                "sample": sample, "result_sha256": ORACLE[case], "outer_ns": 200}
                        if mode == "on":
                            trace_id = str(uuid.uuid4())
                            base.update(trace_id=trace_id, bind_ns=10, transport_ns=100,
                                        parse_ns=10, rows_ns=10, residual_ns=10, total_ns=140,
                                        response_chars=20, result_rows=1 if case == "aggregate" else 16,
                                        wrapper_ns=60)
                            for stage in STAGES:
                                servers.append({"trace_id": trace_id, "stage": stage,
                                                "duration_ns": 60 if stage == "execute_and_map" else 10})
                        clients.append(base)
    return cells, clients, servers


class DiagnosticTests(unittest.TestCase):
    def test_full_cardinality_and_off_mode(self):
        report = audit(*fixture(), expected_oracle=ORACLE)
        self.assertEqual((report["measured_uuids"], report["measured_server_events"]), (126, 882))
        self.assertEqual((report["all_uuids"], report["all_server_events"]), (150, 1050))
        self.assertEqual([cell_order(i) for i in range(3)],
                         [("off", "on"), ("on", "off"), ("off", "on")])

    def test_rejects_join_and_timer_failures(self):
        for target, edit in (
            ("duplicate_uuid", lambda c, s: c[26].update(trace_id=c[25]["trace_id"])),
            ("missing_uuid", lambda c, s: s.pop()),
            ("wrong_phase", lambda c, s: c[0].update(phase="warmup")),
            ("extra_stage", lambda c, s: s.append(copy.deepcopy(s[0]))),
            ("missing_stage", lambda c, s: s.pop(0)),
            ("off_trace", lambda c, s: c[-1].update(trace_id=str(uuid.uuid4()))),
            ("client_reconcile", lambda c, s: c[0].update(total_ns=141)),
            ("server_transport", lambda c, s: s[0].update(duration_ns=200)),
            ("malformed_uuid", lambda c, s: c[0].update(trace_id="bad")),
        ):
            with self.subTest(target=target):
                cells, clients, servers = fixture()
                edit(clients, servers)
                with self.assertRaises(ValueError):
                    audit(cells, clients, servers, expected_oracle=ORACLE)

    def test_rejects_oracle_identity(self):
        cells, clients, servers = fixture()
        cells[0]["result_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "oracle_identity"):
            audit(cells, clients, servers, expected_oracle=ORACLE)

    def test_adapter_joins_after_timer_and_rejects_raw_fields(self):
        adapter = CellAdapter("aggregate", 0, "on", lambda case, rows, count: ORACLE[case])
        def operation():
            adapter.sink({"schema": "alopex.sql-client-timing/v1", "trace_id": str(uuid.uuid4()),
                          "bind_ns": 1, "transport_ns": 2, "parse_ns": 1, "rows_ns": 1,
                          "residual_ns": 1, "total_ns": 6, "response_chars": 2, "result_rows": 1})
            return [{"count": 1}]
        adapter.call("precheck", operation)
        for i in range(3):
            adapter.call("warmup", operation, sample=i)
        for i in range(21):
            adapter.call("measured", operation, sample=i, measure=True)
        self.assertEqual(adapter.finish()["measured"], 21)
        self.assertTrue(all("sql" not in row and "path" not in row and "url" not in row
                            for row in adapter.calls))
        with self.assertRaisesRegex(ValueError, "unexpected client timing field"):
            adapter.phase = "postcheck"
            value = {"schema": "alopex.sql-client-timing/v1", "trace_id": str(uuid.uuid4()),
                     **{name: 0 for name in ("bind_ns", "transport_ns", "parse_ns", "rows_ns",
                                             "residual_ns", "total_ns", "response_chars", "result_rows")},
                     "sql": "private"}
            adapter.sink(value)


if __name__ == "__main__":
    unittest.main()
