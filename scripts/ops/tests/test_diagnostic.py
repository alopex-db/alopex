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
from diagnostic import CellAdapter, STAGES, cell_order, pair_order


ORACLE = {case: hashlib.sha256(case.encode()).hexdigest() for case in ("aggregate", "ordered")}


def fixture():
    cells, clients, servers = [], [], []
    for case in ("aggregate", "ordered"):
        for trial in range(3):
            for mode in cell_order(trial):
                cells.append({"case": case, "trial": trial, "mode": mode,
                              "result_sha256": ORACLE[case], "raw_seconds": [.001 + trial * .00001] * 21,
                              "raw_sha256": "a" * 64,
                              "raw_ordinals": [2 * i + pair_order(trial, i).index(mode)
                                               for i in range(21)],
                              "pair_server": {"pid": trial + 100, "start_ticks": trial + 1000},
                              "fixture_sha256": ORACLE[case],
                              "pair_order": [list(pair_order(trial, i)) for i in range(21)]})
                for phase, count in (("precheck", 1), ("warmup", 3), ("measured", 21)):
                    for sample in range(count):
                        base = {"cell_id": f"{case}-{trial}-{mode}", "case": case,
                                "trial": trial, "mode": mode, "phase": phase,
                                "sample": sample, "result_sha256": ORACLE[case],
                                "outer_ns": round((.001 + trial * .00001) * 1_000_000_000)}
                        if phase == "measured":
                            base["pair_ordinal"] = 2 * sample + pair_order(trial, sample).index(mode)
                        if mode == "on":
                            trace_id = str(uuid.uuid4())
                            base.update(trace_id=trace_id, bind_ns=10, transport_ns=100,
                                        parse_ns=10, rows_ns=10, residual_ns=10, total_ns=140,
                                        response_chars=20, result_rows=1 if case == "aggregate" else 16,
                                        wrapper_ns=base["outer_ns"] - 140)
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
        self.assertEqual([pair_order(i, 0) for i in range(3)],
                         [("off", "on"), ("on", "off"), ("off", "on")])

    def test_pair_identity_and_order_fail_closed(self):
        for mutation in (
            lambda cells: cells[0]["pair_server"].update(pid=999),
            lambda cells: cells[0].update(fixture_sha256="0" * 64),
            lambda cells: cells[0]["pair_order"][0].reverse(),
        ):
            cells, clients, servers = fixture()
            mutation(cells)
            with self.assertRaisesRegex(ValueError, "pair_identity"):
                audit(cells, clients, servers, expected_oracle=ORACLE)

    def test_adjacent_overhead_gate_keeps_original_case_bound(self):
        cells, clients, servers = fixture()
        for cell in cells:
            cell["raw_seconds"] = [0.0011 if cell["mode"] == "on" else 0.001] * 21
        for row in clients:
            if row["phase"] == "measured":
                row["outer_ns"] = 1_100_000 if row["mode"] == "on" else 1_000_000
                if row["mode"] == "on":
                    row["wrapper_ns"] = row["outer_ns"] - row["total_ns"]
        report = audit(cells, clients, servers, expected_oracle=ORACLE)
        for case in ORACLE:
            self.assertAlmostEqual(report["paired"][case]["bound_ms"], 0.05)
            self.assertFalse(report["paired"][case]["small_overhead"])
            self.assertGreater(report["paired"][case]["U_ms"], 0.05)

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

    def test_rejects_raw_timer_and_adjacent_order_mismatch(self):
        cells, clients, servers = fixture()
        cells[0]["raw_seconds"][0] += 0.000001
        with self.assertRaisesRegex(ValueError, "raw_client_timer_join"):
            audit(cells, clients, servers, expected_oracle=ORACLE)
        cells, clients, servers = fixture()
        cells[0]["raw_ordinals"][0] = 41
        for row in clients:
            if (row["case"], row["trial"], row["mode"], row["phase"], row["sample"]) == \
                    (cells[0]["case"], cells[0]["trial"], cells[0]["mode"], "measured", 0):
                row["pair_ordinal"] = 41
        with self.assertRaisesRegex(ValueError, "pair_order"):
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
