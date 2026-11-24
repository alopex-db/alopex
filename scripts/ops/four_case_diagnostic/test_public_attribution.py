"""Public attribution inventory and exposure boundary checks."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest

from . import adapter, public_attribution as attribution


def resource():
    counters = {name: 1 for name in attribution.COUNTERS}
    return {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
            "coverage": attribution.COVERAGE, "client": counters,
            "server_processes": [counters, counters]}


def client(case, trial, sample):
    return {"case": case, "trial": trial, "mode": "on", "phase": "measured",
            "sample": sample, "trace_id": f"private-{case}-{trial}-{sample}",
            "bind_ns": 1, "transport_ns": 2, "parse_ns": 3, "rows_ns": 4,
            "residual_ns": 5, "total_ns": 15, "response_chars": 20,
            "result_rows": 1}


class PublicAttributionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.operations = Path(self.tmp.name).resolve() / "operations"
        self.stages = Path(self.tmp.name).resolve() / "stages"
        self.operations.mkdir(mode=0o700)
        self.stages.mkdir(mode=0o700)

    def fill_operations(self):
        for case in adapter.CASES:
            for trial in range(3):
                for mode in ("on", "off"):
                    with attribution.OperationCellWriter(self.operations, case, trial, mode) as writer:
                        for sample in range(21):
                            ordinal = 2 * sample + adapter.pair_order(trial, sample).index(mode)
                            writer.append(sample, ordinal, 0.001,
                                          client(case, trial, sample) if mode == "on" else None,
                                          resource())

    def fill_stages(self):
        for case in adapter.CASES:
            for trial in range(3):
                clients = [client(case, trial, sample) for sample in range(21)]
                events = [{"trace_id": row["trace_id"], "stage": stage,
                           "duration_ns": sample + 1}
                          for sample, row in enumerate(clients)
                          for stage in attribution._stage_order(case)]
                attribution.write_stage_cell(self.stages, case, trial, clients, events)

    def test_complete_inventory_has_only_fixed_numeric_fields(self):
        self.fill_operations()
        self.fill_stages()
        operations = attribution.load_operation_inventory(self.operations)
        stages = attribution.load_stage_inventory(self.stages)
        self.assertEqual((len(operations), len(stages)), (504, 1764))
        self.assertEqual(operations[0]["server_rss_after_bytes"], 2)
        self.assertEqual(set(row["stage"] for row in stages if row["case"] == "insert-rollback"),
                         set(attribution.STAGE_ORDER_SESSION))
        self.assertNotIn("trace_id", json.dumps(operations + stages))
        self.assertTrue(all("pid" not in row for row in operations + stages))

    def test_missing_duplicate_malformed_out_of_range_operation_rejected(self):
        self.fill_operations()
        path = self.operations / attribution.operation_filename("aggregate", 0, "on")
        original = path.read_bytes()
        with self.subTest("missing"):
            path.write_bytes(b"".join(original.splitlines(keepends=True)[:-1]))
            with self.assertRaises(ValueError):
                attribution.load_operation_inventory(self.operations)
        with self.subTest("duplicate"):
            path.write_bytes(original + original.splitlines(keepends=True)[0])
            with self.assertRaises(ValueError):
                attribution.load_operation_inventory(self.operations)
        with self.subTest("field"):
            row = json.loads(original.splitlines()[0])
            row["pid"] = 123
            path.write_bytes(json.dumps(row, separators=(",", ":")).encode() + b"\n" +
                             b"".join(original.splitlines(keepends=True)[1:]))
            with self.assertRaises(ValueError):
                attribution.load_operation_inventory(self.operations)
        with self.subTest("range"):
            row.pop("pid")
            row["client_rss_after_bytes"] = attribution.MAX_COUNTER_BYTES + 1
            path.write_bytes(json.dumps(row, separators=(",", ":")).encode() + b"\n" +
                             b"".join(original.splitlines(keepends=True)[1:]))
            with self.assertRaises(ValueError):
                attribution.load_operation_inventory(self.operations)

    def test_symlink_and_stage_gaps_rejected(self):
        self.fill_operations()
        self.fill_stages()
        path = self.operations / attribution.operation_filename("aggregate", 0, "on")
        original = path.read_bytes()
        path.unlink()
        target = Path(self.tmp.name) / "target"
        target.write_bytes(original)
        path.symlink_to(target)
        with self.assertRaises(ValueError):
            attribution.load_operation_inventory(self.operations)
        stage_path = self.stages / attribution.stage_filename("insert-rollback", 0)
        rows = stage_path.read_bytes().splitlines(keepends=True)
        stage_path.write_bytes(b"".join(rows[:-1]))
        with self.assertRaises(ValueError):
            attribution.load_stage_inventory(self.stages)

    def test_stage_projection_rejects_duplicate_and_wrong_route(self):
        case, trial = "insert-rollback", 0
        clients = [client(case, trial, sample) for sample in range(21)]
        events = [{"trace_id": row["trace_id"], "stage": stage, "duration_ns": 1}
                  for row in clients for stage in attribution.STAGE_ORDER_SESSION]
        with self.assertRaises(ValueError):
            attribution.stage_records(case, trial, clients, events + [events[0]])
        events[0]["stage"] = "transaction_setup"
        with self.assertRaises(ValueError):
            attribution.stage_records(case, trial, clients, events)


if __name__ == "__main__":
    unittest.main()
