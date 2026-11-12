"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import json
from pathlib import Path
import tempfile
import unittest

from diagnostic_supervisor import (SOURCE, parse_server_timing, require_client_inventory,
                                   server_environment, worker_config)
from diagnostic import CLIENT_RECORDS_PER_CELL


class DiagnosticSupervisorTests(unittest.TestCase):
    def test_exact_frozen_client_inventory_370_accepts_one_extra_rejects(self):
        self.assertEqual(CLIENT_RECORDS_PER_CELL, 370)
        rows = [{} for _ in range(CLIENT_RECORDS_PER_CELL)]
        self.assertIs(require_client_inventory(rows), rows)
        with self.assertRaisesRegex(ValueError, "EVIDENCE_BOUND"):
            require_client_inventory(rows + [{}])

    def test_on_off_server_environment_isolated(self):
        base = {"ALOPEX_SQL_TIMING": "1", "RUST_LOG": "debug", "OTHER": "kept"}
        on = server_environment(base, "on")
        off = server_environment(base, "off")
        self.assertEqual(on["ALOPEX_SQL_TIMING"], "1")
        self.assertEqual(on["RUST_LOG"], "warn,alopex_sql_private_timing=info")
        self.assertNotIn("ALOPEX_SQL_TIMING", off)
        self.assertEqual(off["RUST_LOG"], "warn")
        self.assertEqual(base["RUST_LOG"], "debug")

    def test_worker_schema_and_source_pin(self):
        config = {"schema": "alopex.sql-diagnostic-supervisor/v1", "source": SOURCE,
                  "engine": "alopex-server", "case": "aggregate", "trial": 0,
                  "mode": "on"}
        result = worker_config(config, Path("/work/results"), Path("/work/data"),
                               Path("/work/docs"), Path("/work/product"))
        self.assertEqual(result["schema"], "alopex.sql-diagnostic-cell/v1")
        self.assertEqual(result["client"], "/work/results/client.ndjson")
        for bad in ({**config, "source": {**SOURCE, "commit": "a" * 40}},
                    {**config, "mode": "unknown"}, {**config, "trial": 3}):
            with self.assertRaisesRegex(ValueError, "IDENTITY"):
                worker_config(bad, Path("/work"), Path("/data"), Path("/docs"), Path("/product"))

    def test_exact_target_reduction_and_off_rejection(self):
        trace = "123e4567-e89b-42d3-a456-426614174000"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text("2026 INFO other: sql=secret\n"
                            f"2026 INFO alopex_sql_private_timing: sql diagnostic stage trace_id={trace} stage=\"result_map\" duration_ns=42\n")
            self.assertEqual(parse_server_timing(path, "on"),
                             [{"trace_id": trace, "stage": "result_map", "duration_ns": 42}])
            self.assertNotIn("secret", json.dumps(parse_server_timing(path, "on")))
            with self.assertRaisesRegex(ValueError, "TRACE_FORMAT"):
                parse_server_timing(path, "off")

    def test_malformed_target_and_oversize_log_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text("INFO alopex_sql_private_timing: sql=private\n")
            with self.assertRaisesRegex(ValueError, "TRACE_FORMAT"):
                parse_server_timing(path, "on")
            path.write_bytes(b"x" * 8_000_001)
            with self.assertRaisesRegex(ValueError, "TRACE_BOUND"):
                parse_server_timing(path, "on")


if __name__ == "__main__":
    unittest.main()
