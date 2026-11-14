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

from diagnostic_supervisor import (SOURCE, parse_inner_diagnostics, parse_server_timing, require_client_inventory,
                                   server_environment, worker_config)
from diagnostic import CLIENT_RECORDS_PER_CELL, CLIENT_RECORDS_OFF_CELL


class DiagnosticSupervisorTests(unittest.TestCase):
    def test_private_inner_aggregate_and_ordered_join(self):
        aggregate = "123e4567-e89b-42d3-a456-426614174000"
        ordered = "123e4567-e89b-42d3-a456-426614174001"
        clients = [{"trace_id": trace, "phase": "measured"} for trace in (aggregate, ordered)]
        servers = [{"trace_id": trace, "stage": "route_plan", "duration_ns": 1}
                   for trace in (aggregate, ordered)]
        lines = [
            f'INFO alopex_sql_private_inner: sql inner diagnostic stage trace_id={aggregate} stage="aggregate_parallel_build" duration_ns=42',
            f'INFO alopex_sql_private_inner: sql diagnostic route trace_id={aggregate} statement_index=0 route="local_only" root="aggregate"',
            f'INFO alopex_sql_private_inner: sql diagnostic branch trace_id={aggregate} branch="aggregate_parallel"',
            f'INFO alopex_sql_private_inner: sql diagnostic plan trace_id={aggregate} branch="aggregate"',
            f'INFO alopex_sql_private_inner: sql diagnostic stage trace_id={ordered} stage="worker_plan" duration_ns=12',
            f'INFO alopex_sql_private_inner: sql diagnostic route trace_id={ordered} statement_index=0 route="local_only" root="sort"',
            f'INFO alopex_sql_private_inner: sql diagnostic branch trace_id={ordered} branch="ordered_limit_primary_key"',
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text("\n".join(lines) + "\n")
            report = parse_inner_diagnostics(path, "on", clients, servers)
            self.assertEqual(report["schema"], "alopex.sql-private-inner/v1")
            self.assertIs(report["performance_qualification"], False)
            self.assertEqual(len(report["measured"]), 2)
            self.assertEqual(report["measured"][0]["stages"],
                             [{"stage": "aggregate_parallel_build", "duration_ns": 42}])
            self.assertNotIn("sql=", json.dumps(report))
            for name, changed, error in (
                ("duplicate", lines + [lines[0]], "INNER_DUPLICATE"),
                ("unknown", [lines[0].replace("aggregate_parallel_build", "private_value")], "INNER_FORMAT"),
                ("malformed", [lines[0] + " sql=secret"], "INNER_FORMAT"),
                ("off", lines, "INNER_OFF"),
                ("overflow", [lines[1].replace("statement_index=0", "statement_index=16")], "INNER_FORMAT"),
                ("bad_route", [lines[1].replace("local_only", "secret")], "INNER_FORMAT"),
                ("bad_uuid", [lines[0].replace(aggregate, "00000000-0000-4000-8000-000000000001")], "INNER_JOIN"),
                ("missing_measured", lines[:4], "INNER_JOIN"),
                ("missing_branch", [line for line in lines if "branch=" not in line], "INNER_JOIN"),
            ):
                with self.subTest(name=name):
                    path.write_text("\n".join(changed) + "\n")
                    with self.assertRaisesRegex(ValueError, error):
                        parse_inner_diagnostics(path, "off" if name == "off" else "on", clients, servers)

    def test_exact_frozen_client_inventory_matches_mode(self):
        self.assertEqual(CLIENT_RECORDS_PER_CELL, 370)
        self.assertEqual(CLIENT_RECORDS_OFF_CELL, 25)
        for mode, count in (("on", 370), ("off", 25)):
            with self.subTest(mode=mode):
                rows = [{} for _ in range(count)]
                self.assertIs(require_client_inventory(rows, mode), rows)
                with self.assertRaisesRegex(ValueError, "EVIDENCE_BOUND"):
                    require_client_inventory(rows + [{}], mode)
                with self.assertRaisesRegex(ValueError, "EVIDENCE_BOUND"):
                    require_client_inventory(rows[:-1], mode)

    def test_on_off_server_environment_isolated(self):
        base = {"ALOPEX_SQL_TIMING": "1", "RUST_LOG": "debug", "NO_COLOR": "", "OTHER": "kept"}
        on = server_environment(base, "on")
        off = server_environment(base, "off")
        self.assertEqual(on["ALOPEX_SQL_TIMING"], "1")
        self.assertEqual(on["RUST_LOG"], "warn,alopex_sql_private_timing=info")
        self.assertEqual(on["NO_COLOR"], "1")
        self.assertNotIn("ALOPEX_SQL_TIMING", off)
        self.assertEqual(off["RUST_LOG"], "warn")
        self.assertEqual(off["NO_COLOR"], "1")
        self.assertEqual(base["NO_COLOR"], "")
        self.assertEqual(base["RUST_LOG"], "debug")

    def test_worker_schema_and_source_pin(self):
        config = {"schema": "alopex.sql-diagnostic-supervisor/v1", "source": SOURCE,
                  "engine": "alopex-server", "case": "aggregate", "trial": 0,
                  "modes": ["on", "off"]}
        result = worker_config(config, Path("/work/results"), Path("/work/data"),
                               Path("/work/docs"), Path("/work/product"))
        self.assertEqual(result["schema"], "alopex.sql-diagnostic-pair/v1")
        self.assertEqual(result["outputs"]["on"]["client"], "/work/results/on/client.ndjson")
        for bad in ({**config, "source": {**SOURCE, "commit": "a" * 40}},
                    {**config, "modes": ["unknown"]}, {**config, "trial": 3}):
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

    def test_http_request_span_before_target_is_reduced(self):
        trace = "123e4567-e89b-42d3-a456-426614174000"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text(
                "2026 INFO http_request{correlation_id=diag-" + trace +
                " traceparent= method=POST path=/v1/sql}: "
                "alopex_sql_private_timing: sql diagnostic stage "
                f"trace_id={trace} stage=\"result_map\" duration_ns=42\n")
            self.assertEqual(parse_server_timing(path, "on"),
                             [{"trace_id": trace, "stage": "result_map", "duration_ns": 42}])
            with self.assertRaisesRegex(ValueError, "TRACE_FORMAT"):
                parse_server_timing(path, "off")
            path.write_text(path.read_text().replace("http_request{", "other_request{"))
            with self.assertRaisesRegex(ValueError, "TRACE_FORMAT"):
                parse_server_timing(path, "on")

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
