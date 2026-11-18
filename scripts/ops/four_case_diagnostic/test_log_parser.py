"""Offline source-format and exact-inventory checks for pair SQL logs."""
import tempfile
import unittest
import uuid
from pathlib import Path

from . import log_parser as parser


def pair(case):
    trial = 0
    cells = {}
    lines = ["2026-01-01 INFO unrelated: ordinary server line\n"]
    for mode in ("on", "off"):
        rows = []
        counts = {"setup": parser.SETUP[case] if mode == "on" else 0,
                  "precheck": 1, "warmup": 3, "measured": 21,
                  "postcheck": parser.POSTCHECK[case]}
        for phase, count in counts.items():
            for sample in range(count):
                row = {"case": case, "trial": trial, "mode": mode,
                       "cell_id": f"{case}-{trial}-{mode}",
                       "phase": phase, "sample": sample}
                if mode == "on":
                    trace_id = str(uuid.uuid4())
                    row["trace_id"] = trace_id
                    stages = (parser.SESSION_STAGES if case == "insert-rollback" and
                              phase in ("precheck", "warmup", "measured") else parser.READ_STAGES)
                    lines.extend(f"2026-01-01 INFO alopex_sql_private_timing: sql diagnostic stage "
                                 f'trace_id={trace_id} stage="{stage}" duration_ns=5\n'
                                 for stage in sorted(stages))
                rows.append(row)
        cells[mode] = rows
    return cells, lines


class PairLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "server.log"

    def parse(self, case, cells, lines):
        self.path.write_text("".join(lines), encoding="utf-8")
        self.path.chmod(0o600)
        return parser.parse_server_timing(self.path, case, 0, cells["on"], cells["off"])

    def test_exact_read_and_insert_boundaries(self):
        for case, count in (("aggregate", 2590), ("ordered", 2590),
                            ("select-all", 2590), ("insert-rollback", 203)):
            with self.subTest(case=case):
                cells, lines = pair(case)
                self.assertEqual(len(self.parse(case, cells, lines)), count)

    def test_duration_exact_upper_bound(self):
        cells, lines = pair("insert-rollback")
        lines[1] = lines[1].replace("duration_ns=5", f"duration_ns={parser.MAX_DURATION_NS}")
        self.assertEqual(self.parse("insert-rollback", cells, lines)[0]["duration_ns"],
                         parser.MAX_DURATION_NS)
        lines[1] = lines[1].replace(f"duration_ns={parser.MAX_DURATION_NS}",
                                    f"duration_ns={parser.MAX_DURATION_NS + 1}")
        with self.assertRaisesRegex(ValueError, "TRACE_FORMAT"):
            self.parse("insert-rollback", cells, lines)

    def test_rejects_missing_duplicate_unknown_and_wrong_route_stage(self):
        cells, lines = pair("insert-rollback")
        wrong_route = lines.copy()
        index = next(i for i, line in enumerate(lines) if 'stage="transaction_setup"' in line)
        wrong_route[index] = wrong_route[index].replace("transaction_setup", "session_setup")
        for changed in (lines[:-1], lines + [lines[1]],
                        lines + [lines[1].replace(cells["on"][0]["trace_id"], str(uuid.uuid4()))],
                        wrong_route):
            with self.subTest(size=len(changed)):
                with self.assertRaises(ValueError):
                    self.parse("insert-rollback", cells, changed)

    def test_rejects_malformed_target_lines_but_skips_normal_lines(self):
        cells, lines = pair("insert-rollback")
        self.assertEqual(len(self.parse("insert-rollback", cells, lines)), 203)
        for bad in (lines[1].replace("duration_ns=5", "duration_ns=-1"),
                    lines[1].replace('stage="', 'stage="unknown_'),
                    lines[1].replace("sql diagnostic stage", "sql diagnostic malformed"),
                    "INFO alopex_sql_private_timing: incomplete\n"):
            with self.subTest(bad=bad[-50:]):
                with self.assertRaises(ValueError):
                    self.parse("insert-rollback", cells, lines + [bad])

    def test_rejects_invalid_uuid_and_off_mode_trace(self):
        cells, lines = pair("insert-rollback")
        uppercase = cells["on"][0]["trace_id"].upper()
        changed = lines[:1] + [lines[1].replace(cells["on"][0]["trace_id"], uppercase)] + lines[2:]
        with self.assertRaises(ValueError):
            self.parse("insert-rollback", cells, changed)
        cells["off"][0]["trace_id"] = str(uuid.uuid4())
        with self.assertRaisesRegex(ValueError, "TRACE_OFF"):
            self.parse("insert-rollback", cells, lines)

    def test_rejects_client_inventory_and_log_byte_cap(self):
        cells, lines = pair("aggregate")
        with self.assertRaisesRegex(ValueError, "TRACE_JOIN"):
            self.parse("aggregate", {**cells, "on": cells["on"][:-1]}, lines)
        with self.assertRaisesRegex(ValueError, "TRACE_JOIN"):
            self.parse("aggregate", {**cells, "off": cells["off"] + cells["off"][:1]}, lines)
        self.path.write_bytes(b"x" * (parser.MAX_LOG_BYTES + 1))
        self.path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "TRACE_BOUND"):
            parser.parse_server_timing(self.path, "aggregate", 0, cells["on"], cells["off"])

    def test_rejects_nonprivate_and_symlinked_log(self):
        cells, lines = pair("insert-rollback")
        self.path.write_text("".join(lines), encoding="utf-8")
        self.path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "TRACE_BOUND"):
            parser.parse_server_timing(self.path, "insert-rollback", 0,
                                       cells["on"], cells["off"])
        self.path.chmod(0o600)
        link = self.path.with_name("server-link.log")
        link.symlink_to(self.path)
        with self.assertRaisesRegex(ValueError, "TRACE_BOUND"):
            parser.parse_server_timing(link, "insert-rollback", 0,
                                       cells["on"], cells["off"])


if __name__ == "__main__":
    unittest.main()
