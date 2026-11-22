"""Pure public #681 pair math and upload-boundary checks; no workload run."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public as case
import issue681_public_pair as pair

PINS = {"before": {"source_commit": "a" * 40, "source_tree": "1" * 40,
                   "binary_sha256": "b" * 64, "assets_digest": "2" * 64,
                   "example_sha256": "e" * 64, "parser_sha256": "f" * 64,
                   "parser_record_sha256": "5" * 64,
                   "cargo_lock_sha256": "6" * 64, "rust_version": "1.96.0",
                   "version": "0.8.17",
                   "profile": "release", "platform": "linux/amd64",
                   "parser_contract": "0.28.0"},
        "candidate": {"source_commit": "c" * 40, "source_tree": "3" * 40,
                      "binary_sha256": "d" * 64, "assets_digest": "4" * 64,
                      "example_sha256": "e" * 64, "parser_sha256": "f" * 64,
                      "parser_record_sha256": "5" * 64,
                      "cargo_lock_sha256": "7" * 64, "rust_version": "1.96.0",
                      "version": "0.8.17",
                      "profile": "release", "platform": "linux/amd64",
                      "parser_contract": "0.28.0"}}


def fixture(finish="rollback"):
    reports = []
    for rows in pair.SCALES:
        for trial in range(5):
            for arm in (pair.ARMS if trial % 2 == 0 else reversed(pair.ARMS)):
                phases = []
                for phase in (*case.PHASES, finish):
                    timed = phase != "warm"
                    base = 1000 + trial * 2 + (200 if arm == "candidate" else 0)
                    stats = phase in ("cold_transaction_1", "cold_transaction_2",
                                      "warm", "first_insert", "second_insert")
                    phases.append({"phase": phase, "elapsed_ns": base if timed else None,
                                   "kernel_peak_rss_bytes": 2000 + trial +
                                       (100 if arm == "candidate" else 0),
                                   "cache_hits": 2 if stats else None,
                                   "cache_misses": 1 if stats else None,
                                   "graph_bytes": 12345 if phase == "warm" else None})
                reports.append({"schema": case.SCHEMA, "status": "measured",
                                "source_commit": PINS[arm]["source_commit"],
                                "binary_sha256": PINS[arm]["binary_sha256"],
                                "rows": rows, "dimensions": 384,
                                "trial": trial, "finish": finish,
                                "result_validated": True, "phases": phases,
                                "rss_scope": pair.RSS_SCOPE})
    return reports


class PublicPairTests(unittest.TestCase):
    def test_all_trials_recompute_with_regressions_visible(self):
        result = pair.assemble(fixture(), PINS, "rollback")
        self.assertEqual(len(result["cases"]), 30)
        self.assertEqual(len(result["summary"]), 51)
        row = next(row for row in result["summary"]
                   if row["rows"] == 1000 and row["phase"] == "index_build" and
                   row["metric"] == "elapsed_ns")
        self.assertEqual(row["candidate_minus_before"], 200)
        self.assertEqual(row["three_mad_bound"], 6)
        self.assertTrue(row["increase_beyond_bound"])
        self.assertFalse(result["performance_acceptance"])

    def test_commit_mode_preserves_completion_timing(self):
        result = pair.assemble(fixture("commit"), PINS, "commit")
        self.assertEqual(result["finish"], "commit")
        self.assertTrue(any(row["phase"] == "commit" and row["metric"] == "elapsed_ns"
                            for row in result["summary"]))

    def test_mismatched_example_is_rejected(self):
        pins = copy.deepcopy(PINS)
        pins["candidate"]["example_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
            pair.assemble(fixture(), pins, "rollback")

    def test_reordered_or_extra_fields_are_rejected(self):
        reports = fixture()
        reports[0], reports[1] = reports[1], reports[0]
        with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
            pair.assemble(reports, PINS, "rollback")
        reports = fixture()
        reports[0]["local_path"] = "/private/host"
        with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
            pair.assemble(reports, PINS, "rollback")

    def test_one_canonical_file_and_independent_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp) / "export"
            export.mkdir(mode=0o700)
            report = pair.write(fixture(), PINS, export, finish="rollback",
                                independent_audit=lambda _: True)
            self.assertEqual(list(export.iterdir()), [export / "public.json"])
            self.assertEqual((export / "public.json").read_bytes(), case._canonical(report))
            self.assertEqual(pair.verify(export, pins=PINS,
                                         independent_audit=lambda _: True), report)
            with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
                pair.verify(export, pins=PINS, independent_audit=lambda _: False)
            def leaking_audit(_):
                raise RuntimeError("/private/host/audit")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PAIR$"):
                pair.verify(export, pins=PINS, independent_audit=leaking_audit)
            (export / "private.log").write_text("/private/host")
            with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
                pair.verify(export, pins=PINS, independent_audit=lambda _: True)

    def test_tampered_summary_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp) / "export"
            export.mkdir(mode=0o700)
            report = pair.write(fixture(), PINS, export, finish="rollback",
                                independent_audit=lambda _: True)
            changed = copy.deepcopy(report)
            changed["summary"][0]["candidate_minus_before"] = 0
            (export / "public.json").write_bytes(case._canonical(changed))
            with self.assertRaisesRegex(ValueError, "PUBLIC_PAIR"):
                pair.verify(export, pins=PINS, independent_audit=lambda _: True)


if __name__ == "__main__":
    unittest.main()
