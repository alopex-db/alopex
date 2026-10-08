import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

SPEC = importlib.util.spec_from_file_location(
    "dml_statement_v0816", Path(__file__).resolve().parents[1] / "dml_statement_v0816.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def samples(engine="fixed", case="ordinary_update", rows=600):
    return [dict(kind="sample", engine=engine, case=case, rows=rows,
                 phase="prepared_execute", iteration=i, warmup=i == 0,
                 elapsed_ns=100 + i, affected_rows=rows, result_check=True, status="pass")
            for i in range(8)]


def identity(engine="fixed", case="ordinary_update", rows=600):
    result = dict(kind="identity", engine=engine, case=case, rows=rows,
                  phase="prepared_execute", cpu=1, warmup=1, runs=7,
                  noise_fraction=0.05, started_unix_ns=1, source_revision="a" * 40)
    result.update({key: "a" * 64 for key in (*MODULE.COMMON_IDENTITY, *MODULE.ENGINE_IDENTITY)
                   if key.endswith("sha256")})
    return result


class EvidenceTests(unittest.TestCase):
    def test_spill_matrix_is_separate_and_requires_real_spill_and_resources(self):
        # These JSONL records exercise the renderer, not a simulated database.
        for mutation in (None, "no_spill", "dirty", "wrong_budget", "wrong_phase",
                         "missing_metrics", "mixed_filesystem", "missing_cell"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for engine in ("baseline", "fixed"):
                    for size in MODULE.SIZES:
                        chosen = (engine, size) == ("fixed", 600)
                        if chosen and mutation == "missing_cell":
                            continue
                        meta = identity(engine, "two_membership_no_write", size)
                        meta.update(phase="async_sql_execute", memory_limit_bytes=4096,
                                    spill_policy="SpillToDisk", spill_parent="/tmp/spill-fixture")
                        records = samples(engine, "two_membership_no_write", size)
                        for row in records:
                            row.update(phase="async_sql_execute", affected_rows=0,
                                       memory_limit_bytes=4096, spill_cleanup=True,
                                       spill_bytes=20000 if engine == "fixed" else 0,
                                       spill_files=1 if engine == "fixed" else 0)
                        completion = dict(kind="completion", status="pass", process_metrics=dict(
                            maxrss_kib=1000, user_seconds=0.1, system_seconds=0.1, inblock=0, outblock=10))
                        if chosen:
                            if mutation == "no_spill":
                                records[2]["spill_files"] = 0
                            elif mutation == "dirty":
                                records[2]["spill_cleanup"] = False
                            elif mutation == "wrong_budget":
                                meta["memory_limit_bytes"] = 8192
                            elif mutation == "wrong_phase":
                                meta["phase"] = "prepared_execute"
                            elif mutation == "mixed_filesystem":
                                meta["spill_parent"] = "/different-fixture"
                            elif mutation == "missing_metrics":
                                completion.pop("process_metrics")
                        raw = [meta] + records + [completion]
                        (root / f"{engine}-{size}.jsonl").write_text(
                            "".join(json.dumps(row) + "\n" for row in raw))
                args = SimpleNamespace(suite="forced-spill", raw_directory=root, output=root / "report.json")
                if mutation:
                    with self.assertRaises(SystemExit):
                        MODULE.render(args)
                else:
                    MODULE.render(args)
                report = json.loads(args.output.read_text())
                self.assertEqual(len(report["comparisons"]), 2)
                self.assertEqual(report["reference_contract"], "sqlite_4096B_capability_config_gap")
                self.assertEqual(all(row["status"] == "pass" for row in report["comparisons"]), mutation is None)

    def test_warmup_is_excluded_from_median(self):
        rows = samples()
        rows[0]["elapsed_ns"] = 999999
        self.assertEqual(MODULE.validate(rows, "fixed", "ordinary_update", 600), 104)

    def test_incomplete_failed_or_wrong_identity_cannot_pass(self):
        for key, value in [("result_check", False), ("engine", "sqlite"),
                           ("iteration", 0), ("elapsed_ns", -1), ("affected_rows", 599)]:
            rows = samples()
            rows[3][key] = value
            with self.assertRaises(ValueError):
                MODULE.validate(rows, "fixed", "ordinary_update", 600)
        with self.assertRaises(ValueError):
            MODULE.validate(samples()[:-1], "fixed", "ordinary_update", 600)

    def test_missing_comparisons_fail_without_discarding_available_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "fixed.jsonl"
            rows = [identity()]
            rows += samples() + [dict(kind="completion", status="pass")]
            raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
            before = raw.read_bytes()
            output = root / "summary.json"
            with self.assertRaises(SystemExit):
                MODULE.render(SimpleNamespace(raw_directory=root, output=output))
            summary = json.loads(output.read_text())
            self.assertEqual(len(summary["comparisons"]), 10)
            self.assertTrue(all(row["status"] == "fail" for row in summary["comparisons"]))
            self.assertEqual(raw.read_bytes(), before)

    def test_complete_matrix_passes_but_mixed_identity_fails(self):
        for field, replacement in [(None, None), ("cpu", 2), ("binary_sha256", "b" * 64),
                                   ("source_sha256", "b" * 64), ("manifest_sha256", "b" * 64),
                                   ("runs", 8), ("engine", "unknown"), ("parser_sha256", None)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for engine in MODULE.ENGINES:
                    for case in MODULE.CASES:
                        for size in MODULE.SIZES:
                            record = identity(engine, case, size)
                            if field and (engine, case, size) == ("fixed", "ordinary_update", 600):
                                record[field] = replacement
                            rows = [record] + samples(engine, case, size) + [dict(kind="completion", status="pass")]
                            (root / f"{engine}-{case}-{size}.jsonl").write_text(
                                "".join(json.dumps(row) + "\n" for row in rows))
                args = SimpleNamespace(raw_directory=root, output=root / "summary.json")
                if field:
                    with self.assertRaises(SystemExit):
                        MODULE.render(args)
                else:
                    MODULE.render(args)
                report = json.loads(args.output.read_text())
                self.assertEqual(bool(report["identity_errors"]), bool(field))
                self.assertTrue(all(row["status"] == ("fail" if field else "pass")
                                    for row in report["comparisons"]))


if __name__ == "__main__":
    unittest.main()
