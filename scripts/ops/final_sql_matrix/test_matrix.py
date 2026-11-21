import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from scripts.ops import final_sql_matrix as matrix

WORKER = b"reviewed worker bytes"
HARNESS = b"reviewed isolated harness bytes"


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.pins = {s: {"source": {"commit": s}, "assets_digest": "a" * 64,
                         "resource_digest": "b" * 64,
                         "worker_sha256": hashlib.sha256(WORKER).hexdigest(),
                         "harness_sha256": hashlib.sha256(HARNESS).hexdigest()}
                     for s in matrix.SUBJECTS}

    def resource(self):
        p = {key: 0 for key in matrix.COUNTERS}
        return {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                "coverage": "stable_pid_namespace_allocation", "manifest_sha256": "b" * 64,
                "client": dict(p, pid=1, start_ticks=1),
                "server_processes": [dict(p, pid=2, start_ticks=2)]}

    def cell(self, case="aggregate", subject="candidate", trial=0):
        # Pinned v0816 raw samples contain timing/resources, not result rows.
        raw = [{"sample": i, "seconds": .001, "resources": self.resource()} for i in range(21)]
        data = b"".join(json.dumps(row).encode() + b"\n" for row in raw)
        engine = "postgresql" if subject == "postgresql" else "alopex-server"
        return {"case": case, "subject": subject, "trial": trial, "engine": engine,
                "status": "passed", "cleanup": "deleted", "source": self.pins[subject]["source"],
                "assets_digest": "a" * 64, "worker_bytes": WORKER, "harness_bytes": HARNESS,
                "raw_bytes": data, "trial_median_seconds": .001,
                "measurement": {"case": case, "engine": engine, "trial": trial,
                                "subject_identity": {"source": self.pins[subject]["source"]},
                                "server_client": True, "sql": matrix.SQL[case], "warmups": 3,
                                "samples": 21, "fixture_rows": 1 if case == "insert-rollback" else 34250,
                                "result_sha256": matrix.digest(matrix.expected(case)),
                                "raw_sha256": hashlib.sha256(data).hexdigest(), "latency_p50_ms": 1.0}}

    def change_raw(self, cell, edit):
        rows = [json.loads(line) for line in cell["raw_bytes"].splitlines()]
        edit(rows)
        cell["raw_bytes"] = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
        cell["measurement"]["raw_sha256"] = hashlib.sha256(cell["raw_bytes"]).hexdigest()

    def test_plan_exact_coordinates_and_order(self):
        rows = matrix.plan()
        self.assertEqual(len(rows), 48)
        self.assertEqual(len({(r["case"], r["trial"], r["subject"]) for r in rows}), 48)
        for case in matrix.CASES:
            for trial in matrix.TRIALS:
                subjects = tuple(r["subject"] for r in rows if r["case"] == case and r["trial"] == trial)
                self.assertEqual(subjects, matrix.SUBJECTS if trial % 2 == 0 else matrix.SUBJECTS[::-1])

    def test_real_raw_shape_and_negative_hash_coverage_cases(self):
        original = self.cell()
        self.assertEqual(matrix.audit_cell(original, self.pins["candidate"]), .001)
        edits = [lambda c: self.change_raw(c, lambda r: r.pop()),
                 lambda c: self.change_raw(c, lambda r: r[1].update(sample=0)),
                 lambda c: self.change_raw(c, lambda r: r[0]["resources"]["client"].pop("read_bytes")),
                 lambda c: self.change_raw(c, lambda r: r[0]["resources"]["server_processes"][0].update(pid=1)),
                 lambda c: c["measurement"].update(sql="SELECT 1"),
                 lambda c: c["measurement"].update(raw_sha256="c" * 64),
                 lambda c: c["measurement"].update(latency_p50_ms=2),
                 lambda c: c["measurement"].update(result_sha256="c" * 64),
                 lambda c: c.update(worker_bytes=b"changed"),
                 lambda c: c.update(harness_bytes=b"changed"),
                 lambda c: c.update(cleanup="collected"),
                 lambda c: c.update(assets_digest="c" * 64),
                 lambda c: c.update(source={"commit": "wrong"})]
        for edit in edits:
            cell = copy.deepcopy(original)
            edit(cell)
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                matrix.audit_cell(cell, self.pins["candidate"])

    def test_historical_checksum_select_all_and_rollback(self):
        for case in ("select-all", "insert-rollback"):
            cell = self.cell(case)
            self.assertEqual(matrix.audit_cell(cell, self.pins["candidate"]), .001)
            wanted = hashlib.sha256(json.dumps(matrix.expected(case), sort_keys=True).encode()).hexdigest()
            self.assertEqual(cell["measurement"]["result_sha256"], wanted)

    def test_matrix_coverage_and_1008_count(self):
        cells = [{"case": r["case"], "trial": r["trial"], "subject": r["subject"],
                  "measurement": {"result_sha256": matrix.digest(matrix.expected(r["case"]))}}
                 for r in matrix.plan()]
        with patch.object(matrix, "audit_cell", return_value=.001) as audit:
            result = matrix.audit_matrix(cells, self.pins)
            self.assertEqual(audit.call_count, 48)
            self.assertEqual(result["verified_samples"], 1008)
            self.assertFalse(result["performance_acceptance"])
            for bad in (cells[:-1], cells[:-1] + [cells[0]], cells + [cells[0]]):
                with self.assertRaises(ValueError):
                    matrix.audit_matrix(bad, self.pins)
            with self.assertRaises(ValueError):
                matrix.audit_matrix(cells, {"candidate": self.pins["candidate"]})
            dynamic = {key: "c" * 64 for key in
                       ((row["case"], row["trial"], row["subject"]) for row in matrix.plan())}
            matrix.audit_matrix(cells, self.pins, resource_digests=dynamic)
            self.assertEqual(audit.call_args.args[1]["resource_digest"], "c" * 64)
            dynamic.pop(next(iter(dynamic)))
            with self.assertRaisesRegex(ValueError, "RESOURCE_DIGEST_COVERAGE"):
                matrix.audit_matrix(cells, self.pins, resource_digests=dynamic)

    def test_threshold_noise_and_paired_trials(self):
        self.assertTrue(matrix.compare([100] * 3, [95] * 3)["improvement_proven"])
        self.assertEqual(matrix.compare([100] * 3, [105.001] * 3)["status"], "regression")
        self.assertEqual(matrix.compare([97, 100, 103], [90] * 3)["status"], "pending_noise")
        self.assertFalse(matrix.compare([100, 100, 80], [90] * 3)["improvement_proven"])


if __name__ == "__main__":
    unittest.main()
