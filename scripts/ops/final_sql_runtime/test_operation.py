import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import matrix, operation, same_job, worker


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.now = 0
        self.declared = {"preflight": 100, "cell": 100}

    def callbacks(self):
        def prepare(manifest, destination, harness, record, row, operators, pin):
            self.events.append(("prepare", destination))
            return destination, dict(row)

        def execute(staged, config, *, kind, worker_timeout_seconds, run_timeout_seconds):
            self.events.append(("execute", kind, config["id"], worker_timeout_seconds,
                                run_timeout_seconds))
            self.now += 1
            return config

        def collect(config, kind, row):
            self.events.append(("collect", kind, row["id"]))
            if kind == "preflight":
                return {"rows": 34250, "result": worker.expected("select-all")}
            return {"cell_result": {"case": row["case"], "engine": row["engine"],
                    "server_client": True, "warmups": 3, "samples": 21,
                    "fixture_rows": worker.ISOLATED[row["case"]],
                    "result_sha256": worker.result_digest(row["case"]),
                    "sql": {"aggregate": "SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
                            "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
                            "select-all": "SELECT id, value FROM bench WHERE enabled = true",
                            "insert-rollback": "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"}[row["case"]],
                    "latency_p50_ms": 1.0},
                    "raw": [{"sample": index, "seconds": .001,
                             "resources": {"schema": "alopex.sql-operation-resources/v1",
                                           "status": "observed",
                                           "coverage": "stable_pid_namespace_allocation",
                                           "manifest_sha256": "a" * 64,
                                           "client": self.process(1),
                                           "server_processes": [self.process(2)]}}
                            for index in range(21)],
                    "resource_digest": "a" * 64}

        def cleanup(staged, kind, row):
            self.events.append(("cleanup", kind, row["id"]))

        def capacity(kind, row, future):
            self.events.append(("capacity", kind, row["id"]))

        return dict(prepare=prepare, execute=execute, collect=collect,
                    cleanup=cleanup, capacity=capacity)

    @staticmethod
    def process(pid):
        return {"pid": pid, "start_ticks": 1,
                **{field: 0 for field in worker.COUNTERS}}

    def args(self, **overrides):
        callbacks = self.callbacks()
        args = dict(qualification={"qualification_cases": sorted(same_job.qualification.CASES),
                                   "job_id": "12345678", "run_attempt": "1"},
                    subjects={subject: {"subject": subject} for subject in matrix.SUBJECTS},
                    product_manifests={subject: subject for subject in matrix.SUBJECTS},
                    destination_for=lambda kind, row: (kind, row["id"]),
                    harness="harness", harness_record={}, operators={},
                    clock=lambda: self.now, declared_seconds=self.declared,
                    **callbacks)
        args.update(overrides)
        return args

    def test_four_preflights_then_all_48_cells(self):
        result = operation.run(**self.args())
        self.assertEqual([item["subject"] for item in result["evidence"]["preflights"]],
                         list(matrix.SUBJECTS))
        self.assertEqual(len(result["evidence"]["cells"]), 48)
        self.assertFalse(result["summary"]["performance_acceptance"])
        self.assertEqual(sum(event[0] == "cleanup" for event in self.events), 52)
        self.assertEqual(sum(event[0] == "execute" for event in self.events), 52)

    def test_duplicate_subject_inventory_rejected_before_execution(self):
        subjects = {subject: {} for subject in matrix.SUBJECTS[:-1]}
        with self.assertRaises(operation.OperationFailure):
            operation.run(**self.args(subjects=subjects))
        self.assertFalse(self.events)

    def test_incomplete_qualification_rejected_before_execution(self):
        qualification = {"qualification_cases": ["E01"],
                         "job_id": "12345678", "run_attempt": "1"}
        with self.assertRaises(operation.OperationFailure):
            operation.run(**self.args(qualification=qualification))
        self.assertFalse(self.events)

    def test_duplicate_preflight_subject_rejected_without_summary(self):
        original = matrix.SUBJECTS
        with patch.object(matrix, "SUBJECTS", (original[0], original[0], *original[2:])), \
             patch.object(matrix, "summarize") as summarize, \
             self.assertRaises(operation.OperationFailure):
            operation.run(**self.args())
        summarize.assert_not_called()

    def test_incomplete_matrix_preserves_evidence_and_skips_summary(self):
        callbacks = self.callbacks()
        collect = callbacks["collect"]
        count = 0
        def fail_on_last(execution, kind, row):
            nonlocal count
            count += 1
            if count == 52:
                raise RuntimeError("lost result")
            return collect(execution, kind, row)
        with patch.object(matrix, "summarize") as summarize:
            with self.assertRaises(operation.OperationFailure) as caught:
                operation.run(**self.args(collect=fail_on_last))
        self.assertEqual(len(caught.exception.evidence["cells"]), 47)
        self.assertFalse(caught.exception.evidence["performance_acceptance"])
        summarize.assert_not_called()

    def test_cleanup_failure_stops_after_first_preflight(self):
        def fail(*_):
            raise RuntimeError("owned cleanup failed")
        with patch.object(matrix, "summarize") as summarize:
            with self.assertRaises(operation.OperationFailure) as caught:
                operation.run(**self.args(cleanup=fail))
        self.assertEqual(len(caught.exception.evidence["preflights"]), 1)
        self.assertEqual(sum(event[0] == "execute" for event in self.events), 1)
        summarize.assert_not_called()

    def test_time_and_capacity_admission_fail_closed(self):
        self.declared = {"preflight": 140, "cell": 140}
        with self.assertRaises(operation.OperationFailure):
            operation.run(**self.args())
        self.assertFalse(self.events)
        self.declared = {"preflight": 100, "cell": 100}
        def fail_capacity(*_):
            raise RuntimeError("capacity unavailable")
        with self.assertRaises(operation.OperationFailure):
            operation.run(**self.args(capacity=fail_capacity))
        self.assertFalse(any(event[0] == "execute" for event in self.events))


if __name__ == "__main__":
    unittest.main()
