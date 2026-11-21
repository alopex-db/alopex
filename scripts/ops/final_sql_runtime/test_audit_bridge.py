import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.ops import final_sql_matrix as independent
from scripts.ops.final_sql_runtime import audit_bridge, matrix


WORKER = b"reviewed worker bytes"
HARNESS = b"reviewed archived SQL harness bytes"


class AuditBridgeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.pins = {subject: {
            "source": {"commit": subject}, "assets_digest": "a" * 64,
            "resource_digest": "b" * 64,
            "worker_sha256": hashlib.sha256(WORKER).hexdigest(),
            "harness_sha256": hashlib.sha256(HARNESS).hexdigest(),
        } for subject in independent.SUBJECTS}
        self.collections, self.receipts, operated = [], [], []
        for index, row in enumerate(matrix.plan()):
            case, subject, trial = row["case"], row["subject"], row["trial"]
            resource = {"schema": "alopex.sql-operation-resources/v1",
                        "status": "observed", "coverage": "stable_pid_namespace_allocation",
                        "manifest_sha256": "b" * 64,
                        "client": self.process(1), "server_processes": [self.process(2)]}
            raw = b"".join(json.dumps({"sample": n, "seconds": .001,
                                       "resources": resource}).encode() + b"\n"
                           for n in range(21))
            measurement = {"case": case, "trial": trial, "engine": row["engine"],
                           "server_client": True,
                           "subject_identity": {"source": self.pins[subject]["source"]},
                           "sql": independent.SQL[case], "warmups": 3,
                           "samples": 21,
                           "fixture_rows": 1 if case == "insert-rollback" else 34250,
                           "result_sha256": independent.digest(independent.expected(case)),
                           "raw_sha256": hashlib.sha256(raw).hexdigest(),
                           "latency_p50_ms": 1.0}
            contents = {"measurement": json.dumps(measurement).encode(), "raw": raw,
                        "worker": WORKER, "harness": HARNESS}
            paths = {}
            for name, data in contents.items():
                path = root / f"{index}-{name}"
                path.write_bytes(data)
                paths[name] = path
            coordinate = {key: row[key] for key in ("case", "trial", "subject")}
            self.collections.append({**coordinate, "files": paths})
            self.receipts.append({**coordinate, "cleanup": "deleted",
                                  "source": self.pins[subject]["source"],
                                  "assets_digest": self.pins[subject]["assets_digest"],
                                  "sha256": {name: hashlib.sha256(data).hexdigest()
                                             for name, data in contents.items()},
                                  "authentic": True})
            operated.append({**row, "status": "passed",
                             "result_checksum": measurement["result_sha256"],
                             "trial_median_seconds": .001})
        self.operation = {"evidence": {"preflights": [
            {"subject": subject, "result_checksum":
             independent.digest(independent.expected("select-all"))}
            for subject in independent.SUBJECTS],
            "cells": operated, "performance_acceptance": False},
            "summary": matrix.summarize(operated)}
        self.preflight_collections, self.preflight_receipts = [], []
        full_result = json.dumps({"rows": 34250,
                                  "result": independent.expected("select-all")}).encode()
        for subject in independent.SUBJECTS:
            path = root / f"preflight-{subject}.json"
            path.write_bytes(full_result)
            self.preflight_collections.append({"subject": subject, "result": path})
            self.preflight_receipts.append({"subject": subject,
                                            "source": self.pins[subject]["source"],
                                            "assets_digest": self.pins[subject]["assets_digest"],
                                            "cleanup": "deleted", "authentic": True,
                                            "result_sha256":
                                            hashlib.sha256(full_result).hexdigest()})

    @staticmethod
    def process(pid):
        return {"pid": pid, "start_ticks": 1,
                **{key: 0 for key in independent.COUNTERS}}

    @staticmethod
    def authenticate(receipt):
        return receipt.get("authentic") is True

    def audit(self):
        return audit_bridge.audit_completed(self.operation, self.collections,
                                            self.receipts, self.pins,
                                            self.authenticate,
                                            preflight_collections=self.preflight_collections,
                                            preflight_receipts=self.preflight_receipts)

    def test_exact_matrix_uses_independent_audit(self):
        result = self.audit()
        self.assertEqual((result["verified_cells"], result["verified_samples"]), (48, 1008))
        self.assertIs(result["performance_acceptance"], False)

    def test_missing_substituted_stale_and_duplicate_records_fail(self):
        for mutation in (
            lambda: self.collections.pop(),
            lambda: self.collections[0].update(subject="before"),
            lambda: self.receipts[0].update(authentic=False),
            lambda: self.receipts[0].update(cleanup="collected"),
            lambda: self.receipts[0]["sha256"].update(raw="0" * 64),
            lambda: self.collections[1]["files"].update(raw=self.collections[0]["files"]["raw"]),
            lambda: self.operation["evidence"]["cells"].__setitem__(1,
                copy.deepcopy(self.operation["evidence"]["cells"][0])),
        ):
            with self.subTest(mutation=mutation):
                self.setUp()
                mutation()
                with self.assertRaises(ValueError):
                    self.audit()

    def test_collection_bytes_must_match_operation_summary(self):
        path = self.collections[0]["files"]["measurement"]
        value = json.loads(path.read_bytes())
        value["latency_p50_ms"] = 2.0
        data = json.dumps(value).encode()
        path.write_bytes(data)
        self.receipts[0]["sha256"]["measurement"] = hashlib.sha256(data).hexdigest()
        with self.assertRaisesRegex(ValueError, "OPERATION_COLLECTION_BINDING"):
            self.audit()

    def test_missing_duplicate_substituted_and_stale_preflights_fail(self):
        mutations = (
            lambda: self.preflight_collections.pop(),
            lambda: self.preflight_collections[1].update(
                result=self.preflight_collections[0]["result"]),
            lambda: self.preflight_receipts[1].update(subject="published"),
            lambda: self.preflight_receipts[0].update(authentic=False),
            lambda: self.preflight_receipts[0].update(cleanup="collected"),
            lambda: self.preflight_receipts[0].update(result_sha256="0" * 64),
            lambda: self.preflight_receipts[0].update(source={"commit": "other"}),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.setUp()
                mutation()
                with self.assertRaises(ValueError):
                    self.audit()

    def test_preflight_full_order_is_checked_even_with_matching_file_digest(self):
        path = self.preflight_collections[0]["result"]
        exact = json.loads(path.read_bytes())
        exact["result"][0], exact["result"][1] = exact["result"][1], exact["result"][0]
        data = json.dumps(exact).encode()
        path.write_bytes(data)
        self.preflight_receipts[0]["result_sha256"] = hashlib.sha256(data).hexdigest()
        with self.assertRaises(ValueError):
            self.audit()


if __name__ == "__main__":
    unittest.main()
