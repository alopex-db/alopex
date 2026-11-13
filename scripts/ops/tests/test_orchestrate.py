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

from orchestrate import (DiagnosticFailure, FILES, MAX_CLIENT, ORACLE, SOURCE_COMMIT, SOURCE_TREE, QUALIFICATION_CASES, orchestrate,
                         plan, sha256, validate_dispatch, write_evidence)
from test_diagnostic import fixture
from diagnostic import CLIENT_RECORDS_PER_CELL
from common import Failure


HERE = Path(__file__).resolve().parents[1]
OPERATORS = ("diagnostic_worker.py", "diagnostic_measure.py", "diagnostic.py",
             "diagnostic_supervisor.py", "same_job_1951.py",
             "attestor_1951.py", "live_wrapper.py", "audit.py",
             "orchestrate.py", "evidence.py")


def identities():
    pin = {"source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
           "artifact_id": 1, "artifact_sha256": "a" * 64, "assets_digest": "b" * 64,
           "server_sha256": "c" * 64, "wheel_sha256": "d" * 64,
           "controller_commit": "e" * 40,
           "operator_sha256": {name: sha256(HERE / name) for name in OPERATORS}}
    receipt = {key: pin[key] for key in ("source_commit", "source_tree", "artifact_id",
                                           "artifact_sha256", "assets_digest", "server_sha256",
                                           "wheel_sha256", "controller_commit")}
    receipt.update(job_id="same-job-1", qualification_status="passed",
                   acceptance_cases=list(QUALIFICATION_CASES),
                   image_digest="f" * 64, binding_sha256="1" * 64,
                   security_profile_digest="2" * 64)
    return pin, receipt


def inputs():
    cells, clients, servers = fixture()
    for row in cells:
        row["result_sha256"] = ORACLE[row["case"]]
    for row in clients:
        row["result_sha256"] = ORACLE[row["case"]]
    grouped = {}
    for cell in plan():
        key = (cell["case"], cell["trial"], cell["mode"])
        client = [row for row in clients if (row["case"], row["trial"], row["mode"]) == key]
        ids = {row["trace_id"] for row in client if "trace_id" in row}
        grouped[key] = (next(row for row in cells if (row["case"], row["trial"], row["mode"]) == key),
                        client, [row for row in servers if row["trace_id"] in ids])
    return grouped


class OrchestratorTests(unittest.TestCase):
    def test_fixed_failure_identity_preserves_known_common_code(self):
        from orchestrate import _error_identity
        self.assertEqual(_error_identity(Failure("MEASUREMENT", "private detail")),
                         {"type": "Failure", "code": "MEASUREMENT"})
        self.assertEqual(_error_identity(RuntimeError("private detail")),
                         {"type": "RuntimeError", "code": "INTERNAL"})

    def test_client_bound_covers_frozen_34250_row_setup(self):
        setup = 2 + (34_250 + 99) // 100
        self.assertEqual(setup, 345)
        self.assertEqual(CLIENT_RECORDS_PER_CELL, setup + 1 + 3 + 21)
        self.assertEqual(MAX_CLIENT, 12 * CLIENT_RECORDS_PER_CELL)

    def test_full_trace_residual_inventory_fits_bounded_evidence(self):
        # Six traced cells each record the frozen setup plus measured calls.
        residuals = [{"trace_id": f"00000000-0000-4000-8000-{index:012x}",
                      "unattributed_ns": 2**63 - 1,
                      "combined_transport_residual_ns": 2**63 - 1}
                     for index in range(6 * CLIENT_RECORDS_PER_CELL)]
        with tempfile.TemporaryDirectory() as directory:
            hashes = write_evidence(directory, {}, [{} for _ in range(12)], [], [],
                                    {"timer_residuals": residuals, "paired": {}}, True)
            size = hashes["join-audit.json"]["size"]
            self.assertGreater(size, 131_072)
            self.assertLessEqual(size, FILES["join-audit.json"])

    def run_fake(self, *, mutate=None, cleanup=True, sealed=True,
                 qualification=True, cell_failure=False, job_limit_seconds=6_000,
                 clock=lambda: 0, capture_failure=False, capacity_failure=False):
        pin, receipt = identities()
        groups = inputs()
        if mutate:
            mutate(pin, receipt, groups)
        observed = []
        self.cleanup_calls = 0
        def qualify(supplied, job):
            if not qualification:
                raise ValueError("QUALIFICATION")
            return receipt
        def run_cell(cell, binding):
            observed.append(cell["cell_id"])
            if cell_failure:
                raise ValueError("CELL")
            return groups[(cell["case"], cell["trial"], cell["mode"])]
        def used_bytes():
            if capacity_failure:
                raise ValueError("CAPACITY")
            return 10_000_000_000
        def clean_owned():
            self.cleanup_calls += 1
            return cleanup
        hooks = {
            "qualify": qualify,
            "used_bytes": used_bytes,
            "run_cell": run_cell,
            "cleanup": clean_owned,
            "seal": lambda root, hashes: ({"sha256": "3" * 64, "size": 100,
                                           "verified": sealed} if set(hashes) == set(FILES) and
                                            {p.name for p in root.iterdir()} == set(FILES) else {}),
        }
        with tempfile.TemporaryDirectory() as directory:
            try:
                result = orchestrate(pin, job_id="same-job-1", operator_dir=HERE,
                                     evidence_dir=directory, hooks=hooks,
                                     job_limit_seconds=job_limit_seconds, clock=clock)
            except DiagnosticFailure as error:
                if capture_failure:
                    proof = json.loads((Path(directory) / "failure.private.json").read_text())
                    return error, proof, observed
                raise
            self.assertEqual({p.name for p in Path(directory).iterdir()}, set(FILES))
        return result, observed

    def test_twelve_cells_and_encrypted_allowlist(self):
        result, observed = self.run_fake()
        self.assertEqual(observed, [cell["cell_id"] for cell in plan()])
        self.assertEqual((result["measurement_cells"], result["measured_uuids"],
                          result["measured_server_events"]), (12, 126, 882))
        self.assertFalse(result["performance_qualification"])

    def test_missing_assets_or_wrong_job_block_dispatch(self):
        pin, receipt = identities()
        pin["assets_digest"] = None
        with self.assertRaises(ValueError):
            validate_dispatch(pin, receipt, job_id="same-job-1", operator_dir=HERE)
        pin, receipt = identities()
        receipt["job_id"] = "prior-job"
        with self.assertRaisesRegex(ValueError, "QUALIFICATION"):
            validate_dispatch(pin, receipt, job_id="same-job-1", operator_dir=HERE)

    def test_missing_trace_or_changed_oracle_stops(self):
        for mutate in (
            lambda p, r, groups: groups[("aggregate", 0, "on")][2].pop(),
            lambda p, r, groups: groups[("ordered", 2, "off")][0].update(result_sha256="0" * 64),
        ):
            with self.subTest(mutate=mutate):
                with self.assertRaises(ValueError):
                    self.run_fake(mutate=mutate)

    def test_cleanup_and_encryption_fail_closed(self):
        for kwargs in ({"cleanup": False}, {"sealed": False}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.run_fake(**kwargs)

    def test_qualification_failure_still_cleans_and_preserves_failure(self):
        error, proof, observed = self.run_fake(qualification=False, capture_failure=True)
        self.assertIsInstance(error.primary_error, ValueError)
        self.assertIsNone(error.cleanup_error)
        self.assertEqual(proof["primary"], {"type": "ValueError", "code": "QUALIFICATION"})
        self.assertEqual(observed, [])
        self.assertEqual(self.cleanup_calls, 1)

    def test_initial_capacity_failure_still_cleans(self):
        error, proof, observed = self.run_fake(capacity_failure=True, capture_failure=True)
        self.assertEqual(proof["primary"]["code"], "CAPACITY")
        self.assertEqual(observed, [])
        self.assertEqual(self.cleanup_calls, 1)

    def test_cell_and_cleanup_errors_remain_distinct(self):
        error, proof, observed = self.run_fake(cell_failure=True, cleanup=False, capture_failure=True)
        self.assertEqual(str(error.primary_error), "CELL")
        self.assertEqual(str(error.cleanup_error), "CLEANUP")
        self.assertEqual(proof["primary"]["code"], "CELL")
        self.assertEqual(proof["cleanup"]["code"], "CLEANUP")
        self.assertEqual(len(observed), 1)
        self.assertEqual(self.cleanup_calls, 1)

    def test_job_safety_limit_is_configurable_without_admission_rule(self):
        result, observed = self.run_fake(job_limit_seconds=5_000)
        self.assertEqual(len(observed), 12)
        times = iter((0, 2))
        error, proof, observed = self.run_fake(job_limit_seconds=1,
                                                clock=lambda: next(times), capture_failure=True)
        self.assertEqual(proof["primary"]["code"], "JOB_SAFETY_LIMIT")
        self.assertEqual(observed, [])


if __name__ == "__main__":
    unittest.main()
