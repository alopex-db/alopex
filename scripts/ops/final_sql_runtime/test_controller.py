"""Offline controller boundary tests: no Docker, transfer, or dispatch."""
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.ops.final_sql_runtime import controller, matrix


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"}
        self.subjects = {}
        self.manifests = {}
        self.audit_pins = {}
        for subject in matrix.SUBJECTS[:-1]:
            path = self.root / (subject + ".json")
            path.write_text("{}")
            assets_digest = hashlib.sha256(path.read_bytes()).hexdigest()
            files = [{"role": role, "path": role, "size": 1, "sha256": "a" * 64}
                     for role in ("server", "wheel", "receipt")]
            self.manifests[subject] = path
            self.subjects[subject] = {"subject": subject, "product_subject": subject,
                                      "source": source, "assets_digest": assets_digest,
                                      "platform": "linux", "files": files}
            self.audit_pins[subject] = {"source": source, "assets_digest": assets_digest,
                                        "worker_sha256": "e" * 64,
                                        "harness_sha256": "f" * 64}
        self.subjects["postgresql"] = dict(
            self.subjects["published"], subject="postgresql",
            product_subject="published", reference_image_digest="0" * 64,
            published_client_pin=self.subjects["published"])
        self.manifests["postgresql"] = self.manifests["published"]
        self.audit_pins["postgresql"] = self.audit_pins["published"]
        operator = self.root / "operator.py"
        operator.write_text("pass\n")
        self.operators = {name: {"path": operator,
                                 "sha256": hashlib.sha256(operator.read_bytes()).hexdigest()}
                          for name in ("measurement_controller", "measurement_worker")}
        self.events = []
        self.args = dict(
            lock_path=self.root / "lock.json", expected={
                "lock_sha256": "2" * 64, "source_commit": "a" * 40,
                "source_tree": "b" * 40, "controller_commit": "3" * 40,
                "operator_sha256": "4" * 64},
            repo=self.root, runner_temp=self.root, environment={},
            subjects=self.subjects, product_manifests=self.manifests,
            audit_pins=self.audit_pins, docs_archive=self.root / "docs.tar",
            product_root=self.root, product_files={name: {} for name in (
                "parity_v0811_measure.py", "parity_v0811_gate.py",
                "fixtures/sql-v0.8.11-curated.json")},
            harness_path=self.root / "harness.tar", operators=self.operators,
            destination_for=lambda kind, row: self.root / row["id"],
            profile_for=lambda kind, config: self.root / "profile.json",
            admit_storage=lambda seconds: self.events.append(("storage", seconds)) or True,
            capacity=lambda *args: None,
            independent_evidence=lambda result: self.events.append(("evidence", result)) or
                {"collections": [], "receipts": [], "preflight_collections": [],
                 "preflight_receipts": [], "authenticate_receipt": lambda _: True},
            declared_seconds={"preflight": 100, "cell": 100}, clock=lambda: 0)

    def patches(self, *, operation_result=None):
        result = operation_result or {"summary": {"performance_acceptance": False},
                                      "evidence": {"performance_acceptance": False}}
        return (patch.object(controller.same_job, "attest_live",
                             side_effect=lambda **_: self.events.append("attest") or {"job_id": "1"}),
                patch.object(controller.matrix, "compose_harness",
                             side_effect=lambda *_: self.events.append("compose") or {"sha256": "5" * 64}),
                patch.object(controller.container, "ContainerCallbacks",
                             return_value=Mock(execute=Mock(), collect=Mock(), cleanup=Mock())),
                patch.object(controller.operation, "run",
                             side_effect=lambda **_: self.events.append("operate") or result),
                patch.object(controller.audit_bridge, "audit_completed",
                             side_effect=lambda *args, **kwargs: self.events.append("audit") or
                                 {"performance_acceptance": False}))

    def test_orders_live_attestation_storage_execution_and_independent_audit(self):
        patches = self.patches()
        with patches[0] as attest, patches[1] as compose, patches[2], \
             patches[3] as operate, patches[4] as audit:
            result = controller.run(**self.args)
        self.assertEqual([event if isinstance(event, str) else event[0]
                          for event in self.events],
                         ["attest", "storage", "compose", "operate", "evidence", "audit"])
        self.assertEqual(self.events[1][1], 5200)
        self.assertFalse(result["performance_acceptance"])
        self.assertIs(operate.call_args.kwargs["prepare"], matrix.prepare_cell)
        self.assertIs(audit.call_args.args[3], self.audit_pins)
        attest.assert_called_once()
        compose.assert_called_once()

    def test_attestation_failure_prevents_any_sql_staging(self):
        patches = self.patches()
        with patch.object(controller.same_job, "attest_live", side_effect=ValueError("SAME_JOB")), \
             patches[1] as compose, patches[3] as operate:
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                controller.run(**self.args)
        compose.assert_not_called()
        operate.assert_not_called()

    def test_incomplete_pins_or_timing_prevent_staging(self):
        for change in ({"subjects": {key: value for key, value in self.subjects.items()
                                      if key != "candidate"}},
                       {"declared_seconds": {"preflight": 140, "cell": 140}},
                       {"audit_pins": {key: value for key, value in self.audit_pins.items()
                                       if key != "candidate"}}):
            self.events.clear()
            patches = self.patches()
            with patches[0], patches[1] as compose, patches[3] as operate:
                with self.assertRaises(ValueError):
                    controller.run(**dict(self.args, **change))
            self.assertEqual(self.events, ["attest"])
            compose.assert_not_called()
            operate.assert_not_called()

    def test_storage_denial_and_missing_receipts_fail_closed(self):
        patches = self.patches()
        with patches[0], patches[1] as compose, patches[3] as operate:
            with self.assertRaisesRegex(ValueError, "STORAGE_ADMISSION"):
                controller.run(**dict(self.args, admit_storage=lambda _: False))
        compose.assert_not_called()
        operate.assert_not_called()
        patches = self.patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4] as audit:
            with self.assertRaisesRegex(ValueError, "MISSING_AUDIT_EVIDENCE"):
                controller.run(**dict(self.args, independent_evidence=lambda _: None))
        audit.assert_not_called()

    def test_preflight_and_cell_cannot_reuse_staging_destination(self):
        row = next(item for item in matrix.plan() if item["case"] == "select-all")
        def inspect(**kwargs):
            destination = kwargs["destination_for"]
            self.assertEqual(destination("preflight", row), self.root / row["id"])
            with self.assertRaisesRegex(ValueError, "STAGING_DESTINATION"):
                destination("cell", row)
            return {"summary": {"performance_acceptance": False},
                    "evidence": {"performance_acceptance": False}}
        patches = self.patches()
        with patches[0], patches[1], patches[2], \
             patch.object(controller.operation, "run", side_effect=inspect), patches[4]:
            controller.run(**self.args)


if __name__ == "__main__":
    unittest.main()
