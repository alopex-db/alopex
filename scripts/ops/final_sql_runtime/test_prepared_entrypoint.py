"""Prepared public entrypoint keeps admission and export in one process."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from scripts.ops.final_sql_runtime import prepared_entrypoint as entry


class PreparedEntrypointTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / "public").mkdir(mode=0o700)
        worker = self.root / "worker.py"
        worker.write_bytes(b"fixed public worker")
        operator = self.root / "controller.py"
        operator.write_bytes(b"fixed controller")
        self.arguments = dict(
            qualification_args={"lock_path": self.root / "lock.json",
                                "expected": {}, "repo": self.root,
                                "runner_temp": self.root,
                                "environment": {}},
            manifests={"before": self.root / "before.json",
                       "published": self.root / "published.json",
                       "candidate": self.root / "candidate.json"},
            reviewed_pins={"before": {}, "published": {}, "candidate": {}},
            selected_image_digest="a" * 64, controller_digest="b" * 64,
            worker_path=worker, worker_sha256=hashlib.sha256(worker.read_bytes()).hexdigest(),
            controller_path=operator,
            controller_sha256=hashlib.sha256(operator.read_bytes()).hexdigest(),
            docs_checkout=self.root / "docs", product_checkout=self.root / "product",
            docs_archive=self.root / "docs.tar", harness_path=self.root / "combined.tar",
            profile_dir=self.root / "profiles", staging_root=self.root / "staging",
            public_dir=self.root / "public", declared_seconds={"preflight": 60, "cell": 120})

    def test_admitted_inputs_join_and_each_public_boundary_reattests(self):
        events = []
        evidence = []
        class Profiles:
            profile_hashes = {"fixed-run": "d" * 64}
            def __init__(self, qualified, **kwargs):
                self_test.assertEqual(qualified, self_test.root / "profile.json")
                self_test.assertEqual(kwargs["expected_profile_sha256"], "c" * 64)
                events.append("profiles")
            def profile_for(self, *_args):
                return Path("/profile")
            def destination_for(self, *_args):
                return Path("/destination")
        class Storage:
            def __init__(self, qualified, **kwargs):
                self_test.assertEqual(qualified, self_test.root / "profile.json")
                self_test.assertEqual(kwargs["expected_backing_root"], self_test.root / "backing")
                events.append("storage")
            def admit_storage(self, *_args):
                return True
            def capacity(self, *_args):
                return 0
        def attest(**kwargs):
            events.append("attest")
            self.assertIs(kwargs["pins"], audit_pins)
            self.assertEqual(kwargs["profile_hashes"], Profiles.profile_hashes)
            value = {"fresh": len(evidence)}
            evidence.append(value)
            return value
        def controller_run(**kwargs):
            events.append("controller")
            self.assertIs(kwargs["subjects"], subjects)
            self.assertEqual(kwargs["operators"]["measurement_worker"]["sha256"],
                             self.arguments["worker_sha256"])
            self.assertEqual(kwargs["product_files"], product_files)
            self.assertEqual(kwargs["independent_evidence"]({}), {"fresh": 0})
            return {"private": True}
        def prepare(_result, _pins, _directory, callback):
            events.append("prepare")
            self.assertEqual(callback({}), {"fresh": 1})
        def verify(_directory, *, independent_evidence, **_kwargs):
            events.append("verify")
            self.assertEqual(independent_evidence({}), {"fresh": 2})
        subjects, manifests, audit_pins = {}, {}, {}
        self_test = self
        product_files = {"fixed": "hash"}
        with patch.object(entry.selected_profile, "from_live", return_value=(
            self.root / "profile.json", "c" * 64, self.root / "backing")), patch.object(
            entry.subject_binding, "assemble", side_effect=lambda **_:
                          (subjects, manifests, audit_pins)) as bind, patch.object(
            entry.docs_harness, "build", side_effect=lambda *_: events.append("docs")), patch.object(
            entry.product_harness, "verify", return_value=(self.root, product_files)), patch.object(
            entry.profile_factory, "ProfileFactory", Profiles), patch.object(
            entry.admission, "StorageAdmission", Storage), patch.object(
            entry.collection_attestor, "attest_collections", side_effect=attest), patch.object(
            entry.controller, "run", side_effect=controller_run), patch.object(
            entry.public_upload, "prepare_success", side_effect=prepare), patch.object(
            entry.public_upload, "verify", side_effect=verify):
            path = entry.run(**self.arguments)
        self.assertEqual(path, self.root / "public/public.json")
        self.assertEqual(len(evidence), 3)
        self.assertEqual(events, ["docs", "profiles", "storage", "controller", "attest",
                                  "prepare", "attest", "verify", "attest"])
        self.assertEqual(bind.call_args.kwargs["manifests"], self.arguments["manifests"])

    def test_changed_operator_fails_before_binding_with_fixed_public_line(self):
        self.arguments["worker_sha256"] = "0" * 64
        with patch.object(entry.subject_binding, "assemble") as bind:
            with self.assertRaises(entry.PreparedFailure) as caught:
                entry.run(**self.arguments)
        bind.assert_not_called()
        self.assertNotIn(b"worker.py", caught.exception.public_line)
        self.assertIn(b'"status":"failed"', caught.exception.public_line)
        self.assertFalse((self.root / "public/public.json").exists())

    def test_missing_live_profile_stops_before_subject_binding(self):
        with patch.object(entry.selected_profile, "from_live",
                          side_effect=ValueError("SELECTED_PROFILE")), patch.object(
            entry.subject_binding, "assemble") as bind:
            with self.assertRaises(entry.PreparedFailure) as caught:
                entry.run(**self.arguments)
        bind.assert_not_called()
        self.assertIn(b'"status":"failed"', caught.exception.public_line)
        self.assertFalse((self.root / "public/public.json").exists())

    def test_missing_required_input_is_fixed_failure(self):
        self.arguments["qualification_args"] = {}
        with self.assertRaises(entry.PreparedFailure) as caught:
            entry.run(**self.arguments)
        self.assertIn(b'"completed_runs":"unknown"', caught.exception.public_line)
        self.assertFalse((self.root / "public/public.json").exists())

    def test_failed_final_recheck_removes_public_file(self):
        def write_public(*_args):
            (self.root / "public/public.json").write_bytes(b"pending verification")
        with patch.object(entry.selected_profile, "from_live", return_value=(
            self.root / "profile.json", "c" * 64, self.root / "backing")), patch.object(
            entry.subject_binding, "assemble", return_value=({}, {}, {})), patch.object(
            entry.docs_harness, "build"), patch.object(
            entry.product_harness, "verify", return_value=(self.root, {})), patch.object(
            entry.profile_factory, "ProfileFactory", return_value=MagicMock()), patch.object(
            entry.admission, "StorageAdmission", return_value=MagicMock()), patch.object(
            entry.controller, "run", return_value={}), patch.object(
            entry.public_upload, "prepare_success", side_effect=write_public), patch.object(
            entry.public_upload, "verify", side_effect=ValueError("private path should stay private")):
            with self.assertRaises(entry.PreparedFailure) as caught:
                entry.run(**self.arguments)
        self.assertFalse((self.root / "public/public.json").exists())
        self.assertNotIn(b"private path", caught.exception.public_line)

    def test_writer_raises_after_creating_public_file(self):
        def write_then_fail(*_args):
            (self.root / "public/public.json").write_bytes(b"incomplete")
            raise RuntimeError("private diagnostic")
        with patch.object(entry.selected_profile, "from_live", return_value=(
            self.root / "profile.json", "c" * 64, self.root / "backing")), patch.object(
            entry.subject_binding, "assemble", return_value=({}, {}, {})), patch.object(
            entry.docs_harness, "build"), patch.object(
            entry.product_harness, "verify", return_value=(self.root, {})), patch.object(
            entry.profile_factory, "ProfileFactory", return_value=MagicMock()), patch.object(
            entry.admission, "StorageAdmission", return_value=MagicMock()), patch.object(
            entry.controller, "run", return_value={}), patch.object(
            entry.public_upload, "prepare_success", side_effect=write_then_fail):
            with self.assertRaises(entry.PreparedFailure) as caught:
                entry.run(**self.arguments)
        self.assertFalse((self.root / "public/public.json").exists())
        self.assertNotIn(b"private diagnostic", caught.exception.public_line)


if __name__ == "__main__":
    unittest.main()
