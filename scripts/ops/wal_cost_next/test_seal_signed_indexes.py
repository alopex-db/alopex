"""The fallback seal can retry after an interrupted first encryption."""
import io
import json
import tarfile
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import seal_signed_indexes as seal_module
from common import Failure


class FakePipe(io.BytesIO):
    def fileno(self):
        return 0


class FakeBudget:
    def __init__(self, storage):
        self.storage = storage

    def used(self):
        return 100


class FakeChild:
    def __init__(self, data=b""):
        self.stdout = FakePipe(data)
        self.returncode = 0

    def poll(self):
        return 0

    def wait(self, timeout):
        return 0


class SealRetryTests(unittest.TestCase):
    def make_oversized_run(self, root, *, capture_failed=True, used=100):
        private = root / "private"
        run = private / "runs" / "run-trace-resolve"
        run.mkdir(parents=True)
        storage = {"reserve_bytes": 1, "stop_bytes": 40_000_000_000}
        for path, value in ((private / "profile.json", {"run_id": "run", "storage": storage}),
                            (run / "run.json", {"phase": "deleted"}),
                            (run / "capture-budget.private.json", {
                                "capture_limit_bytes": seal_module.LIMIT,
                                "allocated_before_bytes": 100})):
            path.write_text(json.dumps(value))
            path.chmod(0o600)
        if capture_failed:
            path = run / "failure.json"
            path.write_text(json.dumps({"error_code": "CAPACITY",
                                        "capture_stage": "capture_files"}))
            path.chmod(0o600)
        raw = run / "collected"
        raw.mkdir()
        with (raw / "oversized.raw").open("wb") as stream:
            stream.truncate(seal_module.LIMIT + 1)
        certificate = root / "cert.pem"
        certificate.write_text("test certificate")

        class CurrentBudget:
            def __init__(self, storage):
                pass

            def used(self):
                return used

        return private, run, certificate, CurrentBudget

    def test_logically_oversized_raw_seals_failure_when_allocated_headroom_remains(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, budget = self.make_oversized_run(root)
            inputs = []

            def encrypt(*args, **kwargs):
                inputs.append(kwargs["input"])
                return SimpleNamespace(returncode=0, stdout=b"encrypted failure receipt")

            with patch.object(seal_module, "Budget", budget), \
                 patch.object(seal_module.subprocess, "run", side_effect=encrypt):
                result = seal_module.seal(private, certificate, root / "export")
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual(result["error_code"], "EVIDENCE_FAILURE")
            self.assertEqual((root / "export" / "private-evidence.cms").read_bytes(),
                             b"encrypted failure receipt")
            with tarfile.open(fileobj=io.BytesIO(inputs[0]), mode="r:gz") as archive:
                names = set(archive.getnames())
                self.assertEqual(names, {"evidence/failure.json", "evidence/run.json",
                                         "evidence/capture-budget.private.json",
                                         "evidence/seal-failure.private.json"})
                retained = json.load(archive.extractfile("evidence/failure.json"))
                self.assertEqual(retained["capture_stage"], "capture_files")
                failure = json.load(archive.extractfile("evidence/seal-failure.private.json"))
                self.assertEqual(failure["seal_error_code"], "CAPACITY")
                self.assertEqual(failure["seal_error_stage"], "full_seal")
                self.assertTrue(failure["full_evidence_incomplete"])

    def test_successful_capture_with_unsealable_raw_is_reported_failed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, budget = self.make_oversized_run(root,
                                                                        capture_failed=False)
            with patch.object(seal_module, "Budget", budget), \
                 patch.object(seal_module.subprocess, "run", return_value=SimpleNamespace(
                     returncode=0, stdout=b"encrypted failure receipt")):
                result = seal_module.seal(private, certificate, root / "export")
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual(result["error_code"], "EVIDENCE_FAILURE")

    def test_failure_receipt_refuses_insufficient_host_headroom(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, budget = self.make_oversized_run(
                root, used=40_000_000_000 - 1_000_000)
            with patch.object(seal_module, "Budget", budget), \
                 patch.object(seal_module.subprocess, "run", return_value=SimpleNamespace(
                     returncode=0, stdout=b"encrypted failure receipt")):
                with self.assertRaises(Failure) as raised:
                    seal_module.seal(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            self.assertFalse((root / "export").exists())
            self.assertFalse(list(root.glob("export.pending-*")))

    def test_failure_receipt_refuses_exhausted_capture_allocation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, budget = self.make_oversized_run(
                root, used=100 + seal_module.LIMIT + 1)
            with patch.object(seal_module, "Budget", budget), \
                 patch.object(seal_module.subprocess, "run", return_value=SimpleNamespace(
                     returncode=0, stdout=b"encrypted failure receipt")):
                with self.assertRaises(Failure) as raised:
                    seal_module.seal(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            self.assertFalse((root / "export").exists())
            self.assertFalse(list(root.glob("export.pending-*")))

    def test_cli_marks_sealed_failure_as_failed_step(self):
        result = {"case": "SIGNED_INDEX_CAPTURE", "outcome": "failed",
                  "error_code": "EVIDENCE_FAILURE"}
        with patch.object(seal_module, "seal", return_value=result), \
             patch.object(sys, "argv", ["seal_signed_indexes.py", "--private", "private",
                                        "--certificate", "cert", "--output", "export"]), \
             patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(seal_module.main(), 1)
        self.assertEqual(json.loads(output.getvalue()), result)

    def test_interrupted_first_seal_does_not_block_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "private"
            run = private / "runs" / "run-trace-resolve"
            run.mkdir(parents=True)
            for path, value in ((private / "profile.json", {
                "run_id": "run", "storage": {"reserve_bytes": 1,
                                              "stop_bytes": 40_000_000_000}}),
                                (run / "run.json", {"phase": "stopped"}),
                                (run / "failure.json", {"error_code": "COMMAND"})):
                path.write_text(json.dumps(value))
                path.chmod(0o600)
            certificate = root / "cert.pem"
            certificate.write_text("test certificate")
            collected = run / "collected"
            collected.mkdir()
            (collected / "apt-raw.txt").write_text("raw failed APT output")
            (run / "proof.log").write_text("raw failed gpgv output")
            output = root / "export"
            calls = 0
            commands = []

            def popen(args, **kwargs):
                nonlocal calls
                calls += 1
                commands.append(args)
                if calls == 2:
                    raise OSError("interrupted encryption")
                return FakeChild(b"synthetic ciphertext" if args[0] == "openssl" else b"")

            with patch.object(seal_module, "Budget", FakeBudget), \
                 patch.object(seal_module.select, "select", side_effect=lambda r, w, x, t: (r, [], [])), \
                 patch.object(seal_module.os, "read", side_effect=lambda fd, n: active_stdout[0].read(n)):
                active_stdout = [None]
                original_popen = popen

                def linked_popen(args, **kwargs):
                    child = original_popen(args, **kwargs)
                    if args[0] == "openssl":
                        active_stdout[0] = child.stdout
                    return child

                with patch.object(seal_module.subprocess, "Popen", side_effect=linked_popen):
                    with self.assertRaises(OSError):
                        seal_module._seal_full(private, certificate, output)
                    self.assertFalse(output.exists())
                    self.assertFalse(list(root.glob("export.pending-*")))
                    self.assertTrue((run / "failure.json").is_file())
                    result = seal_module._seal_full(private, certificate, output)
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual((output / "private-evidence.cms").read_bytes(),
                             b"synthetic ciphertext")
            self.assertIn("proof.log", commands[2])
            self.assertIn("collected", commands[2])
            self.assertFalse(list(root.glob("export.pending-*")))

    def test_sealer_uses_hard_file_cap_for_ciphertext(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "private"
            run = private / "runs" / "run-trace-resolve"
            run.mkdir(parents=True)
            for path, value in ((private / "profile.json", {
                "run_id": "run", "storage": {"reserve_bytes": 1,
                                              "stop_bytes": 40_000_000_000}}),
                                (run / "run.json", {"phase": "stopped"}),
                                (run / "failure.json", {"error_code": "COMMAND"}),
                                (run / "capture-budget.private.json", {
                                    "capture_limit_bytes": seal_module.LIMIT,
                                    "allocated_before_bytes": 100})):
                path.write_text(json.dumps(value))
                path.chmod(0o600)
            certificate = root / "cert.pem"
            certificate.write_text("test certificate")
            commands = []

            def popen(args, **kwargs):
                commands.append(args)
                child = FakeChild(b"xx" if args[0] == "openssl" else b"")
                if args[0] == "openssl":
                    active_stdout[0] = child.stdout
                return child

            class NearCapBudget:
                def __init__(self, storage):
                    pass

                def used(self):
                    return 100 + seal_module.LIMIT - seal_module.SEAL_RESERVE - 1

            active_stdout = [None]
            with patch.object(seal_module, "Budget", NearCapBudget), \
                 patch.object(seal_module.subprocess, "Popen", side_effect=popen), \
                 patch.object(seal_module.select, "select", side_effect=lambda r, w, x, t: (r, [], [])), \
                 patch.object(seal_module.os, "read", side_effect=lambda fd, n: active_stdout[0].read(n)):
                with self.assertRaises(Failure) as raised:
                    seal_module._seal_full(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            self.assertFalse((root / "export").exists())
            self.assertFalse(list(root.glob("export.pending-*")))

    def test_sealer_reserves_host_stop_headroom_with_capture_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "private"
            run = private / "runs" / "run-trace-resolve"
            run.mkdir(parents=True)
            stop = 40_000_000_000
            reserve = 1_000_000_000
            used = stop - reserve - 1_000_000
            for path, value in ((private / "profile.json", {
                "run_id": "run", "storage": {"reserve_bytes": reserve,
                                          "stop_bytes": stop}}),
                                (run / "run.json", {"phase": "stopped"}),
                                (run / "failure.json", {"error_code": "COMMAND"}),
                                (run / "capture-budget.private.json", {
                                    "capture_limit_bytes": seal_module.LIMIT,
                                    "allocated_before_bytes": used - 100})):
                path.write_text(json.dumps(value))
                path.chmod(0o600)
            certificate = root / "cert.pem"
            certificate.write_text("test certificate")

            class NearStopBudget:
                def __init__(self, storage):
                    pass

                def used(self):
                    return used

            with patch.object(seal_module, "Budget", NearStopBudget), \
                 patch.object(seal_module.subprocess, "Popen") as popen:
                with self.assertRaises(Failure) as raised:
                    seal_module._seal_full(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            popen.assert_not_called()
            self.assertFalse((root / "export").exists())


if __name__ == "__main__":
    unittest.main()
