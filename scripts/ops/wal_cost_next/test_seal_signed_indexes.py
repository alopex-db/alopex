"""The fallback seal can retry after an interrupted first encryption."""
import io
import json
import shutil
import subprocess
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
            private, run, certificate, budget = self.make_oversized_run(root)
            (run / "failure.json").write_text(json.dumps({
                "error_code": "CAPACITY", "capture_stage": "capture_final_capture_budget",
                "capacity_gate": "final_capture_growth",
                "final_budget_samples": {"final_capture": {
                    "used_bytes": 351, "baseline_bytes": 100, "planned_bytes": 250}},
                "collected_bytes": 200}))
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
                self.assertEqual(retained["capacity_gate"], "final_capture_growth")
                self.assertEqual(retained["final_budget_samples"]["final_capture"], {
                    "used_bytes": 351, "baseline_bytes": 100, "planned_bytes": 250})
                self.assertEqual(retained["collected_bytes"], 200)
                failure = json.load(archive.extractfile("evidence/seal-failure.private.json"))
                self.assertEqual(failure["seal_error_code"], "CAPACITY")
                self.assertEqual(failure["seal_error_stage"], "full_seal")
                self.assertEqual(failure["capacity_gate"], "raw_capture_size")
                self.assertGreater(failure["capacity_numbers"]["raw_bytes"],
                                   seal_module.LIMIT)
                self.assertEqual(set(failure["capacity_numbers"]), {"raw_bytes"})
                self.assertTrue(failure["full_evidence_incomplete"])
            self.assertNotIn("final_budget_samples",
                             (root / "export" / "public.json").read_text())
            self.assertNotIn("capacity_gate",
                             (root / "export" / "public.json").read_text())

    def test_full_seal_distinguishes_precheck_and_headroom(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, _ = self.make_oversized_run(root)
            output = root / "export"

            class CurrentBudget:
                def __init__(self, storage):
                    self.used_bytes = storage["used_bytes"]

                def used(self):
                    return self.used_bytes

            profile = json.loads((private / "profile.json").read_text())
            for used, stop, expected_gate in (
                    (100 + seal_module.LIMIT + 1, 40_000_000_000, "pre_seal_growth"),
                    (100, 100 + 1 + seal_module.SEAL_RESERVE, "seal_headroom")):
                profile["storage"]["used_bytes"] = used
                profile["storage"]["stop_bytes"] = stop
                (private / "profile.json").write_text(json.dumps(profile))
                with patch.object(seal_module, "Budget", CurrentBudget), \
                     patch.object(seal_module, "audit", return_value=1):
                    with self.assertRaises(Failure) as raised:
                        seal_module._seal_full(private, certificate, output)
                self.assertEqual(raised.exception.code, "CAPACITY")
                self.assertEqual(raised.exception.detail, expected_gate)
                self.assertEqual(set(raised.exception.capacity_numbers) -
                                 seal_module.CAPACITY_NUMBERS, set())

    def test_full_seal_distinguishes_periodic_and_final_gates(self):
        for gate in ("periodic_growth", "periodic_host_stop", "final_growth"):
            with self.subTest(gate=gate), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                private, _, certificate, _ = self.make_oversized_run(root)
                profile = json.loads((private / "profile.json").read_text())
                if gate == "periodic_host_stop":
                    profile["storage"]["stop_bytes"] = 100 + 1 + \
                        seal_module.SEAL_RESERVE + 100
                (private / "profile.json").write_text(json.dumps(profile))
                stop = profile["storage"]["stop_bytes"]
                sampled = (stop - 1 if gate == "periodic_host_stop" else
                           100 + seal_module.SEAL_LIMIT + 1)
                values = iter((100, 100, sampled))

                class SampledBudget:
                    def __init__(self, storage):
                        pass

                    def used(self):
                        return next(values)

                active_stdout = [None]

                def popen(args, **kwargs):
                    child = FakeChild(b"x" if args[0] == "openssl" else b"")
                    if args[0] == "openssl":
                        active_stdout[0] = child.stdout
                    return child

                ticks = iter((0, 0, 0, 11))

                def clock():
                    return next(ticks, 11) if gate != "final_growth" else 0

                with patch.object(seal_module, "Budget", SampledBudget), \
                     patch.object(seal_module, "audit", return_value=1), \
                     patch.object(seal_module.subprocess, "Popen", side_effect=popen), \
                     patch.object(seal_module.select, "select",
                                  side_effect=lambda r, w, x, t: (r, [], [])), \
                     patch.object(seal_module.os, "read",
                                  side_effect=lambda fd, n: active_stdout[0].read(n)), \
                     patch.object(seal_module.time, "monotonic", side_effect=clock):
                    with self.assertRaises(Failure) as raised:
                        seal_module._seal_full(private, certificate, root / "export")
                self.assertEqual(raised.exception.code, "CAPACITY")
                self.assertEqual(raised.exception.detail, gate)
                self.assertEqual(raised.exception.capacity_numbers["used_bytes"], sampled)
                self.assertFalse((root / "export").exists())

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

    def test_failure_receipt_keeps_bounded_proof_log_tail(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, run, certificate, budget = self.make_oversized_run(root)
            log = run / "proof.log"
            log.write_bytes(b"a" * seal_module.FAILURE_LOG_TAIL_LIMIT + b"proof failure")
            inputs = []

            def encrypt(*args, **kwargs):
                inputs.append(kwargs["input"])
                return SimpleNamespace(returncode=0, stdout=b"encrypted failure receipt")

            with patch.object(seal_module, "Budget", budget), \
                 patch.object(seal_module.subprocess, "run", side_effect=encrypt):
                result = seal_module.seal(private, certificate, root / "export")
            self.assertEqual(result["outcome"], "failed")
            with tarfile.open(fileobj=io.BytesIO(inputs[0]), mode="r:gz") as archive:
                tail = archive.extractfile("evidence/proof.log.tail").read()
                self.assertEqual(len(tail), seal_module.FAILURE_LOG_TAIL_LIMIT)
                self.assertTrue(tail.endswith(b"proof failure"))
                failure = json.load(archive.extractfile("evidence/seal-failure.private.json"))
                self.assertIn("proof.log.tail", failure["retained"])
                self.assertIn("proof.log", failure["omitted_from_failure_receipt"])

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

    def test_failure_receipt_refuses_exhausted_seal_allocation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private, _, certificate, budget = self.make_oversized_run(
                root, used=100 + seal_module.SEAL_LIMIT + 1)
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
                if calls == 3:
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
            self.assertEqual(commands[1], ["gzip", "-9"])
            self.assertIn("proof.log", commands[3])
            self.assertIn("collected", commands[3])
            self.assertFalse(list(root.glob("export.pending-*")))

    @unittest.skipUnless(all(shutil.which(name) for name in ("tar", "gzip", "openssl")),
                         "tar, gzip, and openssl are required")
    def test_full_seal_gzip9_cms_round_trip_preserves_complete_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "private"
            run = private / "runs" / "run-trace-resolve"
            collected = run / "collected"
            proof = run / "proof"
            collected.mkdir(parents=True)
            proof.mkdir()
            (private / "profile.json").write_text(json.dumps({
                "run_id": "run", "storage": {"reserve_bytes": 1,
                                              "stop_bytes": 40_000_000_000}}))
            (run / "run.json").write_text(json.dumps({"phase": "deleted"}))
            (run / "capture-budget.private.json").write_text(json.dumps({
                "capture_limit_bytes": seal_module.LIMIT,
                "allocated_before_bytes": 100}))
            for name in (private / "profile.json", run / "run.json",
                         run / "capture-budget.private.json"):
                name.chmod(0o600)
            (run / "bootstrap.private.json").write_text("bootstrap evidence")
            (run / "collection.json").write_text("collection evidence")
            (collected / "capture.status").write_text("capture-complete\n")
            (collected / "index.bin").write_bytes(b"index payload" * 1000)
            (proof / "proof.sha256").write_text("signed proof")
            certificate = root / "cert.pem"
            key = root / "key.pem"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                            "-nodes", "-subj", "/CN=seal-test", "-days", "1",
                            "-keyout", str(key), "-out", str(certificate)],
                           check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            with patch.object(seal_module, "Budget", FakeBudget):
                result = seal_module._seal_full(private, certificate, root / "export")
            self.assertEqual(result["outcome"], "passed")
            ciphertext = root / "export" / "private-evidence.cms"
            self.assertEqual(result["evidence_sha256"], seal_module.digest(ciphertext))
            decrypted = subprocess.run(
                ["openssl", "cms", "-decrypt", "-binary", "-inform", "DER",
                 "-in", str(ciphertext), "-recip", str(certificate),
                 "-inkey", str(key)], check=True, capture_output=True).stdout
            self.assertTrue(decrypted.startswith(b"\x1f\x8b"))
            with tarfile.open(fileobj=io.BytesIO(decrypted), mode="r:gz") as archive:
                self.assertEqual(archive.extractfile("collected/index.bin").read(),
                                 b"index payload" * 1000)
                self.assertEqual(archive.extractfile("proof/proof.sha256").read(),
                                 b"signed proof")
                self.assertIn("capture-budget.private.json", archive.getnames())
            self.assertFalse(list(root.glob("export.pending-*")))

    def test_sealer_allows_a_second_copy_but_caps_ciphertext(self):
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
                    return 100 + seal_module.LIMIT

            active_stdout = [None]
            with patch.object(seal_module, "Budget", NearCapBudget), \
                 patch.object(seal_module, "audit", return_value=1), \
                 patch.object(seal_module.subprocess, "Popen", side_effect=popen), \
                 patch.object(seal_module.select, "select", side_effect=lambda r, w, x, t: (r, [], [])), \
                 patch.object(seal_module.os, "read", side_effect=lambda fd, n: active_stdout[0].read(n)):
                # The old combined 250 MB envelope would reject this two-byte
                # ciphertext even though the raw capture stayed at its cap.
                with patch.object(seal_module, "SEAL_LIMIT",
                                  seal_module.LIMIT + seal_module.SEAL_RESERVE + 2):
                    result = seal_module._seal_full(private, certificate, root / "fits")
                self.assertEqual(result["outcome"], "failed")
                self.assertEqual((root / "fits" / "private-evidence.cms").read_bytes(), b"xx")
                with patch.object(seal_module, "SEAL_LIMIT",
                                  seal_module.LIMIT + seal_module.SEAL_RESERVE + 1):
                    with self.assertRaises(Failure) as raised:
                        seal_module._seal_full(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            self.assertEqual(raised.exception.detail, "ciphertext_growth")
            self.assertEqual(raised.exception.capacity_numbers["next_chunk_bytes"], 2)
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
            self.assertEqual(raised.exception.detail, "seal_headroom")
            popen.assert_not_called()
            self.assertFalse((root / "export").exists())


if __name__ == "__main__":
    unittest.main()
