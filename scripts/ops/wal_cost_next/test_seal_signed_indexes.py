"""The fallback seal can retry after an interrupted first encryption."""
import io
import json
from pathlib import Path
import tempfile
import unittest
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
                        seal_module.seal(private, certificate, output)
                    self.assertFalse(output.exists())
                    self.assertFalse(list(root.glob("export.pending-*")))
                    self.assertTrue((run / "failure.json").is_file())
                    result = seal_module.seal(private, certificate, output)
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
                    seal_module.seal(private, certificate, root / "export")
            self.assertEqual(raised.exception.code, "CAPACITY")
            self.assertFalse((root / "export").exists())
            self.assertFalse(list(root.glob("export.pending-*")))


if __name__ == "__main__":
    unittest.main()
