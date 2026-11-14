"""The fallback seal can retry after an interrupted first encryption."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import seal_signed_indexes as seal_module


class FakeBudget:
    def __init__(self, storage):
        self.storage = storage

    def used(self):
        return 100


class FakeChild:
    def __init__(self):
        self.stdout = io.BytesIO()
        self.returncode = 0

    def poll(self):
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
            output = root / "export"
            calls = 0

            def popen(args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("interrupted encryption")
                if args[0] == "openssl":
                    Path(args[args.index("-out") + 1]).write_bytes(b"synthetic ciphertext")
                return FakeChild()

            with patch.object(seal_module, "Budget", FakeBudget), \
                 patch.object(seal_module.subprocess, "Popen", side_effect=popen):
                with self.assertRaises(OSError):
                    seal_module.seal(private, certificate, output)
                self.assertFalse(output.exists())
                self.assertFalse(list(root.glob("export.pending-*")))
                self.assertTrue((run / "failure.json").is_file())
                result = seal_module.seal(private, certificate, output)
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual((output / "private-evidence.cms").read_bytes(),
                             b"synthetic ciphertext")
            self.assertFalse(list(root.glob("export.pending-*")))


if __name__ == "__main__":
    unittest.main()
