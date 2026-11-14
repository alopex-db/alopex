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
from unittest.mock import patch

from live_wrapper import run_prepared
from orchestrate import DiagnosticFailure, sha256
from test_orchestrate import HERE, identities, inputs


class LiveWrapperTests(unittest.TestCase):
    def prepared(self, directory):
        root = Path(directory)
        pin, receipt = identities()
        files = {}
        for key in ("assets_manifest", "server", "wheel"):
            path = root / key
            path.write_bytes(key.encode())
            files[key] = path
        pin.update(assets_digest=sha256(files["assets_manifest"]),
                   server_sha256=sha256(files["server"]),
                   wheel_sha256=sha256(files["wheel"]))
        receipt.update(assets_digest=pin["assets_digest"],
                       server_sha256=pin["server_sha256"],
                       wheel_sha256=pin["wheel_sha256"])
        receipt_path = root / "receipt.json"
        receipt_path.write_text(json.dumps(receipt))
        receipt_path.chmod(0o600)
        cert = root / "cert.pem"
        cert.write_text("fake")
        groups = inputs()
        cleanup = []
        kwargs = dict(job_id="same-job-1", operator_dir=HERE, work=root / "owned",
                      export=root / "upload", receipt_path=receipt_path,
                      expected_receipt_sha256=sha256(receipt_path), certificate=cert,
                      used_bytes=lambda: 10_000_000_000,
                      run_pair=lambda cell, binding: groups[(cell["case"], cell["trial"])],
                      cleanup=lambda: cleanup.append(True) or True,
                      job_limit_seconds=6_000, **files)
        return pin, kwargs, cleanup

    def test_missing_or_invalid_receipt_attempts_cleanup_and_never_exports(self):
        for mutation in ("missing", "wrong_hash", "wrong_job"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                pin, kwargs, cleanup = self.prepared(directory)
                if mutation == "missing":
                    kwargs["receipt_path"].unlink()
                elif mutation == "wrong_hash":
                    kwargs["expected_receipt_sha256"] = "0" * 64
                else:
                    value = json.loads(kwargs["receipt_path"].read_text())
                    value["job_id"] = "other-job"
                    kwargs["receipt_path"].write_text(json.dumps(value))
                    kwargs["expected_receipt_sha256"] = sha256(kwargs["receipt_path"])
                with self.assertRaises(DiagnosticFailure):
                    run_prepared(pin, **kwargs)
                self.assertEqual(cleanup, [True])
                self.assertFalse(kwargs["export"].exists())

    def test_asset_mismatch_and_timeout_cleanup_failure_remain_private(self):
        with tempfile.TemporaryDirectory() as directory:
            pin, kwargs, cleanup = self.prepared(directory)
            kwargs["server"].write_bytes(b"changed")
            with self.assertRaises(DiagnosticFailure) as caught:
                run_prepared(pin, **kwargs)
            self.assertEqual(str(caught.exception.primary_error), "MISSING_ASSET")
            self.assertEqual(cleanup, [True])
            self.assertFalse(kwargs["export"].exists())
        with tempfile.TemporaryDirectory() as directory:
            pin, kwargs, cleanup = self.prepared(directory)
            kwargs["run_pair"] = lambda cell, binding: (_ for _ in ()).throw(ValueError("TIMEOUT"))
            kwargs["cleanup"] = lambda: cleanup.append(True) or False
            with self.assertRaises(DiagnosticFailure) as caught:
                run_prepared(pin, **kwargs)
            self.assertEqual(str(caught.exception.primary_error), "TIMEOUT")
            self.assertEqual(str(caught.exception.cleanup_error), "CLEANUP")
            self.assertEqual(cleanup, [True])
            self.assertFalse(kwargs["export"].exists())

    def test_success_exports_only_ciphertext_and_encryption_failure_exports_none(self):
        def seal(root, hashes, *, ciphertext, certificate):
            ciphertext.write_bytes(b"sealed")
            ciphertext.chmod(0o600)
            return {"sha256": sha256(ciphertext), "size": 6, "verified": True}
        with tempfile.TemporaryDirectory() as directory:
            pin, kwargs, cleanup = self.prepared(directory)
            with patch("live_wrapper.seal_cms", side_effect=seal):
                result = run_prepared(pin, **kwargs)
            self.assertEqual(result["measurement_cells"], 12)
            self.assertEqual({p.name for p in kwargs["export"].iterdir()}, {"evidence.cms"})
            self.assertFalse(kwargs["work"].exists())
            self.assertEqual(cleanup, [True])
        with tempfile.TemporaryDirectory() as directory:
            pin, kwargs, cleanup = self.prepared(directory)
            with patch("live_wrapper.seal_cms", side_effect=ValueError("ENCRYPTION")):
                with self.assertRaises(DiagnosticFailure):
                    run_prepared(pin, **kwargs)
            self.assertFalse(kwargs["export"].exists())
            self.assertTrue((kwargs["work"] / "plain/manifest.json").exists())
            self.assertEqual(cleanup, [True])


if __name__ == "__main__":
    unittest.main()
