"""Synthetic private perf baseline recovery checks."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import stress_perf_baseline as baseline
import stress_private_evidence as private_evidence


def fixture(root: Path, *, omit_second: bool = False,
            invalid_first: bool = False) -> tuple[dict, dict[str, bytes]]:
    workspace = root / "checkout"
    source_dir = workspace / "target/stress-baselines"
    source_dir.mkdir(parents=True)
    subprocess.run(["git", "-C", str(workspace), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(workspace), "-c", "user.name=Test",
                    "-c", "user.email=test@example.invalid", "commit",
                    "--allow-empty", "-qm", "source"], check=True)
    source = subprocess.check_output(["git", "-C", str(workspace),
                                      "rev-parse", "HEAD"], text=True).strip()
    bodies = {
        "perf_mixed_point_latency.json": json.dumps(
            {"metrics": {"latency_ms": float("nan") if invalid_first else 1.25}}
        ).encode(),
        "perf_sequential_throughput_and_amplification.json": json.dumps(
            {"metrics": {"throughput_ops": 100.0}}
        ).encode(),
    }
    if omit_second:
        bodies.pop("perf_sequential_throughput_and_amplification.json")
    for name, body in bodies.items():
        (source_dir / name).write_bytes(body)
    cert, key = root / "cert.pem", root / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                    "-keyout", str(key), "-out", str(cert), "-days", "1",
                    "-subj", "/CN=synthetic-stage2-test"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ciphertext = root / "perf.cms"
    private_evidence.seal(
        workspace=workspace, case_id="perf", source_commit=source,
        certificate=cert,
        expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
        output=ciphertext,
    )
    destination = root / "restored"
    destination.mkdir(mode=0o700)
    args = dict(ciphertext=ciphertext,
                expected_ciphertext_sha256=hashlib.sha256(ciphertext.read_bytes()).hexdigest(),
                expected_source_commit=source, certificate=cert,
                private_key=key, destination=destination)
    return args, bodies


class PerfBaselineTests(unittest.TestCase):
    def test_recovers_only_verified_baselines(self):
        with tempfile.TemporaryDirectory() as directory:
            args, bodies = fixture(Path(directory))
            self.assertEqual(set(baseline.recover(**args)), set(bodies))
            for name, body in bodies.items():
                self.assertEqual((args["destination"] / name).read_bytes(), body)
            self.assertEqual(set(path.name for path in args["destination"].iterdir()),
                             set(bodies))

    def test_rejects_wrong_artifact_or_source_without_output(self):
        with tempfile.TemporaryDirectory() as directory:
            args, _ = fixture(Path(directory))
            for replacement in ({"expected_ciphertext_sha256": "0" * 64},
                                {"expected_source_commit": "0" * 40}):
                with self.assertRaises(ValueError):
                    baseline.recover(**{**args, **replacement})
                self.assertEqual(list(args["destination"].iterdir()), [])

    def test_rejects_missing_or_nonfinite_baseline(self):
        for variant in ({"omit_second": True}, {"invalid_first": True}):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                args, _ = fixture(Path(directory), **variant)
                with self.assertRaises(ValueError):
                    baseline.recover(**args)
                self.assertEqual(list(args["destination"].iterdir()), [])

    def test_rejects_public_or_existing_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            args, _ = fixture(Path(directory))
            destination = args["destination"]
            destination.chmod(0o755)
            with self.assertRaises(ValueError):
                baseline.recover(**args)
            destination.chmod(0o700)
            (destination / "perf_mixed_point_latency.json").write_bytes(b"keep")
            with self.assertRaises(ValueError):
                baseline.recover(**args)
            self.assertEqual((destination / "perf_mixed_point_latency.json").read_bytes(),
                             b"keep")

    def test_cli_rejects_without_sensitive_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args, _ = fixture(root)
            sensitive = root / "sensitive-machine-place"
            result = subprocess.run([
                sys.executable, str(Path(baseline.__file__)),
                "--ciphertext", str(sensitive),
                "--expected-ciphertext-sha256", args["expected_ciphertext_sha256"],
                "--expected-source-commit", args["expected_source_commit"],
                "--certificate", str(args["certificate"]),
                "--private-key", str(args["private_key"]),
                "--destination", str(args["destination"]),
            ], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stderr.strip(), "STRESS_PERF_BASELINE_REJECTED")
            self.assertNotIn("sensitive-machine-place", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
