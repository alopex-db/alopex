"""Synthetic end-to-end contract for private perf baseline reuse."""

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import stress_artifact_intake as intake
import stress_baseline_provenance as provenance
import stress_perf_baseline as baseline
import stress_private_evidence as private_evidence
import stress_public_evidence as public_evidence
from test_stress_artifact_intake import zip_artifact


class BaselinePipelineTests(unittest.TestCase):
    def test_exact_scheduled_run_restores_two_verified_baselines(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "checkout"
            baselines = checkout / "target/stress-baselines"
            baselines.mkdir(parents=True)
            subprocess.run(["git", "-C", str(checkout), "init", "-q"], check=True)
            subprocess.run(["git", "-C", str(checkout), "-c", "user.name=Test",
                            "-c", "user.email=test@example.invalid", "commit",
                            "--allow-empty", "-qm", "source"], check=True)
            source = subprocess.check_output(["git", "-C", str(checkout),
                                              "rev-parse", "HEAD"], text=True).strip()
            bodies = {name: json.dumps({"metrics": {"value": value}}).encode()
                      for name, value in (
                          ("perf_mixed_point_latency.json", 1.5),
                          ("perf_sequential_throughput_and_amplification.json", 120.0))}
            for name, body in bodies.items():
                (baselines / name).write_bytes(body)
            cert, key = root / "cert.pem", root / "key.pem"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                            "-nodes", "-keyout", str(key), "-out", str(cert),
                            "-days", "1", "-subj", "/CN=synthetic-stage2-test"],
                           check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            sealed = root / "sealed.cms"
            private_evidence.seal(
                workspace=checkout, case_id="perf", source_commit=source,
                certificate=cert,
                expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                output=sealed)
            status = public_evidence.build_status(
                case_id="perf", source_commit=source,
                outcome="passed", error_code="NONE", ciphertext=sealed)
            public_zip, private_zip = root / "public.zip", root / "private.zip"
            public_digest = zip_artifact(public_zip, "perf.json", json.dumps(status).encode())
            private_digest = zip_artifact(private_zip, "perf.cms", sealed.read_bytes())
            run_id, repo_id = 123456, 5678
            run = {"id": run_id, "path": provenance.WORKFLOW_PATH,
                   "event": "schedule", "status": "completed", "conclusion": "success",
                   "head_sha": source,
                   "repository": {"id": repo_id, "full_name": provenance.REPOSITORY},
                   "head_repository": {"id": repo_id,
                                       "full_name": provenance.REPOSITORY}}
            artifacts = []
            for artifact_id, name, digest, path in (
                    (11, provenance.PUBLIC_ARTIFACT, public_digest, public_zip),
                    (12, provenance.PRIVATE_ARTIFACT, private_digest, private_zip)):
                artifacts.append({
                    "id": artifact_id, "name": name, "expired": False,
                    "size_in_bytes": path.stat().st_size, "digest": digest,
                    "archive_download_url":
                        f"https://api.github.com/repos/{provenance.REPOSITORY}/actions/artifacts/{artifact_id}/zip",
                    "workflow_run": {"id": run_id, "head_sha": source,
                                     "repository_id": repo_id,
                                     "head_repository_id": repo_id},
                })
            downloaded_status = intake.read_public_status(public_zip, public_digest)
            selected = provenance.select(run=run, artifacts=artifacts,
                                         status=downloaded_status,
                                         expected_run_id=run_id)
            self.assertEqual(selected, (source, status["ciphertext_sha256"],
                                        11, public_digest, 12, private_digest))
            private_dir = root / "private"
            private_dir.mkdir(mode=0o700)
            ciphertext = private_dir / "perf.cms"
            intake.write_private_ciphertext(
                private_zip, selected[5], selected[1], ciphertext)
            recovered = private_dir / "baselines"
            recovered.mkdir(mode=0o700)
            self.assertEqual(set(baseline.recover(
                ciphertext=ciphertext,
                expected_ciphertext_sha256=selected[1],
                expected_source_commit=selected[0], certificate=cert,
                private_key=key, destination=recovered)), set(bodies))
            self.assertEqual({name: (recovered / name).read_bytes() for name in bodies},
                             bodies)


if __name__ == "__main__":
    unittest.main()
