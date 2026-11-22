"""Offline workflow handoff contract for the #712 same-job four-case lane."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/ops"))
import four_case_job as job
from four_case_diagnostic.test_qualification import fixture, JOB


class FourCaseJobTests(unittest.TestCase):
    @staticmethod
    def timing_fixture(private, job_id):
        from four_case_diagnostic import adapter, public_timing
        root = private / "public-timing"
        root.mkdir(mode=0o700)
        for case in adapter.CASES:
            for trial in range(3):
                collected = (private / f"{case}-{trial}" / "owned-runs" /
                             f"f712-{job_id}-{case}-{trial}" / "collected")
                for mode in ("on", "off"):
                    destination = collected / mode
                    destination.mkdir(parents=True, mode=0o700)
                    for directory in (private / f"{case}-{trial}",
                                      private / f"{case}-{trial}" / "owned-runs",
                                      collected.parent, collected, destination):
                        directory.chmod(0o700)
                    rows = []
                    with public_timing.TimingCellWriter(root, case, trial, mode) as writer:
                        for sample in range(21):
                            ordinal = 2 * sample + adapter.pair_order(trial, sample).index(mode)
                            seconds = .011 if mode == "on" else .010
                            writer.append(sample, ordinal, seconds)
                            rows.append({"sample": sample, "seconds": seconds,
                                         "pair_ordinal": ordinal, "resources": {}})
                    raw = b"".join((json.dumps(row) + "\n").encode() for row in rows)
                    path = destination / "raw.ndjson"
                    path.write_bytes(raw)
                    path.chmod(0o600)
                    cell = {"raw_sha256": hashlib.sha256(raw).hexdigest(),
                            "raw_seconds": [row["seconds"] for row in rows],
                            "raw_ordinals": [row["pair_ordinal"] for row in rows]}
                    path = destination / "measurement.json"
                    path.write_text(json.dumps(cell))
                    path.chmod(0o600)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(strict=True)
        lock, _, _ = fixture()
        lock_raw = json.dumps(lock).encode()
        self.lock_sha = hashlib.sha256(lock_raw).hexdigest()
        manifest = {"schema": job.controller.SCHEMA,
                    "lock_sha256": self.lock_sha,
                    "source_commit": lock["pin"]["source_commit"],
                    "source_tree": lock["pin"]["source_tree"],
                    "assets_digest": lock["pin"]["assets_digest"],
                    "fixture_sha256": "a" * 64,
                    "package_files": {f"four_case_diagnostic/{name}.py": "b" * 64
                                      for name in ("__init__", "adapter",
                                                   "container_pair_entrypoint", "log_parser",
                                                   "operation", "oracle", "public_timing", "runner",
                                                   "supervisor", "worker")},
                    "docs_files": {"subject_identity.py": "c" * 64,
                                   "sql_resources.py": "d" * 64},
                    "product_files": {"parity_v0811_measure.py": "e" * 64}}
        manifest_raw = json.dumps(manifest).encode()
        self.env = {"RUNNER_TEMP": str(self.root),
                    "ALOPEX_FOUR_CASE_LOCK_SHA256": self.lock_sha,
                    "ALOPEX_FOUR_CASE_LOCK_B64": base64.b64encode(lock_raw).decode(),
                    "ALOPEX_FOUR_CASE_MANIFEST_SHA256":
                        hashlib.sha256(manifest_raw).hexdigest(),
                    "ALOPEX_FOUR_CASE_MANIFEST_B64":
                        base64.b64encode(manifest_raw).decode()}

    def test_install_requires_independent_hashes_and_private_fixed_paths(self):
        manifest = job.install_inputs(self.env)
        directory = self.root / "alopex-712-lock"
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        for name in ("lock.json", "manifest.json"):
            self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest["lock_sha256"], self.lock_sha)

    def test_install_rejects_changed_bytes_before_file_creation(self):
        self.env["ALOPEX_FOUR_CASE_LOCK_SHA256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            job.install_inputs(self.env)
        self.assertFalse((self.root / "alopex-712-lock").exists())

    def test_install_rejects_noncanonical_base64_and_dynamic_manifest(self):
        self.env["ALOPEX_FOUR_CASE_LOCK_B64"] += "\n"
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            job.install_inputs(self.env)
        self.assertFalse((self.root / "alopex-712-lock").exists())

    def test_workflow_routes_only_four_case_lane_and_uploads_only_cms(self):
        source = (REPO / ".github/workflows/alopex-performance.yml").read_text()
        qualification = source.split("  qualification:\n", 1)[1].split(
            "  wal_cost_682:\n", 1)[0]
        for phase in ("--admission-only", "--acquire-assets", "--execute"):
            self.assertIn("hosted_test_env_qualification_four_case.py " + phase,
                          qualification)
        self.assertIn("scripts/ops/four_case_job.py install", qualification)
        self.assertIn("scripts/ops/four_case_job.py run", qualification)
        self.assertIn("scripts/ops/four_case_job.py seal", qualification)
        self.assertIn("scripts/ops/four_case_job.py verify-public", qualification)
        verify_step = qualification.split(
            "      - name: Verify four-case public measurement\n", 1)[1].split(
                "      - name:", 1)[0]
        self.assertIn("ALOPEX_FOUR_CASE_LOCK_SHA256: ${{ inputs.four_case_lock_sha256 }}",
                      verify_step)
        self.assertIn("steps.matrix_public.outcome == 'success'", qualification)
        self.assertNotIn("scripts/ops/same_job_four_case.py", qualification)
        self.assertIn("native-sql-four-case-export/private-evidence.cms",
                      qualification)
        self.assertIn("native-sql-four-case-export/public.json", qualification)
        self.assertNotIn("native-sql-four-case-export/\n", qualification)
        self.assertIn("if: ${{ !inputs.sql_diagnostic_mode && !inputs.sql_four_case_mode }}",
                      qualification)

    def test_seal_allows_bounded_twelve_pair_inventory_and_checks_capacity(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        for index in range(260):
            path = private / f"member-{index}.json"
            path.write_text("{}")
            path.chmod(0o600)
        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")
        checks = []
        def capacity(root, extra):
            checks.append(extra)
            return {"stop_bytes": 40_000_000_000}
        def encrypt(argv, **kwargs):
            Path(argv[argv.index("-out") + 1]).write_bytes(b"ciphertext")
            return SimpleNamespace(returncode=0)
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_capacity_before_seal", side_effect=capacity), \
             patch.object(job.subprocess, "run", side_effect=encrypt):
            public = job.seal_job(self.env)
        self.assertEqual(public["status"], "sealed")
        self.assertEqual(public["matrix_status"], "failed")
        self.assertEqual(public["matrix_error_code"], "INCOMPLETE")
        self.assertEqual(len(checks), 3)
        self.assertTrue((self.root / "native-sql-four-case-export/private-evidence.cms").is_file())
        self.assertFalse((private / "evidence.tar.gz").exists())

    def test_seal_removes_plaintext_when_archive_creation_fails(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        member = private / "receipt.private.json"
        member.write_text("{}")
        member.chmod(0o600)
        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")

        with patch.object(job, "REPO", self.root), \
             patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_capacity_before_seal", return_value={}), \
             patch.object(job.tarfile.TarFile, "add", side_effect=OSError("archive write failed")), \
             self.assertRaisesRegex(OSError, "archive write failed"):
            job.seal_job(self.env)
        self.assertFalse((private / "evidence.tar.gz").exists())
        self.assertFalse((self.root / "native-sql-four-case-export/private-evidence.cms").exists())

    def test_passed_matrix_exports_only_strict_verified_cms(self):
        from four_case_diagnostic import intake
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        strict = private / "strict-evidence.cms"
        strict.write_bytes(b"strict cms bytes")
        strict.chmod(0o600)
        self.env["GITHUB_RUN_ID"] = JOB
        self.timing_fixture(private, JOB)
        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_matrix_outcome", return_value={
                 "matrix_status": "passed", "matrix_error_code": None}), \
             patch.object(job, "_capacity_before_seal", return_value={}) as capacity, \
             patch.object(job.tarfile, "open") as archive, \
             patch.object(job.subprocess, "run") as encrypt:
            public = job.seal_job(self.env)
        self.assertEqual(public["evidence_schema"], intake.SCHEMA)
        self.assertEqual(public["ciphertext_size"], len(strict.read_bytes()))
        self.assertEqual(public["ciphertext_sha256"], hashlib.sha256(strict.read_bytes()).hexdigest())
        self.assertEqual(public["matrix_status"], "passed")
        self.assertEqual(len(public["measurement"]["timings"]), 504)
        self.assertEqual(set(public["measurement"]["paired"]),
                         set(job.controller.adapter.CASES))
        self.assertEqual((self.root / "native-sql-four-case-export/private-evidence.cms").read_bytes(),
                         strict.read_bytes())
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "_matrix_outcome", return_value={
                 "matrix_status": "passed", "matrix_error_code": None}):
            self.assertTrue(job.verify_public_export(self.env))
            public_path = self.root / "native-sql-four-case-export/public.json"
            altered = json.loads(public_path.read_text())
            altered["local_path"] = "/private/sensitive"
            public_path.write_text(json.dumps(altered, sort_keys=True) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
            public_path.write_text(json.dumps(public, sort_keys=True) + "\n")
            timing_path = private / "public-timing" / "aggregate-trial-0-on.jsonl"
            lines = timing_path.read_text().splitlines()
            altered = json.loads(lines[0])
            altered["elapsed_seconds"] = .012
            lines[0] = json.dumps(altered, separators=(",", ":"))
            timing_path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
        self.assertEqual(capacity.call_count, 1)
        archive.assert_not_called()
        encrypt.assert_not_called()

    def test_passed_matrix_without_strict_evidence_fails_closed(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        receipt = private / "receipt.public.json"
        receipt.write_text("{}")
        receipt.chmod(0o600)
        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_matrix_outcome", return_value={
                 "matrix_status": "passed", "matrix_error_code": None}), \
             self.assertRaisesRegex(ValueError, "EVIDENCE_BOUND"):
            job.seal_job(self.env)
        self.assertFalse((self.root / "native-sql-four-case-export/private-evidence.cms").exists())

    def test_run_attests_once_then_stages_docs_once_for_twelve_pairs(self):
        from four_case_diagnostic import docs_stage, evidence_assembly
        order = []
        self.env.update({"GITHUB_RUN_ID": JOB, "GITHUB_RUN_ATTEMPT": "1",
                         "GITHUB_ACTIONS": "true",
                         "RUNNER_ENVIRONMENT": "github-hosted"})
        job.install_inputs(self.env)
        _, proof, receipt = fixture()

        class Run:
            def __init__(self, output):
                self.root = output
                self.record = {"phase": "deleted"}

            def verify_collection(self):
                pass

        def attest(*args, **kwargs):
            order.append("attest")
            return proof, receipt

        def launch(config, output, proof, receipt):
            order.append("pair")
            return Run(output)

        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")
        with patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_capacity_before_seal", return_value={}), \
             patch.object(evidence_assembly, "assemble_public_timing",
                          side_effect=lambda *a, **kw: order.append("public")), \
             patch.object(evidence_assembly, "seal_runs",
                          side_effect=lambda *a, **kw: order.append("assemble")) as assembled, \
             patch.object(job.entrypoint, "attest_current_job", side_effect=attest) as attested, \
             patch.object(job.entrypoint, "controller_operations", return_value={}), \
             patch.object(docs_stage, "stage_docs",
                          side_effect=lambda *a, **kw: order.append("docs")) as staged_docs, \
             patch.object(job.stage_pair, "make_stage_pair", return_value=launch), \
             patch.object(job.controller.supervisor, "run_collected_pair",
                          return_value={"status": "passed", "collection_verified": True,
                                        "owned_resources_cleaned": True}):
            job.run_job(self.env)
        self.assertEqual(attested.call_count, 1)
        self.assertEqual(staged_docs.call_count, 1)
        self.assertEqual(assembled.call_count, 1)
        self.assertEqual(len(assembled.call_args.args[0]), 12)
        self.assertEqual(order, ["attest", "docs", *("pair" for _ in range(12)),
                                 "public", "assemble"])

    def test_failed_attestation_never_stages_docs(self):
        from four_case_diagnostic import docs_stage
        self.env.update({"GITHUB_RUN_ID": JOB, "GITHUB_RUN_ATTEMPT": "1",
                         "GITHUB_ACTIONS": "true",
                         "RUNNER_ENVIRONMENT": "github-hosted"})
        job.install_inputs(self.env)
        with patch.object(job.entrypoint, "controller_operations", return_value={}), \
             patch.object(job.entrypoint, "attest_current_job",
                          side_effect=ValueError("QUALIFICATION")) as attested, \
             patch.object(docs_stage, "stage_docs") as staged_docs:
            with self.assertRaisesRegex(ValueError, "QUALIFICATION"):
                job.run_job(self.env)
        self.assertEqual(attested.call_count, 1)
        staged_docs.assert_not_called()

    def test_export_status_validates_current_attempt_and_fixed_error(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        self.env.update({"GITHUB_RUN_ID": JOB, "GITHUB_RUN_ATTEMPT": "2"})
        job.install_inputs(self.env)
        pin = fixture()[0]["pin"]
        path = private / "receipt.public.json"

        def save(value):
            path.write_text(json.dumps(value))
            path.chmod(0o600)

        save({"schema": job.controller.SCHEMA, "status": "passed",
              "performance_qualification": False, "job_id": JOB,
              "run_attempt": "2", "qualification_binding_sha256": "a" * 64})
        with patch.object(job.controller, "verify_public_receipt", return_value={
                "source_commit": pin["source_commit"],
                "assets_digest": pin["assets_digest"]}) as verified:
            self.assertEqual(job._matrix_outcome(private, self.env), {
                "matrix_status": "passed", "matrix_error_code": None,
                **{key: pin[key] for key in
                   ("source_commit", "source_tree", "artifact_id", "artifact_sha256",
                    "server_sha256", "wheel_sha256", "assets_digest")}})
        self.assertEqual(verified.call_count, 1)
        with patch.object(job.controller, "verify_public_receipt", return_value={
                "source_commit": pin["source_commit"],
                "assets_digest": "b" * 64}):
            self.assertEqual(job._matrix_outcome(private, self.env), {
                "matrix_status": "failed", "matrix_error_code": "INCOMPLETE"})
        self.env["GITHUB_RUN_ATTEMPT"] = "3"
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "INCOMPLETE"})
        save({"schema": job.controller.SCHEMA, "status": "failed",
              "performance_qualification": False, "run_attempt": "3",
              "error_code": "PAIR_FAILED"})
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "PAIR_FAILED"})
        save({"schema": job.controller.SCHEMA, "status": "failed",
              "performance_qualification": False, "run_attempt": "3",
              "error_code": "private detail"})
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "INCOMPLETE"})


if __name__ == "__main__":
    unittest.main()
