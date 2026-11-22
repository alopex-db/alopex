"""Offline workflow handoff contract for the #712 same-job four-case lane."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/ops"))
import four_case_job as job
from four_case_diagnostic.test_qualification import fixture, JOB


class FourCaseJobTests(unittest.TestCase):
    @staticmethod
    def timing_fixture(private, job_id):
        from four_case_diagnostic import (adapter, log_parser, public_attribution,
                                          public_timing)
        from four_case_diagnostic.test_log_parser import pair
        timing_root = private / "public-timing"
        operation_root = private / "public-operations"
        stage_root = private / "public-stages"
        for root in (timing_root, operation_root, stage_root):
            root.mkdir(mode=0o700)
        for case in adapter.CASES:
            for trial in range(3):
                collected = (private / f"{case}-{trial}" / "owned-runs" /
                             f"f712-{job_id}-{case}-{trial}" / "collected")
                clients, lines = pair(case)
                collected.mkdir(parents=True, mode=0o700)
                for directory in (private / f"{case}-{trial}",
                                  private / f"{case}-{trial}" / "owned-runs",
                                  collected.parent, collected):
                    directory.chmod(0o700)
                log = collected / "server.log"
                log.write_text("".join(lines))
                log.chmod(0o600)
                for mode in ("on", "off"):
                    destination = collected / mode
                    destination.mkdir(mode=0o700)
                    for client in clients[mode]:
                        client["trial"] = trial
                        client["cell_id"] = f"{case}-{trial}-{mode}"
                        if client["phase"] == "measured":
                            sample = client["sample"]
                            client["pair_ordinal"] = 2 * sample + \
                                adapter.pair_order(trial, sample).index(mode)
                            client["outer_ns"] = 11_000_000 if mode == "on" else 10_000_000
                            if mode == "on":
                                client.update({"bind_ns": 1, "transport_ns": 2,
                                               "parse_ns": 1, "rows_ns": 1,
                                               "residual_ns": 1, "total_ns": 6,
                                               "response_chars": 12, "result_rows": 1})
                    path = destination / "client.ndjson"
                    path.write_text("".join(json.dumps(row) + "\n" for row in clients[mode]))
                    path.chmod(0o600)
                    rows = []
                    with public_timing.TimingCellWriter(timing_root, case, trial, mode) as writer, \
                         public_attribution.OperationCellWriter(
                             operation_root, case, trial, mode) as attribution:
                        for sample in range(21):
                            ordinal = 2 * sample + adapter.pair_order(trial, sample).index(mode)
                            seconds = .011 if mode == "on" else .010
                            counters = {"user_cpu_seconds": .1, "system_cpu_seconds": .2,
                                        "rss_before_bytes": 1024, "rss_after_bytes": 2048,
                                        "read_bytes": 0, "write_bytes": 0,
                                        "cancelled_write_bytes": 0}
                            resource = {"schema": "alopex.sql-operation-resources/v1",
                                        "status": "observed",
                                        "coverage": "stable_pid_namespace_allocation",
                                        "manifest_sha256": "a" * 64,
                                        "client": {"pid": 101, "start_ticks": 301, **counters},
                                        "server_processes": [{"pid": 201,
                                                              "start_ticks": 401, **counters}]}
                            writer.append(sample, ordinal, seconds)
                            attribution.append(sample, ordinal, seconds,
                                next(row for row in clients[mode]
                                     if row["phase"] == "measured" and row["sample"] == sample)
                                if mode == "on" else None, resource)
                            rows.append({"sample": sample, "seconds": seconds,
                                         "pair_ordinal": ordinal, "resources": resource})
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
                events = log_parser.parse_server_timing(
                    log, case, trial, clients["on"], clients["off"])
                public_attribution.write_stage_cell(
                    stage_root, case, trial, clients["on"], events)

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
                                                   "operation", "oracle", "public_attribution",
                                                   "public_timing", "runner",
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

    def test_workflow_routes_only_four_case_lane_and_uploads_only_public_json(self):
        source = (REPO / ".github/workflows/alopex-performance.yml").read_text()
        qualification = source.split("  qualification:\n", 1)[1].split(
            "  wal_cost_682:\n", 1)[0]
        for phase in ("--admission-only", "--acquire-assets", "--execute"):
            self.assertIn("hosted_test_env_qualification_four_case.py " + phase,
                          qualification)
        self.assertIn("scripts/ops/four_case_job.py install", qualification)
        self.assertIn("scripts/ops/four_case_job.py run", qualification)
        self.assertIn("scripts/ops/four_case_job.py export", qualification)
        self.assertIn("scripts/ops/four_case_job.py verify-public", qualification)
        self.assertIn("Publish public four-case qualification", qualification)
        self.assertIn("path: native-qualification-export/public.json", qualification)
        self.assertIn("if: always() && !inputs.sql_four_case_mode", qualification)
        self.assertIn("steps.four_case_qualification.outcome == 'success'", qualification)
        verify_step = qualification.split(
            "      - name: Verify four-case public measurement\n", 1)[1].split(
                "      - name:", 1)[0]
        self.assertIn("ALOPEX_FOUR_CASE_LOCK_SHA256: ${{ inputs.four_case_lock_sha256 }}",
                      verify_step)
        self.assertIn("steps.matrix_public.outcome == 'success'", qualification)
        self.assertNotIn("scripts/ops/same_job_four_case.py", qualification)
        self.assertNotIn("native-sql-four-case-export/private-evidence.cms",
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
        checks = []
        def capacity(root, extra):
            checks.append(extra)
            return {"stop_bytes": 40_000_000_000}
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "_capacity_before_seal", side_effect=capacity):
            public = job.prepare_public_export(self.env)
        self.assertEqual(public["status"], "prepared")
        self.assertEqual(public["matrix_status"], "failed")
        self.assertEqual(public["matrix_error_code"], "INCOMPLETE")
        self.assertEqual(len(checks), 1)
        self.assertEqual({path.name for path in
                          (self.root / "native-sql-four-case-export").iterdir()},
                         {"public.json"})

    def test_seal_rejects_unreviewed_export_file(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        member = private / "receipt.private.json"
        member.write_text("{}")
        member.chmod(0o600)
        export = self.root / "native-sql-four-case-export"
        export.mkdir(mode=0o700)
        (export / "private-evidence.cms").write_bytes(b"private")
        with patch.object(job, "REPO", self.root), \
             self.assertRaisesRegex(ValueError, "EVIDENCE_STATE"):
            job.prepare_public_export(self.env)
        self.assertFalse((export / "public.json").exists())

    def test_passed_matrix_exports_only_verified_public_measurement(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        self.env["GITHUB_RUN_ID"] = JOB
        self.timing_fixture(private, JOB)
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "_matrix_outcome", return_value={
                 "matrix_status": "passed", "matrix_error_code": None}), \
             patch.object(job, "_capacity_before_seal", return_value={}) as capacity:
            public = job.prepare_public_export(self.env)
        self.assertNotIn("ciphertext_sha256", public)
        self.assertEqual(public["matrix_status"], "passed")
        self.assertEqual(len(public["measurement"]["timings"]), 504)
        self.assertEqual(len(public["measurement"]["operations"]), 504)
        self.assertEqual(len(public["measurement"]["stages"]), 1764)
        self.assertLessEqual((self.root / "native-sql-four-case-export/public.json").stat().st_size,
                             job.MAX_PUBLIC_BYTES)
        self.assertEqual(set(public["measurement"]["paired"]),
                         set(job.controller.adapter.CASES))
        self.assertNotIn('"trace_id"', json.dumps(public))
        self.assertNotIn('"pid"', json.dumps(public))
        self.assertEqual({path.name for path in
                          (self.root / "native-sql-four-case-export").iterdir()},
                         {"public.json"})
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
            original_timing = timing_path.read_bytes()
            lines = timing_path.read_text().splitlines()
            altered = json.loads(lines[0])
            altered["elapsed_seconds"] = .012
            lines[0] = json.dumps(altered, separators=(",", ":"))
            timing_path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
            timing_path.write_bytes(original_timing)
            operation_path = private / "public-operations" / "aggregate-trial-0-on.jsonl"
            original_operation = operation_path.read_bytes()
            lines = operation_path.read_text().splitlines()
            altered = json.loads(lines[0])
            altered["client_user_cpu_seconds"] += .01
            lines[0] = json.dumps(altered, separators=(",", ":"))
            operation_path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
            operation_path.write_bytes(original_operation)
            client_path = (private / "aggregate-0" / "owned-runs" /
                           f"f712-{JOB}-aggregate-0" / "collected" / "on" /
                           "client.ndjson")
            original_client = client_path.read_bytes()
            lines = client_path.read_text().splitlines()
            changed = False
            for index, line in enumerate(lines):
                row = json.loads(line)
                if row["phase"] == "measured" and row["sample"] == 0:
                    row["outer_ns"] += 1
                    lines[index] = json.dumps(row)
                    changed = True
                    break
            self.assertTrue(changed)
            client_path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
            client_path.write_bytes(original_client)
            stage_path = private / "public-stages" / "aggregate-trial-0-stages.jsonl"
            lines = stage_path.read_text().splitlines()
            altered = json.loads(lines[0])
            altered["duration_ns"] += 1
            lines[0] = json.dumps(altered, separators=(",", ":"))
            stage_path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                job.verify_public_export(self.env)
        self.assertEqual(capacity.call_count, 1)

    def test_passed_matrix_without_public_inventory_fails_closed(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        receipt = private / "receipt.public.json"
        receipt.write_text("{}")
        receipt.chmod(0o600)
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "_matrix_outcome", return_value={
                 "matrix_status": "passed", "matrix_error_code": None}), \
             self.assertRaises(ValueError):
            job.prepare_public_export(self.env)
        self.assertFalse((self.root / "native-sql-four-case-export/public.json").exists())

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

        with patch.object(job, "_capacity_before_seal", return_value={}), \
             patch.object(evidence_assembly, "assemble_public_timing",
                          side_effect=lambda *a, **kw: order.append("public")), \
             patch.object(evidence_assembly, "assemble_files",
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

        def save_failure(code, count):
            private_receipt = {"schema": job.controller.SCHEMA,
                               "status": "failed", "error_code": code,
                               "pair_count": count,
                               "performance_qualification": False}
            raw = json.dumps(private_receipt, sort_keys=True).encode()
            inner = private / "receipt.private.json"
            inner.write_bytes(raw)
            inner.chmod(0o600)
            save({**private_receipt, "run_attempt": "3",
                  "private_receipt_sha256": hashlib.sha256(raw).hexdigest()})

        save({"schema": job.controller.SCHEMA, "status": "passed",
              "performance_qualification": False, "job_id": JOB,
              "run_attempt": "2", "qualification_binding_sha256": "a" * 64})
        with patch.object(job.controller, "verify_public_receipt", return_value={
                "source_commit": pin["source_commit"],
                "assets_digest": pin["assets_digest"]}) as verified:
            self.assertEqual(job._matrix_outcome(private, self.env), {
                "matrix_status": "passed", "matrix_error_code": None,
                "completed_pairs": 12,
                **{key: pin[key] for key in
                   ("source_commit", "source_tree", "artifact_id", "artifact_sha256",
                    "server_sha256", "wheel_sha256", "assets_digest")}})
        self.assertEqual(verified.call_count, 1)
        with patch.object(job.controller, "verify_public_receipt", return_value={
                "source_commit": pin["source_commit"],
                "assets_digest": "b" * 64}):
            self.assertEqual(job._matrix_outcome(private, self.env), {
                "matrix_status": "failed", "matrix_error_code": "INCOMPLETE",
                "completed_pairs": None})
        self.env["GITHUB_RUN_ATTEMPT"] = "3"
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "INCOMPLETE",
            "completed_pairs": None})
        save_failure("PAIR_FAILED", 7)
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "PAIR_FAILED",
            "completed_pairs": 7})
        tampered = json.loads(path.read_text())
        tampered["pair_count"] = 8
        save(tampered)
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "INCOMPLETE",
            "completed_pairs": None})
        save({"schema": job.controller.SCHEMA, "status": "failed",
              "performance_qualification": False, "run_attempt": "3",
              "error_code": "private detail"})
        self.assertEqual(job._matrix_outcome(private, self.env), {
            "matrix_status": "failed", "matrix_error_code": "INCOMPLETE",
            "completed_pairs": None})


if __name__ == "__main__":
    unittest.main()
