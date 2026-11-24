"""Focused offline checks for final SQL workflow ingress."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import io
from types import SimpleNamespace

from scripts.ops import final_sql_job as job
from scripts.ops.final_sql_runtime.controller import ControllerFailure
from scripts.ops.final_sql_runtime import test_reference_public as references


class FinalSqlJobTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.runner = self.root / "runner"
        self.runner.mkdir()
        fixture = references.PublicReferenceTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.refs = fixture.pins
        products = {}
        for subject in ("before", "published"):
            pin = self.refs[subject]
            products[subject] = {
                "source": {"commit": pin["source_commit"], "tree": pin["source_tree"],
                           "profile": "release"},
                "assets_digest": pin["manifest_sha256"],
                "files": [row for row in pin["files"] if row["role"] != "harness"],
                "platform": pin["platform"], "version": pin["version"]}
        products["candidate"] = {
            "source": {"commit": job.subject_binding.SOURCE_COMMITS["candidate"],
                       "tree": "a" * 40, "profile": "release"},
            "assets_digest": "b" * 64,
            "files": [{"role": role, "path": path, "size": 1,
                       "sha256": "c" * 64} for role, path in (
                           ("server", "bin/alopex-server"),
                           ("library", "lib/libalopex_sql_parser.so"),
                           ("wheel", "wheels/alopex-0.8.17-cp38-abi3-linux_x86_64.whl"),
                           ("receipt", "receipts/build.json"))],
            "platform": "linux/amd64", "version": "0.8.17"}
        self.lock = {
            "schema": job.SCHEMA,
            "qualification_expected": {
                "lock_sha256": "d" * 64,
                "source_commit": products["candidate"]["source"]["commit"],
                "source_tree": products["candidate"]["source"]["tree"],
                "controller_commit": "e" * 40,
                "operator_sha256": {name: "f" * 64 for name in
                                    job.four_case_qualification.REQUIRED_OPERATORS}},
            "reference_artifacts": self.refs,
            "reviewed_products": products,
            "worker_sha256": "1" * 64,
            "supervisor_sha256": "2" * 64,
            "controller_digest": "3" * 64,
            "declared_seconds": {"preflight": 60, "cell": 120},
            "trusted_download_hosts": ["reviewed.blob.core.windows.net"]}
        self.environment = {
            "RUNNER_TEMP": str(self.runner),
            "GITHUB_WORKSPACE": str(self.repo),
            "GITHUB_REPOSITORY": "alopex-db/alopex",
            "GITHUB_SHA": "e" * 40,
            "EXPECTED_CONTROLLER_SHA": "e" * 40,
            "ALOPEX_FOUR_CASE_LOCK_SHA256": "d" * 64,
            "GH_TOKEN": "private-token"}

    def encode(self):
        raw = job._canonical(self.lock)
        self.environment["ALOPEX_FINAL_SQL_LOCK_SHA256"] = hashlib.sha256(raw).hexdigest()
        self.environment["ALOPEX_FINAL_SQL_LOCK_B64"] = base64.b64encode(raw).decode()

    def test_install_exact_lock_and_reject_changed_or_noncanonical_bytes(self):
        self.encode()
        job.install(self.environment)
        path = self.runner / job.LOCK_DIR / "lock.json"
        self.assertEqual(path.read_bytes(), job._canonical(self.lock))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(job._installed(self.environment), self.lock)
        self.environment["ALOPEX_FINAL_SQL_LOCK_SHA256"] = "0" * 64
        with self.assertRaises(ValueError):
            job._installed(self.environment)
        raw = json.dumps(self.lock).encode()
        with self.assertRaises(ValueError):
            job._lock(raw, hashlib.sha256(raw).hexdigest())

    def test_lock_rejects_machine_path_and_unbounded_schedule(self):
        self.lock["reviewed_products"]["candidate"]["files"][0]["path"] = "/private/path"
        raw = job._canonical(self.lock)
        with self.assertRaises(ValueError):
            job._lock(raw, hashlib.sha256(raw).hexdigest())
        self.lock["reviewed_products"]["candidate"]["files"][0]["path"] = "bin/alopex-server"
        self.lock["declared_seconds"]["cell"] = 420
        raw = job._canonical(self.lock)
        with self.assertRaises(ValueError):
            job._lock(raw, hashlib.sha256(raw).hexdigest())

    def test_run_order_and_fixed_paths(self):
        self.encode()
        events = []
        proof = {"controller_digest": self.lock["controller_digest"],
                 "assets_digest": self.lock["reviewed_products"]["candidate"]["assets_digest"],
                 "source_commit": self.lock["reviewed_products"]["candidate"]["source"]["commit"],
                 "source_tree": self.lock["reviewed_products"]["candidate"]["source"]["tree"],
                 "image_digest": "4" * 64}
        def retrieve(**kwargs):
            events.append("retrieve")
            self.assertEqual(kwargs["token"], "private-token")
            self.assertEqual(kwargs["destination"], self.runner / "alopex-final-sql-references")
            return {"before": self.runner / "before.json",
                    "published": self.runner / "published.json"}
        def prepared(**kwargs):
            events.append("prepared")
            self.assertEqual(kwargs["manifests"]["candidate"],
                             self.runner / "alopex-804-candidate/assets/assets.json")
            self.assertEqual(kwargs["docs_checkout"], self.repo / "alopex-final-docs")
            self.assertEqual(kwargs["product_checkout"], self.repo / "alopex-final-product")
            self.assertEqual(kwargs["selected_image_digest"], "4" * 64)
            self.assertEqual(kwargs["docs_archive"].parent.stat().st_mode & 0o777, 0o700)
            self.assertEqual(kwargs["profile_dir"].stat().st_mode & 0o777, 0o700)
            self.assertEqual(kwargs["staging_root"].stat().st_mode & 0o777, 0o700)
            return kwargs["public_dir"] / "public.json"
        with patch.object(job, "REPO", self.repo), patch.object(job, "_installed",
             return_value=self.lock), patch.object(job.four_case_entrypoint,
             "load_reviewed_lock", return_value={"pin": {
                 "controller_digest": self.lock["controller_digest"]}}), patch.object(
             job.docs_harness, "_source", side_effect=lambda _: events.append("docs")), patch.object(
             job.product_harness, "verify", side_effect=lambda _: events.append("product")), patch.object(
             job, "_admit_storage", side_effect=lambda *_args, **_kwargs:
                 events.append("budget") or True):
            path = job.run(self.environment, retrieve=retrieve, prepared=prepared,
                           live=lambda **_: events.append("live") or proof,
                           profile=lambda _: events.append("profile"))
        self.assertEqual(path, self.repo / "native-final-sql-export/public.json")
        self.assertEqual(events, ["live", "profile", "budget", "docs", "product", "retrieve", "prepared"])

    def test_source_admission_uses_live_profile_and_reserves_growth(self):
        profile = self.runner / "profile.json"
        backing = self.runner / "backing"
        backing.mkdir()
        storage = {"runtime_storage_roots": [str(backing)],
                   "stop_bytes": job.STOP_BYTES,
                   "reserve_bytes": job.RESERVE_BYTES}
        common = SimpleNamespace(load_profile=lambda path:
                                 {"storage": storage} if path == profile else None)
        budget = SimpleNamespace(used=lambda: 33_000_000_000)
        runtime = lambda: (common, SimpleNamespace(Budget=lambda _: budget),
                           None, None, None)
        args = {"runner_temp": self.runner, "repo": self.repo}
        with patch.object(job.os, "statvfs", return_value=SimpleNamespace(
                f_bavail=10, f_frsize=1_000_000_000)):
            self.assertTrue(job._admit_storage(args, source_bytes=job.SOURCE_BYTES,
                select=lambda _: (profile, "a" * 64, backing), runtime=runtime))
        budget.used = lambda: 35_000_000_000
        with patch.object(job.os, "statvfs", return_value=SimpleNamespace(
                f_bavail=10, f_frsize=1_000_000_000)):
            with self.assertRaises(ValueError):
                job._admit_storage(args, source_bytes=job.SOURCE_BYTES,
                    select=lambda _: (profile, "a" * 64, backing), runtime=runtime)

    def test_missing_qualification_stops_before_fetch(self):
        self.encode()
        with patch.object(job, "REPO", self.repo), patch.object(job, "_installed",
             return_value=self.lock), patch.object(job.four_case_entrypoint,
             "load_reviewed_lock", return_value={"pin": {
                 "controller_digest": self.lock["controller_digest"]}}):
            with self.assertRaises(ValueError):
                job.run(self.environment, live=lambda **_: {},
                        retrieve=lambda **_: self.fail("downloaded"))
        self.assertFalse((self.repo / "native-final-sql-export/public.json").exists())

    def test_main_failure_emits_only_fixed_public_line(self):
        class Stdout:
            buffer = io.BytesIO()
        output = Stdout()
        with patch.object(job, "run", side_effect=RuntimeError("private-token /private/path")), \
                patch.object(job.sys, "stdout", output):
            self.assertEqual(job.main(["run"], self.environment), 1)
        self.assertEqual(output.buffer.getvalue(), job.public_upload.failure_log(
            ControllerFailure(
                "PUBLIC_INCOMPLETE", 0, ValueError("PUBLIC_INCOMPLETE"))))


if __name__ == "__main__":
    unittest.main()
