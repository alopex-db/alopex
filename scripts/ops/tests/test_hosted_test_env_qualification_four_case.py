"""Source binding and fail-closed four-case qualification tests; no Docker."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / "hosted_test_env_qualification_four_case.py"
SPEC = importlib.util.spec_from_file_location("hosted_four_case", SCRIPT)
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


def digest(data):
    return hashlib.sha256(data).hexdigest()


class FourCaseQualification(unittest.TestCase):
    def test_reused_host_controller_is_exactly_pinned(self):
        self.assertIsNotNone(subject._base().admission_facts)
        with patch.object(subject, "BASE_SHA256", "0" * 64):
            with self.assertRaisesRegex(ValueError, "OPERATOR_IDENTITY"):
                subject._base()

    def test_lock_digest_is_independent_of_job_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            lock = root / subject.LOCK_RELATIVE
            lock.parent.mkdir(mode=0o700)
            lock.write_bytes(b"reviewed lock")
            lock.chmod(0o600)
            environment = {"RUNNER_TEMP": str(root), "GITHUB_ACTIONS": "true",
                           "RUNNER_ENVIRONMENT": "github-hosted",
                           "GITHUB_REPOSITORY": "alopex-db/alopex",
                           "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2"}
            self.assertEqual(subject.reviewed_lock_path(environment), lock)
            with patch.object(subject.entrypoint, "load_reviewed_lock",
                              side_effect=ValueError("MISSING_PIN")) as load:
                with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
                    subject.load_inputs(digest(b"reviewed lock"), environment=environment)
            load.assert_called_once_with(lock, digest(b"reviewed lock"))
            environment["GITHUB_RUN_ID"] = "456"
            self.assertEqual(subject.reviewed_lock_path(environment), lock)
            environment["GITHUB_RUN_ATTEMPT"] = "0"
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                subject.load_inputs(digest(b"reviewed lock"), environment=environment)
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                subject.load_inputs(digest(b"reviewed lock"), environment={})

    def test_lock_location_rejects_path_redirection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            outside = root / "outside"
            outside.write_bytes(b"lock")
            (root / subject.LOCK_RELATIVE).parent.mkdir(mode=0o700)
            (root / subject.LOCK_RELATIVE).symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
                subject.reviewed_lock_path({"RUNNER_TEMP": str(root)})
            (root / subject.LOCK_RELATIVE).unlink()
            (root / subject.LOCK_RELATIVE).write_bytes(b"lock")
            (root / subject.LOCK_RELATIVE).chmod(0o644)
            with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
                subject.reviewed_lock_path({"RUNNER_TEMP": str(root)})

    def test_acquisition_installs_v2_receipt_and_archive_at_attestor_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            assets_root = root / "alopex-804-candidate"
            lock = {"pin": {"assets_digest": "b" * 64}}

            def fake_acquire(_lock, *, job_id, directory, fetch):
                directory.mkdir(mode=0o700)
                (directory / "assets-root" / "assets").mkdir(parents=True)
                (directory / "assets-root" / "assets" / "assets.json").write_text("{}")
                (directory / "candidate.original.zip").write_bytes(b"archive")
                (directory / "acquisition.private.json").write_text("{}")
                return {"assets_digest": "b" * 64}

            with patch.object(subject.acquire, "acquire", side_effect=fake_acquire), \
                 patch.object(subject.acquisition, "verify", return_value={}) as verify:
                subject.acquire_assets(lock, private=private, assets_root=assets_root,
                                       job_id="123", fetch=Mock())
            self.assertEqual((private / "candidate.original.zip").read_bytes(), b"archive")
            self.assertTrue((private / "acquisition.private.json").is_file())
            self.assertTrue((assets_root / "assets/assets.json").is_file())
            self.assertEqual(verify.call_args.kwargs["job_id"], "123")
            self.assertEqual(verify.call_args.kwargs["manifest_path"],
                             assets_root / "assets/assets.json")

    def test_old_asset_identity_is_not_used(self):
        base = subject._base()
        pin = {"controller_digest": "1" * 64, "image_lock_sha256": "2" * 64,
               "seccomp_sha256": "3" * 64, "source_commit": "4" * 40,
               "assets_digest": "5" * 64}
        subject._configure_base(base, pin)
        self.assertEqual(base.PRODUCT, pin["source_commit"])
        self.assertEqual(base.ASSETS_DIGEST, pin["assets_digest"])
        self.assertEqual(base.POLICY_DIGEST, pin["seccomp_sha256"])

    def test_verification_rejects_stale_manifest_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "assets").mkdir()
            (root / "assets/assets.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "IDENTITY"):
                subject.verified_inputs(Mock(), {"assets_digest": "0" * 64}, root)

    def test_public_qualification_writes_only_typed_status(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Mock(EXPORT=Path(directory) / "export",
                        EXPECTED_TEMP_SNAPSHOT=None,
                        ERRORS={"INCOMPLETE": 1, "INTERNAL": 1})
            failed = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "failed",
                      "error_code": "INCOMPLETE"}
            subject.export_public_qualification(base, failed)
            output = base.EXPORT / "public.json"
            self.assertEqual(json.loads(output.read_bytes()), failed)
            self.assertEqual({path.name for path in base.EXPORT.iterdir()}, {"public.json"})
            self.assertEqual(os.stat(output).st_mode & 0o777, 0o600)
            passed = {**failed, "outcome": "passed", "error_code": None,
                      "source_commit": "a" * 40, "asset_digest": "b" * 64,
                      "cases": [{"id": case, "status": "passed", "error_code": None}
                                for case in sorted(subject.qualification.CASES)]}
            subject.export_public_qualification(base, passed)
            self.assertEqual(json.loads(output.read_bytes()), passed)
            self.assertEqual({path.name for path in base.EXPORT.iterdir()}, {"public.json"})

    def test_failed_final_budget_check_cannot_replace_public_status(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Mock(EXPORT=Path(directory) / "export",
                        EXPECTED_TEMP_SNAPSHOT=None,
                        ERRORS={"INCOMPLETE": 1})
            public = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "failed",
                      "error_code": "INCOMPLETE"}
            subject.export_public_qualification(base, public)
            original = (base.EXPORT / "public.json").read_bytes()
            passed = {**public, "outcome": "passed", "error_code": None,
                      "source_commit": "a" * 40, "asset_digest": "b" * 64,
                      "cases": [{"id": case, "status": "passed", "error_code": None}
                                for case in sorted(subject.qualification.CASES)]}
            budget = Mock()
            budget.check.side_effect = ValueError("CAPACITY")
            with self.assertRaisesRegex(ValueError, "CAPACITY"):
                subject.export_public_qualification(base, passed, budget)
            self.assertEqual((base.EXPORT / "public.json").read_bytes(), original)
            self.assertEqual({path.name for path in base.EXPORT.iterdir()}, {"public.json"})

    def test_public_qualification_rejects_local_fields_and_extra_files(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Mock(EXPORT=Path(directory) / "export",
                        EXPECTED_TEMP_SNAPSHOT=None,
                        ERRORS={"INCOMPLETE": 1})
            public = {"schema": "alopex.hosted-test-env-qualification/v1",
                      "case": "HOST_QUALIFICATION", "outcome": "failed",
                      "error_code": "INCOMPLETE"}
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                subject.export_public_qualification(base,
                                                    {**public, "local_path": "private"})
            subject.export_public_qualification(base, public)
            (base.EXPORT / "private-evidence.cms").write_bytes(b"private")
            with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
                subject.export_public_qualification(base, public)


if __name__ == "__main__":
    unittest.main()
