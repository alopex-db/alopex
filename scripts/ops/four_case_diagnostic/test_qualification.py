"""Focused offline checks for the four-case same-job qualification contract."""
import copy
import hashlib
import json
import unittest

from . import qualification as q


SOURCE = "1" * 39 + "2"
TREE = "2" * 39 + "3"
CONTROLLER = "3" * 39 + "4"
OPERATORS = {"four_case_supervisor.py": "4" * 63 + "5",
             "four_case_worker.py": "5" * 63 + "6"}
JOB = "12345678"


def fixture():
    pin = {"source_commit": SOURCE, "source_tree": TREE,
           "artifact_id": 77, "artifact_sha256": "6" * 63 + "7",
           "assets_digest": "7" * 63 + "8", "server_sha256": "8" * 63 + "9",
           "wheel_sha256": "9" * 63 + "a", "controller_commit": CONTROLLER,
           "controller_digest": "a" * 63 + "b", "asset_run_id": 88,
           "asset_run_head": "b" * 39 + "c", "zip_sha256": "6" * 63 + "7",
           "asset_zip_size_bytes": 12345,
           "asset_file_digests": {"server": "8" * 63 + "9",
                                  "wheel": "9" * 63 + "a",
                                  "library": "1" * 63 + "a",
                                  "receipt": "2" * 63 + "b",
                                  "harness": "3" * 63 + "c"},
           "common_verifier_sha256": "4" * 63 + "d",
           "harness_commit": "c" * 39 + "d", "image_lock_sha256": "d" * 63 + "e",
           "seccomp_sha256": "e" * 63 + "f", "operator_sha256": OPERATORS}
    lock = {"schema": "alopex.sql-four-case-pins/v1", "pin": pin}
    cases = [{"id": case, "status": "passed", "error_code": None}
             for case in q.CASES]
    proof = {"job_id": JOB,
             "qualification_public": {"schema": "alopex.hosted-test-env-qualification/v1",
                                      "case": "HOST_QUALIFICATION", "outcome": "passed",
                                      "error_code": None, "source_commit": SOURCE,
                                      "asset_digest": pin["assets_digest"], "cases": cases},
             "profile": {"run_id": "hosted-804-" + JOB,
                         "assets_digest": pin["assets_digest"],
                         "image_lock_sha256": pin["image_lock_sha256"],
                         "security": {"lsm_kind": "apparmor",
                                      "lsm_profile": "docker-default",
                                      "seccomp_sha256": pin["seccomp_sha256"]},
                         "storage": {"stop_bytes": 40_000_000_000,
                                     "reserve_bytes": 1_000_000_000,
                                     "poll_seconds": 10}},
             "acquisition": {"schema": "alopex.sql-four-case-acquisition/v2",
                             "qualification_job_id": JOB,
                             "run": pin["asset_run_id"],
                             "head": pin["asset_run_head"],
                             "artifact_id": pin["artifact_id"],
                             "zip_sha256": pin["zip_sha256"],
                             "zip_size_bytes": pin["asset_zip_size_bytes"],
                             "assets_digest": pin["assets_digest"]},
             "product": {"source_commit": SOURCE, "source_tree": TREE,
                         "artifact_sha256": pin["artifact_sha256"],
                         "server_sha256": pin["server_sha256"],
                         "wheel_sha256": pin["wheel_sha256"],
                         "assets_digest": pin["assets_digest"]},
             "runs": {name: {"phase": "deleted", "collection_verified": True,
                             "cleanup_errors": []}
                      for name in ("primary", "repeat", "probe")},
             "acceptance": {"status": "passed", "cases": cases},
             "accepted_public": {"status": "passed", "assets_digest": pin["assets_digest"],
                                 "image_digest": "f" * 63 + "0",
                                 "controller_commit": CONTROLLER,
                                 "controller_digest": pin["controller_digest"]},
             "image_digest": "f" * 63 + "0",
             "qualification_public_sha256": "1" * 63 + "2",
             "acceptance_sha256": "2" * 63 + "3",
             "profile_sha256": "3" * 63 + "4",
             "temp_snapshot_verified": True}
    binding = {"job_id": JOB,
               "qualification_public_sha256": proof["qualification_public_sha256"],
               "acceptance_sha256": proof["acceptance_sha256"],
               "profile_sha256": proof["profile_sha256"],
               "image_digest": proof["image_digest"],
               "assets_digest": pin["assets_digest"]}
    receipt = {"job_id": JOB, "source_commit": SOURCE, "source_tree": TREE,
               "artifact_id": pin["artifact_id"],
               "artifact_sha256": pin["artifact_sha256"],
               "assets_digest": pin["assets_digest"],
               "server_sha256": pin["server_sha256"],
               "wheel_sha256": pin["wheel_sha256"],
               "controller_commit": CONTROLLER, "qualification_status": "passed",
               "acceptance_cases": ["E01", "E02", "E03", "E04", "E05", "E06",
                                    "E07", "S01", "S02", "S03", "F01", "F02",
                                    "R01", "P01"], "image_digest": proof["image_digest"],
               "binding_sha256": hashlib.sha256(json.dumps(
                   binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
               "security_profile_digest": pin["seccomp_sha256"]}
    return lock, proof, receipt


def checked(lock, proof, receipt, *, job_id=JOB):
    return q.validate(lock, proof, receipt, job_id=job_id,
                      expected_source_commit=SOURCE, expected_source_tree=TREE,
                      expected_controller_commit=CONTROLLER,
                      expected_operator_sha256=OPERATORS)


class QualificationContractTests(unittest.TestCase):
    def test_current_job_full_proof_and_receipt(self):
        lock, proof, receipt = fixture()
        self.assertEqual(checked(lock, proof, receipt)["job_id"], JOB)

    def test_rejects_wrong_lock_identity_and_extra_field(self):
        lock, proof, receipt = fixture()
        lock["schema"] = "alopex.sql-diagnostic-pins/v1"
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            checked(lock, proof, receipt)
        for path, value in (("source_commit", "0" * 40),
                            ("source_tree", "0" * 40),
                            ("server_sha256", "0" * 64),
                            ("operator_sha256", {}),
                            ("image_lock_sha256", "0" * 64),
                            ("seccomp_sha256", "0" * 64)):
            with self.subTest(path=path):
                lock, proof, receipt = fixture()
                lock["pin"][path] = value
                with self.assertRaises(ValueError):
                    checked(lock, proof, receipt)
        for change in (lambda p: p["asset_file_digests"].pop("library"),
                       lambda p: p["asset_file_digests"].update(library="0" * 64),
                       lambda p: p.update(common_verifier_sha256="0" * 64)):
            lock, proof, receipt = fixture()
            change(lock["pin"])
            with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
                checked(lock, proof, receipt)
        lock, proof, receipt = fixture()
        lock["pin"]["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            checked(lock, proof, receipt)

    def test_rejects_prior_job_and_partial_checks(self):
        for change in (lambda p: p.update(job_id="12345677"),
                       lambda p: p["profile"].update(run_id="hosted-804-12345677"),
                       lambda p: p["qualification_public"]["cases"].pop(),
                       lambda p: p["qualification_public"]["cases"][0].update(status="failed"),
                       lambda p: p["acceptance"]["cases"][0].update(status="failed"),
                       lambda p: p["runs"]["probe"].update(collection_verified=False),
                       lambda p: p.update(temp_snapshot_verified=False)):
            lock, proof, receipt = fixture()
            change(proof)
            with self.assertRaises(ValueError):
                checked(lock, proof, receipt)

    def test_rejects_wrong_proof_and_receipt_binding(self):
        for change in (lambda p: p["qualification_public"].update(source_commit="0" * 40),
                       lambda p: p["acquisition"].update(assets_digest="0" * 64),
                       lambda p: p["acquisition"].update(run=87),
                       lambda p: p["acquisition"].update(head="0" * 40),
                       lambda p: p["acquisition"].update(zip_size_bytes=12344),
                       lambda p: p["acquisition"].update(qualification_job_id="12345677"),
                       lambda p: p["acquisition"].update(unexpected=True),
                       lambda p: p["accepted_public"].update(image_digest="0" * 64),
                       lambda p: p["profile"]["security"].update(lsm_profile="unconfined")):
            lock, proof, receipt = fixture()
            change(proof)
            with self.assertRaises(ValueError):
                checked(lock, proof, receipt)
        for name, value in (("job_id", "12345677"), ("binding_sha256", "0" * 64),
                            ("security_profile_digest", "0" * 64),
                            ("qualification_status", "failed")):
            lock, proof, receipt = fixture()
            receipt[name] = value
            with self.assertRaisesRegex(ValueError, "QUALIFICATION_BINDING"):
                checked(lock, proof, receipt)


if __name__ == "__main__":
    unittest.main()
