"""Offline #712 same-job gate tests; no Docker, fetch or dispatch."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import same_job
from scripts.ops.four_case_diagnostic import qualification
from scripts.ops.four_case_diagnostic.test_qualification import (
    CONTROLLER, JOB, OPERATORS, SOURCE, TREE, fixture,
)


def fixture_live():
    lock, proof, receipt = fixture()
    cases = [row for row in proof["acceptance"]["cases"] if row["id"] != "R01"]
    cases.sort(key=lambda row: ("ESFP".index(row["id"][0]), row["id"]))
    observations = {}
    for name, suffix in same_job.RUNS.items():
        selected = cases if name != "probe" else [row for row in cases if row["id"] in same_job.PROBE]
        observations[name] = {
            "record": {"schema": "alopex.test-env-run/v1",
                       "run_id": "hosted-804-" + JOB + suffix,
                       "phase": "deleted", "resources": [{"id": name + "-owned"}],
                       "ownership_token": name + "-token",
                       "expected_cases": sorted(qualification.CASES if name == "primary"
                                                else same_job.PROBE if name == "probe"
                                                else same_job.EXECUTED),
                       "identities": {"controller_commit": CONTROLLER,
                                      "controller_digest": lock["pin"]["controller_digest"],
                                      "assets_digest": lock["pin"]["assets_digest"],
                                      "image_digest": proof["image_digest"]}},
            "result": {"status": "passed", "cases": copy.deepcopy(selected)},
            "collection_sha256": ("5" if name == "primary" else
                                  "6" if name == "repeat" else "7") * 63 + "8"}
    proof["acceptance"]["cases"] = copy.deepcopy(cases) + [{
        "id": "R01", "status": "passed", "error_code": None,
        "evidence": {"first": observations["primary"]["collection_sha256"],
                     "repeat": observations["repeat"]["collection_sha256"]}}]
    expected = {"lock_sha256": "1" * 63 + "2", "source_commit": SOURCE,
                "source_tree": TREE, "controller_commit": CONTROLLER,
                "operator_sha256": OPERATORS}
    environment = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
                   "GITHUB_REPOSITORY": "alopex-db/alopex", "GITHUB_RUN_ID": JOB,
                   "GITHUB_RUN_ATTEMPT": "2", "GITHUB_SHA": CONTROLLER,
                   "EXPECTED_CONTROLLER_SHA": CONTROLLER}
    return lock, proof, receipt, observations, expected, environment


def checked(parts):
    lock, proof, receipt, observations, expected, environment = parts
    return same_job.verify(lock, proof, receipt, observations,
                           expected=expected, environment=environment)


class SameJobTests(unittest.TestCase):
    def test_fresh_complete_job(self):
        result = checked(fixture_live())
        self.assertEqual((result["job_id"], result["run_attempt"]), (JOB, "2"))
        self.assertEqual(result["controller_digest"], fixture_live()[0]["pin"]["controller_digest"])
        self.assertEqual(len(result["qualification_cases"]), 14)

    def test_stale_job_attempt_and_source(self):
        for index, key, value in ((5, "GITHUB_RUN_ID", "12345677"),
                                  (5, "GITHUB_RUN_ATTEMPT", "0"),
                                  (4, "source_commit", "0" * 40),
                                  (4, "source_tree", "0" * 40),
                                  (5, "GITHUB_SHA", "0" * 40)):
            with self.subTest(key=key):
                parts = fixture_live()
                parts[index][key] = value
                with self.assertRaises(ValueError):
                    checked(parts)

    def test_missing_r01_or_repeat(self):
        for change in (lambda p: p[1]["acceptance"]["cases"].pop(),
                       lambda p: p[1]["acceptance"]["cases"][-1]["evidence"].update(repeat="0" * 64),
                       lambda p: p[3].pop("repeat"),
                       lambda p: p[3]["repeat"]["result"]["cases"].pop()):
            parts = fixture_live()
            change(parts)
            with self.assertRaises(ValueError):
                checked(parts)

    def test_undeleted_or_uncollected_run(self):
        for change in (lambda p: p[3]["probe"]["record"].update(phase="collected"),
                       lambda p: p[3]["primary"]["record"].update(resources=[]),
                       lambda p: p[1]["runs"]["repeat"].update(collection_verified=False)):
            parts = fixture_live()
            change(parts)
            with self.assertRaises(ValueError):
                checked(parts)

    def test_image_and_profile_mismatch(self):
        for change in (lambda p: p[3]["primary"]["record"]["identities"].update(image_digest="0" * 64),
                       lambda p: p[1]["profile"]["security"].update(lsm_profile="unconfined"),
                       lambda p: p[2].update(security_profile_digest="0" * 64)):
            parts = fixture_live()
            change(parts)
            with self.assertRaises(ValueError):
                checked(parts)

    def test_private_reader_rejects_symlink_mode_oversize_and_duplicate_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "proof.json"
            path.write_text('{"ok":true}')
            path.chmod(0o600)
            self.assertEqual(same_job._read_private(path)[0], {"ok": True})
            link = root / "link.json"
            link.symlink_to(path)
            with self.assertRaisesRegex(ValueError, "COLLECTION"):
                same_job._read_private(link)
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "COLLECTION"):
                same_job._read_private(path)
            path.chmod(0o600)
            path.write_bytes(b" " * 100)
            with self.assertRaisesRegex(ValueError, "COLLECTION"):
                same_job._read_private(path, max_bytes=99)
            path.write_text('{"ok":true,"ok":false}')
            with self.assertRaisesRegex(ValueError, "COLLECTION"):
                same_job._read_private(path)

    def test_live_reader_checks_collection_digest_without_docker(self):
        parts = fixture_live()
        lock, proof, receipt, observations, expected, environment = parts
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment["RUNNER_TEMP"] = str(root)
            lock_path = root / "alopex-712-lock/lock.json"
            lock_path.parent.mkdir()
            for name, suffix in same_job.RUNS.items():
                run_root = root / "alopex-804-qualification/runs" / ("hosted-804-" + JOB + suffix)
                (run_root / "collected").mkdir(parents=True)
                result = json.dumps(observations[name]["result"]).encode()
                collection = json.dumps({"result.json": hashlib.sha256(result).hexdigest()}).encode()
                record = json.dumps(observations[name]["record"]).encode()
                for path, raw in ((run_root / "collected/result.json", result),
                                  (run_root / "collection.json", collection),
                                  (run_root / "run.json", record)):
                    path.write_bytes(raw)
                    path.chmod(0o600)
                observations[name]["collection_sha256"] = hashlib.sha256(collection).hexdigest()
            proof["acceptance"]["cases"][-1]["evidence"] = {
                "first": observations["primary"]["collection_sha256"],
                "repeat": observations["repeat"]["collection_sha256"]}
            with patch.object(same_job.entrypoint, "load_reviewed_lock", return_value=lock), \
                 patch.object(same_job.entrypoint, "attest_current_job", return_value=(proof, receipt)):
                arguments = {"lock_path": lock_path, "expected": expected,
                             "repo": root, "runner_temp": root,
                             "environment": environment}
                self.assertEqual(same_job.attest_live(**arguments)["job_id"], JOB)
                result_path = root / "alopex-804-qualification/runs" / ("hosted-804-" + JOB) / "collected/result.json"
                result_path.write_text('{"status":"passed","cases":[]}')
                result_path.chmod(0o600)
                with self.assertRaisesRegex(ValueError, "COLLECTION"):
                    same_job.attest_live(**arguments)


if __name__ == "__main__":
    unittest.main()
