"""Source-bound metadata checks before private perf baseline reuse."""

import copy
import unittest

import stress_baseline_provenance as provenance


SOURCE = "a" * 40
DIGEST = "b" * 64
RUN_ID = 123456
REPO_ID = 5678


def fixture():
    run = {
        "id": RUN_ID, "path": provenance.WORKFLOW_PATH,
        "event": "schedule", "status": "completed", "conclusion": "success",
        "head_sha": SOURCE,
        "repository": {"id": REPO_ID, "full_name": provenance.REPOSITORY},
        "head_repository": {"id": REPO_ID, "full_name": provenance.REPOSITORY},
    }
    artifacts = [{
        "id": artifact_id, "name": name, "expired": False, "size_in_bytes": 1024,
        "digest": "sha256:" + ("1" if artifact_id == 11 else "2") * 64,
        "archive_download_url":
            f"https://api.github.com/repos/{provenance.REPOSITORY}/actions/artifacts/{artifact_id}/zip",
        "workflow_run": {"id": RUN_ID, "head_sha": SOURCE,
                         "repository_id": REPO_ID,
                         "head_repository_id": REPO_ID},
    } for artifact_id, name in ((11, provenance.PUBLIC_ARTIFACT),
                               (12, provenance.PRIVATE_ARTIFACT))]
    status = {"schema": "alopex.stress-public/v1", "case_id": "perf",
              "source_commit": SOURCE, "outcome": "passed", "error_code": "NONE",
              "ciphertext_sha256": DIGEST, "ciphertext_size": 1000}
    return dict(run=run, artifacts=artifacts, status=status,
                expected_run_id=RUN_ID)


class ProvenanceTests(unittest.TestCase):
    def test_accepts_exact_successful_scheduled_run(self):
        self.assertEqual(provenance.select(**fixture()),
                         (SOURCE, DIGEST, 11, "sha256:" + "1" * 64,
                          12, "sha256:" + "2" * 64))

    def test_rejects_wrong_run_event_source_or_conclusion(self):
        for field, value in (("id", 0), ("event", "workflow_dispatch"),
                             ("status", "in_progress"), ("conclusion", "failure"),
                             ("head_sha", "c" * 40),
                             ("path", ".github/workflows/other.yml")):
            with self.subTest(field=field):
                args = fixture()
                args["run"][field] = value
                with self.assertRaises(ValueError):
                    provenance.select(**args)

    def test_rejects_mismatched_or_missing_artifact(self):
        for change in ("missing", "duplicate", "expired", "source", "repository",
                       "oversized", "url", "digest"):
            with self.subTest(change=change):
                args = fixture()
                if change == "missing":
                    args["artifacts"].pop()
                elif change == "duplicate":
                    args["artifacts"].append(copy.deepcopy(args["artifacts"][0]))
                elif change == "expired":
                    args["artifacts"][0]["expired"] = True
                elif change == "source":
                    args["artifacts"][0]["workflow_run"]["head_sha"] = "c" * 40
                elif change == "repository":
                    args["artifacts"][0]["workflow_run"]["repository_id"] = 0
                elif change == "oversized":
                    args["artifacts"][1]["size_in_bytes"] = 106_000_000
                elif change == "digest":
                    args["artifacts"][1]["digest"] = "sha256:invalid"
                else:
                    args["artifacts"][1]["archive_download_url"] = "https://example.invalid/zip"
                with self.assertRaises(ValueError):
                    provenance.select(**args)

    def test_rejects_unmatched_public_status(self):
        for field, value in (("case_id", "asan"), ("source_commit", "c" * 40),
                             ("outcome", "failed"), ("error_code", "PERF"),
                             ("ciphertext_size", True),
                             ("ciphertext_sha256", "not-a-digest")):
            with self.subTest(field=field):
                args = fixture()
                args["status"][field] = value
                with self.assertRaises(ValueError):
                    provenance.select(**args)


if __name__ == "__main__":
    unittest.main()
