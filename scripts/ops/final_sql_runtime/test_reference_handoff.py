"""The host must not expose original ZIPs to a different runner job."""
from contextlib import contextmanager
import json
import unittest

from .reference_handoff import fetch_and_verify, verify_assignment


class JsonResponse:
    status = 200

    def __init__(self, payload):
        self.data = json.dumps(payload).encode()

    def getheader(self, name):
        return str(len(self.data)) if name == "Content-Length" else None

    def read(self, count):
        return self.data[:count]


class ReferenceHandoffTest(unittest.TestCase):
    def setUp(self):
        self.expected = {"run_id": 17, "repository_id": 19, "workflow_id": 23,
                         "workflow_path": ".github/workflows/reference-staging.yml",
                         "head_sha": "a" * 40, "job_name": "stage_original_references",
                         "runner_id": 29, "runner_group_id": 31,
                         "runner_label": "alopex-712-originals-one-use"}
        self.run = {"id": 17, "repository": {"id": 19}, "workflow_id": 23,
                    "path": ".github/workflows/reference-staging.yml@candidate",
                    "head_sha": "a" * 40, "run_attempt": 1,
                    "event": "workflow_dispatch", "status": "in_progress",
                    "conclusion": None}
        self.runner = {"id": 29, "status": "online", "busy": True,
                       "ephemeral": True,
                       "labels": [{"name": "alopex-712-originals-one-use"}]}
        self.jobs = {"total_count": 1, "jobs": [
            {"id": 37, "run_id": 17, "head_sha": "a" * 40,
             "runner_id": 29, "runner_group_id": 31,
             "name": "stage_original_references", "status": "in_progress",
             "conclusion": None, "labels": ["alopex-712-originals-one-use"]}]}

    def verify(self):
        return verify_assignment(expected=self.expected, run=self.run,
                                 attempt_jobs=self.jobs, runner=self.runner)

    def test_exact_active_assignment(self):
        self.assertEqual(self.verify(), {"run_id": 17, "job_id": 37,
                                         "runner_id": 29, "head_sha": "a" * 40})

    def test_unassigned_runner_is_pending_only_while_idle(self):
        self.jobs = {"total_count": 0, "jobs": []}
        self.runner["busy"] = False
        self.assertIsNone(self.verify())
        self.runner["busy"] = True
        with self.assertRaisesRegex(ValueError, "^HANDOFF_RUNNER_OCCUPIED$"):
            self.verify()

    def test_other_workflow_or_rerun_rejected(self):
        for key, value in (("workflow_id", 24), ("path", ".github/workflows/other.yml@candidate"),
                           ("head_sha", "b" * 40),
                           ("run_attempt", 2), ("event", "pull_request")):
            original = self.run[key]
            self.run[key] = value
            with self.assertRaisesRegex(ValueError, "^HANDOFF_RUN$"):
                self.verify()
            self.run[key] = original

    def test_wrong_job_or_group_rejected(self):
        job = self.jobs["jobs"][0]
        for key, value in (("name", "untrusted"), ("runner_group_id", 32),
                           ("status", "completed"), ("head_sha", "b" * 40)):
            original = job[key]
            job[key] = value
            with self.assertRaisesRegex(ValueError, "^HANDOFF_JOB$"):
                self.verify()
            job[key] = original

    def test_busy_or_non_ephemeral_runner_rejected(self):
        for key, value in (("busy", False), ("ephemeral", False),
                           ("status", "offline")):
            original = self.runner[key]
            self.runner[key] = value
            with self.assertRaisesRegex(ValueError, "^HANDOFF_RUNNER$|^HANDOFF_JOB$"):
                self.verify()
            self.runner[key] = original

    def test_truncated_job_page_rejected(self):
        self.jobs["total_count"] = 2
        with self.assertRaisesRegex(ValueError, "^HANDOFF_JOB_LIST$"):
            self.verify()

    def test_api_fetch_uses_exact_attempt_and_runner(self):
        seen = []
        responses = [self.run, self.jobs, self.runner]

        @contextmanager
        def transport(url, headers, timeout):
            seen.append((url, headers, timeout))
            yield JsonResponse(responses[len(seen) - 1])

        result = fetch_and_verify(repository="asopitech/alopex", expected=self.expected,
                                  token="test-token", transport=transport)
        self.assertEqual(result["job_id"], 37)
        self.assertEqual([item[0].split("/alopex")[-1] for item in seen], [
            "/actions/runs/17",
            "/actions/runs/17/attempts/1/jobs?per_page=100",
            "/actions/runners/29"])
        self.assertTrue(all(item[1]["Authorization"] == "Bearer test-token"
                            for item in seen))


if __name__ == "__main__":
    unittest.main()
