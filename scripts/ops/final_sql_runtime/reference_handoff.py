"""Read-only admission of the exact Actions job before original ZIP handoff.

The operator supplies an independently reviewed run/source/job pin and the
runner ID returned by a separate JIT registration. A missing assignment is
pending only while the runner is idle; a busy runner on another job fails
closed. This module never registers a runner, copies bytes or releases a gate.
"""
from __future__ import annotations

import re
import os
import time

from . import reference_download as download


HEX40 = re.compile(r"[0-9a-f]{40}")


def _require(ok: bool, code: str) -> None:
    if not ok:
        raise ValueError(code)


def _positive(value: object) -> bool:
    return type(value) is int and value > 0


def _review_expected(expected: dict) -> None:
    _require(type(expected) is dict and set(expected) == {
        "run_id", "repository_id", "workflow_id", "workflow_path",
        "head_sha", "job_name",
        "runner_id", "runner_group_id", "runner_label"} and
        all(_positive(expected[key]) for key in (
            "run_id", "repository_id", "workflow_id", "runner_id", "runner_group_id")) and
        type(expected["head_sha"]) is str and HEX40.fullmatch(expected["head_sha"]) and
        type(expected["workflow_path"]) is str and
        expected["workflow_path"].startswith(".github/workflows/") and
        expected["workflow_path"].endswith((".yml", ".yaml")) and
        "@" not in expected["workflow_path"] and ".." not in expected["workflow_path"] and
        type(expected["job_name"]) is str and expected["job_name"] and
        type(expected["runner_label"]) is str and expected["runner_label"],
        "HANDOFF_PIN")


def verify_assignment(*, expected: dict, run: dict, attempt_jobs: dict,
                      runner: dict) -> dict | None:
    """Return the exact active job identity, or None while safely unassigned.

    ``attempt_jobs`` must come from the attempt-specific jobs endpoint. The
    caller must independently authenticate all three GitHub API responses and
    recheck immediately before transferring any original bytes.
    """
    _review_expected(expected)
    _require(type(run) is dict and run.get("id") == expected["run_id"] and
        type(run.get("id")) is int and
        type(run.get("repository")) is dict and
        run["repository"].get("id") == expected["repository_id"] and
        type(run["repository"].get("id")) is int and
        run.get("workflow_id") == expected["workflow_id"] and
        type(run.get("workflow_id")) is int and
        type(run.get("path")) is str and
        run["path"].split("@", 1)[0] == expected["workflow_path"] and
        run.get("head_sha") == expected["head_sha"] and
        run.get("run_attempt") == 1 and type(run.get("run_attempt")) is int and
        run.get("event") == "workflow_dispatch" and
        run.get("status") in ("queued", "in_progress") and
        run.get("conclusion") is None,
        "HANDOFF_RUN")
    _require(type(runner) is dict and runner.get("id") == expected["runner_id"] and
        type(runner.get("id")) is int and
        runner.get("status") in ("online", "offline") and
        type(runner.get("busy")) is bool and
        runner.get("ephemeral") is True and
        type(runner.get("labels")) is list and
        expected["runner_label"] in {
            item.get("name") for item in runner["labels"] if type(item) is dict},
        "HANDOFF_RUNNER")
    _require(type(attempt_jobs) is dict and
        type(attempt_jobs.get("total_count")) is int and
        type(attempt_jobs.get("jobs")) is list and
        attempt_jobs["total_count"] == len(attempt_jobs["jobs"]) and
        attempt_jobs["total_count"] <= 100,
        "HANDOFF_JOB_LIST")
    assigned = [job for job in attempt_jobs["jobs"]
                if type(job) is dict and job.get("runner_id") == expected["runner_id"]]
    _require(len(assigned) <= 1, "HANDOFF_MULTIPLE_JOBS")
    if not assigned:
        _require(not runner["busy"], "HANDOFF_RUNNER_OCCUPIED")
        return None
    job = assigned[0]
    _require(_positive(job.get("id")) and
        job.get("run_id") == expected["run_id"] and
        job.get("head_sha") == expected["head_sha"] and
        job.get("runner_group_id") == expected["runner_group_id"] and
        job.get("name") == expected["job_name"] and
        job.get("status") == "in_progress" and
        job.get("conclusion") is None and
        type(job.get("labels")) is list and
        expected["runner_label"] in job["labels"] and
        runner["status"] == "online" and runner["busy"],
        "HANDOFF_JOB")
    return {"run_id": expected["run_id"], "job_id": job["id"],
            "runner_id": expected["runner_id"], "head_sha": expected["head_sha"]}


def fetch_and_verify(*, repository: str, expected: dict, token: str | None = None,
                     transport=download._https_transport, timeout_seconds: int = 30
                     ) -> dict | None:
    """Get three authenticated API snapshots and validate one assignment.

    A None result is safe to poll only while the JIT runner remains idle. The
    caller owns the poll deadline and must abort on any ValueError.
    """
    _review_expected(expected)
    _require(type(repository) is str and download.REPOSITORY.fullmatch(repository) and
             all(part not in (".", "..") for part in repository.split("/")),
             "HANDOFF_REPOSITORY")
    _require(type(timeout_seconds) is int and 0 < timeout_seconds <= 60,
             "HANDOFF_TIMEOUT")
    token = os.environ.get("GITHUB_TOKEN") if token is None else token
    _require(type(token) is str and token and "\n" not in token and "\r" not in token,
             "HANDOFF_TOKEN")
    base = f"https://{download.API_HOST}/repos/{repository}"
    deadline = time.monotonic() + timeout_seconds
    run_id = expected["run_id"]
    runner_id = expected["runner_id"]
    run = download._json(transport, f"{base}/actions/runs/{run_id}", token, deadline)
    jobs = download._json(
        transport, f"{base}/actions/runs/{run_id}/attempts/1/jobs?per_page=100",
        token, deadline)
    runner = download._json(
        transport, f"{base}/actions/runners/{runner_id}", token, deadline)
    return verify_assignment(expected=expected, run=run,
                             attempt_jobs=jobs, runner=runner)


def wait_for_assignment(*, repository: str, expected: dict, token: str | None = None,
                        timeout_seconds: int = 600, poll_seconds: int = 5,
                        fetch=fetch_and_verify) -> dict:
    """Bound the host's read-only wait; never reinterpret a mismatch as pending.

    The returned identity is admission for the *next* independent transfer
    gate, not proof that any bytes have been copied or uploaded.
    """
    _review_expected(expected)
    _require(type(timeout_seconds) is int and 0 < timeout_seconds <= 900 and
             type(poll_seconds) is int and 0 < poll_seconds <= 30,
             "HANDOFF_POLL_INPUT")
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        _require(remaining > 0, "HANDOFF_TIMEOUT")
        result = fetch(repository=repository, expected=expected, token=token,
                       timeout_seconds=max(1, min(30, int(remaining))))
        if result is not None:
            return result
        remaining = deadline - time.monotonic()
        _require(remaining > 0, "HANDOFF_TIMEOUT")
        time.sleep(min(poll_seconds, remaining))
