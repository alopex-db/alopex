"""Check GitHub run, artifact, and public-status provenance for perf reuse.

The caller must obtain run and artifact metadata from the GitHub API for the
same explicit run ID, then download its two named artifacts. This module does
not make network requests or decide where a private key is kept.
"""

from __future__ import annotations

from stress_public_evidence import SHA40, SHA256


REPOSITORY = "alopex-db/alopex"
WORKFLOW_PATH = ".github/workflows/stress-tests.yml"
PUBLIC_ARTIFACT = "perf-public"
PRIVATE_ARTIFACT = "perf-private"
MAX_PUBLIC_ZIP_BYTES = 1_000_000
MAX_PRIVATE_ZIP_BYTES = 105_000_000


def select(*, run: dict, artifacts: list[dict], status: dict,
           expected_run_id: int) -> tuple[str, str, int, str, int, str]:
    """Return source, CMS digest, and exact public/private ZIP IDs and digests."""
    if (not isinstance(expected_run_id, int) or isinstance(expected_run_id, bool)
            or expected_run_id <= 0 or not isinstance(run, dict)
            or run.get("id") != expected_run_id
            or run.get("path") != WORKFLOW_PATH
            or run.get("event") != "schedule"
            or run.get("status") != "completed"
            or run.get("conclusion") != "success"
            or not isinstance(run.get("repository"), dict)
            or run["repository"].get("full_name") != REPOSITORY
            or not isinstance(run["repository"].get("id"), int)
            or run["repository"]["id"] <= 0
            or not isinstance(run.get("head_repository"), dict)
            or run["head_repository"].get("full_name") != REPOSITORY
            or run["head_repository"].get("id") != run["repository"]["id"]
            or not isinstance(run.get("head_sha"), str)
            or not SHA40.fullmatch(run["head_sha"])):
        raise ValueError("RUN")
    source = run["head_sha"]
    if (not isinstance(status, dict)
            or set(status) != {"schema", "case_id", "source_commit", "outcome",
                                   "error_code", "ciphertext_sha256", "ciphertext_size"}
            or status.get("schema") != "alopex.stress-public/v1"
            or status.get("case_id") != "perf"
            or status.get("source_commit") != source
            or status.get("outcome") != "passed"
            or status.get("error_code") != "NONE"
            or not isinstance(status.get("ciphertext_sha256"), str)
            or not SHA256.fullmatch(status["ciphertext_sha256"])
            or not isinstance(status.get("ciphertext_size"), int)
            or isinstance(status["ciphertext_size"], bool)
            or not 0 < status["ciphertext_size"] <= 100_000_000):
        raise ValueError("STATUS")
    if not isinstance(artifacts, list):
        raise ValueError("ARTIFACT")
    selected: dict[str, dict] = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ValueError("ARTIFACT")
        name = artifact.get("name")
        if name not in {PUBLIC_ARTIFACT, PRIVATE_ARTIFACT}:
            continue
        if name in selected or artifact.get("expired") is not False:
            raise ValueError("ARTIFACT")
        bound = (MAX_PUBLIC_ZIP_BYTES if name == PUBLIC_ARTIFACT
                 else MAX_PRIVATE_ZIP_BYTES)
        size = artifact.get("size_in_bytes")
        artifact_id = artifact.get("id")
        zip_digest = artifact.get("digest")
        identity = artifact.get("workflow_run")
        if (not isinstance(size, int) or isinstance(size, bool)
                or not 0 < size <= bound
                or not isinstance(artifact_id, int) or isinstance(artifact_id, bool)
                or artifact_id <= 0
                or not isinstance(zip_digest, str)
                or not zip_digest.startswith("sha256:")
                or not SHA256.fullmatch(zip_digest.removeprefix("sha256:"))
                or artifact.get("archive_download_url") !=
                f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"
                or not isinstance(identity, dict)
                or identity.get("id") != expected_run_id
                or identity.get("head_sha") != source
                or identity.get("repository_id") != run["repository"].get("id")
                or identity.get("head_repository_id") != run["head_repository"].get("id")):
            raise ValueError("ARTIFACT")
        selected[name] = artifact
    if set(selected) != {PUBLIC_ARTIFACT, PRIVATE_ARTIFACT}:
        raise ValueError("ARTIFACT")
    return (source, status["ciphertext_sha256"],
            selected[PUBLIC_ARTIFACT]["id"], selected[PUBLIC_ARTIFACT]["digest"],
            selected[PRIVATE_ARTIFACT]["id"], selected[PRIVATE_ARTIFACT]["digest"])
