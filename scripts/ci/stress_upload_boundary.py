"""Admit only fixed Stage-2 evidence files to a bounded artifact upload.

The companion composite action pins upload-artifact and invokes this before
and after its single-file streaming upload. Detailed inventory stays private.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile


WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE / "scripts/compatibility"))
sys.path.insert(0, str(WORKSPACE / "scripts/release"))
import release_storage  # noqa: E402
import storage  # noqa: E402
from stress_public_case import (assemble, CASES, MAX_CASE_BYTES, read_json,
                                SHA40)


STOP_BYTES = 40_000_000_000
UPLOAD_RESERVE_BYTES = 1_000_000_000
MAX_METADATA_BYTES = 1_000_000
MAX_ZIP_BYTES = MAX_CASE_BYTES + 1_000_000
REPOSITORY_ID = 1100234936
REPOSITORY = "alopex-db/alopex"
API = f"https://api.github.com/repos/{REPOSITORY}"
SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, _request, _fp, _code, _message, _headers, _url):
        return None


class HttpsRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        parsed = urlparse(url)
        if (parsed.scheme != "https" or not parsed.hostname
                or parsed.username or parsed.password):
            raise ValueError("RECEIPT")
        return super().redirect_request(request, fp, code, message, headers, url)


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("RECEIPT")
        result[key] = value
    return result


def api_request(url: str, token: str):
    if not token or not url.startswith(API + "/"):
        raise ValueError("RECEIPT")
    request = Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    return build_opener(NoRedirect()).open(request, timeout=30)


def fetch_metadata(url: str, token: str) -> dict:
    with api_request(url, token) as response:
        body = response.read(MAX_METADATA_BYTES + 1)
    if len(body) > MAX_METADATA_BYTES:
        raise ValueError("RECEIPT")
    result = json.loads(body, object_pairs_hook=unique_object)
    if not isinstance(result, dict):
        raise ValueError("RECEIPT")
    return result


def fetch_zip(artifact_id: int, token: str) -> bytes:
    url = f"{API}/actions/artifacts/{artifact_id}/zip"
    try:
        with api_request(url, token):
            raise ValueError("RECEIPT")
    except HTTPError as redirect:
        if redirect.code != 302:
            raise ValueError("RECEIPT") from None
        location = redirect.headers.get("Location", "")
    parsed = urlparse(location)
    if (parsed.scheme != "https" or not parsed.hostname
            or parsed.hostname == "api.github.com" or parsed.username
            or parsed.password):
        raise ValueError("RECEIPT")
    # The signed URL receives no bearer token, including on later redirects.
    with build_opener(HttpsRedirect()).open(Request(location), timeout=60) as response:
        body = response.read(MAX_ZIP_BYTES + 1)
    if not 0 < len(body) <= MAX_ZIP_BYTES:
        raise ValueError("RECEIPT")
    return body


def public_bytes(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb", buffering=0) as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_CASE_BYTES):
            raise ValueError("RECEIPT")
        body = stream.read(MAX_CASE_BYTES + 1)
        after = os.fstat(stream.fileno())
        if (len(body) != before.st_size or after.st_size != before.st_size
                or after.st_mtime_ns != before.st_mtime_ns):
            raise ValueError("RECEIPT")
    return body


def workflow_path_matches(run: dict) -> bool:
    expected = ".github/workflows/stress-tests.yml"
    path = run.get("path")
    if path == expected:
        return True
    branch = run.get("head_branch")
    return (isinstance(branch, str) and bool(branch)
            and path in {f"{expected}@{branch}",
                         f"{expected}@refs/heads/{branch}"})


def verify_receipt(*, workspace: Path, private: Path, case_id: str,
                   source: str, run_id: int, run_attempt: int,
                   artifact_id: int, artifact_digest: str, token: str) -> Path:
    output = verify_output(workspace=workspace, case_id=case_id, source=source)
    original = public_bytes(output)
    if (run_id <= 0 or run_attempt <= 0 or artifact_id <= 0
            or not SHA256.fullmatch(artifact_digest)):
        raise ValueError("RECEIPT")
    run = fetch_metadata(f"{API}/actions/runs/{run_id}", token)
    artifact = fetch_metadata(f"{API}/actions/artifacts/{artifact_id}", token)
    repository = run.get("repository")
    head_repository = run.get("head_repository")
    identity = artifact.get("workflow_run")
    if (run.get("id") != run_id or run.get("run_attempt") != run_attempt
            or not workflow_path_matches(run)
            or run.get("head_sha") != source
            or not isinstance(repository, dict)
            or repository.get("id") != REPOSITORY_ID
            or repository.get("full_name") != REPOSITORY
            or not isinstance(head_repository, dict)
            or head_repository.get("id") != REPOSITORY_ID
            or artifact.get("id") != artifact_id
            or artifact.get("name") != f"{case_id}-public"
            or artifact.get("expired") is not False
            or artifact.get("digest") != artifact_digest
            or type(artifact.get("size_in_bytes")) is not int
            or not 0 < artifact["size_in_bytes"] <= MAX_ZIP_BYTES
            or artifact.get("archive_download_url") !=
            f"{API}/actions/artifacts/{artifact_id}/zip"
            or not isinstance(identity, dict)
            or identity.get("id") != run_id
            or identity.get("head_sha") != source
            or identity.get("repository_id") != REPOSITORY_ID
            or identity.get("head_repository_id") != REPOSITORY_ID):
        raise ValueError("RECEIPT")
    archive_bytes = fetch_zip(artifact_id, token)
    if (len(archive_bytes) != artifact["size_in_bytes"]
            or "sha256:" + hashlib.sha256(archive_bytes).hexdigest() != artifact_digest):
        raise ValueError("RECEIPT")
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = archive.infolist()
        if len(members) != 1 or members[0].filename != f"{case_id}.json":
            raise ValueError("RECEIPT")
        member = members[0]
        unix_type = stat.S_IFMT(member.external_attr >> 16)
        if (member.is_dir() or unix_type not in (0, stat.S_IFREG)
                or member.flag_bits & 1 or member.file_size != len(original)):
            raise ValueError("RECEIPT")
        with archive.open(member) as stream:
            if stream.read(len(original) + 1) != original:
                raise ValueError("RECEIPT")
    if public_bytes(output) != original:
        raise ValueError("RECEIPT")
    receipt = private / f"{case_id}-public-upload.receipt.private.json"
    descriptor = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump({"run_id": run_id, "run_attempt": run_attempt,
                   "source_commit": source, "case_id": case_id,
                   "artifact_id": artifact_id, "artifact_digest": artifact_digest,
                   "public_sha256": hashlib.sha256(original).hexdigest(),
                   "zip_sha256": hashlib.sha256(archive_bytes).hexdigest()},
                  stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
    return receipt


def verify_output(*, workspace: Path, case_id: str, source: str) -> Path:
    if case_id not in CASES:
        raise ValueError("STATE")
    case_dir = workspace / "target/stress-publish" / case_id
    for parent in (workspace / "target", workspace / "target/stress-publish", case_dir):
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError("OUTPUT")
    info = case_dir.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("OUTPUT")
    status_path = case_dir / f"{case_id}.json"
    if set(os.listdir(case_dir)) != {status_path.name}:
        raise ValueError("OUTPUT")
    status = read_json(status_path, limit=MAX_CASE_BYTES, canonical=True)
    if status.get("outcome") not in {"passed", "failed"}:
        raise ValueError("STATUS")
    expected = assemble(workspace=workspace, case_id=case_id,
                        source=source, outcome=status["outcome"])
    if status != expected:
        raise ValueError("STATUS")
    return status_path


def measure(*, workspace: Path, runner_temp: Path, private: Path,
            case_id: str, stage: str) -> int:
    home = Path.home()
    roots = [workspace, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp"),
             *release_storage.tool_roots()]
    local = storage.inventory(roots)
    external = release_storage.docker_inventory()
    total = local["allocated_bytes"] + external["allocated_bytes"]
    log = private / f"{case_id}-public-{stage}.storage.private.json"
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump({"local": local, "docker": external, "total": total,
                   "stage": stage}, stream, sort_keys=True)
        stream.write("\n")
    if (total >= STOP_BYTES or
            (stage == "before" and total + UPLOAD_RESERVE_BYTES >= STOP_BYTES)):
        raise ValueError("STORAGE")
    return total


def main() -> None:
    os.umask(0o077)
    args = sys.argv[1:]
    if (len(args) != 4 or args[0] != "--case" or args[2] != "--stage"
            or args[3] not in {"before", "after", "receipt"}):
        raise ValueError("ARGUMENT")
    case_id, stage = args[1], args[3]
    source = os.environ.get("GITHUB_SHA", "")
    if (not SHA40.fullmatch(source)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)):
        raise ValueError("STATE")
    head = subprocess.check_output(
        ["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    private = runner_temp / "stress-private"
    if (runner_temp.is_symlink() or not runner_temp.is_dir()
            or private.is_symlink() or not private.is_dir()
            or private.stat().st_uid != os.getuid()
            or stat.S_IMODE(private.stat().st_mode) != 0o700):
        raise ValueError("STATE")
    if stage == "receipt":
        if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
            raise ValueError("STATE")
        verify_receipt(
            workspace=WORKSPACE, private=private, case_id=case_id, source=source,
            run_id=int(os.environ["GITHUB_RUN_ID"]),
            run_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
            artifact_id=int(os.environ["STRESS_ARTIFACT_ID"]),
            artifact_digest=os.environ["STRESS_ARTIFACT_DIGEST"],
            token=os.environ["GITHUB_TOKEN"])
    else:
        verify_output(workspace=WORKSPACE, case_id=case_id, source=source)
        measure(workspace=WORKSPACE, runner_temp=runner_temp, private=private,
                case_id=case_id, stage=stage)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, UnicodeDecodeError,
            json.JSONDecodeError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired, HTTPError, URLError, TimeoutError,
            zipfile.BadZipFile, zipfile.LargeZipFile, RuntimeError):
        raise SystemExit("STRESS_UPLOAD_BOUNDARY_REJECTED") from None
