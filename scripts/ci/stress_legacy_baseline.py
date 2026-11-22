"""Load the exact historical perf baseline ZIP without reseeding metrics.

The pinned source is the latest successful scheduled Stage-2 run observed when
this source was prepared. Run only under stress_guarded_command.py.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import (HTTPRedirectHandler, Request, build_opener,
                            urlopen)
import zipfile

WORKSPACE = Path(__file__).resolve().parents[2]
BASELINES = frozenset({"perf_mixed_point_latency.json",
                       "perf_sequential_throughput_and_amplification.json"})
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
MAX_BASELINE_BYTES = 1_000_000
RUN_ID = 44059890460
ARTIFACT_ID = 14317682831
SOURCE_SHA = "a5fb412bb826689bb685c98d73213b5153bf1e22"
ZIP_SHA256 = "af73bac907d8bcdeb3f2acd1722e4baf0c19c946bcfe370599cc2770f943aadf"
REPOSITORY_ID = 1100234936
API = "https://api.github.com/repos/alopex-db/alopex"
MAX_METADATA_BYTES = 1_000_000
MAX_ZIP_BYTES = 1_000_000


def _valid_baseline(body: bytes) -> bool:
    if not 0 < len(body) <= MAX_BASELINE_BYTES:
        return False
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    metrics = value.get("metrics") if isinstance(value, dict) else None
    return (set(value) == {"metrics"} and isinstance(metrics, dict)
            and bool(metrics) and all(
                isinstance(key, str) and key and type(number) in (int, float)
                and math.isfinite(number) for key, number in metrics.items()))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, _request, _fp, _code, _message, _headers, _url):
        return None


class HttpsRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("TRANSPORT")
        return super().redirect_request(request, fp, code, message, headers, url)


def api_request(url: str, token: str):
    if not token or not url.startswith(API + "/"):
        raise ValueError("TRANSPORT")
    request = Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    # API metadata must not redirect: urllib can forward Authorization headers.
    opener = build_opener(NoRedirect())
    return opener.open(request, timeout=30)


def fetch_json(url: str, token: str) -> dict:
    with api_request(url, token) as response:
        body = response.read(MAX_METADATA_BYTES + 1)
    if len(body) > MAX_METADATA_BYTES:
        raise ValueError("METADATA")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("METADATA")
    return value


def verify_metadata(run: dict, artifact: dict) -> None:
    if not isinstance(run, dict) or not isinstance(artifact, dict):
        raise ValueError("METADATA")
    repo = run.get("repository") if isinstance(run, dict) else None
    head_repo = run.get("head_repository") if isinstance(run, dict) else None
    identity = artifact.get("workflow_run") if isinstance(artifact, dict) else None
    if (run.get("id") != RUN_ID or run.get("path") != ".github/workflows/stress-tests.yml"
            or run.get("event") != "schedule" or run.get("status") != "completed"
            or run.get("conclusion") != "success" or run.get("head_sha") != SOURCE_SHA
            or not isinstance(repo, dict) or repo.get("id") != REPOSITORY_ID
            or repo.get("full_name") != "alopex-db/alopex"
            or not isinstance(head_repo, dict) or head_repo.get("id") != REPOSITORY_ID
            or artifact.get("id") != ARTIFACT_ID
            or artifact.get("name") != "perf-baselines"
            or artifact.get("expired") is not False
            or artifact.get("digest") != "sha256:" + ZIP_SHA256
            or not isinstance(artifact.get("size_in_bytes"), int)
            or not 0 < artifact["size_in_bytes"] <= MAX_ZIP_BYTES
            or artifact.get("archive_download_url") !=
            f"{API}/actions/artifacts/{ARTIFACT_ID}/zip"
            or not isinstance(identity, dict)
            or identity.get("id") != RUN_ID
            or identity.get("head_sha") != SOURCE_SHA
            or identity.get("repository_id") != REPOSITORY_ID
            or identity.get("head_repository_id") != REPOSITORY_ID):
        raise ValueError("METADATA")


def fetch_zip(token: str) -> bytes:
    url = f"{API}/actions/artifacts/{ARTIFACT_ID}/zip"
    try:
        with api_request(url, token):
            raise ValueError("TRANSPORT")
    except HTTPError as redirect:
        if redirect.code != 302:
            raise ValueError("TRANSPORT") from None
        location = redirect.headers.get("Location", "")
    parsed = urlparse(location)
    if parsed.scheme != "https" or not parsed.hostname or parsed.hostname == "api.github.com":
        raise ValueError("TRANSPORT")
    # This second request has no bearer token. Never forward it to the signed
    # artifact destination or any subsequent HTTPS redirect.
    with build_opener(HttpsRedirect()).open(Request(location), timeout=60) as response:
        body = response.read(MAX_ZIP_BYTES + 1)
    if (not 0 < len(body) <= MAX_ZIP_BYTES
            or hashlib.sha256(body).hexdigest() != ZIP_SHA256):
        raise ValueError("ARTIFACT")
    return body


def extract_baselines(body: bytes) -> dict[str, bytes]:
    if (not 0 < len(body) <= MAX_ZIP_BYTES
            or hashlib.sha256(body).hexdigest() != ZIP_SHA256):
        raise ValueError("ARTIFACT")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = archive.infolist()
        if len(members) != 2 or {member.filename for member in members} != BASELINES:
            raise ValueError("ARTIFACT")
        files = {}
        for member in members:
            unix_type = stat.S_IFMT(member.external_attr >> 16)
            if (member.is_dir() or unix_type not in (0, stat.S_IFREG)
                    or member.flag_bits & 1
                    or not 0 < member.file_size <= 1_000_000):
                raise ValueError("ARTIFACT")
            with archive.open(member) as stream:
                contents = stream.read(1_000_001)
            if len(contents) != member.file_size or not _valid_baseline(contents):
                raise ValueError("ARTIFACT")
            files[member.filename] = contents
    return files


def write_baselines(workspace: Path, files: dict[str, bytes]) -> None:
    target = workspace / "target"
    if target.is_symlink():
        raise ValueError("OUTPUT")
    target.mkdir(mode=0o700, exist_ok=True)
    destination = target / "stress-baselines"
    if destination.exists() or destination.is_symlink():
        raise ValueError("OUTPUT")
    destination.mkdir(mode=0o700)
    created = []
    try:
        for name in sorted(BASELINES):
            output = destination / name
            descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                 os.O_NOFOLLOW, 0o600)
            created.append(output)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(files[name])
    except OSError:
        for output in created:
            output.unlink(missing_ok=True)
        raise ValueError("OUTPUT") from None


def main() -> None:
    os.umask(0o077)
    source = os.environ.get("GITHUB_SHA", "")
    if (not SHA40.fullmatch(source)
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        raise ValueError("SOURCE")
    head = subprocess.check_output(
        ["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")
    token = os.environ["GITHUB_TOKEN"]
    run = fetch_json(f"{API}/actions/runs/{RUN_ID}", token)
    artifact = fetch_json(f"{API}/actions/artifacts/{ARTIFACT_ID}", token)
    verify_metadata(run, artifact)
    files = extract_baselines(fetch_zip(token))
    write_baselines(WORKSPACE, files)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, HTTPError, URLError,
            zipfile.BadZipFile, zipfile.LargeZipFile,
            subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise SystemExit("STRESS_LEGACY_BASELINE_REJECTED") from None
