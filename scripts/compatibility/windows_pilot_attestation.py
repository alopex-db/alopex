#!/usr/bin/env python3
"""Admit one exact-source Windows pilot artifact before full construction.

The input is the original GitHub Actions artifact ZIP, not independently copied
files: its API digest binds the public JSON and encrypted CMS to the run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile


SCHEMA = "alopex.windows-overflow-pilot/v1"
WORKFLOW = ".github/workflows/compatibility.yml"
PUBLIC_KEYS = frozenset({
    "schema", "case", "outcome", "error_code", "source_sha", "source_tree",
    "workflow_sha256", "driver_sha256", "run_id", "run_attempt",
    "budget_admitted", "ciphertext_sha256",
})
MAX_ARCHIVE = 300_000_000
MAX_PUBLIC = 16_384
MAX_CMS = 268_435_456
REPO_ROOT = Path(__file__).resolve().parents[2]


class AdmissionError(Exception):
    """A fixed public failure code; underlying API and file errors stay private."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _FixedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdmissionError("INPUT")


def require(value: bool, code: str) -> None:
    if not value:
        raise AdmissionError(code)


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def checkout_identity() -> dict:
    """Read the full job's checked-out source, without exposing Git errors."""
    def git(*arguments: str) -> str:
        return subprocess.check_output(["git", "-C", str(REPO_ROOT), *arguments],
                                       text=True, stderr=subprocess.DEVNULL,
                                       timeout=10).strip()
    return {"source_sha": git("rev-parse", "HEAD"),
            "source_tree": git("rev-parse", "HEAD^{tree}"),
            "workflow_sha256": sha256_file(REPO_ROOT / WORKFLOW),
            "driver_sha256": sha256_file(
                REPO_ROOT / "scripts/compatibility/windows_overflow_acceptance.py")}


def _hex(value: object, length: int) -> bool:
    return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def _decimal(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value) is not None


def _exact_int(value: object, expected: int) -> bool:
    return type(value) is int and value == expected


def validate_metadata(run: dict, workflow: dict, artifacts: dict, *,
                      repository: str, source_sha: str, source_tree: str,
                      workflow_sha256: str, driver_sha256: str,
                      run_id: int, run_attempt: int) -> dict:
    """Validate REST provenance before any artifact download."""
    require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)), "INPUT")
    require(all(_hex(v, n) for v, n in ((source_sha, 40), (source_tree, 40),
                                         (workflow_sha256, 64), (driver_sha256, 64)))
            and run_id > 0 and run_attempt > 0, "INPUT")
    require(isinstance(run, dict) and _exact_int(run.get("id"), run_id)
            and _exact_int(run.get("run_attempt"), run_attempt)
            and run.get("event") == "workflow_dispatch"
            and run.get("status") == "completed" and run.get("conclusion") == "success"
            and run.get("head_sha") == source_sha
            and isinstance(run.get("path"), str)
            and run["path"].split("@", 1)[0] == WORKFLOW
            and isinstance(run.get("head_branch"), str) and bool(run["head_branch"])
            and ("@" not in run["path"] or run["path"].split("@", 1)[1]
                 in (run["head_branch"], "refs/heads/" + run["head_branch"]))
            and isinstance(run.get("repository"), dict)
            and run["repository"].get("full_name") == repository
            and isinstance(run.get("head_repository"), dict)
            and run["head_repository"].get("full_name") == repository
            and _exact_int(run.get("workflow_id"), workflow.get("id") if isinstance(workflow, dict) else -1),
            "RUN")
    require(isinstance(workflow, dict) and workflow.get("path") == WORKFLOW
            and _exact_int(workflow.get("id"), run.get("workflow_id")), "WORKFLOW")
    require(isinstance(artifacts, dict) and isinstance(artifacts.get("artifacts"), list)
            and _exact_int(artifacts.get("total_count"), len(artifacts["artifacts"])),
            "ARTIFACT")
    expected_name = f"windows-overflow-pilot-{source_sha}-{run_id}-{run_attempt}"
    matches = [a for a in artifacts["artifacts"] if isinstance(a, dict)
               and a.get("name") == expected_name]
    require(len(matches) == 1, "ARTIFACT")
    artifact = matches[0]
    digest = artifact.get("digest")
    require(artifact.get("expired") is False
            and type(artifact.get("size_in_bytes")) is int
            and 0 < artifact["size_in_bytes"] <= MAX_ARCHIVE
            and isinstance(artifact.get("workflow_run"), dict)
            and _exact_int(artifact["workflow_run"].get("id"), run_id)
            and artifact["workflow_run"].get("head_sha") == source_sha
            and _hex(digest[7:] if isinstance(digest, str) and digest.startswith("sha256:") else None, 64),
            "ARTIFACT")
    artifact_id = artifact.get("id")
    expected_url = f"https://api.github.com/repos/{repository}/actions/artifacts/{artifact_id}/zip"
    require(type(artifact_id) is int and artifact_id > 0
            and artifact.get("archive_download_url") == expected_url, "ARTIFACT")
    return artifact


def verify(run: dict, workflow: dict, artifacts: dict, archive: Path, *,
           repository: str, source_sha: str, source_tree: str,
           workflow_sha256: str, driver_sha256: str,
           run_id: int, run_attempt: int) -> dict:
    """Return a small admission receipt or raise one fixed failure code.

    All expected identities come from the full run's trusted checkout/context.
    ``run``, ``workflow`` and ``artifacts`` are GitHub REST API responses.
    """
    artifact = validate_metadata(run, workflow, artifacts,
                                 repository=repository, source_sha=source_sha,
                                 source_tree=source_tree, workflow_sha256=workflow_sha256,
                                 driver_sha256=driver_sha256, run_id=run_id,
                                 run_attempt=run_attempt)
    digest = artifact["digest"]
    require(archive.is_file() and not archive.is_symlink()
            and archive.stat().st_size == artifact["size_in_bytes"], "ARCHIVE")
    require(sha256_file(archive) == digest[7:], "ARTIFACT_DIGEST")
    try:
        with zipfile.ZipFile(archive) as bundle:
            entries = bundle.infolist()
            require(len(entries) == 2 and {e.filename for e in entries} ==
                    {"public.json", "private-evidence.cms"}
                    and all(not e.is_dir() for e in entries), "ARCHIVE")
            public_info = bundle.getinfo("public.json")
            cms_info = bundle.getinfo("private-evidence.cms")
            require(0 < public_info.file_size <= MAX_PUBLIC
                    and 0 < cms_info.file_size <= MAX_CMS, "ARCHIVE")
            public_bytes = bundle.read(public_info)
            cms_hash = hashlib.sha256()
            with bundle.open(cms_info) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    cms_hash.update(block)
    except (zipfile.BadZipFile, RuntimeError, KeyError):
        raise AdmissionError("ARCHIVE") from None
    try:
        public = json.loads(public_bytes.decode("utf-8"))
    except (UnicodeError, ValueError):
        raise AdmissionError("PUBLIC") from None
    require(isinstance(public, dict) and set(public) == PUBLIC_KEYS
            and public.get("schema") == SCHEMA
            and public.get("case") == "WINDOWS_OVERFLOW_PILOT"
            and public.get("outcome") == "passed"
            and public.get("error_code") is None
            and public.get("source_sha") == source_sha
            and public.get("source_tree") == source_tree
            and public.get("workflow_sha256") == workflow_sha256
            and public.get("driver_sha256") == driver_sha256
            and public.get("run_id") == str(run_id)
            and public.get("run_attempt") == str(run_attempt)
            and public.get("budget_admitted") is True
            and public.get("ciphertext_sha256") == cms_hash.hexdigest(), "PUBLIC")
    return {"schema": "alopex.windows-overflow-pilot-admission/v1",
            "pilot_run_id": str(run_id), "pilot_run_attempt": str(run_attempt),
            "source_sha": source_sha, "source_tree": source_tree,
            "workflow_sha256": workflow_sha256, "driver_sha256": driver_sha256,
            "artifact_sha256": digest[7:], "admitted": True}


def github_json(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"})
    with urllib.request.build_opener(_NoRedirect).open(request, timeout=20) as response:
        require(response.status == 200, "API")
        data = response.read(2_000_001)
    require(len(data) <= 2_000_000, "API")
    return json.loads(data)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def _signed_artifact_url(value: str) -> bool:
    parsed = urllib.parse.urlsplit(value)
    host = (parsed.hostname or "").lower()
    query = urllib.parse.parse_qs(parsed.query)
    github_signed = host.endswith(".actions.githubusercontent.com") and bool(parsed.query)
    azure_signed = (host.endswith(".blob.core.windows.net")
                    and all(query.get(key) for key in ("sig", "se", "sp"))
                    and "r" in query["sp"][0])
    return (parsed.scheme == "https" and parsed.username is None
            and parsed.password is None and parsed.port in (None, 443)
            and (github_signed or azure_signed) and parsed.fragment == "")


def download_artifact(artifact: dict, token: str, destination: Path) -> None:
    """Fetch only the metadata-selected ZIP; never send the token to redirect host."""
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(artifact["archive_download_url"], headers={
        "Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"})
    try:
        opener.open(request, timeout=20)
    except urllib.error.HTTPError as error:
        require(error.code == 302, "DOWNLOAD")
        location = error.headers.get("Location", "")
    else:
        raise AdmissionError("DOWNLOAD")
    require(_signed_artifact_url(location), "DOWNLOAD_REDIRECT")
    signed_request = urllib.request.Request(location, headers={"Accept": "application/zip"})
    with opener.open(signed_request, timeout=60) as response:
        require(response.status == 200, "DOWNLOAD")
        total = 0
        with destination.open("xb") as stream:
            while block := response.read(1024 * 1024):
                total += len(block)
                require(total <= MAX_ARCHIVE, "DOWNLOAD_SIZE")
                stream.write(block)
    require(total > 0, "DOWNLOAD")


def main(argv: list[str] | None = None) -> int:
    parser = _FixedParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--token-env", default="GITHUB_TOKEN")
    try:
        args = parser.parse_args(argv)
        import os
        token = os.environ.get(args.token_env, "")
        require(bool(token) and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository) is not None
                and _decimal(args.run_id) and _decimal(args.run_attempt)
                and len(args.run_id) <= 19 and len(args.run_attempt) <= 19, "INPUT")
        args.run_id = int(args.run_id)
        args.run_attempt = int(args.run_attempt)
        identity = checkout_identity()
        base = f"https://api.github.com/repos/{args.repository}/actions"
        run = github_json(f"{base}/runs/{args.run_id}", token)
        require(isinstance(run, dict) and type(run.get("workflow_id")) is int
                and run["workflow_id"] > 0, "RUN")
        workflow = github_json(f"{base}/workflows/{run.get('workflow_id')}", token)
        artifacts = github_json(f"{base}/runs/{args.run_id}/artifacts?per_page=100", token)
        artifact = validate_metadata(run, workflow, artifacts,
                                     repository=args.repository, **identity,
                                     run_id=args.run_id, run_attempt=args.run_attempt)
        require(not args.output.exists() and not args.output.is_symlink(), "OUTPUT")
        with tempfile.TemporaryDirectory(prefix="alopex-pilot-artifact-",
                                         dir=args.output.parent) as temporary:
            archive = Path(temporary) / "artifact.zip"
            download_artifact(artifact, token, archive)
            receipt = verify(run, workflow, artifacts, archive,
                             repository=args.repository, **identity,
                             run_id=args.run_id, run_attempt=args.run_attempt)
        args.output.write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
    except AdmissionError as error:
        print("WINDOWS_PILOT_" + error.code, file=sys.stderr)
        return 1
    except Exception:
        print("WINDOWS_PILOT_API_OR_IO", file=sys.stderr)
        return 1
    print("WINDOWS_PILOT_ADMITTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
