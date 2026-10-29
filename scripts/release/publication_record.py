"""Data-only contract for independent publication-failure evidence (#431).

Kept byte-identical in the engine and docs repositories. No incoming code runs
on the receiver; this module is part of the receiver's reviewed source.
"""
from __future__ import annotations

import hashlib
import json
import re

SCHEMA = "alopex-report-publication/v1"
STAGES = {"artifact_download", "report_validation", "source_retention", "docs_ingestion", "branch_cleanup", "orchestration"}
MAX_BYTES = 8 * 1024 * 1024


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate(record):
    if record.get("schema") != SCHEMA or record.get("outcome") != "failure":
        raise ValueError("invalid publication schema/outcome")
    if record.get("repository") != "alopex-db/alopex":
        raise ValueError("invalid publication repository")
    if not re.fullmatch(r"(?:[0-9]+\.[0-9]+\.[0-9]+|unknown)", record.get("version", "")):
        raise ValueError("invalid publication version")
    if not re.fullmatch(r"[1-9][0-9]*", record.get("run_id", "")):
        raise ValueError("invalid publication run")
    attempt = record.get("publication_attempt")
    if type(attempt) is not int or attempt < 1 or record.get("stage") not in STAGES:
        raise ValueError("invalid publication attempt/stage")
    if not re.fullmatch(r"[0-9a-f]{40}", record.get("publisher_commit", "")):
        raise ValueError("invalid publisher commit")
    artifact = record.get("source_artifact", "")
    match = re.fullmatch(r"release-verification-([1-9][0-9]*)-([1-9][0-9]*)", artifact)
    if artifact and (not match or match[1] != record["run_id"] or int(match[2]) > attempt):
        raise ValueError("invalid producing artifact")
    verification = record.get("verification")
    if verification is not None:
        if not match or verification.get("producing_attempt") != int(match[2]):
            raise ValueError("verification producer mismatch")
        if type(verification.get("producing_attempt")) is not int:
            raise ValueError("invalid verification producing attempt")
        if verification.get("outcome") not in {"success", "failure", "incomplete"}:
            raise ValueError("invalid verification outcome")
        for extension in ("json", "md"):
            if not re.fullmatch(r"[0-9a-f]{64}", verification.get("sha256", {}).get(extension, "")):
                raise ValueError("invalid verification digest")
    return record


def stem(record):
    validate(record)
    return (f"reports/release-verification/v{record['version']}/"
            f"run-{record['run_id']}-{record['publication_attempt']}-publication-failure")


def original_stem(record):
    validate(record)
    original = record["verification"]
    if original is None:
        return None
    return (f"reports/release-verification/v{record['version']}/"
            f"run-{record['run_id']}-{original['producing_attempt']}")


def render(record):
    validate(record)
    lines = [f"# Report publication failure: v{record['version']}", "",
             "Publication failed; this record does not change the verification outcome.", "",
             f"- Stage: {record['stage']}",
             f"- Publishing attempt: {record['publication_attempt']}",
             f"- Publisher commit: {record['publisher_commit']}",
             f"- Run: https://github.com/{record['repository']}/actions/runs/{record['run_id']}/attempts/{record['publication_attempt']}",
             f"- Source artifact: {record['source_artifact'] or 'unavailable'}"]
    verification = record["verification"]
    if verification is None:
        lines += ["- Original verification: unavailable or rejected; no success is inferred."]
    else:
        lines += [f"- Original verification outcome: {verification['outcome']}",
                  f"- Original report: {original_stem(record)}"]
        lines += [f"- Original {ext} SHA-256: {verification['sha256'][ext]}" for ext in ("json", "md")]
    return ("\n".join(lines) + "\n").encode()


def verify_bundle(record, blobs):
    """Validate all paths and hashes before a receiver writes any file."""
    prefix = stem(record)
    expected = {prefix + ".json", prefix + ".md"}
    original = original_stem(record)
    if original:
        expected.update(original + "." + ext for ext in ("json", "md"))
    if set(blobs) != expected or any(len(data) > MAX_BYTES for data in blobs.values()):
        raise ValueError("invalid publication bundle inventory/size")
    if json.loads(blobs[prefix + ".json"]) != record or blobs[prefix + ".md"] != render(record):
        raise ValueError("publication pair mismatch")
    if original:
        for ext in ("json", "md"):
            if digest(blobs[original + "." + ext]) != record["verification"]["sha256"][ext]:
                raise ValueError("original verification digest mismatch")
        payload = json.loads(blobs[original + ".json"])
        identity = payload.get("identity", {})
        if (payload.get("schema"), payload.get("version"), payload.get("outcome"),
            identity.get("run_id"), identity.get("run_attempt")) != (
            "alopex-release-verification/v2", record["version"], record["verification"]["outcome"],
            record["run_id"], record["verification"]["producing_attempt"]):
            raise ValueError("original verification identity mismatch")
