#!/usr/bin/env python3
"""Retain publication failures separately from the original verification pair."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

import publication_record as contract
import report_pair


def prepare(root, artifact, run_id, attempt, version, commit, stage, output):
    record = {
        "schema": contract.SCHEMA, "repository": "alopex-db/alopex", "version": version,
        "run_id": run_id, "publication_attempt": attempt, "publisher_commit": commit,
        "outcome": "failure", "stage": stage, "source_artifact": artifact, "verification": None,
    }
    contract.validate(record)
    blobs = {}
    try:
        selected = report_pair.select_pair(root, artifact, run_id, record["repository"])
    except (ValueError, OSError, KeyError, TypeError, SystemExit):
        # The missing/rejected original stays unavailable. Never manufacture a
        # verification success merely to make failure evidence publishable.
        selected = None
    if selected:
        record["version"] = selected["VERSION"]
        source = Path(selected["REPORT_ROOT"]) / f"v{selected['VERSION']}"
        originals = {ext: report_pair.read_regular(Path(str(source) + "." + ext)) for ext in ("json", "md")}
        original = json.loads(originals["json"])
        record["verification"] = {
            "producing_attempt": original["identity"]["run_attempt"], "outcome": original["outcome"],
            "sha256": {ext: contract.digest(data) for ext, data in originals.items()},
        }
        prefix = contract.original_stem(record)
        blobs.update({prefix + "." + ext: data for ext, data in originals.items()})
    prefix = contract.stem(record)
    blobs[prefix + ".json"] = (json.dumps(record, indent=2) + "\n").encode()
    blobs[prefix + ".md"] = contract.render(record)
    contract.verify_bundle(record, blobs)
    # A clean destination keeps retries from mixing files from another attempt.
    output.mkdir(parents=True, exist_ok=False)
    for name, data in blobs.items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return record


def load_bundle(root):
    candidates = list(root.rglob("*-publication-failure.json"))
    if len(candidates) != 1:
        raise ValueError("expected one publication record")
    record = json.loads(report_pair.read_regular(candidates[0]))
    blobs = {}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("symlink bundle refused")
        if path.is_file():
            blobs[path.relative_to(root).as_posix()] = report_pair.read_regular(path)
    contract.verify_bundle(record, blobs)
    return record, blobs


def git(*args, check=True):
    return subprocess.run(["git", *args], capture_output=True, check=check)


def retain(root):
    record, blobs = load_bundle(root)
    branch = f"report/verify-release-failure-v{record['version']}-{record['run_id']}-{record['publication_attempt']}"
    remote = git("ls-remote", "--exit-code", "--heads", "origin", branch, check=False)
    if remote.returncode == 0:
        ref = "refs/remotes/origin/" + branch
        git("fetch", "origin", f"refs/heads/{branch}:{ref}")
        for path, data in blobs.items():
            if git("show", f"{ref}:{path}").stdout != data:
                raise ValueError("refusing to overwrite immutable publication evidence")
        return branch
    if remote.returncode != 2:
        raise RuntimeError("cannot determine existing publication branch")
    for path, data in blobs.items():
        target = Path(path)
        if target.is_symlink() or (target.exists() and target.read_bytes() != data):
            raise ValueError("local publication evidence collision")
    git("switch", "-c", branch)
    for path, data in blobs.items():
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    git("add", "--", *sorted(blobs))
    git("-c", "user.name=github-actions[bot]", "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
        "commit", "-m", "docs(release): retain independent publication failure")
    git("push", "origin", f"HEAD:refs/heads/{branch}")
    return branch


def confirm(root, attempts=60, interval=30):
    record, blobs = load_bundle(root)
    for attempt in range(attempts):
        try:
            for path, expected in blobs.items():
                url = f"https://raw.githubusercontent.com/alopex-db/docs/main/{path}?publication={record['run_id']}-{record['publication_attempt']}-{attempt}"
                with urllib.request.urlopen(url, timeout=15) as response:
                    actual = response.read(contract.MAX_BYTES + 1)
                if actual != expected:
                    raise ValueError("public bytes differ: " + path)
            return
        except (urllib.error.URLError, TimeoutError, ValueError) as error:
            print(f"docs ingestion attempt {attempt + 1}/{attempts}: {error}", flush=True)
        if attempt + 1 < attempts:
            time.sleep(interval)
    raise RuntimeError("docs did not publish the exact publication evidence bundle")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--root", type=Path, required=True)
    prepare_parser.add_argument("--artifact", required=True)
    prepare_parser.add_argument("--run-id", required=True)
    prepare_parser.add_argument("--attempt", type=int, required=True)
    prepare_parser.add_argument("--version", required=True)
    prepare_parser.add_argument("--commit", required=True)
    prepare_parser.add_argument("--stage", choices=sorted(contract.STAGES), required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    retain_parser = commands.add_parser("retain")
    retain_parser.add_argument("--root", type=Path, required=True)
    retain_parser.add_argument("--github-output", type=Path, required=True)
    confirm_parser = commands.add_parser("confirm")
    confirm_parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.root, args.artifact, args.run_id, args.attempt, args.version, args.commit, args.stage, args.output)
    elif args.command == "retain":
        branch = retain(args.root)
        with args.github_output.open("a") as stream:
            stream.write(f"branch={branch}\n")
    else:
        confirm(args.root)


if __name__ == "__main__":
    main()
