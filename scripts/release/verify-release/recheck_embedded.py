#!/usr/bin/env python3
"""Recheck the sole failed Embedded demo and retain source-bound passing evidence."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

import report

ROOT = Path(__file__).resolve().parents[3]
CLIENT = "crates/alopex-tools/src/bin/demo_v08_embedded.rs"


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def validate_baseline(payload, data, digest, run_id, attempt):
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("baseline report SHA-256 mismatch")
    report.validate_report_payload(payload)
    version = payload["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("invalid version")
    identity = payload["identity"]
    if (identity["run_id"], identity["run_attempt"]) != (run_id, attempt):
        raise ValueError("baseline producing run/attempt mismatch")
    name = f"v{version} Embedded API 全シナリオ"
    steps = payload["steps"]
    if (
        payload.get("suite") != "scenarios"
        or len(steps) != 17
        or len({s["name"] for s in steps}) != 17
    ):
        raise ValueError("baseline must contain all seventeen recorded scenarios")
    failed = [s for s in steps if s["status"] != "success"]
    if (
        len(failed) != 1
        or failed[0]["name"] != name
        or failed[0]["status"] != "failure"
    ):
        raise ValueError("only the Embedded scenario may need rechecking")
    if identity["tag"] != f"alopex-py-v{version}" or not re.fullmatch(
        r"[0-9a-f]{40}", payload.get("verifier_commit", "")
    ):
        raise ValueError("baseline source/verifier identity missing")
    return name


def validate_input_changes(previous, current):
    paths = git("diff", "--name-only", previous, current).splitlines()
    for path in paths:
        if path == "scripts/release/verify-release/run.sh":
            old = git("show", f"{previous}:{path}")
            new = git("show", f"{current}:{path}")
            if new != old.replace("10シナリオ", "11シナリオ"):
                raise ValueError("passing scenario commands changed")
        elif path == ".github/workflows/public-release-verification.yml":
            old_workflow = git("show", f"{previous}:{path}")
            new_workflow = git("show", f"{current}:{path}")
            start = "      - name: Run complete public-package scenarios\n"
            end = "      - name: Finalize verification report"
            if (
                old_workflow.split(start, 1)[1].split(end, 1)[0]
                != new_workflow.split(start, 1)[1].split(end, 1)[0]
            ):
                raise ValueError("passing scenario execution command changed")
        elif (
            path not in {CLIENT, "docs/release-pipeline-responsibilities.md"}
            and not path.startswith("scripts/release/")
            and path
            not in {
                ".github/workflows/public-release-embedded-recheck.yml",
                ".github/workflows/publish-public-report.yml",
            }
        ):
            raise ValueError(
                f"previous passing inputs or product source changed: {path}"
            )


def compose(payload, digest, context):
    result = copy.deepcopy(payload)
    original = dict(payload["identity"], verifier_commit=payload["verifier_commit"])
    result["resumed_from"] = dict(original, sha256=digest)
    for step in result["steps"]:
        if step["status"] == "success":
            step.setdefault("evidence", original.copy())
        else:
            step.update(
                status="incomplete",
                evidence=context.copy(),
                log_excerpt=[],
                diagnostics=[],
                duration_ms=None,
            )
    result["identity"] = dict(
        payload["identity"],
        **context,
        responsibility="Public package scenarios / source-bound Embedded recheck",
    )
    result["verifier_commit"] = context["verifier_commit"]
    result["outcome"] = "incomplete"
    result["completed_at"] = None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--baseline-run", required=True)
    parser.add_argument("--baseline-attempt", type=int, required=True)
    parser.add_argument("--baseline-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    files = list(args.baseline_dir.rglob("v*.json"))
    if len(files) != 1:
        parser.error("exactly one baseline result is required")
    raw = files[0].read_bytes()
    baseline = json.loads(raw)
    name = validate_baseline(
        baseline, raw, args.baseline_sha256, args.baseline_run, args.baseline_attempt
    )
    version = baseline["version"]
    current = git("rev-parse", "HEAD")
    validate_input_changes(baseline["verifier_commit"], current)
    public_sha = git("rev-parse", f"{baseline['identity']['tag']}^{{commit}}")
    if public_sha != baseline["identity"]["commit"]:
        raise ValueError("baseline differs from the public tag")
    if (
        subprocess.check_output(["rustc", "--version"], text=True).strip()
        != baseline["environment"]["rust"]
    ):
        raise ValueError("recheck must use the same Rust compiler")
    context = {
        "run_id": os.environ["GITHUB_RUN_ID"],
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "run_url": f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}",
        "verifier_commit": current,
    }
    result = compose(baseline, args.baseline_sha256, context)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = args.output_dir / f"v{version}.json"
    step = next(s for s in result["steps"] if s["name"] == name)
    step.update(
        status="incomplete",
        evidence=context,
        description="crates.ioの完全一致版だけを依存としてRustクライアントを再ビルドし、継続実行の回帰テストと11項目のEmbedded APIシナリオを実行する。元runの他16成功項目は入力が変わっていないことを検証して保持する。",
    )
    report.save(results, result)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        stream.write(f"version={version}\ncommit={public_sha}\n")
    log = args.output_dir / "embedded-recheck.log"
    started = time.monotonic()
    outcome = 0
    with (
        tempfile.TemporaryDirectory(prefix="alopex-embedded-recheck-") as directory,
        log.open("w") as stream,
    ):
        stream.write(
            f"cargo-mutants available: {bool(shutil.which('cargo-mutants'))}\n"
        )
        work = Path(directory)
        (work / "src").mkdir()
        (work / "src/main.rs").write_bytes((ROOT / CLIENT).read_bytes())
        manifest = work / "Cargo.toml"
        manifest.write_text(
            f'[workspace]\n[package]\nname="alopex-embedded-recheck"\nversion="0.0.0"\nedition="2024"\n[dependencies]\nserde_json="1.0"\nalopex-embedded="={version}"\nalopex-core="={version}"\nalopex-sql="={version}"\n'
        )
        env = dict(os.environ)
        for variable in [
            "ALOPEX_SQL_PARSER_LIB_DIR",
            "ALOPEX_SQL_PARSER_LIB_PATH",
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
        ]:
            env.pop(variable, None)
        for command in [
            ["cargo", "test", "--release", "--manifest-path", str(manifest)],
            ["cargo", "run", "--release", "--manifest-path", str(manifest)],
        ]:
            stream.write("command: " + " ".join(command) + "\n")
            stream.flush()
            completed = subprocess.run(
                command, env=env, stdout=stream, stderr=subprocess.STDOUT
            )
            if completed.returncode:
                outcome = completed.returncode
                break
        if outcome == 0:
            import tomllib

            lock = tomllib.loads((work / "Cargo.lock").read_text())
            owned = [
                p
                for p in lock["package"]
                if p["name"].startswith("alopex-")
                and p["name"] != "alopex-embedded-recheck"
            ]
            if not owned or any(
                p["version"] != version
                or not p.get("source", "").startswith("registry+")
                for p in owned
            ):
                outcome = 1
                stream.write("published dependency identity mismatch\n")
    if outcome == 0:
        passes = set(re.findall(r"^PASS (EMB-[^\s]+)$", log.read_text(), re.MULTILINE))
        if len(passes) != 11:
            outcome = 1
            with log.open("a") as stream:
                stream.write(
                    "Embedded scenario count mismatch: expected eleven PASS IDs\n"
                )
    result = report.load(results)
    step = next(s for s in result["steps"] if s["name"] == name)
    step.update(
        status="success" if outcome == 0 else "failure",
        duration_ms=round((time.monotonic() - started) * 1000),
        log_excerpt=log.read_text(errors="replace").splitlines(),
        diagnostics=[
            line
            for line in log.read_text(errors="replace").splitlines()
            if report.DIAGNOSTIC.search(line)
        ],
    )
    report.save(results, result)
    report.finalize(argparse.Namespace(results=results))
    report.validate_report_payload(report.load(results))
    report.render(argparse.Namespace(results=results, output_dir=args.output_dir))
    raise SystemExit(outcome)


if __name__ == "__main__":
    main()
