"""Check declared Rust config source contracts, without opening a database.

Known 0.8.15 literal failures remain explicitly incompatible results, separate
from supported clients and from the runtime compatibility matrix.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
CASES = (
    ("rust-old", "config-constructor", "supported"),
    ("rust-v0816", "config-constructor", "supported"),
    ("rust-current", "config-constructor", "supported"),
    ("rust-old", "config-literal-v0815", "supported"),
    ("rust-v0816", "config-literal-v0815", "known_incompatible"),
    ("rust-current", "config-literal-v0815", "known_incompatible"),
    ("rust-v0816", "config-literal-v0816", "supported"),
    ("rust-current", "config-literal-v0816", "supported"),
    ("rust-current", "config-options", "supported"),
)


def classify(returncode, messages, timed_out=False):
    """Only the exact client E0063 is a declared incompatibility."""
    if timed_out:
        return "failed"
    errors = [
        item["message"]
        for item in messages
        if item.get("reason") == "compiler-message"
        and item["message"].get("level") == "error"
    ]
    if returncode == 0:
        return "supported" if not errors else "failed"
    if returncode != 101 or len(errors) != 1:
        return "failed"
    error = errors[0]
    primary = [span for span in error.get("spans", []) if span.get("is_primary")]
    missing_fields = re.findall(
        r"`([^`]+)`", error["message"].split(" in initializer")[0]
    )
    if (
        (error.get("code") or {}).get("code") == "E0063"
        and sorted(missing_fields) == ["hnsw_cache_bytes", "hnsw_cache_entries"]
        and primary
        and all(
            Path(span["file_name"]).name == "legacy_config_literal.rs"
            for span in primary
        )
    ):
        return "known_incompatible"
    return "failed"


def run(output):
    output.mkdir(parents=True, exist_ok=True)
    source_files = [
        *HERE.glob("config_*_client.rs"),
        HERE / "legacy_config_literal.rs",
        HERE / "source_config.py",
        *[
            HERE / directory / file
            for directory in ("rust-old", "rust-v0816", "rust-current")
            for file in ("Cargo.toml", "Cargo.lock")
        ],
        *[
            REPO / "crates/alopex-embedded/src" / file
            for file in ("lib.rs", "options.rs", "columnar_api.rs")
        ],
    ]
    report = {
        "status": "in_progress",
        "source_commit": subprocess.check_output(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True
        ).strip(),
        "tracked_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "-C", str(REPO), "diff", "HEAD", "--"])
        ).hexdigest(),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "files": {
            str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source_files
        },
        "cases": [],
    }

    def save():
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    save()
    for directory, client, expected in CASES:
        label = f"{directory}-{client}"
        env = dict(os.environ)
        if directory != "rust-current":
            env.pop("NIM_SQL_PARSER_LIB_DIR", None)
            env.pop("ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD", None)
        command = [
            "cargo",
            "check",
            "--locked",
            "--manifest-path",
            str(HERE / directory / "Cargo.toml"),
            "--bin",
            client,
            "--features",
            "source-config-clients",
            "--message-format=json",
        ]
        print(f"start {label}: expected {expected}", flush=True)
        timed_out = False
        with (
            (output / f"{label}.jsonl").open("w") as stdout,
            (output / f"{label}.stderr").open("w") as stderr,
        ):
            process = subprocess.Popen(
                command, env=env, stdout=stdout, stderr=stderr, start_new_session=True
            )
            try:
                returncode = process.wait(timeout=600)
            except subprocess.TimeoutExpired:
                returncode = 124
                timed_out = True
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
        try:
            messages = [
                json.loads(line)
                for line in (output / f"{label}.jsonl").read_text().splitlines()
            ]
            actual = classify(returncode, messages, timed_out)
        except (ValueError, KeyError):
            actual = "failed"
        report["cases"].append(
            {
                "graph": directory,
                "client": client,
                "expected": expected,
                "actual": actual,
                "returncode": returncode,
                "timed_out": timed_out,
            }
        )
        save()
        if actual != expected:
            report["status"] = "failed"
            save()
            raise RuntimeError(
                f"{label}: expected {expected}, got {actual}; see {output}"
            )
        print(f"{label}: {actual}", flush=True)
    report["status"] = "declared_contracts_match"
    for outcome in ("supported", "known_incompatible"):
        report[outcome] = sum(case["actual"] == outcome for case in report["cases"])
    save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    # Relay the outer storage watchdog's stop through the child-group cleanup.
    def stop(_signum, _frame):
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, stop)
    run(args.output)


if __name__ == "__main__":
    main()
