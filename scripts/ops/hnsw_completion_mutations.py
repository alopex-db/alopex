"""Execute cargo-mutants generated example mutations with tiny native cases.

The example is an interactive manual program, not a Rust test target. This
adapter builds each generated mutation serially and checks its exit/protocol.
It does not measure performance or qualify a release asset.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

EXAMPLE = "crates/alopex-embedded/examples/issue681_hnsw_first_write.rs"
PHASES = ("seed", "index_build", "cold_transaction_1", "cold_transaction_2",
          "warm", "writer_begin", "first_insert", "second_insert")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def apply_mutation(original, mutation):
    lines = original.splitlines(keepends=True)
    offsets = []
    for key in ("start", "end"):
        point = mutation["span"][key]
        line, column = point["line"], point["column"]
        if not (1 <= line <= len(lines) and 1 <= column <= len(lines[line - 1]) + 1):
            raise ValueError("mutation span outside source")
        offsets.append(sum(map(len, lines[:line - 1])) + column - 1)
    start, end = offsets
    if not 0 <= start < end <= len(original):
        raise ValueError("empty or reversed mutation span")
    replacement = mutation["replacement"].encode("ascii")
    return original[:start] + replacement + original[end:]


def assess(returncode, events, stderr, mode, *, empty_main=False):
    sequence = [(phase, kind) for phase in (*PHASES, mode)
                for kind in ("start", "end")] + [("complete", None)]
    actual = [(e.get("phase"), e.get("data", {}).get("event")) for e in events]
    valid = (returncode == 0 and actual == sequence
             and events[0]["data"].get("rows") == 3
             and events[0]["data"].get("dimensions") == 4
             and type(events[-2]["data"].get("elapsed_ns")) is int
             and events[-2]["data"]["elapsed_ns"] > 0
             and events[-1]["data"].get("finish") == mode
             and events[-1]["data"].get("result_validated") is True)
    if valid:
        return "passed"
    if empty_main and returncode == 0 and not events and not stderr:
        return "caught_missing_protocol"
    # Do not count an allocation, loader, timeout, or unrelated assertion as a
    # successful mutation witness. These mutants must reach the new assertion.
    if (returncode != 0 and actual == sequence[:-1]
            and "transaction completion differs" in stderr):
        return "caught_completion_assertion"
    return "unresolved_failure"


def execute(binary, directory, mode, env, empty_main=False):
    directory.mkdir(mode=0o700)
    acknowledgements = directory / "stdin.log"
    acknowledgements.write_bytes(b"continue\n" * 19)
    stdout, stderr = directory / "stdout.log", directory / "stderr.log"
    child_env = dict(env, ALOPEX_MANUAL_681="1", RAYON_NUM_THREADS="2", OMP_NUM_THREADS="2")
    with acknowledgements.open("rb") as input_stream, stdout.open("wb") as out, stderr.open("wb") as err:
        # Keep the outer Budget process group so a cancelled build also reaps
        # this child. The example creates threads, not descendant processes.
        child = subprocess.Popen([str(binary), "3", "4", mode], stdin=input_stream,
                                 stdout=out, stderr=err, env=child_env)
        deadline = time.monotonic() + 30
        try:
            while child.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("tiny completion case exceeded 30 seconds")
                if stdout.stat().st_size + stderr.stat().st_size > 65536:
                    raise RuntimeError("tiny completion output exceeded bound")
                try:
                    status = Path(f"/proc/{child.pid}/status").read_text()
                except FileNotFoundError:
                    if child.poll() is None:
                        raise
                else:
                    for line in status.splitlines():
                        if line.startswith("VmRSS:") and int(line.split()[1]) * 1024 >= 268435456:
                            raise RuntimeError("tiny completion RSS exceeded bound")
                time.sleep(0.01)
            code = child.wait()
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
    if stdout.stat().st_size + stderr.stat().st_size > 65536:
        raise RuntimeError("tiny completion output exceeded bound")
    events = [json.loads(line) for line in stdout.read_text().splitlines()]
    result = dict(mode=mode, rows=3, dimensions=4, returncode=code,
                  outcome=assess(code, events, stderr.read_text(), mode, empty_main=empty_main),
                  binary_sha256=sha(binary.read_bytes()),
                  stdout_sha256=sha(stdout.read_bytes()), stderr_sha256=sha(stderr.read_bytes()),
                  waited=True, performance_qualification=False)
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def run_suite(source, target, work, env, command, config, mutations):
    example = source / EXAMPLE
    original = example.read_bytes()
    if sha(original) != config["example_sha256"] or len(mutations) != 5:
        raise ValueError("mutation source or scope differs")
    evidence = work / "completion-mutations"
    evidence.mkdir(mode=0o700)
    report = dict(source_commit=config["source_commit"], example_sha256=sha(original),
                  generator="cargo-mutants 27.1.0 --Zmutate-file --list --json --diff",
                  executor="serial native example adapter", baseline=None, mutations=[],
                  restored=False, performance_qualification=False)
    binary = target / "debug/examples/issue681_hnsw_first_write"
    build = ["cargo", "build", "--locked", "-p", "alopex-embedded",
             "--example", "issue681_hnsw_first_write"]
    try:
        command(build, "example", "mutation-baseline-build", config["build_seconds"])
        report["baseline"] = [execute(binary, evidence / ("baseline-" + mode), mode, env)
                              for mode in ("commit", "rollback")]
        if any(r["outcome"] != "passed" for r in report["baseline"]):
            raise RuntimeError("mutation baseline failed")
        for index, mutation in enumerate(mutations):
            mutated = apply_mutation(original, mutation)
            example.write_bytes(mutated)
            row = dict(mutation=mutation, mutated_source_sha256=sha(mutated), cases=[])
            report["mutations"].append(row)
            command(build, "example", f"mutation-{index}-build", 900)
            row["cases"] = [execute(binary, evidence / f"mutation-{index}-{mode}", mode, env,
                                    empty_main=mutation["genre"] == "FnValue")
                            for mode in ("commit", "rollback")]
            row["caught"] = (any(r["outcome"].startswith("caught_") for r in row["cases"])
                             and all(r["outcome"] != "unresolved_failure" for r in row["cases"]))
            if not row["caught"]:
                raise RuntimeError("mutation survived or failed without a behavioral witness")
            example.write_bytes(original)
    finally:
        example.write_bytes(original)
        report["restored"] = example.read_bytes() == original
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
