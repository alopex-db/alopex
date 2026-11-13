"""Separate, non-acceptance #682 worker for the frozen tiny commit fixture.

The frozen comparator and its original timer are imported byte-for-byte from
the specified checkout. This worker is not called by the acceptance workflow.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

if __package__:
    from .diagnostic_commit_observer import observe as diagnostic_observe
    from .schedstat_observer import observe as scheduler_observe
    from . import sync_capture
else:
    from diagnostic_commit_observer import observe as diagnostic_observe
    from schedstat_observer import observe as scheduler_observe
    import sync_capture

FROZEN = "490f002dfb2c7f509a2444891808d9704fe6b001"


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def hold_writer_exit(original_exit, output):
    def delayed_exit(code):
        # The frozen writer calls os._exit(0) after flushing its result.
        # Keep that same process observable without unwinding its WAL state.
        if code == 0 and (output / "commit-observation.json").is_file():
            time.sleep(10)
        original_exit(code)
    return delayed_exit


def run_with_verify_hold(phase, child):
    child()
    # The frozen verifier returns after printing its result. Keep that same
    # process alive for the supervisor's first live RSS sample.
    if phase == "verify":
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--configuration", required=True, type=Path)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--frozen-checkout", required=True, type=Path)
    args = parser.parse_args()
    require(sys.platform == "linux" and not sys.flags.optimize and os.getuid() != 0,
            "diagnostic runtime differs")
    require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and
            os.environ.get("RUNNER_KIND") == "github-hosted", "diagnostic job differs")
    root = args.frozen_checkout.resolve(strict=True)
    require(subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"],
                                    text=True, timeout=10).strip() == FROZEN,
            "frozen comparator checkout differs")
    require(args.configuration.is_file() and not args.configuration.is_symlink() and
            sha(args.configuration) == args.config_sha256, "diagnostic config differs")
    config = json.loads(args.configuration.read_bytes())
    require(sorted(os.sched_getaffinity(0)) == config["affinity"], "affinity differs")
    for name, expected in config["sources"].items():
        require(sha(root / name) == expected, f"frozen source differs: {name}")
    require(config["phase"] in ("write", "verify"), "phase differs")
    mode = config.get("diagnostic_mode", "baseline")
    require(mode in ("baseline", "sync") and
            (mode != "sync" or (config["surface"], config["value_bytes"]) == ("sql", 2048)),
            "diagnostic mode differs")
    if mode == "baseline":
        require("LD_PRELOAD" not in os.environ, "baseline preload differs")
    sys.path[:0] = [str(root / "scripts/ops"), str(root / "scripts/compatibility"),
                    str(root / "scripts/ops/dataframe_harness")]
    import public_api_cost as workload
    from subject_identity import verify_subject
    from alopex import _alopex
    from phase_rss import parse_status
    from wal_cost_commit_observer import observe as baseline_observe

    boundary_before_identity = parse_status(Path("/proc/self/status").read_text())
    identity = verify_subject()
    require(identity["manifest_sha256"] == config["subject_manifest_sha256"],
            "product identity differs")
    require(workload.OPTIONS == config["options"] and
            (config["surface"], config["value_bytes"]) in workload.SHAPES,
            "frozen fixture differs")
    output = Path(config["output"])
    require(output.is_dir() and not output.is_symlink() and
            output.stat().st_mode & 0o077 == 0, "private output differs")
    (output / "identity.json").write_text(json.dumps(identity, allow_nan=False) + "\n")
    shim = None
    if mode == "sync" and config["phase"] == "write":
        shim = sync_capture.open_shim(Path(config["sync_interposer"]),
                                      config["sync_interposer_sha256"], sha)
        sync_capture.controlled_check(shim, output)
    native_digest = sha(Path(_alopex.__file__))
    (output / "memory-before-workload.json").write_text(json.dumps(dict(
        before_identity=boundary_before_identity,
        after_identity_and_native_digest=parse_status(Path("/proc/self/status").read_text()),
        lifetime_high_water_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        scope="after import and product identity; before diagnostic workload")) + "\n")

    original_measure = workload.measure
    if mode == "baseline" and config["phase"] == "write":
        # A no-op original timer measures only observer reads and serialization.
        # This calibration is outside the real commit and never an acceptance sample.
        started = time.monotonic_ns()
        _, calibration = diagnostic_observe(lambda: None,
            lambda action: (action(), 0), baseline_observe=baseline_observe,
            scheduler_observe=scheduler_observe)
        observation_ns = time.monotonic_ns() - started
        started = time.monotonic_ns()
        payload = json.dumps(calibration, allow_nan=False) + "\n"
        serialization_ns = time.monotonic_ns() - started
        require(len(payload) <= 65_536 and observation_ns <= 250_000_000 and
                serialization_ns <= 250_000_000,
                "outside-timer calibration exceeded bound")
        (output / "calibration.private.json").write_text(json.dumps(dict(
            schema="alopex.682-outside-timer-calibration/v1",
            observer_noop_ns=observation_ns, serialization_ns=serialization_ns,
            observer=calibration, acceptance_sample=False), allow_nan=False) + "\n")
    def measure(action):
        if getattr(action, "__name__", None) != "commit":
            return original_measure(action)
        sync_before = sync_capture.snapshot(shim) if shim else None
        result, observation = diagnostic_observe(
            action, original_measure, baseline_observe=baseline_observe,
            scheduler_observe=scheduler_observe)
        if shim:
            observation["intercepted_sync"] = sync_capture.commit_calls(
                sync_before, sync_capture.snapshot(shim), config["subject"])
        observation["acceptance_sample"] = False
        (output / "commit-observation.json").write_text(
            json.dumps(observation, allow_nan=False) + "\n")
        return result
    workload.measure = measure
    require(workload.os is os, "frozen exit differs")
    original_exit = os._exit
    if config["phase"] == "write":
        os._exit = hold_writer_exit(original_exit, output)
    try:
        run_with_verify_hold(
            config["phase"],
            lambda: workload.child(config["phase"], Path(config["fixture"]),
                                   config["surface"], config["value_bytes"], native_digest))
    finally:
        os._exit = original_exit


if __name__ == "__main__":
    main()
