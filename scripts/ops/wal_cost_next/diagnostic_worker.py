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

if __package__:
    from .diagnostic_commit_observer import observe as diagnostic_observe
    from .schedstat_observer import observe as scheduler_observe
else:
    from diagnostic_commit_observer import observe as diagnostic_observe
    from schedstat_observer import observe as scheduler_observe

FROZEN = "490f002dfb2c7f509a2444891808d9704fe6b001"


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


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
    native_digest = sha(Path(_alopex.__file__))
    (output / "memory-before-workload.json").write_text(json.dumps(dict(
        before_identity=boundary_before_identity,
        after_identity_and_native_digest=parse_status(Path("/proc/self/status").read_text()),
        lifetime_high_water_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        scope="after import and product identity; before diagnostic workload")) + "\n")

    original_measure = workload.measure
    def measure(action):
        if getattr(action, "__name__", None) != "commit":
            return original_measure(action)
        result, observation = diagnostic_observe(
            action, original_measure, baseline_observe=baseline_observe,
            scheduler_observe=scheduler_observe)
        observation["acceptance_sample"] = False
        (output / "commit-observation.json").write_text(
            json.dumps(observation, allow_nan=False) + "\n")
        return result
    workload.measure = measure
    workload.child(config["phase"], Path(config["fixture"]), config["surface"],
                   config["value_bytes"], native_digest)


if __name__ == "__main__":
    main()
