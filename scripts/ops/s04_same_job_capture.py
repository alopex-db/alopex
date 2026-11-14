#!/usr/bin/env python3
"""Capture the selected CLI against a fake socket after same-job #804 acceptance."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))

from common import CASES, digest, load_profile, read_json, require  # noqa: E402
from capacity import Budget  # noqa: E402
from lifecycle import Run  # noqa: E402
import hosted_test_env_qualification as qualification  # noqa: E402
import s04_wire_preflight as wire  # noqa: E402


def accepted_inputs(private: Path) -> tuple[dict, str, str, str, str, str]:
    """Use only durable records from this job; never query the live daemon."""
    require(os.environ.get("GITHUB_ACTIONS") == "true" and
            os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            os.environ.get("GITHUB_SHA") == os.environ.get("EXPECTED_CONTROLLER_SHA"), "IDENTITY")
    public = read_json(REPO / "native-qualification-export/public.json")
    require(public.get("outcome") == "passed" and public.get("error_code") is None and
            public.get("source_commit") == qualification.PRODUCT and
            public.get("asset_digest") == qualification.ASSETS_DIGEST and
            len(public.get("cases", [])) == len(CASES) and
            {row["id"] for row in public["cases"]} == set(CASES) and
            all(row["status"] == "passed" and row["error_code"] is None
                for row in public["cases"]), "IDENTITY")
    profile_path = private / "profile.json"
    profile = load_profile(profile_path)
    require(profile["run_id"] == "hosted-804-" + os.environ["GITHUB_RUN_ID"] and
            profile["assets"] == str(Path(os.environ["RUNNER_TEMP"]) /
                                     "alopex-804-candidate/assets/assets.json") and
            digest(profile["assets"]) == qualification.ASSETS_DIGEST and
            digest(profile["image_lock"]) == qualification.LOCK_DIGEST and
            digest(profile["security"]["seccomp_profile"]) == qualification.POLICY_DIGEST,
            "IDENTITY")
    acquisition = read_json(private / "acquisition.private.json", private=True)
    require(acquisition.get("assets_digest") == qualification.ASSETS_DIGEST and
            acquisition.get("artifact_id") == qualification.ASSET_ARTIFACT_ID and
            acquisition.get("zip_sha256") == qualification.ASSET_ZIP_DIGEST and
            digest(private / "candidate.original.zip") == acquisition["zip_sha256"], "IDENTITY")
    run_id = profile["run_id"]
    for name in (run_id, run_id + "-repeat", run_id + "-probe"):
        run = Run(profile["private_output"], name).load()
        require(run.record["phase"] == "deleted" and
                not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
        run.verify_collection()
    primary = Run(profile["private_output"], run_id).load()
    acceptance = read_json(primary.root / "acceptance.json")
    require(digest(primary.root / "acceptance.json") == primary.record["acceptance_digest"] and
            acceptance["status"] == "passed", "IDENTITY")
    image = "sha256:" + primary.record["identities"]["image_digest"]
    require(primary.record["identities"]["assets_digest"] == qualification.ASSETS_DIGEST and
            read_json(primary.root / "public.json")["image_digest"] == image[7:], "IDENTITY")
    resources = primary.record["resources"]
    volumes = {suffix: next((item["name"] for item in resources
                             if item["kind"] == "volume" and
                             item["name"] == "alopex-test-" + run_id + "-" + suffix and
                             item["id"]), None) for suffix in ("input", "work")}
    require(all(volumes.values()) and len([item for item in resources if
            item["kind"] == "volume"]) == 2, "IDENTITY")
    return profile, run_id, primary.record["ownership_token"], image, volumes["input"], volumes["work"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    args = parser.parse_args()
    try:
        profile, run_id, token, image, inputs, work = accepted_inputs(args.private)
        observed = wire.inspect(argparse.Namespace(cli=args.cli, context=profile["docker_context"]))
        server = observed["version"]["Server"]
        api_version = server.get("ApiVersion") or server.get("APIVersion")
        require(server.get("Version") == "28.0.4" and isinstance(api_version, str),
                "IDENTITY")
        budget = Budget(profile["storage"])
        require(budget.used() + 10_000_000 < 40_000_000_000, "CAPACITY")
        output = args.private / "s04-wire.private.json"
        capture_args = argparse.Namespace(source_head=os.environ["EXPECTED_CONTROLLER_SHA"],
                                          profile=args.private / "profile.json", cli=args.cli,
                                          output=output, run_id=run_id, token=token,
                                          image=image, input_volume=inputs, work_volume=work,
                                          server_api_version=api_version,
                                          server_version=server["Version"])
        result = wire.capture(capture_args)
        require(budget.used() < 40_000_000_000, "CAPACITY")
        require(result["cli_sha256"] == observed["cli_sha256"], "IDENTITY")
        wire.private_write(output, result)
        require(result["outcome"] == "CAPTURE_COMPLETE", "CAPTURE_INCOMPLETE")
        print('{"case":"S04_WIRE_CAPTURE","outcome":"captured_offline"}')
        return 0
    except BaseException as error:
        code = error.code if hasattr(error, "code") else (
            error.args[0] if isinstance(error, wire.PreflightError) else "CAPTURE_FAILED")
        if code not in {"IDENTITY", "CLEANUP", "CAPACITY", "CAPTURE_INCOMPLETE",
                        "SOURCE_MISMATCH"}:
            code = "CAPTURE_FAILED"
        print(json.dumps({"case": "S04_WIRE_CAPTURE", "outcome": "failed", "error_code": code}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
