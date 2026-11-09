"""Manual, bounded SQL comparison on one isolated managed VM."""
from __future__ import annotations

import base64
import gzip
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from hosted_native_image import construct
from hosted_native_transfer import verify_run
from hosted_native_sql import verify_products


def settings():
    value = json.loads(base64.b64decode(os.environ["PUBLIC_PERFORMANCE_CONFIG"], validate=True))
    if (set(value["subjects"]) != {"published", "before", "candidate"} or
            type(value["matrix_seconds"]) is not int or not 0 < value["matrix_seconds"] <= 7200 or
            value["limits"]["case_seconds"] != 300 or value["limits"]["run_seconds"] != 420):
        raise ValueError("unexpected fixed matrix configuration")
    return value


def fetch(repo, config):
    from common import atomic_json, digest, private_dir, require
    from capacity import Budget
    roots = [repo.parent, Path(os.environ["RUNNER_TEMP"])]
    for path in (repo.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if path.exists(): roots.append(path)
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": config["download_reserve_bytes"], "poll_seconds": 2,
                     "planned_bytes": {"fetch": config["download_bound_bytes"]}})
    budget.before("fetch")
    private = private_dir(repo / "native-fetch-private")
    private_dir(private / "tmp")
    os.environ["TMPDIR"] = str(private / "tmp")
    def metadata(endpoint):
        result = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True,
                                check=True, timeout=30)
        return json.loads(result.stdout)
    def download(run, source, name, directory):
        api = "repos/alopex-db/alopex/actions/runs/" + str(run)
        verify_run(metadata(api), source)
        artifacts = [a for a in metadata(api + "/artifacts")["artifacts"] if a["name"] == name]
        require(len(artifacts) == 1 and not artifacts[0]["expired"] and
                artifacts[0]["size_in_bytes"] < config["artifact_bound_bytes"], "IDENTITY")
        budget.before("fetch")
        budget.run(["gh", "run", "download", str(run), "--repo", "alopex-db/alopex", "--name", name,
                    "--dir", str(repo / directory)], timeout=300, log=private / (directory + ".log"))
    try:
        acceptance = config["native_acceptance"]
        download(acceptance["run"], acceptance["controller"], "native-sql-environment-acceptance", "native-acceptance-import")
        accepted = repo / "native-acceptance-import"
        require(digest(accepted / "sql-acceptance.json") == acceptance["public_sha256"], "IDENTITY")
        require(json.loads((accepted / "sql-acceptance.json").read_text())["assets_digest"] ==
                config["subjects"]["candidate"]["assets_digest"], "IDENTITY")
        summary = json.loads((accepted / "result.json").read_text())
        require(summary["status"] == "passed" and summary["native_sql_acceptance"] and
                summary["full_acceptance"] and summary["owned_resources_cleaned"] and
                summary["image_digest"] == config["image"]["image_record"]["id"].removeprefix("sha256:"), "IDENTITY")
        download(config["image_run"], config["image_run_source"], "native-test-environment", "native-image-import")
        require(digest(repo / config["image"]["image_archive"]) == config["image"]["image_archive_sha256"], "IDENTITY")
        image_result = json.loads((repo / "native-image-import/result.json").read_text())
        require(image_result["status"] == "passed" and image_result["owned_resources_cleaned"] is True and
                image_result["controller_commit"] == config["image_run_source"] and
                image_result["image_digest"] == summary["image_digest"], "IDENTITY")
        harnesses = set()
        for role, subject in config["subjects"].items():
            require(subject["platform"] == config["platform"], "IDENTITY")
            directory = "native-" + role + "-import"
            wheel_only = role == "before" and subject["source_commit"] == "ce88bbfadd8030ff5cd67262a72f915e50e5b8da"
            download(subject["run"], subject["controller"],
                     "native-wheel-assets" if wheel_only else "native-product-assets", directory)
            imported = repo / directory
            if wheel_only:
                from native_begin_supervisor import extension_digest, verify_begin_assets
                manifest = verify_begin_assets(imported / "assets/assets.json")
                result = json.loads((imported / "result.json").read_text())
                require(result["schema"] == "alopex.native-wheel-assets-result/v1" and
                        result["status"] == "passed" and result["source"] ==
                        {"commit": subject["source_commit"], "tree": subject["source_tree"],
                         "profile": "release"} and
                        result["assets_digest"] == subject["assets_digest"] and
                        manifest["source_commit"] == subject["source_commit"] and
                        manifest["source_tree"] == subject["source_tree"], "IDENTITY")
                wheel = next(item for item in manifest["files"] if item["role"] == "wheel")
                require(extension_digest(imported / "assets" / wheel["path"]) ==
                        subject["extension_sha256"], "IDENTITY")
            else:
                manifest = verify_products(imported / "assets/assets.json", imported / "result.json", subject)
                harnesses.add(next(f["sha256"] for f in manifest["files"] if f["role"] == "harness"))
            require(digest(imported / "assets/assets.json") == subject["assets_digest"], "IDENTITY")
            if role == "published":
                wheel = next(f for f in manifest["files"] if f["role"] == "wheel")
                require(wheel["sha256"] == subject["published_wheel_sha256"], "IDENTITY")
        require(harnesses == {config["docs_harness_digest"]}, "IDENTITY")
        budget.check()
    except BaseException as error:
        atomic_json(private / "failure.json", {"error": repr(error)})
        export = private_dir(repo / "native-export")
        subprocess.run(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER",
                        "-in", str(private / "failure.json"), "-out", str(export / "transfer-failure.cms"),
                        str(repo / ".github/ops/native-evidence-cert.pem")], capture_output=True, check=True, timeout=30)
        raise


def operation(repo, config):
    def run(profile, docker, budget, work, export, public):
        from common import atomic_json, digest, private_dir, require
        from native_sql_matrix import compose_harness, plan, prepare_cell, summarize, median_mad
        from native_sql_measure_container import execute_cell
        from lifecycle import Run

        budget.before("accept")
        operators = {role: {"path": repo / "scripts/ops" / item["filename"], "sha256": item["sha256"]}
                     for role, item in config["operators"].items()}
        candidate = repo / "native-candidate-import/assets"
        assets = json.loads((candidate / "assets.json").read_text())
        archive = candidate / next(f["path"] for f in assets["files"] if f["role"] == "harness")
        require(digest(archive) == config["docs_harness_digest"], "IDENTITY")
        harness = work / "measurement-harness.tar"
        harness_record = compose_harness(archive, repo / "scripts/performance",
                                         config["product_harness"]["files"], harness)
        atomic_json(work / "measurement-harness.json", harness_record)
        atomic_json(work / "matrix-plan.json", {"cells": plan(), "configuration": config,
                                                "image": public["image_digest"], "cpu": profile["cpu"]})
        started = time.monotonic()
        rows = []
        fatal = None
        for cell in plan():
            if fatal is not None or time.monotonic() - started + 420 > config["matrix_seconds"]:
                rows.append(dict(cell, status="not_run", error_code="STOPPED"))
                continue
            inputs = work / "cell-input"
            selected = dict(profile, run_id=cell["id"])
            try:
                budget.before("accept")
                role = "published" if cell["subject"] == "postgresql" else cell["subject"]
                manifest = repo / ("native-" + role + "-import") / "assets/assets.json"
                path, worker = prepare_cell(manifest, inputs, harness, harness_record, cell, operators)
                selected["assets"] = str(path)
                execute_cell(selected, budget, worker)
                run = Run(selected["private_output"], cell["id"]).load()
                run.verify_collection()
                collected = run.root / "collected/cell"
                result = json.loads((collected / "measurement.json").read_text())
                raw = [json.loads(line) for line in (collected / "raw.ndjson").read_text().splitlines()]
                values = [item["seconds"] for item in raw]
                median, mad = median_mad(values)
                rows.append(dict(cell, status="passed", trial_median_seconds=median, sample_mad_seconds=mad,
                                 sample_range_seconds=[min(values), max(values)], result_checksum=result["result_sha256"],
                                 input_checksum=result.get("input_sha256"), raw_sha256=result["raw_sha256"],
                                 source=result["subject_identity"]["source"]))
            except BaseException as error:
                code = getattr(error, "code", "INTERNAL")
                rows.append(dict(cell, status="failed", error_code=code, detail=repr(error)))
                # A functional cell can fail independently. Infrastructure and
                # evidence failures stop new work; partial rows remain evidence.
                if code != "CASE" or getattr(error, "cleanup_errors", []):
                    fatal = error
            finally:
                atomic_json(work / "matrix-progress.json", rows)
                if inputs.exists():
                    shutil.rmtree(inputs)
            budget.check()
        atomic_json(work / "matrix-results.json", rows)
        result = summarize(rows)
        atomic_json(work / "comparison.json", result)
        # These fields contain only controlled case names, ratios and timings.
        # Device identity, configuration, paths and process observations remain
        # in the separately encrypted evidence archive.
        atomic_json(export / "comparison.json", result)
        public.update(schema="alopex.native-sql-matrix/v1", measurement_cells=len(rows),
                      completed_cells=sum(row["status"] == "passed" for row in rows),
                      matrix_complete=all(row["status"] == "passed" for row in rows),
                      comparison_sha256=digest(export / "comparison.json"), performance_qualification=False)
        if fatal is not None:
            raise fatal
        require(all(row["status"] == "passed" for row in rows), "CASE")
    return run


def main(*, settings_reader=None, operation_factory=None):
    os.umask(0o077)
    if os.environ.get("RUNNER_KIND") != "github-hosted":
        return 78
    repo = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    sys.path.insert(0, str(repo / "scripts/test-env"))
    config = (settings if settings_reader is None else settings_reader)()
    if sys.argv[1:] == ["fetch"]:
        if os.geteuid() == 0:
            return 78
        fetch(repo, config)
        return 0
    if sys.argv[1:] != ["run"] or os.geteuid() != 0:
        return 78
    inputs = json.loads(gzip.decompress(base64.b64decode(os.environ.pop("NATIVE_IMAGE_INPUTS"), validate=True)))
    inputs.update(config["image"])
    inputs["bounds"].update(config["bounds"])
    inputs["limits"].update(config["limits"])
    # The timer interrupts work inside construct(), allowing its finally block
    # to collect evidence and stop the owned daemon before the workflow timeout.
    signal.alarm(config["matrix_seconds"] + 600)
    try:
        return construct(repo, Path(os.environ["RUNNER_TEMP"]) / "alopex-native-private", repo / "native-export",
                         inputs, repo / ".github/ops/native-evidence-cert.pem",
                         (operation if operation_factory is None else operation_factory)(repo, config))
    finally:
        signal.alarm(0)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("measurement controller interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    try:
        code = main()
    except BaseException:
        code = 1
    print(json.dumps({"status": "passed" if code == 0 else "failed", "error_code": None if code == 0 else "PERFORMANCE"}))
    raise SystemExit(code)
