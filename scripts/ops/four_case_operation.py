"""Four-case SQL operation on a previously qualified same-job image."""
import json
from pathlib import Path
import shutil
import time

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
        # The archived SQL has no ORDER BY on select-all. Exercise the strict
        # worker order oracle for all subjects before any comparison cell.
        preflight = []
        for cell in (row for row in plan() if row["case"] == "select-all" and row["trial"] == 0):
            inputs = work / "preflight-input"
            selected = dict(profile, run_id="preflight-" + cell["id"])
            try:
                budget.before("accept")
                role = "published" if cell["subject"] == "postgresql" else cell["subject"]
                manifest = repo / ("native-" + role + "-import") / "assets/assets.json"
                path, worker = prepare_cell(manifest, inputs, harness, harness_record, cell, operators)
                selected["assets"] = str(path)
                execute_cell(selected, budget, worker)
                observed = Run(selected["private_output"], selected["run_id"]).load()
                observed.verify_collection()
                preflight.append({"case": cell["case"], "subject": cell["subject"],
                                  "status": "passed"})
            finally:
                atomic_json(work / "preflight-progress.json", preflight)
                if inputs.exists():
                    shutil.rmtree(inputs)
            budget.check()
            require(budget.used() - config["_initial_storage_bytes"] <=
                    config["matrix_additional_bound_bytes"], "CAPACITY")
        require(len(preflight) == len(config["subjects"]) + 1, "CASE")
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
            require(budget.used() - config["_initial_storage_bytes"] <=
                    config["matrix_additional_bound_bytes"], "CAPACITY")
        atomic_json(work / "matrix-results.json", rows)
        result = summarize(rows)
        atomic_json(work / "comparison.json", result)
        # These fields contain only controlled case names, ratios and timings.
        # Device identity, configuration, paths and process observations remain
        # in the separately encrypted evidence archive.
        atomic_json(export / "comparison.json", result)
        public.update(schema="alopex.native-sql-four-case-matrix/v1", measurement_cells=len(rows),
                      completed_cells=sum(row["status"] == "passed" for row in rows),
                      matrix_complete=all(row["status"] == "passed" for row in rows),
                      comparison_sha256=digest(export / "comparison.json"), performance_qualification=False)
        if fatal is not None:
            raise fatal
        require(all(row["status"] == "passed" for row in rows), "CASE")
    return run
