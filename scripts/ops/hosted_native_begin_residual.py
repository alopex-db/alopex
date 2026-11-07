"""Manual #684 comparison using the qualified VM, image and container boundary."""
from __future__ import annotations

import base64
import json
import os
import shutil
import signal
import time

import hosted_native_performance as shared


def settings():
    value = json.loads(base64.b64decode(os.environ["PUBLIC_PERFORMANCE_CONFIG"], validate=True))
    if (set(value["subjects"]) != {"before", "candidate"} or
            type(value["matrix_seconds"]) is not int or not 0 < value["matrix_seconds"] <= 3600 or
            value["limits"]["case_seconds"] != 300 or value["limits"]["run_seconds"] != 420 or
            value["schema"] != "alopex.native-begin-residual-input/v1"):
        raise ValueError("unexpected fixed begin comparison configuration")
    return value


def operation(repo, config):
    def run(profile, docker, budget, work, export, public):
        from common import atomic_json, digest, require
        from lifecycle import Run
        from native_begin_residual_matrix import execute_cell, plan, prepare_cell

        budget.before("accept")
        operators = {role: {"path": repo / "scripts/ops" / item["filename"], "sha256": item["sha256"]}
                     for role, item in config["operators"].items()}
        cells = plan()
        atomic_json(work / "begin-plan.json", dict(cells=cells, configuration=config,
                                                   image=public["image_digest"], cpu=profile["cpu"]))
        started = time.monotonic()
        rows = []
        fatal = None
        for cell in cells:
            if fatal is not None or time.monotonic() - started + config["limits"]["run_seconds"] > config["matrix_seconds"]:
                rows.append(dict(cell, status="not_run", error_code="STOPPED"))
                continue
            inputs = work / "begin-input"
            selected = dict(profile, run_id=cell["id"])
            try:
                budget.before("accept")
                manifest = repo / ("native-" + cell["subject"] + "-import") / "assets/assets.json"
                path, worker = prepare_cell(manifest, inputs, cell, operators)
                selected["assets"] = str(path)
                result = execute_cell(selected, budget, worker)
                owned = Run(selected["private_output"], cell["id"]).load()
                owned.verify_collection()
                require(owned.record["phase"] == "deleted", "CLEANUP")
                rows.append(dict(cell, **result, assets_digest=digest(path)))
            except BaseException as error:
                code = getattr(error, "code", "INTERNAL")
                rows.append(dict(cell, status="failed", error_code=code, detail=repr(error)))
                if code != "CASE" or getattr(error, "cleanup_errors", []):
                    fatal = error
            finally:
                atomic_json(work / "begin-progress.json", rows)
                if inputs.exists():
                    shutil.rmtree(inputs)
            budget.check()
        atomic_json(work / "begin-results.json", rows)
        # No timings or private process identity escape the encrypted evidence.
        # A separate auditor will recompute comparisons from the collected raw.
        summary = dict(schema="alopex.native-begin-residual/v1",
                       cells=[{key: row[key] for key in ("id", "rows", "indexes", "trial", "subject", "status")}
                              for row in rows],
                       complete=all(row["status"] == "passed" for row in rows),
                       performance_acceptance=False)
        atomic_json(export / "begin-comparison.json", summary)
        public.update(schema="alopex.native-begin-residual/v1", measurement_cells=len(rows),
                      completed_cells=sum(row["status"] == "passed" for row in rows),
                      matrix_complete=summary["complete"], comparison_sha256=digest(export / "begin-comparison.json"),
                      performance_qualification=False)
        if fatal is not None:
            raise fatal
        require(summary["complete"], "CASE")
    return run


def main():
    return shared.main(settings_reader=settings, operation_factory=operation)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("begin comparison interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    try:
        code = main()
    except BaseException:
        code = 1
    print(json.dumps({"status": "passed" if code == 0 else "failed", "error_code": None if code == 0 else "PERFORMANCE"}))
    raise SystemExit(code)
