"""Private same-job 1951 #712 diagnostic glue. Never a 48-cell acceptance run."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import traceback

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))

from common import ERRORS, Failure, atomic_json, contained, digest, load_profile, private_dir, require, verify_assets  # noqa: E402
from capacity import Budget  # noqa: E402
from lifecycle import Run  # noqa: E402
import envctl  # noqa: E402
from native_sql_matrix import compose_harness  # noqa: E402
from attestor_1951 import attest_current_job, frozen_operations  # noqa: E402
from diagnostic_container import MeasurementDocker, execute_pair  # noqa: E402
from live_wrapper import run_prepared  # noqa: E402
from orchestrate import DiagnosticFailure, SOURCE_COMMIT, SOURCE_TREE, plan, validate_dispatch  # noqa: E402
from same_job_1951 import derive_receipt, prepare_diagnostic_cell, validate_pins, OPERATORS  # noqa: E402

PIN_FILE = HERE / "sql-diagnostic-lock.json"
CERT = REPO / ".github/ops/native-evidence-cert.pem"
JOB_LIMIT_SECONDS = 10_800  # Separate 12-cell operational safety bound.
OPERATOR_NAMES = {"diagnostic_worker.py", "diagnostic_measure.py", "diagnostic.py",
                  "diagnostic_supervisor.py", "same_job_1951.py", "attestor_1951.py",
                  "live_wrapper.py", "audit.py", "orchestrate.py", "evidence.py",
                  "diagnostic_container.py", "diagnostic_entrypoint.py",
                  "hosted_test_env_qualification_1951.py"}
HOST_ONLY_OPERATORS = {"diagnostic_container.py", "diagnostic_entrypoint.py",
                       "hosted_test_env_qualification_1951.py"}


def load_pins(path, *, controller_sha, operator_dir):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and
            path.stat().st_size <= 16_384, "MISSING_PIN")
    value = json.loads(path.read_text())
    require(value.get("schema") == "alopex.sql-diagnostic-pins/v1" and
            set(value) == {"schema", "pin"} and
            set(value["pin"]["operator_sha256"]) == OPERATOR_NAMES and
            controller_sha == os.environ.get("GITHUB_SHA"), "MISSING_PIN")
    pin = dict(value["pin"], controller_commit=controller_sha)
    validate_pins(pin)
    require(pin["source_commit"] == SOURCE_COMMIT and pin["source_tree"] == SOURCE_TREE and
            all(digest(Path(operator_dir) / name) == expected
                for name, expected in pin["operator_sha256"].items()), "OPERATOR_IDENTITY")
    return pin


def cleanup_owned_runs(known, *, owned_root, budget, run_factory=Run,
                       docker_factory=MeasurementDocker, collect=envctl.collect):
    """Reconcile only cell IDs journaled by this invocation; retain unresolved input."""
    errors = []
    owned_root = Path(owned_root)
    for selected, input_path in known:
        try:
            require(selected["private_output"] == str(owned_root / "runs") and
                    selected["run_id"].startswith("diag-") and
                    input_path == owned_root / "cells" / selected["run_id"].removeprefix("diag-") and
                    not input_path.is_symlink(), "CLEANUP")
            run = run_factory(selected["private_output"], selected["run_id"])
            if run.path.exists():
                run.load()
                docker = docker_factory(selected)
                if run.record["phase"] not in ("collected", "deleted"):
                    collect(selected, docker, budget, selected["run_id"])
                    run.load()
                if run.record["phase"] != "deleted":
                    run.clean(docker.inspect, docker.remove)
                    run.load()
                require(run.record["phase"] == "deleted" and
                        not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
                run.verify_collection()
            if input_path.exists():
                require(input_path.is_dir(), "CLEANUP")
                shutil.rmtree(input_path)
        except BaseException as error:
            errors.append(error)
    if errors:
        raise ValueError("CLEANUP") from errors[0]
    return True


def execute_diagnostic(*, repo=REPO, runner_temp=None, environment=None):
    environment = os.environ if environment is None else environment
    repo = Path(repo)
    runner_temp = Path(environment["RUNNER_TEMP"] if runner_temp is None else runner_temp)
    controller_sha = environment["EXPECTED_CONTROLLER_SHA"]
    job_id = environment["GITHUB_RUN_ID"]
    pin = load_pins(repo / "scripts/ops/sql-diagnostic-lock.json",
                    controller_sha=controller_sha, operator_dir=repo / "scripts/ops")
    require(environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            environment.get("GITHUB_ACTIONS") == "true", "POLICY")
    root = runner_temp / "alopex-712-diagnostic"
    require(not root.exists(), "EVIDENCE_STATE")
    root = private_dir(root)
    private = runner_temp / "alopex-804-qualification"
    candidate = runner_temp / "alopex-804-candidate"
    operations = frozen_operations(repo)
    proof = attest_current_job(pin, job_id, repo=repo, runner_temp=runner_temp,
                               environment=environment, operations=operations)
    receipt = derive_receipt(pin, job_id=job_id, attestor=lambda _pin, _job: proof)
    receipt_path = root / "same-job-receipt.private.json"
    atomic_json(receipt_path, receipt)
    receipt_path.chmod(0o600)
    narrow = {key: pin[key] for key in ("source_commit", "source_tree", "artifact_id",
                                        "artifact_sha256", "assets_digest", "server_sha256",
                                        "wheel_sha256", "controller_commit")}
    narrow["operator_sha256"] = {name: value for name, value in
                                 pin["operator_sha256"].items()
                                 if name not in HOST_ONLY_OPERATORS}
    validate_dispatch(narrow, receipt, job_id=job_id, operator_dir=repo / "scripts/ops")
    profile_path = private / "profile.json"
    profile = load_profile(profile_path)
    budget = Budget(profile["storage"])
    require(profile["storage"]["poll_seconds"] == 10 and
            budget.used() + 4_000_000_000 + 1_000_000_000 < 40_000_000_000,
            "CAPACITY")
    manifest_path = candidate / "assets/assets.json"
    manifest = verify_assets(manifest_path, "linux/amd64")
    entries = {item["role"]: item for item in manifest["files"]}
    server = contained(manifest_path.parent, entries["server"]["path"])
    wheel = contained(manifest_path.parent, entries["wheel"]["path"])
    archive = contained(manifest_path.parent, entries["harness"]["path"])
    require(digest(archive) == "9012c7ead25e606fbe945298b435cdae2a02a6d293a946c44fd07dbc69c60b37",
            "IDENTITY")
    old_config = json.loads((repo / "scripts/ops/same-job-four-case-config.private.json").read_text())
    require(old_config["product_harness"]["commit"] ==
            "4b88e0844b0a6cc1dab93a69a9e7bffbd8a1f39c", "IDENTITY")
    harness = root / "measurement-harness.tar"
    harness_record = compose_harness(archive, repo / "scripts/performance",
                                     old_config["product_harness"]["files"], harness)
    operators = {role: {"path": repo / "scripts/ops" / source_name,
                        "sha256": pin["operator_sha256"][source_name]}
                 for role, (source_name, _) in OPERATORS.items()}
    cells_root = private_dir(root / "cells")
    results_root = private_dir(root / "runs")
    known = []

    def run_pair(cell, binding):
        require(binding["assets_digest"] == pin["assets_digest"], "IDENTITY")
        inputs = cells_root / cell["pair_id"]
        selected = dict(profile, run_id="diag-" + cell["pair_id"],
                        assets=str(inputs / "assets.json"),
                        private_output=str(results_root),
                        limits=dict(profile["limits"], case_seconds=300, run_seconds=420))
        known.append((selected, inputs))
        path, worker = prepare_diagnostic_cell(
            manifest_path, inputs, harness, harness_record, cell, pin, operators,
            verify_assets=verify_assets)
        require(str(path) == selected["assets"], "IDENTITY")
        try:
            return execute_pair(selected, budget, worker)
        except BaseException as error:
            if getattr(error, "code", None) == "CASE":
                atomic_json(root / "failed-cell.private.json", {
                    "schema": "alopex.sql-diagnostic-failed-cell/v1",
                    "cell_id": cell["pair_id"], "error_code": "CASE"})
            raise

    def cleanup():
        return cleanup_owned_runs(known, owned_root=root, budget=budget)

    return run_prepared(narrow, job_id=job_id, operator_dir=repo / "scripts/ops",
                        work=root / "work", export=root / "export",
                        receipt_path=receipt_path, expected_receipt_sha256=digest(receipt_path),
                        assets_manifest=manifest_path, server=server, wheel=wheel,
                        certificate=repo / ".github/ops/native-evidence-cert.pem",
                        used_bytes=budget.used, run_pair=run_pair, cleanup=cleanup,
                        job_limit_seconds=JOB_LIMIT_SECONDS)


def seal_failure(root):
    """Export only the fixed failure record as bounded encrypted evidence."""
    record = root / "diagnostic-failure.private.json"
    require(record.is_file() and not record.is_symlink() and
            record.stat().st_mode & 0o077 == 0 and
            record.stat().st_size <= 4096 and CERT.is_file(), "ENCRYPTION")
    export = root / "export"
    require(not export.is_symlink(), "ENCRYPTION")
    if export.exists():
        require(export.is_dir() and export.stat().st_mode & 0o077 == 0 and
                not any(export.iterdir()), "ENCRYPTION")
    else:
        export.mkdir(mode=0o700)
    archive = root / "diagnostic-failure.tar.gz"
    pending = export / ".evidence.pending.cms"
    ciphertext = export / "evidence.cms"
    require(not archive.exists() and not pending.exists() and
            not ciphertext.exists(), "EVIDENCE_STATE")
    inner = root / "work/plain/failure.private.json"
    if inner.exists() or inner.is_symlink():
        require(inner.is_file() and not inner.is_symlink() and
                inner.stat().st_mode & 0o077 == 0 and inner.stat().st_size <= 4096,
                "EVIDENCE_BOUND")
        value = json.loads(inner.read_text())
        require(type(value) is dict and set(value) == {
            "schema", "status", "phase", "primary", "cleanup",
            "performance_qualification"} and
            value["schema"] == "alopex.sql-diagnostic-failure/v1" and
            value["status"] == "failed" and value["performance_qualification"] is False and
            value["phase"] in {"qualification", "capacity", "cells", "audit",
                               "final_capacity", "evidence", "encryption"} and
            all(item is None or (type(item) is dict and set(item) == {"type", "code"} and
                type(item["type"]) is str and len(item["type"]) <= 64 and
                type(item["code"]) is str and len(item["code"]) <= 64 and
                item["code"].replace("_", "").isalpha() and item["code"].isupper())
                for item in (value["primary"], value["cleanup"])), "EVIDENCE_ALLOWLIST")
    cell_evidence = []
    marker = root / "failed-cell.private.json"
    selected_cell = None
    if marker.exists() or marker.is_symlink():
        require(marker.is_file() and not marker.is_symlink() and
                marker.stat().st_mode & 0o077 == 0 and marker.stat().st_size <= 512,
                "EVIDENCE_BOUND")
        selected_cell = json.loads(marker.read_text())
        require(type(selected_cell) is dict and selected_cell.get("schema") ==
                "alopex.sql-diagnostic-failed-cell/v1" and
                set(selected_cell) == {"schema", "cell_id", "error_code"} and
                selected_cell["error_code"] == "CASE" and
                selected_cell["cell_id"] in {cell["pair_id"] for cell in plan()},
                "EVIDENCE_ALLOWLIST")
    runs = root / "runs"
    require(selected_cell is None or (runs.is_dir() and not runs.is_symlink()),
            "EVIDENCE_STATE")
    if runs.is_dir() and not runs.is_symlink():
        for cell in plan():
            if selected_cell is not None and cell["pair_id"] != selected_cell["cell_id"]:
                continue
            run_id = "diag-" + cell["pair_id"]
            run = Run(runs, run_id)
            if not run.path.exists():
                require(selected_cell is None, "EVIDENCE_STATE")
                continue
            require(run.root.is_dir() and not run.root.is_symlink() and
                    run.path.is_file() and not run.path.is_symlink(), "EVIDENCE_BOUND")
            run.load()
            if run.record["phase"] != "deleted":
                require(selected_cell is None, "EVIDENCE_STATE")
                continue  # The outer cleanup receipt retains any unresolved run.
            run.verify_collection()
            collected = run.root / "collected"
            report = collected / "result.json"
            require(report.is_file() and not report.is_symlink() and
                    report.stat().st_size <= 4096, "EVIDENCE_BOUND")
            if selected_cell is None and json.loads(report.read_text()).get("status") != "failed":
                continue
            require(not cell_evidence, "EVIDENCE_BOUND")
            for name, limit in (("result.json", 4096), ("pair/status.json", 4096),
                                ("pair/worker.log", 32768)):
                path = collected / name
                if not path.exists() and name == "pair/worker.log":
                    continue
                info = path.lstat()
                require(stat.S_ISREG(info.st_mode) and info.st_mode & 0o077 == 0 and
                        info.st_size <= limit, "EVIDENCE_BOUND")
                cell_evidence.append((path, "diagnostic/failed-cell/" + name))
    with tarfile.open(archive, "x:gz") as bundle:
        bundle.add(record, arcname="diagnostic/diagnostic-failure.private.json",
                   recursive=False)
        if inner.exists():
            bundle.add(inner, arcname="diagnostic/orchestrate-failure.private.json",
                       recursive=False)
        if selected_cell is not None:
            bundle.add(marker, arcname="diagnostic/failed-cell.private.json",
                       recursive=False)
        for path, name in cell_evidence:
            bundle.add(path, arcname=name, recursive=False)
    archive.chmod(0o600)
    require(archive.stat().st_size <= 65536, "EVIDENCE_BOUND")
    try:
        subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
                        "-outform", "DER", "-in", str(archive), "-out", str(pending),
                        str(CERT)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=True, timeout=120)
        require(pending.is_file() and not pending.is_symlink() and
                0 < pending.stat().st_size <= 80_000, "ENCRYPTION")
        pending.chmod(0o600)
        pending.replace(ciphertext)
        archive.unlink()
    except BaseException:
        pending.unlink(missing_ok=True)
        raise


def fixed_error_site(error):
    """Return only source basename/function/line; never a path or exception text."""
    if isinstance(error, DiagnosticFailure) and error.primary_error is not None:
        error = error.primary_error
    for frame in reversed(traceback.extract_tb(error.__traceback__)):
        source = Path(frame.filename)
        if source.is_relative_to(REPO / "scripts") and source.name not in ("common.py", "orchestrate.py"):
            return {"module": source.name, "function": frame.name, "line": frame.lineno}
    return None


def fixed_error_code(error):
    """Allow only fixed failure codes, including the orchestrator's primary error."""
    if isinstance(error, DiagnosticFailure) and error.primary_error is not None:
        error = error.primary_error
    allowed = set(ERRORS) | {"MISSING_PIN", "MISSING_ASSET", "QUALIFICATION", "POLICY",
                           "IDENTITY", "OPERATOR_IDENTITY", "CAPACITY", "CLEANUP",
                           "EVIDENCE_STATE", "EVIDENCE_BOUND", "ENCRYPTION",
                           "QUALIFICATION_BINDING", "SOURCE_IDENTITY"}
    code = error.code if isinstance(error, Failure) else str(error)
    return code if type(error) in (ValueError, Failure) and code in allowed else "INTERNAL"


def main():
    os.umask(0o077)
    try:
        result = execute_diagnostic()
        print(json.dumps({"schema": "alopex.sql-diagnostic-public/v1", "status": "passed",
                          "measurement_cells": result["measurement_cells"],
                          "performance_qualification": False}, sort_keys=True))
        return 0
    except BaseException as error:
        # Never print the raw exception, path, SQL, machine profile or trace.
        try:
            root = Path(os.environ["RUNNER_TEMP"]) / "alopex-712-diagnostic"
            if root.is_dir() and not root.is_symlink() and root.stat().st_mode & 0o077 == 0:
                atomic_json(root / "diagnostic-failure.private.json",
                            {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                             "error_code": fixed_error_code(error), "error_site": fixed_error_site(error),
                             "performance_qualification": False})
                seal_failure(root)
        except BaseException:
            pass
        print('{"schema":"alopex.sql-diagnostic-public/v1","status":"failed","error_code":"DIAGNOSTIC_FAILED","performance_qualification":false}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
