"""Fixed #684 fixture plan, immutable inputs, and collected-process verification."""
import copy
import json
from pathlib import Path
import shutil

from native_begin_supervisor import extension_digest, probes, require, verify_begin_assets, verify_events

REGRESSION_FIXTURES = ((0, 1), (100, 1))
TRIALS = 8


def plan():
    cells = []
    for rows, indexes in REGRESSION_FIXTURES:
        for trial in range(TRIALS):
            for subject in (("before", "candidate") if trial % 2 == 0 else ("candidate", "before")):
                cells.append(dict(id=f"begin-{rows}-i{indexes}-{trial}-{subject}",
                                  rows=rows, indexes=indexes, trial=trial, subject=subject))
    return cells


def prepare_cell(product_manifest, destination, cell, operators, harness_path=None):
    from common import atomic_json, contained, digest, private_dir
    require(cell in plan() and not destination.exists(), "invalid or existing fixture")
    source = json.loads(product_manifest.read_text())
    source = verify_begin_assets(product_manifest)
    roles = {item["role"] for item in source["files"]}
    require(roles.issubset({"server", "wheel", "receipt", "library", "harness", "wheel_receipt"}),
            "unexpected product asset role")
    receipt_entry = next(item for item in source["files"] if item["role"] == "receipt")
    receipt = json.loads(contained(product_manifest.parent, receipt_entry["path"]).read_text())
    wheel_build = receipt.get("build", {}).get("wheel")
    reused = isinstance(wheel_build, dict) and wheel_build.get("mode") == "reused_development_asset"
    if reused or "wheel_receipt" in roles:
        from reused_native_wheel import verify_composed_receipts
        require(reused and "wheel_receipt" in roles, "original wheel receipt is required")
        verify_composed_receipts(product_manifest.parent, source)
    private_dir(destination)
    manifest = copy.deepcopy(source)
    for item in source["files"]:
        target = contained(destination, item["path"])
        private_dir(target.parent)
        shutil.copyfile(contained(product_manifest.parent, item["path"]), target)
    receipt_entry = next(item for item in source["files"] if item["role"] == "receipt")
    receipt = json.loads(contained(product_manifest.parent, receipt_entry["path"]).read_text())
    wheel = next(item for item in source["files"] if item["role"] == "wheel")
    config = dict(cell, schema="alopex.native-begin-supervisor/v1", source=receipt["source"],
                  extension_sha256=extension_digest(contained(product_manifest.parent, wheel["path"])))
    def add(role, name):
        path = destination / name
        manifest["files"].append(dict(role=role, path=name, size=path.stat().st_size, sha256=digest(path)))
    if source["schema"] == "alopex.native-wheel-assets/v1":
        require(harness_path is not None and harness_path.is_file(), "wheel-only harness missing")
        shutil.copyfile(harness_path, destination / "harness.tar")
        add("harness", "harness.tar")
    require(not (destination / "measurement.json").exists(), "configuration collides with product asset")
    atomic_json(destination / "measurement.json", config)
    add("measurement_config", "measurement.json")
    require(set(operators) == {"measurement_controller", "measurement_worker"}, "operator inventory differs")
    for role, operator in operators.items():
        require(digest(operator["path"]) == operator["sha256"], "operator bytes changed")
        name = "controller.py" if role == "measurement_controller" else "worker.py"
        require(not (destination / name).exists(), "operator collides with product asset")
        shutil.copyfile(operator["path"], destination / name)
        add(role, name)
    path = destination / "assets.json"
    atomic_json(path, manifest)
    verify_begin_assets(path)
    return path, config


def report(run, config, assets_digest):
    from common import digest, read_json
    run.verify_collection()
    root = run.root / "collected"
    value = read_json(root / "result.json")
    require(value["schema"] == "alopex.native-begin-measurement/v1" and
            value["assets_digest"] == assets_digest and value["source"] == config["source"] and
            value["extension_sha256"] == config["extension_sha256"] and
            all(value[key] == config[key] for key in ("rows", "indexes", "trial")), "result identity differs")
    require(digest(root / "cell/processes.json") == value["processes_sha256"], "process list digest differs")
    return root, value


def verify_collected(run, config, assets_digest):
    from common import digest, read_json
    root, value = report(run, config, assets_digest)
    require(value["status"] == "passed" and value["worker_error"] is None and
            value["finalization_error"] is None and not (root / "cleanup-errors.json").exists(),
            "fixture failed")
    records = read_json(root / "cell/processes.json")
    specs = [None, *probes(config["rows"])]
    require(len(records) == len(specs), "missing fixture processes")
    for index, (record, spec) in enumerate(zip(records, specs)):
        require(record["ordinal"] == index and record["spec"] == spec and record["status"] == "passed",
                "process identity or status differs")
        raw = root / "cell" / f"process-{index}.ndjson"
        require(digest(raw) == record["raw_sha256"], "process bytes changed")
        verify_events([json.loads(line) for line in raw.read_text().splitlines()], config,
                      config["extension_sha256"], spec)
    return dict(status="passed", processes=len(records), processes_sha256=value["processes_sha256"],
                source=value["source"], extension_sha256=value["extension_sha256"])


def recovered(run, config, assets_digest):
    if run.record["phase"] != "deleted":
        return False
    root, value = report(run, config, assets_digest)
    return (value["status"] == "failed" and value["worker_error"] is not None and
            value["finalization_error"] is None and not (root / "cleanup-errors.json").exists())


def execute_cell(profile, budget, config):
    import envctl
    from native_sql_measure_container import execute_cell as execute
    original = envctl.verify_assets
    envctl.verify_assets = verify_begin_assets
    try:
        return execute(profile, budget, config, verify=verify_collected, recovered=recovered)
    finally:
        envctl.verify_assets = original
