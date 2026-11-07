"""Fixed SQL matrix inputs and the predeclared latency decision rules."""
from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import shutil
import statistics
import tarfile

from native_sql_measure_worker import ISOLATED, WRITES

SUBJECTS = ("published", "before", "candidate", "postgresql")
CASES = tuple(ISOLATED) + ("insert-commit", "batch-insert-commit", "random-get-hit",
                           "random-get-miss", "random-update-commit", "random-delete-commit")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def plan():
    rows = []
    for case in CASES:
        for trial in range(3):
            order = SUBJECTS if trial % 2 == 0 else tuple(reversed(SUBJECTS))
            for subject in order:
                rows.append({"id": f"sql-{case}-{trial}-{subject}", "case": case,
                             "trial": trial, "subject": subject,
                             "engine": "postgresql" if subject == "postgresql" else "alopex-server"})
    return rows


def safe_name(name):
    value = PurePosixPath(name)
    require(not value.is_absolute() and value.parts and ".." not in value.parts,
            "invalid harness path")
    return value.as_posix()


def compose_harness(docs_archive, product_root, product_files, destination):
    """Bundle unchanged docs workers and the fixed product fixture dependency closure."""
    require(not destination.exists(), "harness output already exists")
    contents = {}
    docs_files = {}
    with tarfile.open(docs_archive) as source:
        members = source.getmembers()
        require(len(members) < 256 and sum(m.size for m in members) < 8_000_000, "harness bound")
        for member in members:
            name = safe_name(member.name)
            require(member.isfile() or member.isdir(), "non-regular harness input")
            if member.isdir():
                continue
            require(name.startswith("scripts/performance/") and "docs/" + name not in contents,
                    "unexpected docs harness member")
            data = source.extractfile(member).read()
            contents["docs/" + name] = data
            docs_files[name.removeprefix("scripts/performance/")] = hashlib.sha256(data).hexdigest()
    require("sql_resources.py" in docs_files and "subject_identity.py" in docs_files, "missing docs observer")
    required_product = {"parity_v0811_measure.py", "parity_v0811_gate.py", "fixtures/sql-v0.8.11-curated.json"}
    require(set(product_files) == required_product, "product dependency closure mismatch")
    product_hashes = {}
    for name, record in product_files.items():
        path = product_root / safe_name(name)
        require(not path.is_symlink() and path.resolve().is_relative_to(product_root.resolve()), "product path mismatch")
        data = path.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        require(len(data) == record["size"] and actual == record["sha256"], "product harness identity mismatch")
        contents["product/scripts/performance/" + name] = data
        product_hashes[name] = actual
    with tarfile.open(destination, "w") as output:
        for name, data in sorted(contents.items()):
            item = tarfile.TarInfo(name)
            item.size, item.mode, item.mtime, item.uid, item.gid = len(data), 0o644, 0, 0, 0
            output.addfile(item, io.BytesIO(data))
    return {"docs_subdirectory": "docs", "product_subdirectory": "product",
            "docs_files": docs_files, "product_files": product_hashes,
            "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}


def prepare_cell(product_manifest, destination, harness, harness_record, cell, operators):
    from common import atomic_json, contained, digest, private_dir, verify_assets
    require(not destination.exists(), "cell input already exists")
    source = json.loads(product_manifest.read_text())
    source = verify_assets(product_manifest, source["platform"])
    require({item["role"] for item in source["files"]}.issubset(
        {"server", "wheel", "receipt", "library", "harness"}), "unexpected product input role")
    require(cell in plan(), "cell outside fixed matrix")
    private_dir(destination)
    manifest = copy.deepcopy(source)
    manifest["files"] = []
    for item in source["files"]:
        if item["role"] == "harness":
            continue
        target = contained(destination, item["path"])
        private_dir(target.parent)
        shutil.copyfile(contained(product_manifest.parent, item["path"]), target)
        manifest["files"].append(item)
    def add(role, name):
        path = destination / name
        manifest["files"].append({"role": role, "path": name, "size": path.stat().st_size, "sha256": digest(path)})
    shutil.copyfile(harness, destination / "measurement-harness.tar")
    require(digest(destination / "measurement-harness.tar") == harness_record["sha256"], "combined harness changed")
    add("harness", "measurement-harness.tar")
    receipt_entry = next(item for item in source["files"] if item["role"] == "receipt")
    receipt = json.loads(contained(product_manifest.parent, receipt_entry["path"]).read_text())
    config = {"schema": "alopex.native-sql-supervisor/v1", **cell, "source": receipt["source"],
              **{key: harness_record[key] for key in ("docs_subdirectory", "product_subdirectory", "docs_files", "product_files")}}
    atomic_json(destination / "measurement.json", config)
    add("measurement_config", "measurement.json")
    require(set(operators) == {"measurement_controller", "measurement_worker"}, "operator inventory mismatch")
    for role, record in operators.items():
        name = "controller.py" if role == "measurement_controller" else "worker.py"
        require(digest(record["path"]) == record["sha256"], "operator bytes changed")
        shutil.copyfile(record["path"], destination / name)
        add(role, name)
    path = destination / "assets.json"
    atomic_json(path, manifest)
    verify_assets(path, source["platform"])
    return path, config


def median_mad(values):
    import math
    require(values and all(type(value) in (float, int) and math.isfinite(value) and value > 0
                           for value in values), "invalid measurement values")
    median = statistics.median(values)
    return median, statistics.median(abs(value - median) for value in values)


def compare(baseline, candidate):
    """#690 comment 6578203208; D is MAD of three trial medians."""
    require(len(baseline) == len(candidate) == 3, "three trials required")
    import math
    require(all(type(value) in (float, int) and math.isfinite(value) and value > 0
                for value in baseline + candidate), "invalid trial median")
    b, db = median_mad(baseline)
    c, dc = median_mad(candidate)
    noise = 3 * max(db, dc)
    ratio = c / b
    status = "pending_noise" if noise > .05 * b else ("regression" if ratio > 1.05 else "within_tolerance")
    improvement = (b - c) / b
    return {"baseline_median": b, "candidate_median": c, "baseline_trial_mad": db,
            "candidate_trial_mad": dc, "noise": noise, "latency_ratio": ratio,
            "status": status, "increase": c > b, "improvement_fraction": improvement,
            "exceeds_regression_threshold": ratio > 1.05,
            "improvement_proven": status != "pending_noise" and improvement >= .05 and
            b - c > noise and all(after / before < 1 for before, after in zip(baseline, candidate)),
            "paired_ratios": [after / before for before, after in zip(baseline, candidate)]}


def summarize(cells):
    expected = {(row["case"], row["trial"], row["subject"]) for row in plan()}
    require(len(cells) == len(expected) and
            {(row["case"], row["trial"], row["subject"]) for row in cells} == expected,
            "incomplete or duplicated matrix")
    results = []
    for case in CASES:
        selected = [row for row in cells if row["case"] == case]
        if any(row["status"] != "passed" for row in selected):
            results.append({"case": case, "status": "failed_cell"})
            continue
        require(len({row["result_checksum"] for row in selected}) == 1, "cross-subject result mismatch")
        if case in WRITES:
            require(len({row["input_checksum"] for row in selected}) == 1, "cross-subject input mismatch")
        values = {subject: [next(row["trial_median_seconds"] for row in selected
                                 if row["subject"] == subject and row["trial"] == trial)
                            for trial in range(3)] for subject in SUBJECTS}
        comparisons = {baseline: compare(values[baseline], values["candidate"])
                       for baseline in ("before", "published")}
        ref, _ = median_mad(values["postgresql"])
        candidate, _ = median_mad(values["candidate"])
        results.append({"case": case, "status": "measured", "trials": values,
                        "comparisons": comparisons, "reference_latency_ratio": candidate / ref,
                        "reference_throughput_ratio": ref / candidate, "reference_goal_met": candidate <= ref})
    return {"schema": "alopex.native-sql-comparison/v1", "cases": results,
            "performance_acceptance": False}
