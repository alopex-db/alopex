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

from .worker import ISOLATED, result_digest

SUBJECTS = ("published", "before", "candidate", "postgresql")
CASES = tuple(ISOLATED)


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
    require({"sql_resources.py", "subject_identity.py", "server-sql-isolated-v0816.py"}.issubset(docs_files),
            "missing pinned v0816 SQL harness or observer")
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


def _hex(value, length):
    return type(value) is str and len(value) == length and all(char in "0123456789abcdef" for char in value)


def verify_subject_pin(product_manifest, manifest, receipt, cell, expected_subject):
    """Bind a cell to an independently supplied product pin before copying bytes.

    The PostgreSQL reference image is a separate pin. Its client wheel and
    supporting product files are the published product, never a fifth build.
    """
    require(type(expected_subject) is dict and cell in plan(), "untrusted cell or subject pin")
    subject = cell["subject"]
    product_subject = "published" if subject == "postgresql" else subject
    require(expected_subject.get("subject") == subject and
            expected_subject.get("product_subject") == product_subject,
            "cross-subject product substitution")
    source = expected_subject.get("source")
    require(type(source) is dict and set(source) == {"commit", "tree", "profile"} and
            _hex(source["commit"], 40) and _hex(source["tree"], 40) and
            source["profile"] == "release", "untrusted source pin")
    require(_hex(expected_subject.get("assets_digest"), 64) and
            hashlib.sha256(product_manifest.read_bytes()).hexdigest() == expected_subject["assets_digest"] and
            type(expected_subject.get("platform")) is str and
            manifest.get("platform") == expected_subject["platform"] and
            manifest.get("source_commit") == source["commit"] and
            ("source_tree" not in manifest or manifest["source_tree"] == source["tree"]),
            "product manifest pin mismatch")
    require(type(receipt) is dict and receipt.get("source") == source,
            "product receipt source mismatch")
    files = expected_subject.get("files")
    require(type(files) is list and files and all(type(item) is dict and
            set(item) == {"role", "path", "size", "sha256"} and
            type(item["role"]) is str and type(item["path"]) is str and
            type(item["size"]) is int and item["size"] >= 0 and
            _hex(item["sha256"], 64) for item in files), "untrusted product file pin")
    actual = [item for item in manifest["files"] if item["role"] != "harness"]
    require(len(actual) == len(files) and
            {tuple(sorted(item.items())) for item in actual} ==
            {tuple(sorted(item.items())) for item in files} and
            len({item["path"] for item in files}) == len(files) and
            {item["role"] for item in files}.issuperset({"server", "wheel", "receipt"}) and
            len([item for item in files if item["role"] == "receipt"]) == 1 and
            len([item for item in files if item["role"] == "wheel"]) == 1 and
            len([item for item in files if item["role"] == "server"]) == 1,
            "product role bytes mismatch")
    if subject == "postgresql":
        published = expected_subject.get("published_client_pin")
        require(_hex(expected_subject.get("reference_image_digest"), 64) and
                type(published) is dict and published.get("subject") == "published" and
                published.get("product_subject") == "published" and
                all(expected_subject[key] == published.get(key) for key in
                    ("source", "assets_digest", "platform", "files")),
                "missing independent PostgreSQL image or published client pin")
    else:
        require("reference_image_digest" not in expected_subject and
                "published_client_pin" not in expected_subject,
                "reference image assigned to product subject")
    return source


def prepare_cell(product_manifest, destination, harness, harness_record, cell, operators,
                 expected_subject):
    from common import atomic_json, contained, digest, private_dir, verify_assets
    require(not destination.exists(), "cell input already exists")
    source = json.loads(product_manifest.read_text())
    source = verify_assets(product_manifest, expected_subject["platform"])
    require({item["role"] for item in source["files"]}.issubset(
        {"server", "wheel", "receipt", "library", "harness"}), "unexpected product input role")
    receipt_entry = next((item for item in source["files"] if item["role"] == "receipt"), None)
    require(receipt_entry is not None, "missing product receipt")
    receipt = json.loads(contained(product_manifest.parent, receipt_entry["path"]).read_text())
    pinned_source = verify_subject_pin(product_manifest, source, receipt, cell, expected_subject)
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
    config = {"schema": "alopex.native-sql-supervisor/v1", **cell, "source": pinned_source,
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
    require(type(cells) is list and all(type(row) is dict for row in cells) and
            len(cells) == len(expected) and
            {(row["case"], row["trial"], row["subject"]) for row in cells} == expected,
            "incomplete or duplicated matrix")
    require(all(row.get("engine") == ("postgresql" if row["subject"] == "postgresql" else "alopex-server")
                and row.get("id") == f"sql-{row['case']}-{row['trial']}-{row['subject']}"
                for row in cells), "matrix identity or engine mismatch")
    results = []
    for case in CASES:
        selected = [row for row in cells if row["case"] == case]
        require(all(row.get("status") == "passed" for row in selected), "failed or missing cell observation")
        require(all(type(row.get("result_checksum")) is str and len(row["result_checksum"]) == 64 and
                    type(row.get("trial_median_seconds")) in (int, float)
                    for row in selected), "missing cell result or timing")
        require(len({row["result_checksum"] for row in selected}) == 1, "cross-subject result mismatch")
        require(selected[0]["result_checksum"] == result_digest(case), "SQL oracle mismatch")
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
    return {"schema": "alopex.native-sql-four-case-comparison/v1", "cases": results,
            "performance_acceptance": False}
