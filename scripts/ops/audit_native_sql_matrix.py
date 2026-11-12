"""Independent, private audit of collected fixed SQL measurements; no execution."""
import hashlib
import json
import math
from pathlib import PurePosixPath
import statistics
import tarfile

CASES = ("aggregate", "ordered", "select-all", "insert-rollback")
SUBJECTS = ("published", "before", "candidate", "postgresql")
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes", "rss_after_bytes",
            "read_bytes", "write_bytes", "cancelled_write_bytes")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def select_all_checksum():
    expected = [{"id": index, "value": float(index)} for index in range(0, 34250, 2)]
    return sha(json.dumps(expected, sort_keys=True).encode())


def middle(values):
    require(values and all(type(v) in (int, float) and math.isfinite(v) and v > 0 for v in values), "invalid latency")
    center = statistics.median(values)
    return center, statistics.median(abs(v - center) for v in values)


def decision(before, after):
    require(len(before) == len(after) == 3, "three trial medians required")
    baseline, baseline_mad = middle(before)
    candidate, candidate_mad = middle(after)
    noise = 3 * max(baseline_mad, candidate_mad)
    ratio = candidate / baseline
    noisy = noise > baseline * .05
    return {"baseline_median": baseline, "candidate_median": candidate,
            "baseline_trial_mad": baseline_mad, "candidate_trial_mad": candidate_mad,
            "noise": noise, "latency_ratio": ratio,
            "status": "pending_noise" if noisy else "regression" if ratio > 1.05 else "within_tolerance",
            "increase": candidate > baseline, "improvement_fraction": (baseline - candidate) / baseline,
            "exceeds_regression_threshold": ratio > 1.05,
            "improvement_proven": not noisy and (baseline - candidate) / baseline >= .05 and
            baseline - candidate > noise and all(c < b for b, c in zip(before, after)),
            "paired_ratios": [c / b for b, c in zip(before, after)]}


def equal(actual, expected):
    if type(expected) is float:
        require(type(actual) in (int, float) and math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-15), "numeric summary mismatch")
    elif isinstance(expected, dict):
        require(isinstance(actual, dict) and set(actual) == set(expected), "summary keys mismatch")
        for key in expected: equal(actual[key], expected[key])
    elif isinstance(expected, list):
        require(isinstance(actual, list) and len(actual) == len(expected), "summary list mismatch")
        for a, e in zip(actual, expected): equal(a, e)
    else:
        require(type(actual) is type(expected) and actual == expected, "summary value mismatch")


class Evidence:
    def __init__(self, path):
        self.archive = tarfile.open(path)
        self.members = {}
        total = 0
        try:
            for member in self.archive.getmembers():
                name = PurePosixPath(member.name)
                require(not name.is_absolute() and ".." not in name.parts and member.name not in self.members, "unsafe or duplicate evidence member")
                require(member.isfile() or member.isdir(), "nonregular evidence member")
                total += member.size
                require(member.size <= 32_000_000 and total <= 512_000_000, "evidence size bound")
                self.members[member.name] = member
            require(len(self.members) <= 30000, "evidence inventory bound")
        except BaseException:
            self.archive.close()
            raise

    def read(self, name):
        require(name in self.members and self.members[name].isfile(), "missing evidence file: " + name)
        return self.archive.extractfile(self.members[name]).read()

    def json(self, name):
        return json.loads(self.read(name))

    def close(self):
        self.archive.close()


def audit_cell(evidence, row, config, *, run_prefix=None):
    case, subject, trial = row["case"], row["subject"], row["trial"]
    prefix = run_prefix or f"results/sql-{case}-{trial}-{subject}/"
    run = evidence.json(prefix + "run.json")
    require(run["phase"] == "deleted", "owned run cleanup incomplete")
    collected = prefix + "collected/"
    manifest = evidence.json(prefix + "collection.json")
    require(manifest and "result.json" in manifest, "missing collection manifest")
    for name, digest in manifest.items():
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and ".." not in relative.parts, "invalid collected path")
        require(sha(evidence.read(collected + name)) == digest, "collection hash mismatch")
    report = evidence.json(collected + "result.json")
    status = evidence.json(collected + "cell/status.json")
    require(report["schema"] == "alopex.native-sql-measurement/v1" and
            report["status"] == status["status"] == "passed" and status["worker_error"] is None and
            status["stop_error"] is None, "failed cell reported as measured")
    require(report["assets_digest"] == run["identities"]["assets_digest"] and
            report["cell_status_sha256"] == sha(evidence.read(collected + "cell/status.json")), "cell identity mismatch")
    raw_bytes = evidence.read(collected + "cell/raw.ndjson")
    raw = [json.loads(line) for line in raw_bytes.splitlines()]
    result_bytes = evidence.read(collected + "cell/measurement.json")
    result = json.loads(result_bytes)
    require(sha(result_bytes) == status["measurement"]["result_sha256"] and
            sha(raw_bytes) == result["raw_sha256"] == status["measurement"]["raw_sha256"] == row["raw_sha256"], "measurement hash mismatch")
    role = "published" if subject == "postgresql" else subject
    expected = config["subjects"][role]
    source = result["subject_identity"]["source"]
    require(source == row["source"] == report["source"] and
            source == {"commit": expected["source_commit"], "tree": expected["source_tree"], "profile": "release"}, "subject source mismatch")
    engine = "postgresql" if subject == "postgresql" else "alopex-server"
    require(all(result[k] == report[k] == value for k, value in (("case", case), ("engine", engine), ("trial", trial))), "cell coordinate mismatch")
    require(result["server_client"] is True and result["warmups"] == 3 and result["samples"] == len(raw) == 21, "sample boundary mismatch")
    rows = 1 if case == "insert-rollback" else 34250
    require(result["fixture_rows"] == rows, "fixture size mismatch")
    resource_sha = sha(evidence.read(collected + "cell/resources.json"))
    for index, sample in enumerate(raw):
        require(sample.get("sample", sample.get("index")) == index, "raw sample ordering mismatch")
        observation = sample["resources"]
        require(observation["schema"] == "alopex.sql-operation-resources/v1" and
                observation["status"] == "observed" and observation["coverage"] == "stable_pid_namespace_allocation" and
                observation["manifest_sha256"] == resource_sha, "resource coverage missing")
        servers, client = observation["server_processes"], observation["client"]
        require(servers and len({p["pid"] for p in servers}) == len(servers) and client["pid"] not in {p["pid"] for p in servers}, "duplicated resource process")
        for process in [client, *servers]:
            require(type(process["pid"]) is int and process["pid"] > 0 and type(process["start_ticks"]) is int and process["start_ticks"] > 0, "missing process identity")
            require(all(type(process[k]) in (int, float) and math.isfinite(process[k]) and process[k] >= 0 for k in COUNTERS), "invalid resource counter")
    median, mad = middle([sample["seconds"] for sample in raw])
    equal(row["trial_median_seconds"], median)
    equal(row["sample_mad_seconds"], mad)
    equal(result["latency_p50_ms"], median * 1000)
    require(result["result_sha256"] == row["result_checksum"], "result checksum mismatch")
    if case == "select-all":
        require(result["result_sha256"] == select_all_checksum(),
                "select-all result differs from the fixed complete reference")
    return median


def audit_preflight(evidence, config):
    """Audit the four same-job select-all oracle runs before matrix verdicts."""
    progress = evidence.json("preflight-progress.json")
    wanted = [{"case": "select-all", "subject": subject, "status": "passed"}
              for subject in SUBJECTS]
    require(progress == wanted, "preflight subject/order/coverage mismatch")
    checksums = set()
    for subject in SUBJECTS:
        prefix = f"results/preflight-sql-select-all-0-{subject}/"
        collected = prefix + "collected/cell/"
        result = evidence.json(collected + "measurement.json")
        raw = [json.loads(line) for line in evidence.read(collected + "raw.ndjson").splitlines()]
        median, mad = middle([sample["seconds"] for sample in raw])
        expected = config["subjects"]["published" if subject == "postgresql" else subject]
        row = {"case": "select-all", "subject": subject, "trial": 0,
               "source": {"commit": expected["source_commit"],
                          "tree": expected["source_tree"], "profile": "release"},
               "trial_median_seconds": median, "sample_mad_seconds": mad,
               "raw_sha256": sha(evidence.read(collected + "raw.ndjson")),
               "result_checksum": result["result_sha256"]}
        audit_cell(evidence, row, config, run_prefix=prefix)
        checksums.add(result["result_sha256"])
    require(checksums == {select_all_checksum()},
            "preflight subjects differ from the fixed complete reference")
    return checksums.pop()


def audit(export, evidence_path, config):
    evidence = Evidence(evidence_path)
    try:
        public = json.loads((export / "result.json").read_text())
        require(public["schema"] == "alopex.native-sql-four-case-matrix/v1" and
                public["image_digest"] == config["image"]["image_record"]["id"].removeprefix("sha256:"), "public image or schema mismatch")
        require(public["performance_qualification"] is False and public["owned_resources_cleaned"] is True, "missing cleanup or unsupported qualification claim")
        require("cleanup-errors.json" not in evidence.members, "cleanup error retained")
        require(evidence.json("matrix-plan.json")["configuration"] == config, "configuration differs from dispatched plan")
        binding_bytes = evidence.read("same-job-acceptance.json")
        binding = json.loads(binding_bytes)
        require(sha(binding_bytes) == config["_same_job_binding_sha256"] ==
                public["qualification_binding_sha256"] and
                binding["qualification_run"] == config["_same_job_qualification_run"] ==
                public["qualification_run"] and
                binding["candidate_assets_digest"] == config["subjects"]["candidate"]["assets_digest"] and
                binding["image_id"] == config["image"]["image_record"]["id"],
                "same-job acceptance binding mismatch")
        imports = evidence.json("reference-imports.json")
        require(len(imports) == 2 and {row["role"] for row in imports} ==
                {"before", "published"}, "reference inventory mismatch")
        for row in imports:
            expected = config["subjects"][row["role"]]
            require(row == {"role": row["role"], "run": expected["run"],
                            "artifact_id": expected["artifact_id"],
                            "zip_sha256": expected["artifact_digest"].removeprefix("sha256:"),
                            "assets_digest": expected["assets_digest"]},
                    "reference original identity mismatch")
        preflight_checksum = audit_preflight(evidence, config)
        rows = evidence.json("matrix-results.json")
        expected = {(case, trial, subject) for case in CASES for trial in range(3) for subject in SUBJECTS}
        require(len(rows) == 48 and {(r["case"], r["trial"], r["subject"]) for r in rows} == expected, "matrix coverage mismatch")
        complete = all(row["status"] == "passed" for row in rows)
        require(public["status"] == ("passed" if complete else "failed"), "workflow and cell status mismatch")
        require(public["matrix_complete"] is complete and public["completed_cells"] == sum(row["status"] == "passed" for row in rows), "public matrix status mismatch")
        require(public["measurement_cells"] == 48, "public four-case cell count mismatch")
        result = json.loads((export / "comparison.json").read_text())
        require(sha((export / "comparison.json").read_bytes()) == public["comparison_sha256"], "public comparison hash mismatch")
        require(result == evidence.json("comparison.json") and result["performance_acceptance"] is False and result["schema"] == "alopex.native-sql-four-case-comparison/v1", "private/public comparison mismatch")
        verified = {}
        for row in rows:
            if row["status"] == "passed":
                verified[row["case"], row["trial"], row["subject"]] = audit_cell(evidence, row, config)
                if row["case"] == "select-all":
                    require(row["result_checksum"] == preflight_checksum,
                            "matrix differs from select-all preflight")
        require(len(result["cases"]) == 4 and {r["case"] for r in result["cases"]} == set(CASES), "comparison case coverage mismatch")
        statuses = {}
        for actual in result["cases"]:
            case = actual["case"]
            selected = [row for row in rows if row["case"] == case]
            if any(row["status"] != "passed" for row in selected):
                require(actual == {"case": case, "status": "failed_cell"}, "failed case accepted")
                statuses[case] = "failed_cell"
                continue
            require(len({row["result_checksum"] for row in selected}) == 1, "cross-subject results differ")
            trials = {subject: [verified[case, trial, subject] for trial in range(3)] for subject in SUBJECTS}
            reference = statistics.median(trials["postgresql"])
            candidate = statistics.median(trials["candidate"])
            wanted = {"case": case, "status": "measured", "trials": trials,
                      "comparisons": {name: decision(trials[name], trials["candidate"]) for name in ("before", "published")},
                      "reference_latency_ratio": candidate / reference, "reference_throughput_ratio": reference / candidate,
                      "reference_goal_met": candidate <= reference}
            equal(actual, wanted)
            statuses[case] = {name: value["status"] for name, value in wanted["comparisons"].items()}
        return {"artifact_integrity": "verified", "measurement_complete": complete,
                "verified_cells": len(verified), "verified_samples": len(verified) * 21,
                "case_status": statuses, "performance_acceptance": False}
    finally:
        evidence.close()
