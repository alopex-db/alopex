"""Independently audit #684 raw phases and compute predeclared comparisons."""
import json
import math
from pathlib import PurePosixPath
import statistics

from audit_native_sql_matrix import Evidence, require, sha


def middle(values, positive=False):
    require(values and all(type(v) in (int, float) and math.isfinite(v) and
                           (not positive or v > 0) for v in values), "invalid observations")
    center = statistics.median(values)
    return center, statistics.median(abs(value - center) for value in values)


def compare(before, after, metric, rows):
    require(len(before) == len(after) == 3, "three diagnostic trial medians required")
    b, db = middle(before)
    c, dc = middle(after)
    return dict(baseline_median=b, candidate_median=c, baseline_trial_mad=db,
                candidate_trial_mad=dc, difference=c-b, noise=3*max(db,dc),
                ratio=c/b if b > 0 else None, performance_acceptance=False)


def expected_specs(rows):
    require(rows == 1000, "unexpected page fixture")
    return [dict(mode="ro", operation="noop", samples=1, repeat=index, prefault_extension=value)
            for index, value in enumerate((False, True))]


def verify_page_observations(events, spec):
    first = events[0]
    require(first.get("prefault_extension") is spec["prefault_extension"], "wrong intervention")
    unit = first.get("page_size")
    require(type(unit) is int and unit > 0, "unknown page unit")
    groups = [first.get("prefault_observation"), first.get("page_observer_control")]
    groups += [sample.get("extension_pages") for sample in events[1:-1]]
    identities = None
    for group in groups:
        require(isinstance(group, list) and group, "missing extension mapping evidence")
        current = []
        for mapping in group:
            require(set(mapping) == {"file_offset", "permissions", "mapped_pages", "present_before", "present_after", "newly_present", "no_longer_present"}, "unknown mapping fields")
            start, count = mapping["file_offset"], mapping["mapped_pages"]
            require(type(start) is int and type(count) is int and start >= 0 and count > 0 and start % unit == 0 and start + count*unit < 128_000_000, "invalid software page range")
            require(mapping["permissions"].startswith("r"), "unreadable mapping")
            current.append((start, count, mapping["permissions"]))
            require(all(type(mapping[k]) is int and 0 <= mapping[k] <= count for k in ("present_before", "present_after")), "invalid present counts")
            gained, lost = mapping["newly_present"], mapping["no_longer_present"]
            for offsets in (gained, lost):
                require(isinstance(offsets, list) and offsets == sorted(set(offsets)) and all(type(o) is int and start <= o < start + count*unit and o % unit == 0 for o in offsets), "invalid software page offsets")
            require(not set(gained).intersection(lost) and mapping["present_after"]-mapping["present_before"] == len(gained)-len(lost), "page accounting differs")
        if identities is None: identities = current
        require(current == identities, "native mappings changed")
    require(all(not row["newly_present"] and not row["no_longer_present"] for row in groups[1]), "observer/background interference")
    if spec["prefault_extension"]:
        require(all(row["present_after"] == row["mapped_pages"] for row in groups[0]), "incomplete prefault")
        require(all(row["present_before"] == row["mapped_pages"] for row in groups[2]), "prefault not retained")


def extract(events, spec, row, extension):
    require(len(events) == spec["samples"] + 2, "sample count differs")
    first, final = events[0], events[-1]
    verify_page_observations(events, spec)
    require(first["event"] == "ready" and first["extension_sha256"] == extension and
            first["initial_cache"]["entries"] == 0 and
            all(first[key] == row[key] for key in ("rows", "indexes")) and
            all(first[key] == spec[key] for key in ("mode", "operation")), "fresh-reader identity differs")
    require(final["event"] == "complete" and final["all_rows_unchanged"] is True,
            "full final state validation missing")
    middle([final["peak_before_validation_bytes"], final["peak_after_validation_bytes"]], positive=True)
    output = {}
    explicit = spec["mode"] != "auto"
    querying = spec["operation"] == "query"
    phases = (["begin_ns", "rollback_ns"] + (["query_ns"] if querying else [])) if explicit else ["auto_total_ns"]
    for index, event in enumerate(events[1:-1]):
        require(event["event"] == "sample" and event["iteration"] == index and
                event["first_after_open"] is (index == 0), "sample sequence differs")
        middle([event[key] for key in phases], positive=True)
        require(all(event[key] is None for key in {"begin_ns", "rollback_ns", "query_ns", "auto_total_ns"} - set(phases)),
                "phase boundaries differ")
        if index:
            require(event["cache_before"]["entries"] == row["indexes"], "warm cache missing")
            if querying and row["rows"] and row["indexes"]:
                require(event["cache_after"]["hits"] > event["cache_before"]["hits"], "warm query missed cache")
        snapshots = ["before", "after_finish"] + (["after_begin"] if explicit else []) + (["after_query"] if querying else [])
        for name in snapshots:
            snapshot = event[name]
            middle([snapshot["rss_bytes"]], positive=True)
            require(snapshot["rss_bytes"] < 2 * 1024**3 and
                    all(type(snapshot[key]) is int and snapshot[key] >= 0 for key in
                        ("allocated_bytes", "logical_bytes")), "invalid memory/storage observation")
        require(all(event[key] is None for key in {"after_begin", "after_query"} - set(snapshots)),
                "unexpected memory observation")
        state = "first_after_open" if index == 0 else "explicitly_warmed"
        for metric in phases:
            output.setdefault((state, metric), []).append(event[metric])
        if explicit:
            output.setdefault((state, "begin_retained_rss_bytes"), []).append(
                event["after_begin"]["rss_bytes"] - event["before"]["rss_bytes"])
        output.setdefault((state, "finish_retained_rss_bytes"), []).append(
            event["after_finish"]["rss_bytes"] - event["before"]["rss_bytes"])
    require(first.get("diagnostic_schema") == "alopex.begin-phase-diagnostics/v1", "missing diagnostic schema")
    def numeric_observation(value):
        require(isinstance(value, dict) and set(value) == {"wall_ns", "process_cpu_ns", "caller_cpu_ns", "counters", "io_bytes"},
                "missing diagnostic observation")
        require(set(value["counters"]) == {"ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw", "ru_inblock", "ru_oublock"},
                "incomplete scheduler/fault counters")
        require(set(value["io_bytes"]) == {"read_bytes", "write_bytes", "cancelled_write_bytes"}, "incomplete physical I/O")
        flat = {name: value[name] for name in ("wall_ns", "process_cpu_ns", "caller_cpu_ns")}
        flat.update(value["counters"])
        flat.update(value["io_bytes"])
        require(all(type(v) is int and v >= 0 for v in flat.values()) and flat["wall_ns"] > 0,
                "invalid diagnostic counters")
        return flat
    controls = first.get("timer_controls", [])
    require(len(controls) == 21, "diagnostic timer controls missing")
    for control in controls:
        for name, value in numeric_observation(control).items():
            output.setdefault(("observer_control", name), []).append(value)
    names = {"auto": "auto_total_ns"} if not explicit else {"begin": "begin_ns", "rollback": "rollback_ns"}
    if explicit and querying: names["query"] = "query_ns"
    for index, sample in enumerate(events[1:-1]):
        require(set(sample.get("diagnostic_phases", {})) == set(names), "diagnostic phase set differs")
        for phase, timing in names.items():
            flat = numeric_observation(sample["diagnostic_phases"][phase])
            require(flat["wall_ns"] == sample[timing], "wall interval differs")
            for name, value in flat.items():
                output.setdefault(("first_after_open" if index == 0 else "explicitly_warmed", phase + "." + name), []).append(value)
    require("ExactKnnScan" in json.dumps(final.get("query_plan")), "physical plan evidence missing")
    return output


def audit_cell(evidence, row, config):
    prefix = "results/" + row["id"] + "/"
    run = evidence.json(prefix + "run.json")
    require(run["phase"] == "deleted", "owned resources not deleted")
    collected = prefix + "collected/"
    collection = evidence.json(prefix + "collection.json")
    require("result.json" in collection and "cell/processes.json" in collection, "collection incomplete")
    for name, expected in collection.items():
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts, "unsafe collection path")
        require(sha(evidence.read(collected + name)) == expected, "collection digest differs")
    require(collected + "cleanup-errors.json" not in evidence.members, "cell cleanup error")
    report = evidence.json(collected + "result.json")
    expected = config["subjects"][row["subject"]]
    source = dict(commit=expected["source_commit"], tree=expected["source_tree"], profile="release")
    require(report["schema"] == "alopex.native-begin-measurement/v1" and report["status"] == "passed" and
            report["worker_error"] is None and report["finalization_error"] is None, "failed fixture")
    require(report["source"] == row["source"] == source and
            report["assets_digest"] == row["assets_digest"] == run["identities"]["assets_digest"] and
            report["extension_sha256"] == row["extension_sha256"] == expected["extension_sha256"] and
            all(report[key] == row[key] for key in ("rows", "indexes", "trial")), "fixture identity differs")
    raw = evidence.read(collected + "cell/processes.json")
    require(sha(raw) == report["processes_sha256"] == row["processes_sha256"], "process inventory differs")
    processes = json.loads(raw)
    specs = [None, *expected_specs(row["rows"])]
    require(len(processes) == len(specs) == row["processes"], "process coverage differs")
    values = {}
    for index, (process, spec) in enumerate(zip(processes, specs)):
        require(process["ordinal"] == index and process["spec"] == spec and process["status"] == "passed",
                "process result differs")
        raw = evidence.read(collected + f"cell/process-{index}.ndjson")
        require(sha(raw) == process["raw_sha256"], "raw digest differs")
        events = [json.loads(line) for line in raw.splitlines()]
        if spec is None:
            require(len(events) == 1 and events[0]["event"] == "seeded" and
                    events[0]["all_rows_unchanged"] is True and events[0]["extension_sha256"] == expected["extension_sha256"] and
                    all(events[0][key] == row[key] for key in ("rows", "indexes")), "seed validation differs")
            continue
        for (state, metric), samples in extract(events, spec, row, expected["extension_sha256"]).items():
            key = spec["mode"] + ("-prefault" if spec["prefault_extension"] else "-natural"), spec["operation"], state, metric
            values.setdefault(key, []).extend(samples)
    return {key: middle(samples)[0] for key, samples in values.items()}


def audit(export, evidence_path, config):
    evidence = Evidence(evidence_path)
    try:
        public = json.loads((export / "result.json").read_text())
        require(public["schema"] == "alopex.native-begin-pages/v1" and public["status"] == "passed" and
                public["matrix_complete"] is True and public["performance_qualification"] is False and
                public["owned_resources_cleaned"] is True and public["measurement_cells"] == public["completed_cells"] == 6 and
                public["image_digest"] == config["image"]["image_record"]["id"].removeprefix("sha256:"), "public completion differs")
        require("cleanup-errors.json" not in evidence.members, "VM cleanup failed")
        plan = evidence.json("begin-plan.json")
        require(plan["configuration"] == config and plan["image"] == public["image_digest"], "dispatched plan differs")
        rows = evidence.json("begin-results.json")
        expected = [dict(id=f"pages-{size}-i{indexes}-{trial}-{subject}", rows=size, indexes=indexes,
                         trial=trial, subject=subject)
                    for size in (1000,) for indexes in (0,) for trial in range(3)
                    for subject in (("before", "candidate") if trial % 2 == 0 else ("candidate", "before"))]
        require(plan["cells"] == expected and len(rows) == 6, "matrix coverage differs")
        for row, cell in zip(rows, expected):
            require(all(row[key] == value for key, value in cell.items()) and row["status"] == "passed", "cell coverage/status differs")
        summary_bytes = (export / "begin-comparison.json").read_bytes()
        require(sha(summary_bytes) == public["comparison_sha256"], "public summary hash differs")
        summary = json.loads(summary_bytes)
        require(summary == dict(schema="alopex.native-begin-pages/v1", complete=True, performance_acceptance=False,
                                cells=[dict(cell, status="passed") for cell in expected]), "public summary differs")
        measurements = {(row["rows"], row["indexes"], row["trial"], row["subject"]): audit_cell(evidence, row, config)
                        for row in rows}
        comparisons = []
        for size in (1000,):
            for indexes in (0,):
                keys = set(measurements[size, indexes, 0, "before"])
                require(all(set(measurements[size, indexes, trial, subject]) == keys
                            for trial in range(3) for subject in ("before", "candidate")), "phase coverage differs")
                for mode, operation, state, metric in sorted(keys):
                    trials = {subject: [measurements[size, indexes, trial, subject][mode, operation, state, metric]
                                        for trial in range(3)] for subject in ("before", "candidate")}
                    comparisons.append(dict(rows=size, indexes=indexes, mode=mode, operation=operation,
                                            cache_state=state, metric=metric, trials=trials,
                                            comparison=compare(trials["before"], trials["candidate"], metric, size)))
        page_evidence = []
        for cell in rows:
            for ordinal, spec in enumerate(expected_specs(1000), start=1):
                events = [json.loads(line) for line in evidence.read("results/" + cell["id"] + "/collected/cell/process-" + str(ordinal) + ".ndjson").splitlines()]
                page_evidence.append(dict(subject=cell["subject"], trial=cell["trial"], prefault=spec["prefault_extension"],
                                          extension_sha256=events[0]["extension_sha256"], page_size=events[0]["page_size"],
                                          intervention=events[0]["prefault_observation"], control=events[0]["page_observer_control"],
                                          begin=events[1]["extension_pages"], counters=events[1]["diagnostic_phases"]["begin"]))
        return dict(artifact_integrity="verified", verified_cells=6, comparisons=comparisons, page_evidence=page_evidence,
                    performance_acceptance=False,
                    limitations=["Fresh reader is not OS-cold.", "Phase RSS observations are not operation peaks.",
                                 "Query improvement does not establish whole-transaction improvement."])
    finally:
        evidence.close()
