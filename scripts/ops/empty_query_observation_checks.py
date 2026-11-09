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


def expected_specs(rows, indexes, trial=0, subject="before"):
    require(type(trial) is int and trial in range(3) and subject in ("before", "candidate"), "observer order identity differs")
    require(type(rows) is int and type(indexes) is int, "invalid fixture type")
    require((rows, indexes) == (0, 2), "unexpected empty query fixture")
    operations = [("rw", "query")]
    modes = ['plain', 'counters']
    if (trial % 2 == 1) != (subject == 'candidate'):
        modes.reverse()
    return [dict(mode=mode, operation=operation, samples=6, repeat=0, observer_mode=observer)
            for mode, operation in operations for observer in modes]


def extract(events, spec, row, extension):
    require(len(events) == spec["samples"] + 2, "sample count differs")
    first, final = events[0], events[-1]
    require(first["event"] == "ready" and first["extension_sha256"] == extension and
            first["initial_cache"]["entries"] == 0 and
            all(first[key] == row[key] for key in ("rows", "indexes")) and
            all(first[key] == spec[key] for key in ("mode", "operation", "observer_mode")), "fresh-reader identity differs")
    require(final["event"] == "complete" and final["all_rows_unchanged"] is True,
            "full final state validation missing")
    middle([final["peak_before_validation_bytes"], final["peak_after_validation_bytes"]], positive=True)
    output = {}
    explicit = spec["mode"] != "auto"
    querying = spec["operation"] == "query"
    phases = (["begin_ns", "rollback_ns"] + (["query_ns"] if querying else [])) if explicit else ["auto_total_ns"]
    for index, event in enumerate(events[1:-1]):
        require(event["event"] == "sample" and event["iteration"] == index and
                event["first_after_open"] is (index == 0) and event.get("observer_mode") == spec["observer_mode"], "sample sequence differs")
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
    require(first.get("diagnostic_schema") == "alopex.begin-observer-comparison/v1", "missing diagnostic schema")
    def numeric_observation(value):
        if spec['observer_mode'] == 'plain':
            require(isinstance(value,dict) and set(value)=={'wall_ns'} and type(value['wall_ns']) is int and value['wall_ns']>0,
                    'plain timer observation differs')
            return value
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
    require('timer_controls' not in first, 'unexpected observer priming')
    names = {"auto": "auto_total_ns"} if not explicit else {"begin": "begin_ns", "rollback": "rollback_ns"}
    if explicit and querying: names["query"] = "query_ns"
    for index, sample in enumerate(events[1:-1]):
        require(set(sample.get("diagnostic_phases", {})) == set(names), "diagnostic phase set differs")
        for phase, timing in names.items():
            flat = numeric_observation(sample["diagnostic_phases"][phase])
            require(flat["wall_ns"] == sample[timing], "wall interval differs")
            for name, value in flat.items():
                output.setdefault(("first_after_open" if index == 0 else "explicitly_warmed", phase + "." + name), []).append(value)
    expected_plan = "HnswSearch" if row["indexes"] else "ExactKnnScan"
    require(expected_plan in json.dumps(final.get("query_plan")), "physical plan evidence missing")
    return output

