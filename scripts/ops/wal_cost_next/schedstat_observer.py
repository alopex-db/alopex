"""Diagnostic-only Linux scheduler counters around an unchanged commit timer.

The proc reads and result formatting occur outside ``measure(action)``. This
module never supplies an acceptance comparator sample.
"""

from pathlib import Path
import time


FIELDS = ("runtime_ns", "runqueue_wait_ns", "timeslices")


def parse_schedstat(raw):
    parts = raw.strip().split()
    if len(parts) != 3 or any(not part.isascii() or not part.isdecimal() for part in parts):
        raise ValueError("invalid schedstat fields")
    return tuple(int(part) for part in parts)


def snapshot(root=Path("/proc/self/task"), *, clock=time.monotonic_ns):
    start_ns = clock()
    tids = sorted(int(entry.name) for entry in root.iterdir()
                  if entry.name.isascii() and entry.name.isdecimal())
    if not tids or len(set(tids)) != len(tids):
        raise RuntimeError("invalid task inventory")
    records = {}
    vanished = []
    for tid in tids:
        try:
            records[str(tid)] = parse_schedstat((root / str(tid) / "schedstat").read_text())
        except FileNotFoundError:
            vanished.append(str(tid))
    end_ns = clock()
    return {"start_ns": start_ns, "end_ns": end_ns,
            "tasks": records, "vanished_tids": vanished}


def observe(action, measure, *, take_snapshot=snapshot):
    before = take_snapshot()
    result = measure(action)
    after = take_snapshot()
    if type(result) is not tuple or len(result) != 2:
        raise RuntimeError("original timer result differs")
    if before["end_ns"] > after["start_ns"]:
        raise RuntimeError("observer brackets overlap")
    before_tasks, after_tasks = before["tasks"], after["tasks"]
    stable = (before_tasks.keys() == after_tasks.keys() and
              not before.get("vanished_tids") and not after.get("vanished_tids"))
    deltas = {}
    for tid in before_tasks.keys() & after_tasks.keys():
        values = tuple(end - start for start, end in zip(before_tasks[tid], after_tasks[tid]))
        if any(value < 0 for value in values):
            raise RuntimeError("schedstat counter decreased")
        deltas[tid] = dict(zip(FIELDS, values))
    totals = {field: sum(row[field] for row in deltas.values()) for field in FIELDS} if stable else None
    return result, {
        "schema": "alopex.682-schedstat-diagnostic/v1",
        "scope": "observed task counters around original commit timer; proc reads outside timer",
        "before": before, "after": after, "task_set_stable": stable,
        "common_task_deltas": deltas, "matched_task_totals": totals,
        "coverage": "sampled task IDs only; transient tasks may be missed",
        "original_timer_ns": result[1], "acceptance_sample": False,
    }
