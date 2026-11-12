"""Compose the frozen CPU/I/O observer with diagnostic scheduler sampling.

The supplied original ``measure`` function remains the sole commit timer.
"""


def observe(action, measure, *, baseline_observe, scheduler_observe):
    scheduler_records = []

    def measured_action(pending_action):
        result, scheduler_record = scheduler_observe(pending_action, measure)
        scheduler_records.append(scheduler_record)
        return result

    result, baseline_record = baseline_observe(action, measured_action)
    if len(scheduler_records) != 1:
        raise RuntimeError("original observer changed commit timer calls")
    scheduler_record = scheduler_records[0]
    if result[1] != scheduler_record["original_timer_ns"]:
        raise RuntimeError("diagnostic timer differs from original")
    if not isinstance(baseline_record, dict) or "schedstat" in baseline_record:
        raise RuntimeError("original observation shape differs")
    return result, dict(baseline_record, schedstat=scheduler_record)
