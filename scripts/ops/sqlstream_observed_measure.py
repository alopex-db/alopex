"""Preserve the SQL stream consumer clocks and observe resources outside them."""
import resource
import time
from sql_stream_profile import validate_result
from sqlite_observed_measure import measure_plan


def measure(consume, expected, rows_lane, observer, *, warmups=3, samples=21, record):
    for index in range(warmups + samples):
        observer.begin(index)
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        started = time.perf_counter_ns()
        result = consume()
        elapsed = time.perf_counter_ns() - started
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        observation = observer.finish(index)
        validate_result(result, expected, rows_lane)
        first_ns, total_ns = round(result[0] * 1e9), round(result[1] * 1e9)
        record(dict(event="sample", iteration=index, phase="warmup" if index < warmups else "measure",
                    first_ns=first_ns, remaining_ns=total_ns-first_ns, total_ns=total_ns,
                    outside_original_total_ns=elapsed-total_ns, rows=result[2], first_chunk_rows=result[3],
                    cpu_including_close_seconds=(usage_after.ru_utime + usage_after.ru_stime -
                                                 usage_before.ru_utime - usage_before.ru_stime),
                    input_blocks_including_close=usage_after.ru_inblock-usage_before.ru_inblock,
                    output_blocks_including_close=usage_after.ru_oublock-usage_before.ru_oublock,
                    validation="full typed rows" if rows_lane else "count; full values before/after case",
                    resources=observation))
        result = None
    return warmups + samples
