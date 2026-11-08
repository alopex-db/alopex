"""Resource windows around the original manual CSV timing boundaries."""
import resource
import time

from csv_stream_profile import observe_phase
from sql_stream_profile import validate_result


def measure_plan_or_collect(operation, event, *, warmups, samples, observer, record):
    """Reuse the original timer, including disposal before its end clock."""
    for index in range(warmups + samples):
        observation_index = len(observer.completed)
        observer.begin(observation_index)

        def capture(row):
            resources = observer.finish(observation_index)
            row.update(iteration=index, phase='warmup' if index < warmups else 'measure',
                       resources=resources)
            record(row)

        observe_phase(operation, event, warmups=0, samples=1, record=capture)


def measure_stream(consume, expected, *, warmups, samples, observer, record):
    """Keep first/total consumer clocks; observe through close, before validation."""
    for index in range(warmups + samples):
        observation_index = len(observer.completed)
        observer.begin(observation_index)
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        started = time.perf_counter_ns()
        result = consume()
        elapsed = time.perf_counter_ns() - started
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        resources = observer.finish(observation_index)
        validate_result(result, expected, False)
        first_ns, total_ns = round(result[0] * 1e9), round(result[1] * 1e9)
        record(dict(event='sample', iteration=index, phase='warmup' if index < warmups else 'measure',
                    first_ns=first_ns, remaining_ns=total_ns-first_ns, total_ns=total_ns,
                    outside_original_total_ns=elapsed-total_ns, rows=result[2], first_chunk_rows=result[3],
                    cpu_including_close_seconds=(usage_after.ru_utime + usage_after.ru_stime -
                                                 usage_before.ru_utime - usage_before.ru_stime),
                    input_blocks_including_close=usage_after.ru_inblock-usage_before.ru_inblock,
                    output_blocks_including_close=usage_after.ru_oublock-usage_before.ru_oublock,
                    validation='count; full values before/after case', resources=resources))
        result = None
    return warmups + samples
