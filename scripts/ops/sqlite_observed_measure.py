"""Original composite clock with resource observation outside timing."""
import resource
import time
from sql_delete_profile import emit

def measure(operation, validate, state, *, warmups=3, samples=21,
            clock=time.perf_counter_ns, record=emit, observer=None):
    for iteration in range(warmups + samples):
        observer.begin(iteration)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        started = clock()
        actual = operation()
        elapsed = clock() - started
        after = resource.getrusage(resource.RUSAGE_SELF)
        observation = observer.finish(iteration)
        validate(actual)
        state()
        record(dict(event="sample", phase="warmup" if iteration < warmups else "measure",
                    iteration=iteration, total_ns=elapsed, resources=observation,
                    operation_cpu_seconds=after.ru_utime + after.ru_stime - usage.ru_utime - usage.ru_stime,
                    operation_input_blocks=after.ru_inblock - usage.ru_inblock,
                    operation_output_blocks=after.ru_oublock - usage.ru_oublock))
        actual = None
    return warmups + samples
