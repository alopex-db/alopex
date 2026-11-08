"""Diagnostic clocks for a complete CSV stream; not acceptance comparison clocks."""
import time

STAGES = ('plan', 'open', 'first', 'remaining', 'close', 'dispose')


def consume_stages(build, clock=time.perf_counter_ns):
    points = [clock()]
    plan = build()
    points.append(clock())
    stream = plan.collect_batches(chunk_size=8192)
    del plan
    with stream as batches:
        points.append(clock())
        first = next(batches)
        points.append(clock())
        first_rows = first.height
        count = first_rows + sum(batch.height for batch in batches)
        points.append(clock())
    points.append(clock())
    del first, batches, stream
    points.append(clock())
    if not all(type(v) is int for v in points) or not all(b > a for a, b in zip(points, points[1:])):
        raise RuntimeError('invalid stage clock')
    return dict(points_ns=points, stages_ns=dict(zip(STAGES, [b-a for a,b in zip(points,points[1:])])),
                total_ns=points[-1]-points[0], rows=count, first_chunk_rows=first_rows)


def measure(build, expected_rows, *, warmups, samples, observer, record, clock=time.perf_counter_ns):
    for index in range(warmups + samples):
        observer.begin(index)
        row = consume_stages(build, clock)
        resources = observer.finish(index)
        if row['rows'] != expected_rows or not 0 < row['first_chunk_rows'] <= expected_rows:
            raise RuntimeError('incomplete stage stream')
        record(dict(row, event='stages', iteration=index,
                    phase='warmup' if index < warmups else 'measure', resources=resources))
