"""Original CSV phase sequence followed by diagnostic stream clocks."""
from csv_phase_measure import measure_plan_or_collect, measure_stream
from csv_stage_measure import consume_stages


def measure(build, consume, expected, *, warmups, samples, observer, record):
    options = dict(warmups=warmups, samples=samples, observer=observer, record=record)
    measure_plan_or_collect(build, 'plan_sample', **options)
    measure_plan_or_collect(lambda: build().collect().height, 'collect_sample', **options)
    measure_stream(consume, expected, **options)
    for index in range(warmups + samples):
        observation_index = len(observer.completed)
        observer.begin(observation_index)
        row = consume_stages(build)
        resources = observer.finish(observation_index)
        if row['rows'] != len(expected) or row['first_chunk_rows'] != len(expected):
            raise RuntimeError('incomplete three-row diagnostic stream')
        record(dict(row, event='stages', iteration=index,
                    phase='warmup' if index < warmups else 'measure', resources=resources))
