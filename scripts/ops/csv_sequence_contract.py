"""Retain the original three phases and audit the appended diagnostic separately."""
from csv_phase_contract import verify_cell as verify_original
from csv_stage_contract import plan
from csv_stage_measure import STAGES
from dataframe_phase_contract import require, verify_resources


def verify_cell(cell, result, raw, observations, rss_limit):
    count = cell['warmups'] + cell['samples']
    prefix = 3 * count
    require(len(raw) == 4 * count == len(observations['completed'])
            == result['resource_observations'] and observations['pending'] is None,
            'sequence observation coverage incomplete')
    original = verify_original(cell, dict(result, resource_observations=prefix), raw[:prefix],
        dict(completed=observations['completed'][:prefix], pending=None), rss_limit)
    for index, row in enumerate(raw[prefix:]):
        observed_index = prefix + index
        require(row['event'] == 'stages' and row['iteration'] == index
                and row['phase'] == ('warmup' if index < cell['warmups'] else 'measure'),
                'stage sequence differs')
        verify_resources(row['resources'], observed_index, rss_limit)
        require(row['resources'] == observations['completed'][observed_index], 'resource differs')
        points, stages = row['points_ns'], row['stages_ns']
        require(len(points) == 7 and all(type(v) is int and v >= 0 for v in points)
                and all(b > a for a, b in zip(points, points[1:])), 'invalid stage clocks')
        require(list(stages) == list(STAGES)
                and list(stages.values()) == [b-a for a, b in zip(points, points[1:])]
                and row['total_ns'] == points[-1]-points[0] == sum(stages.values()),
                'stage sum differs')
        require(type(row['rows']) is int and row['rows'] == cell['rows'] == 3
                and type(row['first_chunk_rows']) is int and row['first_chunk_rows'] == 3,
                'incomplete stream')
    return dict(input_sha256=original['input_sha256'], result_sha256=original['result_sha256'],
                measured_samples=4 * cell['samples'], observations=4 * count)
