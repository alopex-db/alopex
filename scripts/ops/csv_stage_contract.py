"""Bounded six-cell diagnosis of the small-input CSV difference."""
from dataframe_phase_contract import require, verify_resources
from csv_stage_measure import STAGES


def plan(mode):
    require(mode == 'diagnostic', 'unexpected diagnostic mode')
    return [dict(id=f'trial{trial}-rows3-{subject}', trial=trial, rows=3, subject=subject,
                 engine='alopex', warmups=3, samples=21)
            for trial in range(3)
            for subject in (('before','candidate') if trial % 2 == 0 else ('candidate','before'))]


def verify_cell(cell, result, raw, observations, rss_limit):
    for key in ('rows', 'engine', 'warmups', 'samples'):
        require(result[key] == cell[key], 'diagnostic identity differs')
    count = cell['warmups'] + cell['samples']
    require(len(raw) == count == len(observations['completed']) == result['resource_observations']
            and observations['pending'] is None, 'diagnostic observation coverage incomplete')
    require(result['validation_before'] == result['validation_after'] == result['result_sha256'], 'oracle differs')
    for index, row in enumerate(raw):
        require(row['event'] == 'stages' and row['iteration'] == index
                and row['phase'] == ('warmup' if index < cell['warmups'] else 'measure'), 'stage sequence differs')
        verify_resources(row['resources'], index, rss_limit)
        require(row['resources'] == observations['completed'][index], 'resource differs')
        points, stages = row['points_ns'], row['stages_ns']
        require(len(points) == 7 and all(type(v) is int and v >= 0 for v in points)
                and all(b > a for a,b in zip(points,points[1:])), 'invalid stage clocks')
        require(list(stages) == list(STAGES)
                and list(stages.values()) == [b-a for a,b in zip(points,points[1:])]
                and row['total_ns'] == points[-1]-points[0] == sum(stages.values()), 'stage sum differs')
        require(type(row['rows']) is int and row['rows'] == cell['rows']
                and type(row['first_chunk_rows']) is int and row['first_chunk_rows'] == 3, 'incomplete stream')
    return dict(input_sha256=result['input_sha256'], result_sha256=result['result_sha256'],
                measured_samples=cell['samples'], observations=count)
