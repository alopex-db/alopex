"""Inventory and evidence contracts for the fixed plain-CSV comparison."""
from dataframe_phase_contract import number, require, verify_resources

SUBJECTS = ('published', 'before', 'candidate', 'polars')
EVENTS = ('plan_sample', 'collect_sample', 'sample')


def plan(mode):
    require(mode in ('preflight', 'full'), 'unknown CSV mode')
    cells = []
    for trial in range(1 if mode == 'preflight' else 3):
        for offset, rows in enumerate((3,) if mode == 'preflight' else (3, 34250)):
            shift = (trial + offset) % len(SUBJECTS)
            for subject in SUBJECTS[shift:] + SUBJECTS[:shift]:
                cells.append(dict(id=f'trial{trial}-rows{rows}-{subject}', trial=trial,
                    rows=rows, subject=subject, engine='polars' if subject == 'polars' else 'alopex',
                    warmups=0 if mode == 'preflight' else 3, samples=1 if mode == 'preflight' else 21))
    return cells


def verify_cell(cell, result, raw, observations, rss_limit):
    for key in ('rows', 'engine', 'warmups', 'samples'):
        require(result[key] == cell[key], 'CSV measurement identity differs')
    count = cell['warmups'] + cell['samples']
    require(len(raw) == 3 * count and observations['pending'] is None
        and len(observations['completed']) == len(raw), 'CSV observation coverage incomplete')
    require(result['resource_observations'] == len(raw), 'CSV observation count differs')
    require(result['validation_before'] == result['validation_after'] == result['result_sha256'], 'CSV oracle differs')
    for key in ('input_sha256', 'result_sha256'):
        require(type(result[key]) is str and len(result[key]) == 64
            and all(c in '0123456789abcdef' for c in result[key]), 'CSV digest missing')
    for index, row in enumerate(raw):
        phase_index = index % count
        require(row['event'] == EVENTS[index // count] and row['iteration'] == phase_index
            and row['phase'] == ('warmup' if phase_index < cell['warmups'] else 'measure'), 'CSV sample order differs')
        verify_resources(row['resources'], index, rss_limit)
        require(row['resources'] == observations['completed'][index], 'CSV raw resource differs')
        require(type(row['total_ns']) is int and row['total_ns'] > 0, 'invalid CSV elapsed time')
        if row['event'] == 'sample':
            require(type(row['first_ns']) is int and 0 <= row['first_ns'] <= row['total_ns']
                and row['remaining_ns'] == row['total_ns'] - row['first_ns'], 'invalid CSV consumer boundary')
            require(type(row['outside_original_total_ns']) is int and row['outside_original_total_ns'] >= 0,
                'invalid CSV outside duration')
            require(type(row['rows']) is int and row['rows'] == cell['rows']
                and type(row['first_chunk_rows']) is int and 0 < row['first_chunk_rows'] <= cell['rows'],
                'incomplete CSV stream')
            require(all(number(row[k]) for k in ('cpu_including_close_seconds',
                'input_blocks_including_close', 'output_blocks_including_close')), 'invalid CSV usage')
    return dict(input_sha256=result['input_sha256'], result_sha256=result['result_sha256'],
                measured_samples=sum(r['phase'] == 'measure' for r in raw), observations=len(raw))
