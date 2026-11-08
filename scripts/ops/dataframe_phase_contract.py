"""Fixed DataFrame comparison inventory and raw evidence validation."""
import math

SUBJECTS = ('published','before','candidate','polars')
CASES = ('construct','compute','convert','complete')
IO = ('read_bytes','write_bytes','cancelled_write_bytes')
CPU = ('user_cpu_seconds','system_cpu_seconds')


def require(value, message):
    if not value:
        raise RuntimeError(message)


def number(value):
    return type(value) in (int,float) and math.isfinite(value) and value >= 0


def plan(mode):
    require(mode in ('preflight','full'), 'unknown measurement mode')
    rows, warmups, samples, trials = (4,0,1,1) if mode == 'preflight' else (34250,3,21,3)
    cells = []
    for trial in range(trials):
        for offset, case in enumerate(CASES):
            rotation = (trial + offset) % len(SUBJECTS)
            order = SUBJECTS[rotation:] + SUBJECTS[:rotation]
            for subject in order:
                cells.append(dict(id=f'trial{trial}-{case}-{subject}',trial=trial,case=case,subject=subject,
                    engine='polars' if subject=='polars' else 'alopex',rows=rows,warmups=warmups,samples=samples))
    return cells


def verify_resources(value, index, rss_limit):
    require(value['index'] == index, 'resource sequence differs')
    before, after, delta, rss = (value[key] for key in ('before','after','delta','rss'))
    require(set(delta) == set(IO+CPU), 'resource metric set differs')
    require(set(before['io']) == set(after['io']) == set(IO), 'I/O metric set differs')
    require(before['waited_children'] == after['waited_children']
        and len(before['waited_children']) == 16
        and all(number(v) for v in before['waited_children']), 'child usage differs')
    for key in CPU:
        require(number(before[key]) and number(after[key]) and number(delta[key])
            and delta[key] == after[key]-before[key], 'CPU delta differs')
    for key in IO:
        require(all(type(v) is int and v >= 0 for v in (before['io'][key],after['io'][key],delta[key]))
            and delta[key] == after['io'][key]-before['io'][key], 'I/O delta differs')
    require(rss['phase'] == 'sample-'+str(index), 'RSS phase differs')
    for point in ('before_reset','after_reset','after'):
        sample = rss[point]
        require(set(sample) == {'rss_bytes','high_water_bytes'}
            and all(type(v) is int and v >= 0 for v in sample.values())
            and sample['high_water_bytes'] >= sample['rss_bytes'], 'RSS counters invalid')
    start,end = rss['after_reset']['high_water_bytes'],rss['after']['high_water_bytes']
    require(rss['kernel_reported_peak_rss_bytes'] == max(start,end) <= rss_limit
        and rss['kernel_high_water_decreased'] is (end < start)
        and rss['aggregation'] == 'maximum of reset and end kernel high-water readings', 'RSS aggregation differs')


def verify_cell(cell, result, raw, observations, rss_limit):
    for key, expected in (('case',cell['case']),('engine',cell['engine']),('fixture_rows',cell['rows']),
        ('warmups',cell['warmups']),('samples',cell['samples'])):
        require(result[key] == expected, 'measurement identity differs')
    require(observations['pending'] is None and len(observations['completed']) == cell['warmups']+cell['samples'],
        'resource coverage incomplete')
    for index,value in enumerate(observations['completed']):verify_resources(value,index,rss_limit)
    require(len(raw) == cell['samples'], 'sample count differs')
    for index,row in enumerate(raw):
        require(set(row) == {'phase','index','seconds','resources'} and row['phase'] == 'measure'
            and row['index'] == index and number(row['seconds']) and row['seconds'] > 0, 'invalid timed sample')
        require(row['resources'] == observations['completed'][cell['warmups']+index], 'raw observation differs')
    require(result['resource_observations'] == len(observations['completed']), 'observation count differs')
    for field in ('input_sha256','result_sha256'):
        value=result[field]
        require(isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value), 'oracle digest missing')
    return dict(input_sha256=result['input_sha256'], result_sha256=result['result_sha256'],
        seconds=[row['seconds'] for row in raw],
        peak_rss_bytes=[row['resources']['rss']['kernel_reported_peak_rss_bytes'] for row in raw])
