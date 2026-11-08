"""Complete matrix, recovery and noise checks for manual public API costs."""
import statistics

SHAPES = (('kv',16), ('kv',2048), ('sql',16), ('sql',2048))
TIMES = ('prepare_ns','open_ns','schema_ns','index_ns','prior_ns','begin_ns','stage_ns','commit_ns')
METRICS = TIMES + ('peak_rss_bytes','wal_bytes','spool_bytes','fixture_bytes','allocated_bytes')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def plan(mode):
    require(mode in ('preflight','full'), 'unknown comparison mode')
    return [dict(id=f'{round_id}-{surface}-{size}-{subject}',round=round_id,surface=surface,
                 value_bytes=size,subject=subject)
            for round_id in range(1 if mode == 'preflight' else 6)
            for surface,size in SHAPES
            for subject in (('before','candidate') if round_id % 2 == 0 else ('candidate','before'))]


def verify_record(row, cell, phase, native):
    require(row['status'] == 'passed' and row['phase'] == phase, 'unsuccessful phase')
    require(row['surface'] == cell['surface'] and row['value_bytes'] == cell['value_bytes']
            and row['native_sha256'] == native, 'shape or native differs')
    required = METRICS if phase == 'write' else ('open_ns','wal_bytes','spool_bytes','fixture_bytes','allocated_bytes')
    for metric in required:
        require(type(row[metric]) is int and row[metric] >= 0, 'invalid metric')
    require(row['fixture_bytes'] <= 1_048_576, 'fixture bound exceeded')
    if phase == 'write':
        require(0 < row['peak_rss_bytes'] <= 1_073_741_824, 'writer RSS bound exceeded')
        require(row['overflow_stats']['gc_failures'] == 0, 'writer GC failure')
    else:
        require(row['verified'] is True, 'recovery not verified')


def summarize(rows, mode):
    expected = plan(mode)
    require(len(rows) == len(expected), 'incomplete matrix')
    for actual, cell in zip(rows, expected):
        require(actual['cell'] == cell and len(actual['readers']) == 2, 'wrong order or reader count')
        native = actual['writer']['native_sha256']
        verify_record(actual['writer'],cell,'write',native)
        for reader in actual['readers']:
            verify_record(reader,cell,'verify',native)
    if mode == 'preflight':
        return dict(status='preflight_verified',writers=len(rows),readers=len(rows)*2,performance_acceptance=False)
    medians = {}
    for subject in ('before','candidate'):
        for surface,size in SHAPES:
            selected = [r['writer'] for r in rows if r['cell']['subject']==subject
                        and r['cell']['surface']==surface and r['cell']['value_bytes']==size and r['cell']['round']>0]
            require(len(selected)==5,'sample count differs')
            values = {key:statistics.median(r[key] for r in selected) for key in METRICS}
            values['commit_mad_ns'] = statistics.median(abs(r['commit_ns']-values['commit_ns']) for r in selected)
            medians[f'{subject}-{surface}-{size}'] = values
    comparison = {}
    for surface,size in SHAPES:
        before=medians[f'before-{surface}-{size}']; after=medians[f'candidate-{surface}-{size}']
        require(before['commit_ns']>0,'zero baseline time')
        ratio=after['commit_ns']/before['commit_ns']
        noise=3*max(before['commit_mad_ns'],after['commit_mad_ns'])/before['commit_ns']
        comparison[f'{surface}-{size}']=dict(commit_ratio=ratio,commit_noise_ratio=noise,
            decision='pending_noise' if noise>0.05 else ('investigate_increase' if ratio>1.05 else 'within_five_percent'),
            resource_increases={key:after[key]-before[key] for key in ('peak_rss_bytes','wal_bytes','spool_bytes') if after[key]>before[key]})
    return dict(status='measured',writers=len(rows),readers=len(rows)*2,medians=medians,comparison=comparison,
                performance_acceptance=False,peak_scope='writer lifetime through commit including import and identity checks')
