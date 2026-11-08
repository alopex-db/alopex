"""Validate tiny native completion runs before retaining a comparison asset."""

PHASES = ('seed', 'index_build', 'cold_transaction_1', 'cold_transaction_2',
          'warm', 'writer_begin', 'first_insert', 'second_insert')


def verify(identity, report, events, mode, binary_sha256, runner_sha256):
    def require(value):
        if not value:
            raise ValueError('incomplete native completion smoke')
    require(mode in ('commit', 'rollback'))
    require(identity['finish'] == mode and identity['rows'] == 2 and identity['dimensions'] == 4
            and identity['binary_sha256'] == binary_sha256 and identity['runner_sha256'] == runner_sha256
            and identity['phase_kernel_rss'] is True and identity['timeout_seconds'] == 30
            and identity['rss_limit'] == 268435456)
    phases = (*PHASES, mode)
    sequence = [(phase, kind) for phase in phases for kind in ('start', 'end')] + [('complete', None)]
    require([(e['phase'], e['data'].get('event')) for e in events] == sequence)
    elapsed = events[-2]['data'].get('elapsed_ns')
    require(type(elapsed) is int and elapsed > 0)
    require(events[-1]['data'].get('finish') == mode
            and events[-1]['data'].get('result_validated') is True)
    require(report['outcome'] == 'passed' and report['failure'] is None
            and report['kernel_rss_reset'] is True and report['incomplete_kernel_phase'] is None
            and report['children_lifetime_peak_rss_bytes'] is None
            and [p['phase'] for p in report['phase_kernel_rss']] == list(phases))
    for row in report['phase_kernel_rss']:
        peak = row['kernel_reported_peak_rss_bytes']
        require(type(peak) is int and 0 < peak < identity['rss_limit'])
        require(peak == max(row['after_reset']['high_water_bytes'], row['after']['high_water_bytes']))
    return dict(mode=mode, rows=2, dimensions=4, phases=9, events=19,
                elapsed_ns=elapsed, result_validated=True, status='passed')
