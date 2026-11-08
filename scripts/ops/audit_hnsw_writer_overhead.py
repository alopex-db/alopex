"""Read-only reconstruction of one collected five-sample HNSW completion stage."""
import json
import statistics

from audit_native_sql_matrix import require, sha
from hnsw_writer_overhead_contract import PHASES, TIMED_PHASES, plan, verify_sample


def comparison(before, candidate):
    require(len(before) == len(candidate) == 5, 'missing comparison samples')
    b, c = (statistics.median(values) for values in (before, candidate))
    db = statistics.median(abs(value-b) for value in before)
    dc = statistics.median(abs(value-c) for value in candidate)
    noise = 3 * max(db, dc)
    return dict(normal=before, diagnostic=candidate, baseline_median=b, candidate_median=c,
                baseline_mad=db, candidate_mad=dc, noise=noise, difference=c-b, ratio=c/b,
                overhead_fraction=(c-b)/b, noise_within_five_percent=noise <= .05*b,
                increase_exceeds_noise=c-b > noise,
                verdict=('pending_noise' if noise > .05*b else 'overhead_detected' if c-b > noise else 'within_noise'))


def audit(evidence, public, cleanup, config, controller):
    finish = config['finish']
    require(finish in ('commit', 'rollback'), 'explicit completion mode required')
    phases = (*PHASES[:-1], finish)
    timed_phases = (*TIMED_PHASES, finish)
    expected_public = dict(schema='alopex.hnsw-writer-overhead/v1', finish=finish, status='passed',
                           stage_rows=config['rows'], controller=controller,
                           completed_samples=10, owned_resources_cleaned=True,
                           assets={role:s['assets_digest'] for role,s in config['subjects'].items()},
                           performance_qualification=False)
    require(public == expected_public, 'public result differs')
    frozen = evidence.json('plan.json')
    cells = plan(config['rows'], finish)
    require(frozen['configuration'] == config and frozen['controller'] == controller
            and frozen['cells'] == cells, 'executed plan differs')
    affinity = frozen['affinity']
    require(len(affinity) == config['cpu_count'] and len(set(affinity)) == len(affinity)
            and all(type(cpu) is int and cpu >= 0 for cpu in affinity), 'affinity evidence differs')
    require(cleanup['imports_removed'] is True and cleanup['raw_work_removed'] is True
            and set(affinity) <= set(cleanup['restored_affinity']), 'cleanup incomplete')
    for size in (evidence.json('storage-before.json')['allocated_bytes'], cleanup['allocated_bytes']):
        require(type(size) is int and 0 < size < 40_000_000_000, 'storage evidence differs')
    require('failure.json' not in evidence.members, 'failed stage')
    records = evidence.json('results.json')
    require(records == evidence.json('progress.json') and len(records) == 10, 'progress incomplete')
    raw = {'normal': [], 'diagnostic': []}
    required_files = {'output/identity.json', 'output/summary.json', 'output/events.jsonl',
                      'output/rss.jsonl', 'output/stderr.log', 'runner.log'}
    for row, cell in zip(records, cells):
        inventory = set(row['files'])
        require(all(row.get(k) == v for k,v in cell.items()) and row['status'] == 'passed'
                and required_files <= inventory <= required_files | {'output/storage.jsonl'},
                'cell order, status or inventory differs')
        prefix = cell['id'] + '/'
        actual = {name[len(prefix):] for name,member in evidence.members.items()
                  if name.startswith(prefix) and member.isfile()}
        require(actual == inventory, 'unlisted sample file')
        for name, digest in row['files'].items():
            require(sha(evidence.read(prefix+name)) == digest, 'sample file hash differs')
        identity = evidence.json(prefix+'output/identity.json')
        summary = evidence.json(prefix+'output/summary.json')
        events = [json.loads(line) for line in evidence.read(prefix+'output/events.jsonl').splitlines()]
        verified = verify_sample(identity, summary, events, cell, config['subjects'][cell['subject']],
                                 config['instrument'], controller, config['limits'])
        require(verified == row['values'], 'stored derived sample differs')
        # Reconstruct reported comparisons from raw phase events/counters, not saved values.
        times = {e['phase']: e['data']['elapsed_ns'] for e in events
                 if e['phase'] in timed_phases and e['data'].get('event') == 'end'}
        require(sum(times.values()) < 30_000_000_000 or 'output/storage.jsonl' in inventory,
                'periodic storage observations missing')
        if 'output/storage.jsonl' in inventory:
            storage = [json.loads(line) for line in evidence.read(prefix+'output/storage.jsonl').splitlines()]
            require(storage, 'empty periodic storage evidence')
            for sample in storage:
                require(type(sample['allocated_bytes']) is int and 0 < sample['allocated_bytes'] < 40_000_000_000
                        and sample['roots'] and sample['method'].startswith('native allocated bytes'),
                        'invalid periodic storage observation')
        peaks = {r['phase']: max(r['after_reset']['high_water_bytes'], r['after']['high_water_bytes'])
                 for r in summary['phase_kernel_rss']}
        raw[cell['subject']].append(dict(elapsed_ns=times, kernel_peak_rss_bytes=peaks))
    internal = {cell['id']: row['values']['writer_internal'] for row, cell in zip(records, cells) if cell['subject'] == 'diagnostic'}
    comparisons = {}
    for metric, phases in (('elapsed_ns', timed_phases), ('kernel_peak_rss_bytes', phases)):
        comparisons[metric] = {phase: comparison([row[metric][phase] for row in raw['normal']],
                                                 [row[metric][phase] for row in raw['diagnostic']])
                               for phase in phases}
    return dict(status='verified', finish=finish, stage_rows=config['rows'], controller=controller,
                assets=public['assets'], completed_samples=10, comparisons=comparisons,
                cold_cache_contracts_verified=True, cleanup_verified=True, writer_internal=internal,
                performance_qualification=False)
