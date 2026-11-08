"""Explicit completion-mode contracts for the five-sample manual #681 comparison."""
import hashlib
import json
from pathlib import PurePosixPath
import statistics

PHASES = ('seed', 'index_build', 'cold_transaction_1', 'cold_transaction_2', 'warm',
          'writer_begin', 'first_insert', 'second_insert', 'rollback')
TIMED_PHASES = tuple(p for p in PHASES if p not in ('warm', 'rollback'))
STAGES = (1000, 10000, 100000)


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(value):
    return hashlib.sha256(value).hexdigest()


def plan(rows, finish):
    require(finish in ("commit", "rollback"), "explicit completion mode required")
    require(type(rows) is int and rows in STAGES, 'unsupported stage')
    return [dict(id=f'hnsw-{finish}-{rows}-{sample}-{subject}', rows=rows, dimensions=384, finish=finish,
                 sample=sample, subject=subject)
            for sample in range(5)
            for subject in (('before', 'candidate') if sample % 2 == 0 else ('candidate', 'before'))]


def regular(root, relative):
    name = PurePosixPath(relative)
    require(not name.is_absolute() and '..' not in name.parts and str(name) == relative,
            'unsafe asset path')
    path = root / name
    require(path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)),
            'nonregular asset')
    return path


def verify_assets(root, expected):
    manifest_bytes = regular(root / 'assets', 'assets.json').read_bytes()
    require(sha(manifest_bytes) == expected['assets_digest'], 'asset manifest differs')
    manifest = json.loads(manifest_bytes)
    public = json.loads(regular(root, 'result.json').read_bytes())
    source = dict(commit=expected['source_commit'], tree=expected['source_tree'], profile='release')
    require(manifest['schema'] == 'alopex.hnsw-example-assets/v1' and manifest['source'] == source
            and manifest['platform'] == expected['platform'], 'source/profile/platform differs')
    require(public['status'] == 'passed' and public['source'] == source
            and public['assets_digest'] == expected['assets_digest']
            and public['performance_qualification'] is False, 'producer outcome differs')
    require(type(manifest['expanded_limit_bytes']) is int
            and 0 < manifest['expanded_limit_bytes'] <= 268435456, 'asset bound differs')
    files, paths, size = {}, set(), 0
    require(len(manifest['files']) == 4, 'asset inventory differs')
    for item in manifest['files']:
        require(item['role'] not in files and item['path'] not in paths, 'duplicate asset')
        file = regular(root / 'assets', item['path'])
        require(type(item['size']) is int and 0 < item['size'] <= manifest['expanded_limit_bytes']
                and file.stat().st_size == item['size'], 'asset size differs')
        require(sha(file.read_bytes()) == item['sha256'], 'asset hash differs')
        size += item['size']
        files[item['role']] = (file, item)
        paths.add(item['path'])
    require(set(files) == {'binary', 'library', 'example', 'receipt'}
            and size <= manifest['expanded_limit_bytes'], 'asset inventory or total bound differs')
    actual = {p.relative_to(root / 'assets').as_posix() for p in (root / 'assets').rglob('*') if p.is_file()}
    require(actual == paths | {'assets.json'}, 'unlisted asset')
    receipt = json.loads(files['receipt'][0].read_bytes())
    require(receipt['schema'] == 'alopex.hnsw-example-build/v1' and receipt['source'] == source,
            'receipt source differs')
    for role, field in (('binary', 'binary_sha256'), ('library', 'parser_sha256'), ('example', 'example_source_sha256')):
        require(receipt[field] == files[role][1]['sha256'] == expected[field], 'receipt content differs')
    require(receipt['build']['command'] == ['cargo', 'build', '--locked', '--release', '-p',
                                           'alopex-embedded', '--example', 'issue681_hnsw_first_write']
            and receipt['build']['incremental'] is False and receipt['build']['release_debug'] is False
            and receipt['rust_version'] == expected['rust_version']
            and receipt['parser_contract'] == expected['parser_contract']
            and receipt['cargo_lock_sha256'] == expected['cargo_lock_sha256'], 'build conditions differ')
    require(receipt['smoke']['status'] == 'passed' and receipt['smoke']['rows'] == 2
            and receipt['smoke']['dimensions'] == 4 and receipt['smoke']['phases'] == 9,
            'producer smoke missing')
    completion = receipt.get('completion_smoke', {})
    require(set(completion) == {'commit', 'rollback'}, 'completion producer smoke missing')
    for mode, result in completion.items():
        require(result.get('mode') == mode and result.get('status') == 'passed'
                and result.get('result_validated') is True
                and result.get('rows') == 2 and result.get('dimensions') == 4
                and result.get('phases') == 9 and result.get('events') == 19
                and type(result.get('elapsed_ns')) is int and result['elapsed_ns'] > 0,
                'completion producer smoke invalid')
    return {role: file for role, (file, _) in files.items()}


def verify_sample(identity, summary, events, cell, expected, instrument, controller, limits):
    finish = cell['finish']
    require(finish in ('commit', 'rollback') and identity.get('finish') == finish,
            'completion mode differs')
    phases = (*PHASES[:-1], finish)
    timed_phases = (*TIMED_PHASES, finish)
    require(identity['source'] == controller and identity['binary_sha256'] == expected['binary_sha256']
            and identity['example_source_sha256'] == expected['example_source_sha256']
            and identity['runner_sha256'] == instrument['runner_sha256']
            and identity['phase_rss_source_sha256'] == instrument['phase_rss_source_sha256'],
            'sample source binding differs')
    require(identity['rows'] == cell['rows'] and identity['dimensions'] == cell['dimensions'] == 384
            and identity['phase_kernel_rss'] is True and identity['rss_limit'] == limits['rss_bytes']
            and identity['timeout_seconds'] == limits['case_seconds'], 'sample input or limits differ')
    require(summary['outcome'] == 'passed' and summary['failure'] is None
            and summary['kernel_rss_reset'] is True and summary['children_lifetime_peak_rss_bytes'] is None,
            'failed or misleading RSS sample')
    sequence = [(phase, event) for phase in phases for event in ('start', 'end')] + [('complete', None)]
    require([(e['phase'], e['data'].get('event')) for e in events] == sequence, 'phase sequence differs')
    require(events[0]['data']['rows'] == cell['rows'] and events[0]['data']['dimensions'] == 384,
            'workload arguments differ')
    require(events[-1]['data'].get('finish') == finish
            and events[-1]['data'].get('result_validated') is True,
            'completion result verification missing')
    ends = {e['phase']: e['data'] for e in events if e['data'].get('event') == 'end'}
    times = {phase: ends[phase]['elapsed_ns'] for phase in timed_phases}
    require(all(type(v) is int and v > 0 for v in times.values()), 'phase time missing')
    require(0 < ends['warm']['graph_bytes'] <= 256 * 1024**2, 'graph does not fit fixed cache')
    first, second = (ends[p]['stats'] for p in ('cold_transaction_1', 'cold_transaction_2'))
    cold_start = next(e['data']['stats'] for e in events
                      if e['phase'] == 'cold_transaction_1' and e['data'].get('event') == 'start')
    require(cold_start['entries'] == 0 and first['misses'] > cold_start['misses'],
            'first reader did not load from an empty shared cache')
    if cell['subject'] == 'candidate':
        require(first['entries'] == second['entries'] == 1 and second['misses'] == first['misses']
                and second['hits'] > first['hits'], 'candidate reloads transaction graph')
    else:
        require(cell['subject'] == 'before' and first['entries'] == second['entries'] == 0
                and second['misses'] > first['misses'], 'baseline cache behavior differs')
    observations = summary['phase_kernel_rss']
    require(summary.get('incomplete_kernel_phase') is None, 'incomplete kernel phase')
    require([r['phase'] for r in observations] == list(phases), 'kernel phase coverage differs')
    peaks = {}
    for row in observations:
        peak = row['kernel_reported_peak_rss_bytes']
        require(type(peak) is int and 0 < peak < limits['rss_bytes']
                and peak == max(row['after']['high_water_bytes'], row['after_reset']['high_water_bytes']),
                'kernel peak or limit differs')
        require(row.get('kernel_high_water_decreased') is (row['after']['high_water_bytes'] < row['after_reset']['high_water_bytes'])
                and row.get('aggregation') == 'maximum of reset and end kernel high-water readings',
                'kernel peak aggregation differs')
        for field in ('before_reset', 'after_reset', 'after'):
            require(type(row[field]['high_water_bytes']) is int and type(row[field]['rss_bytes']) is int
                    and row[field]['high_water_bytes'] >= row[field]['rss_bytes'] > 0, 'invalid RSS counter')
        peaks[row['phase']] = peak
    for inventory in (identity['initial_inventory'], summary['final_inventory']):
        require(type(inventory['allocated_bytes']) is int and 0 < inventory['allocated_bytes'] < 40_000_000_000
                and inventory['roots'] and inventory['method'].startswith('native allocated bytes'),
                'host storage evidence missing')
    return dict(finish=finish, result_validated=True, elapsed_ns=times, kernel_peak_rss_bytes=peaks, graph_bytes=ends['warm']['graph_bytes'],
                cold_cache=dict(first=first, second=second))


def compare(before, candidate):
    require(len(before) == len(candidate) == 5 and all(type(v) is int and v > 0 for v in before+candidate),
            'five positive samples per subject required')
    b, c = statistics.median(before), statistics.median(candidate)
    db = statistics.median(abs(v-b) for v in before)
    dc = statistics.median(abs(v-c) for v in candidate)
    noise = 3 * max(db, dc)
    return dict(before=before, candidate=candidate, baseline_median=b, candidate_median=c,
                baseline_mad=db, candidate_mad=dc, baseline_range=[min(before),max(before)],
                candidate_range=[min(candidate),max(candidate)], difference=c-b, noise=noise,
                ratio=c/b, improvement_at_least_ten_percent=(b-c)/b >= .10 and b-c > noise,
                increase_exceeds_noise=c-b > noise)
