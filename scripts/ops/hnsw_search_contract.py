"""Fixed search matrix, recall validation and evidence checks for issue #712."""
import math
import statistics

SUBJECTS = ('published', 'before', 'candidate', 'hnswlib', 'faiss-hnsw')
EF_VALUES = (16, 32, 64, 128, 256)
RESOURCE_SCOPE = ('Current process around the original search function, including '
    'percentile aggregation and observation overhead; excludes raw output, '
    'index construction, oracle and warmup.')


def require(value, message):
    if not value:
        raise RuntimeError(message)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def plan(mode):
    require(mode in ('preflight', 'full'), 'unknown HNSW mode')
    cells = []
    for trial in range(1 if mode == 'preflight' else 3):
        subjects = SUBJECTS[trial:] + SUBJECTS[:trial]
        for subject in subjects:
            cells.append(dict(id=f'trial{trial}-{subject}', trial=trial, subject=subject,
                engine=subject if subject in ('hnswlib', 'faiss-hnsw') else 'alopex-hnsw',
                rows=32 if mode == 'preflight' else 9171, dimension=128,
                query_vectors=4 if mode == 'preflight' else 200, k=10,
                seed=42, m=16, ef_construction=200, ef_values=list(EF_VALUES),
                min_queries=4 if mode == 'preflight' else 10000,
                min_seconds=0.0 if mode == 'preflight' else 2.0))
    return cells


def validate_predictions(predicted, truth, acceptable, *, rows, k=10):
    require(type(predicted) is list and len(predicted) == len(truth) == len(acceptable)
        and len(truth) > 0, 'query result coverage differs')
    strict = tied = 0
    for result, expected, allowed in zip(predicted, truth, acceptable):
        for values, exact in ((result, True), (expected, True), (allowed, False)):
            require(type(values) is list and (len(values) == k if exact else len(values) >= k),
                'top-k cardinality differs')
            require(all(type(v) is int and 0 <= v < rows for v in values)
                and len(set(values)) == len(values), 'invalid or duplicate neighbor ID')
        require(set(expected) <= set(allowed), 'tie oracle omits exact neighbors')
        strict += len(set(result) & set(expected))
        tied += len(set(result) & set(allowed))
    return dict(recall_at_10=strict / (k * len(truth)),
        tie_aware_recall_at_10=tied / (k * len(truth)))


def verify_point(cell, row, predictions, truth, acceptable):
    require(row['engine'] == cell['engine'] and row['ef_search'] in cell['ef_values']
        and row['run'] == 1 and row['dataset_size'] == cell['rows']
        and row['dimension'] == cell['dimension'], 'search identity differs')
    count, duration, samples = row['query_count'], row['duration_seconds'], row['query_latencies_us']
    require(type(count) is int and count >= cell['min_queries']
        and finite(duration) and duration > 0 and duration >= cell['min_seconds'], 'search work incomplete')
    require(type(samples) is list and len(samples) == count
        and all(finite(s) and s > 0 for s in samples), 'query latency samples invalid')
    ordered = sorted(samples)
    calculated = dict(queries_per_second=count/duration, latency_us=duration*1e6/count,
        query_latency_p50_us=statistics.median(samples),
        query_latency_p95_us=ordered[max(0, (95*count+99)//100-1)],
        query_latency_p99_us=ordered[max(0, (99*count+99)//100-1)])
    for key, expected in calculated.items():
        require(finite(row[key]) and math.isclose(row[key], expected, rel_tol=1e-12, abs_tol=1e-12),
            'search summary differs from raw')
    for phase in ('before', 'after'):
        actual = validate_predictions(predictions[phase], truth, acceptable, rows=cell['rows'], k=cell['k'])
        for key, value in actual.items():
            require(finite(row[key]) and math.isclose(row[key], value, rel_tol=1e-12, abs_tol=1e-12),
                'recall changed or differs from saved results')
    # Exact ordering may vary for distance ties; retain both metrics and predictions.
    # Low recall is a real result, not a process failure or grounds to erase a row.
    native = row['native_search_latency_us']
    require((native is None and cell['engine'] != 'alopex-hnsw') or
        (cell['engine'] == 'alopex-hnsw' and finite(native) and native >= 0), 'native timing missing')
    residual = row['python_binding_residual_us']
    require((native is None and residual is None) or
        (native is not None and finite(residual) and residual == max(0.0, row['latency_us']-native)), 'legacy residual differs')
    return calculated


def same_recall_point(rows, threshold):
    """Choose only settings where every trial reaches the declared recall."""
    require(threshold in (0.95, 0.99), 'unsupported recall target')
    require(len(rows) == 15, 'three-trial search matrix incomplete')
    groups = {}
    for row in rows:
        key = row['ef_search']
        require(key in EF_VALUES and row['trial'] in (0, 1, 2), 'unexpected search point')
        groups.setdefault(key, []).append(row)
        require(finite(row['latency_us']) and row['latency_us'] > 0
            and finite(row['query_latency_p50_us']) and row['query_latency_p50_us'] > 0
            and finite(row['queries_per_second']) and row['queries_per_second'] > 0
            and finite(row['recall_at_10']) and 0 <= row['recall_at_10'] <= 1, 'invalid search point')
    require(set(groups) == set(EF_VALUES) and all(len(g) == 3 and
        {r['trial'] for r in g} == {0, 1, 2} for g in groups.values()), 'duplicate or absent trial')
    qualified = [(-statistics.median(r['queries_per_second'] for r in group), ef)
        for ef, group in groups.items() if all(r['recall_at_10'] >= threshold for r in group)]
    if not qualified:
        return None
    negative_qps, ef = min(qualified)
    return dict(ef_search=ef, latency_us=statistics.median(r['query_latency_p50_us'] for r in groups[ef]),
        mean_latency_us=statistics.median(r['latency_us'] for r in groups[ef]),
        queries_per_second=-negative_qps, recall_threshold=threshold,
        minimum_trial_recall=min(r['recall_at_10'] for r in groups[ef]))
