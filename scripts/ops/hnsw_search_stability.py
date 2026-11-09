"""Predeclared paired diagnostic for the ten-trial search matrix.

This is a new observation, not a replacement for the three-trial result. Every
subject is first in two of the ten rotating blocks. The ninth ordered ratio
is a conservative one-sided upper confidence bound for the median paired
ratio (nominal coverage 1013/1024 if trials are independent and exchangeable).
The calculation does not itself establish those conditions or RC acceptance.
"""

import math
import statistics

from hnsw_search_contract import EF_VALUES, SUBJECTS, require

TRIALS = 10
MAX_ACCEPTABLE_RATIO = 1.05
MIN_RECALL = 0.99


def assess(rows):
    require(len(rows) == TRIALS * len(SUBJECTS) * len(EF_VALUES),
            'ten-trial HNSW matrix incomplete')
    indexed = {}
    for row in rows:
        key = row['subject'], row['trial'], row['ef_search']
        require(key[0] in SUBJECTS and type(key[1]) is int and 0 <= key[1] < TRIALS
                and key[2] in EF_VALUES and key not in indexed,
                'unexpected or duplicate HNSW point')
        for field in ('query_latency_p50_us', 'recall_at_10'):
            require(type(row[field]) in (int, float) and math.isfinite(row[field]),
                    'nonfinite HNSW point')
        require(row['query_latency_p50_us'] > 0 and 0 <= row['recall_at_10'] <= 1,
                'invalid HNSW point')
        indexed[key] = row
    require(len(indexed) == len(rows), 'HNSW point coverage differs')

    comparisons = []
    for reference in ('published', 'before'):
        for ef in EF_VALUES:
            paired = []
            recall_ok = True
            for trial in range(TRIALS):
                before = indexed[reference, trial, ef]
                candidate = indexed['candidate', trial, ef]
                paired.append(candidate['query_latency_p50_us'] /
                              before['query_latency_p50_us'])
                recall_ok &= candidate['recall_at_10'] >= MIN_RECALL
            ordered = sorted(paired)
            comparisons.append(dict(reference=reference, ef_search=ef,
                paired_ratios=paired, median_ratio=statistics.median(paired),
                upper_median_bound=ordered[8], minimum_candidate_recall=min(
                    indexed['candidate', trial, ef]['recall_at_10']
                    for trial in range(TRIALS)),
                paired_bound_within_limit=(recall_ok and
                    statistics.median(paired) <= 1.0 and
                    ordered[8] <= MAX_ACCEPTABLE_RATIO)))
    return dict(schema='alopex.hnsw-search-stability/v1', trials=TRIALS,
        nominal_upper_bound_coverage=1013 / 1024, comparisons=comparisons,
        all_paired_bounds_within_limit=all(row['paired_bound_within_limit']
                                           for row in comparisons))
