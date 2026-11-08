import copy
import json
import os
from types import SimpleNamespace
import unittest

from hnsw_search_phase_capture import (PHASES, MAX_BYTES, capture, parse_records,
                                      run_pair)


def record():
    return dict(schema='alopex.hnsw-search-phase/v1', scope='layer', level=0,
        ef=16, total_ns=10, residual_ns=3, nodes_visited=5, distance_computations=4,
        phases={name: dict(ns=1, calls=1) for name in PHASES})


def encoded(value):
    return (json.dumps(value) + '\n').encode()


class PhaseCaptureTests(unittest.TestCase):
    def test_phase_sum_and_public_traversal_are_independently_checked(self):
        row = record()
        stats = dict(nodes_visited=7, distance_computations=8)
        self.assertEqual(parse_records(encoded(row), enabled=True, ef=16, stats=stats), [row])
        for change in ('sum', 'traversal', 'boolean', 'coverage', 'ef', 'zero-call'):
            bad = copy.deepcopy(row)
            if change == 'sum': bad['residual_ns'] += 1
            elif change == 'traversal': bad['nodes_visited'] = 8
            elif change == 'boolean': bad['total_ns'] = True
            elif change == 'coverage': del bad['phases']['ordering']
            elif change == 'ef': bad['ef'] = 32
            else: bad['phases']['node_read']['calls'] = 0
            with self.subTest(change=change), self.assertRaises(ValueError):
                parse_records(encoded(bad), enabled=True, ef=16, stats=stats)

    def test_missing_disabled_duplicate_and_oversized_output_are_rejected(self):
        stats = dict(nodes_visited=7, distance_computations=8)
        cases = [(b'', True), (encoded(record()), False), (b'x' * (MAX_BYTES + 1), True),
            (encoded(record()).replace(b'"level": 0', b'"level": 0, "level": 0'), True)]
        for raw, enabled in cases:
            with self.subTest(enabled=enabled, size=len(raw)), self.assertRaises(ValueError):
                parse_records(raw, enabled=enabled, ef=16, stats=stats)

    def test_capture_preserves_return_value_and_restores_environment_on_failure(self):
        prior = os.environ.get('ALOPEX_HNSW_PHASE_PROBE')
        marker = object()
        def operation():
            self.assertEqual(os.environ['ALOPEX_HNSW_PHASE_PROBE'], '1')
            os.write(2, b'bounded output\n')
            return marker
        result, raw, duration = capture(operation, enabled=True)
        self.assertIs(result, marker)
        self.assertEqual(raw, b'bounded output\n')
        self.assertGreater(duration['wall_ns'], 0)
        self.assertEqual(os.environ.get('ALOPEX_HNSW_PHASE_PROBE'), prior)
        def failure(): raise LookupError('original error')
        with self.assertRaisesRegex(LookupError, 'original error'):
            capture(failure, enabled=False)
        self.assertEqual(os.environ.get('ALOPEX_HNSW_PHASE_PROBE'), prior)
        self.assertEqual(capture(lambda: 7, enabled=False)[0], 7)

    def test_output_overflow_is_drained_and_rejected(self):
        def excessive():
            for _ in range(10): os.write(2, b'x' * 8192)
        with self.assertRaisesRegex(ValueError, 'exceeds bound'):
            capture(excessive, enabled=True)
        self.assertEqual(capture(lambda: 7, enabled=False)[0], 7)

    def test_public_pair_compares_score_bits_metadata_order_and_traversal(self):
        def search_factory(change):
            def search(query, k, ef):
                enabled = os.environ['ALOPEX_HNSW_PHASE_PROBE'] == '1'
                if enabled: os.write(2, encoded(record()))
                results = [SimpleNamespace(key=b'0', score=0.0, metadata=None, vector=None),
                    SimpleNamespace(key=b'1', score=0.5, metadata=b'raw', vector=None)]
                stats = SimpleNamespace(nodes_visited=7, distance_computations=8,
                                        search_time_us=10 if enabled else 9)
                if enabled:
                    if change == 'bits': results[0].score = -0.0
                    elif change == 'metadata': results[1].metadata = b'changed'
                    elif change == 'order': results.reverse()
                    elif change == 'traversal': stats.nodes_visited += 1
                return results, stats
            return search
        pair = run_pair(search_factory(None), None, k=2, ef=16, active_rows=32, reverse=True)
        self.assertEqual(pair['on']['results'], pair['off']['results'])
        for change in ('bits', 'metadata', 'order', 'traversal'):
            with self.subTest(change=change), self.assertRaises(ValueError):
                run_pair(search_factory(change), None, k=2, ef=16, active_rows=32)

    def test_upper_beam_and_two_base_routes_have_distinct_widths(self):
        upper = record()
        upper['level'] = 1
        base = record()
        base['ef'] = 32
        raw = encoded(upper) + encoded(base) * 2
        stats = dict(nodes_visited=20, distance_computations=20)
        rows = parse_records(raw, enabled=True, ef=32, stats=stats)
        self.assertEqual([row['ef'] for row in rows], [16, 32, 32])
        for bad in (encoded(base) + encoded(upper), encoded(upper), encoded(base) * 3):
            with self.assertRaises(ValueError):
                parse_records(bad, enabled=True, ef=32, stats=stats)


if __name__ == '__main__': unittest.main()
