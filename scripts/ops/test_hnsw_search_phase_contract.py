import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from hnsw_search_phase_contract import plan, verify_cell
from test_hnsw_search_phase_audit import fixture


class PhaseContractTests(unittest.TestCase):
    def test_fixed_two_subject_matrix_is_alternated_and_bounded(self):
        self.assertEqual([c['subject'] for c in plan('preflight')], ['before', 'candidate'])
        cells = plan('full')
        self.assertEqual([c['subject'] for c in cells],
                         ['before', 'candidate', 'candidate', 'before', 'before', 'candidate'])
        self.assertEqual({c['rows'] for c in cells}, {9171})
        self.assertEqual({c['query_vectors'] for c in cells}, {200})
        self.assertEqual({tuple(c['ef_values']) for c in cells}, {(16, 32, 64, 128, 256)})
        with self.assertRaises(ValueError): plan('unbounded')

    def test_worker_results_are_separate_from_supervisor_files_and_source_bound(self):
        cell = plan('preflight')[0]
        sources = {'scripts/ops/hnsw_search_phase_worker.py': 'a' * 64}
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            output = directory / 'diagnostic'
            output.mkdir()
            (directory / 'worker.log').write_text('supervisor output')
            parameters = dict(subject_manifest_sha256='b' * 64, input_sha256='c' * 64)
            (directory / 'configuration.json').write_text(json.dumps(parameters))
            raw = ''.join(json.dumps(row) + '\n' for row in fixture())
            (output / 'query-pairs.jsonl').write_text(raw)
            result = dict(schema='alopex.hnsw-search-diagnostic/v1', mode='preflight',
                subject='before', trial=0, index_closed=True, performance_qualification=False,
                sources=sources, worker_sha256='a' * 64, subject_identity=dict(manifest_sha256='b' * 64),
                input_sha256='c' * 64, query_pairs_sha256=hashlib.sha256(raw.encode()).hexdigest(), pairs=20)
            (output / 'diagnostic.json').write_text(json.dumps(result))
            (output / 'worker-status.json').write_text(json.dumps(dict(status='passed', completed_pairs=20)))
            self.assertEqual(len(verify_cell(cell, directory, {}, dict(mode='preflight', sources=sources))), 5)
            for key, value in [('subject', 'candidate'), ('trial', 1), ('worker_sha256', 'd' * 64),
                               ('input_sha256', 'd' * 64), ('pairs', 19), ('index_closed', False)]:
                bad = copy.deepcopy(result)
                bad[key] = value
                (output / 'diagnostic.json').write_text(json.dumps(bad))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    verify_cell(cell, directory, {}, dict(mode='preflight', sources=sources))
