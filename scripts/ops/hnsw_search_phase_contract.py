"""Fixed diagnostic matrix and complete-result checks, without a performance gate."""
import hashlib
import json

from hnsw_search_phase_capture import require
from hnsw_search_phase_audit import audit_pairs


def plan(mode):
    require(mode in ('preflight', 'full'), 'unknown diagnostic mode')
    result = []
    for trial in range(1 if mode == 'preflight' else 3):
        for subject in (('before', 'candidate') if trial % 2 == 0 else ('candidate', 'before')):
            result.append(dict(id=f'trial{trial}-{subject}', trial=trial, subject=subject,
                rows=32 if mode == 'preflight' else 9171, dimension=128,
                query_vectors=4 if mode == 'preflight' else 200, k=10,
                seed=42, m=16, ef_construction=200, ef_values=[16, 32, 64, 128, 256]))
    return result


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_cell(cell, directory, truth, config):
    output = directory / 'diagnostic'
    result = json.loads((output / 'diagnostic.json').read_bytes())
    parameters = json.loads((directory / 'configuration.json').read_bytes())
    require(result['schema'] == 'alopex.hnsw-search-diagnostic/v1'
        and result['mode'] == config['mode'] and result['subject'] == cell['subject']
        and result['trial'] == cell['trial'] and result['index_closed'] is True
        and result['performance_qualification'] is False, 'diagnostic identity differs')
    require(result['sources'] == config['sources'] and result['worker_sha256'] ==
        config['sources']['scripts/ops/hnsw_search_phase_worker.py'], 'worker source differs')
    require(result['subject_identity']['manifest_sha256'] == parameters['subject_manifest_sha256']
        and result['input_sha256'] == parameters['input_sha256'], 'subject/input differs')
    raw_path = output / 'query-pairs.jsonl'
    require(raw_path.stat().st_size <= 32_000_000 and sha(raw_path) == result['query_pairs_sha256'],
            'diagnostic raw identity differs')
    with raw_path.open() as stream:
        pairs, summaries = audit_pairs(stream, mode=config['mode'])
    require(len(pairs) == result['pairs'] == cell['query_vectors'] * len(cell['ef_values']),
            'diagnostic query count differs')
    require(json.loads((output / 'worker-status.json').read_bytes()) ==
        dict(status='passed', completed_pairs=len(pairs)), 'worker completion missing')
    return summaries
