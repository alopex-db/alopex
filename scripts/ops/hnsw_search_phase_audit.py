"""Reconstruct phase and overhead summaries from complete paired query records."""
import json
import math
import statistics
import struct

from hnsw_search_phase_capture import PHASES, integer, parse_records, require


def audit_pairs(lines, *, mode):
    require(mode in ('preflight', 'full'), 'unknown diagnostic mode')
    rows, count = (32, 4) if mode == 'preflight' else (9171, 200)
    expected = {(ef, query) for ef in (16, 32, 64, 128, 256) for query in range(count)}
    pairs = {}
    query_hashes = {}
    for line in lines:
        require(len(line) <= 131072, 'diagnostic row exceeds bound')
        row = json.loads(line)
        key = row['ef_search'], row['query_index']
        require(all(integer(value) for value in key), 'invalid query coordinates')
        require(key in expected and key not in pairs, 'duplicate or unexpected query pair')
        digest = row['query_sha256']
        require(type(digest) is str and len(digest) == 64
            and all(c in '0123456789abcdef' for c in digest), 'query identity missing')
        require(query_hashes.setdefault(key[1], digest) == digest, 'query differs between widths')
        observations = row['observations']
        require(set(observations) == {'off', 'on'}, 'probe mode missing')
        for label, observation in observations.items():
            require(len(observation['results']) == 10, 'top-k size differs')
            keys = []
            for result in observation['results']:
                require(set(result) == {'key_hex', 'score_f32_hex', 'metadata_hex'}, 'result fields differ')
                decoded = bytes.fromhex(result['key_hex'])
                require(decoded == str(int(decoded)).encode() and 0 <= int(decoded) < rows,
                    'neighbor key outside fixed fixture')
                keys.append(decoded)
                score = bytes.fromhex(result['score_f32_hex'])
                require(len(score) == 4 and math.isfinite(struct.unpack('>f', score)[0]), 'score bits invalid')
                if result['metadata_hex'] is not None:
                    bytes.fromhex(result['metadata_hex'])
            require(len(set(keys)) == 10, 'duplicate neighbor key')
            require(set(observation['stats']) == {'nodes_visited', 'distance_computations', 'search_time_us'}
                and all(integer(v) for v in observation['stats'].values()), 'public stats invalid')
            duration = observation['duration']
            require(set(duration) == {'wall_ns', 'cpu_ns'} and all(integer(v) for v in duration.values())
                and duration['wall_ns'] > 0, 'query duration invalid')
            raw = b''.join((json.dumps(layer) + '\n').encode() for layer in observation['layers'])
            parse_records(raw, enabled=label == 'on', ef=min(key[0], rows), stats=observation['stats'])
        off, on = observations['off'], observations['on']
        require(off['results'] == on['results'], 'probe changed public results')
        require(all(off['stats'][k] == on['stats'][k] for k in
            ('nodes_visited', 'distance_computations')), 'probe changed traversal')
        pairs[key] = row
    require(set(pairs) == expected, 'diagnostic matrix incomplete')
    summaries = []
    for ef in (16, 32, 64, 128, 256):
        observations = [pairs[ef, query]['observations'] for query in range(count)]
        durations = {label: {metric: statistics.median(o[label]['duration'][metric] for o in observations)
            for metric in ('wall_ns', 'cpu_ns')} for label in ('off', 'on')}
        phases = {phase: dict(ns=sum(layer['phases'][phase]['ns'] for o in observations
                    for layer in o['on']['layers']),
                calls=sum(layer['phases'][phase]['calls'] for o in observations for layer in o['on']['layers']))
            for phase in PHASES}
        residual = sum(layer['residual_ns'] for o in observations for layer in o['on']['layers'])
        total = sum(layer['total_ns'] for o in observations for layer in o['on']['layers'])
        require(sum(p['ns'] for p in phases.values()) + residual == total, 'aggregate phase sum differs')
        summaries.append(dict(ef_search=ef, query_pairs=count, duration_medians=durations,
            phase_totals=phases, layer_total_ns=total, layer_residual_ns=residual,
            unclassified_traversal={key: sum(o['on']['stats'][key] - sum(layer[key]
                for layer in o['on']['layers']) for o in observations)
                for key in ('nodes_visited', 'distance_computations')}))
    return pairs, summaries


def compare_subjects(before, candidate):
    require(before.keys() == candidate.keys(), 'subject query coverage differs')
    for key, row in before.items():
        other = candidate[key]
        require(row['query_sha256'] == other['query_sha256'], 'subjects used different queries')
        left, right = row['observations']['off'], other['observations']['off']
        require(left['results'] == right['results'], 'subject result bits/order/metadata differ')
        require(all(left['stats'][name] == right['stats'][name]
            for name in ('nodes_visited', 'distance_computations')), 'subject traversal differs')
    return dict(query_pairs=len(before), results_and_traversal_identical=True,
                performance_qualification=False)
