"""Bounded diagnostic capture; these durations are not release performance data."""
import json
import math
import os
import selectors
import struct
import sys
import threading
import time

PHASES = ('setup', 'resolve_score', 'node_read', 'frontier', 'result_heap',
          'ordering', 'materialize')
MAX_BYTES = 65536
MAX_RECORDS = 32


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value):
    return type(value) is int and value >= 0


def parse_records(raw, *, enabled, ef, stats, upper_width=16):
    require(len(raw) <= MAX_BYTES, 'diagnostic output exceeds bound')
    if not enabled:
        require(not raw, 'disabled probe emitted output')
        return []
    lines = raw.splitlines()
    require(0 < len(lines) <= MAX_RECORDS, 'missing or excessive layer records')
    records = []
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate diagnostic JSON field')
            result[key] = value
        return result
    for line in lines:
        row = json.loads(line, object_pairs_hook=unique_object)
        require(set(row) == {'schema', 'scope', 'level', 'ef', 'total_ns',
            'residual_ns', 'nodes_visited', 'distance_computations', 'phases'},
            'layer fields differ')
        require(row['schema'] == 'alopex.hnsw-search-phase/v1'
            and row['scope'] == 'layer', 'layer schema differs')
        require(all(integer(row[k]) for k in ('level', 'ef', 'total_ns',
            'residual_ns', 'nodes_visited', 'distance_computations')),
            'layer counter is not a nonnegative integer')
        expected_width = ef if row['level'] == 0 else min(upper_width, ef)
        require(row['ef'] == expected_width, 'search layer identity differs')
        require(type(row['phases']) is dict and set(row['phases']) == set(PHASES),
            'phase coverage differs')
        for phase in row['phases'].values():
            require(type(phase) is dict and set(phase) == {'ns', 'calls'}
                and all(integer(v) for v in phase.values()), 'phase counters invalid')
            require(phase['calls'] != 0 or phase['ns'] == 0, 'time without a phase call')
        require(sum(v['ns'] for v in row['phases'].values()) + row['residual_ns']
            == row['total_ns'], 'layer phase sum differs')
        require(row['phases']['setup']['calls'] == 1
            and row['phases']['materialize']['calls'] == 1
            and row['phases']['ordering']['calls'] == 1, 'incomplete search layer')
        records.append(row)
    levels = [row['level'] for row in records]
    base_count = levels.count(0)
    require(base_count in (1, 2) and levels == list(range(levels[0], 0, -1))
        + [0] * base_count, 'search layer order or coverage differs')
    for key in ('nodes_visited', 'distance_computations'):
        require(integer(stats[key]) and sum(row[key] for row in records) <= stats[key],
            'layer traversal exceeds public traversal')
    return records


def capture(operation, *, enabled):
    """Capture this single worker's stderr with bounded memory and a drain thread."""
    original = os.environ.get('ALOPEX_HNSW_PHASE_PROBE')
    os.environ['ALOPEX_HNSW_PHASE_PROBE'] = '1' if enabled else '0'
    saved = os.dup(2)
    read_fd, write_fd = os.pipe()
    stop = threading.Event()
    chunks = bytearray()
    errors = []
    overflow = False

    def drain():
        nonlocal overflow
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(read_fd, selectors.EVENT_READ)
                while not stop.is_set():
                    if not selector.select(0.05):
                        continue
                    chunk = os.read(read_fd, 8192)
                    if not chunk:
                        return
                    remaining = MAX_BYTES - len(chunks)
                    chunks.extend(chunk[:remaining])
                    overflow |= len(chunk) > remaining
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=drain, daemon=True)
    started = False
    try:
        sys.stderr.flush()
        thread.start()
        started = True
        os.dup2(write_fd, 2)
        os.close(write_fd)
        write_fd = None
        wall = time.perf_counter_ns()
        cpu = time.process_time_ns()
        value = operation()
        cpu_ns = time.process_time_ns() - cpu
        wall_ns = time.perf_counter_ns() - wall
        sys.stderr.flush()
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        if write_fd is not None:
            os.close(write_fd)
        if original is None:
            os.environ.pop('ALOPEX_HNSW_PHASE_PROBE', None)
        else:
            os.environ['ALOPEX_HNSW_PHASE_PROBE'] = original
        if started:
            thread.join(2)
            if thread.is_alive():
                stop.set()
                thread.join(1)
                errors.append(RuntimeError('diagnostic pipe did not close'))
        os.close(read_fd)
    require(not errors, 'diagnostic capture failed')
    require(not overflow, 'diagnostic output exceeds bound')
    return value, bytes(chunks), {'wall_ns': wall_ns, 'cpu_ns': cpu_ns}


def snapshot(results, stats, *, k):
    require(len(results) == k, 'top-k size differs')
    rows = []
    for result in results:
        require(math.isfinite(result.score), 'nonfinite score')
        require(result.vector is None, 'unexpected returned vector')
        rows.append({'key_hex': bytes(result.key).hex(),
            'score_f32_hex': struct.pack('>f', result.score).hex(),
            'metadata_hex': None if result.metadata is None else bytes(result.metadata).hex()})
    require(len({r['key_hex'] for r in rows}) == k, 'duplicate result key')
    counters = {key: getattr(stats, key) for key in
        ('nodes_visited', 'distance_computations', 'search_time_us')}
    require(all(integer(v) for v in counters.values()), 'public counters invalid')
    return {'results': rows, 'stats': counters}


def run_pair(search, query, *, k, ef, active_rows, reverse=False):
    observations = {}
    for enabled in ((True, False) if reverse else (False, True)):
        (results, stats), raw, duration = capture(
            lambda: search(query, k, ef), enabled=enabled)
        observation = snapshot(results, stats, k=k)
        observation['layers'] = parse_records(raw, enabled=enabled, ef=min(max(k, ef), active_rows),
                                             stats=observation['stats'])
        observation['duration'] = duration
        observations['on' if enabled else 'off'] = observation
    off, on = observations['off'], observations['on']
    require(off['results'] == on['results'], 'instrumentation changed result bits/order/metadata')
    for key in ('nodes_visited', 'distance_computations'):
        require(off['stats'][key] == on['stats'][key], 'instrumentation changed traversal')
    return observations
